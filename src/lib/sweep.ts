import { COMPETITOR_BANKS, OUR_BANK } from "./banks";
import { PRODUCTS } from "./products";
import { fetchBankRates } from "./rates";
import { getInternalRates } from "./internal";
import { runInBatches } from "./batch";
import { storeGet, storeSet, acquireLock, releaseLock } from "./storage";
import { getNextSweepAt } from "./schedule";
import { getProfile } from "./profile";
import { fetchApplicationDraft } from "./apply";
import { SweepState, ApplicationDraft, RateListing, Bank } from "./types";

const STATE_KEY = "sweep-state";
const APPS_KEY = "application-drafts";
const LOCK_KEY = "sweep-lock";
const LOCK_TTL_SECONDS = 6 * 60; // longer than any realistic sweep

// The 2 banks that get a real application auto-staged every sweep, using
// whatever the current saved profile is — Shinhan (the sponsor, obviously
// worth it) plus the first competitor, as a live comparison point. Easy to
// change to a different pair later; nothing else depends on which 2.
const AUTO_APPLY_BANK_IDS = [OUR_BANK.id, COMPETITOR_BANKS[0].id];

// Runs the application-fill agent for one bank using the current profile,
// and persists the result — used by both the batch and streaming sweeps so
// this stays real infrastructure, not a one-off. Silently does nothing if
// the profile's chosen product has no listing for this bank yet.
async function autoApplyForBank(bank: Bank, listing: RateListing | undefined): Promise<void> {
  if (!listing) return;
  const profile = await getProfile();
  const product = PRODUCTS.find((p) => p.id === profile.productId);
  if (!product) return;
  try {
    const draft = await fetchApplicationDraft(bank, product, listing, profile);
    await addApplication(draft);
    console.log(`[sweep] auto-staged application at ${bank.name} using the current saved profile`);
  } catch (err) {
    console.error(`[sweep] auto-apply failed for ${bank.id} (sweep continues regardless):`, err);
  }
}

export async function getState(): Promise<SweepState> {
  const state = await storeGet<SweepState>(STATE_KEY);
  if (state) return state;
  return { listings: [], lastSweepAt: null, nextSweepAt: getNextSweepAt(), live: false, sweepInProgress: false };
}

export async function getApplications(): Promise<ApplicationDraft[]> {
  return (await storeGet<ApplicationDraft[]>(APPS_KEY)) ?? [];
}

export async function addApplication(draft: ApplicationDraft): Promise<void> {
  const all = await getApplications();
  await storeSet(APPS_KEY, [draft, ...all]);
}

export async function markSubmitted(id: string): Promise<void> {
  const all = await getApplications();
  await storeSet(
    APPS_KEY,
    all.map((a) => (a.id === id ? { ...a, status: "submitted_by_user" as const } : a))
  );
}

function mergeListings(existing: RateListing[], incoming: RateListing[]): RateListing[] {
  const map = new Map(existing.map((l) => [`${l.bankId}:${l.productId}`, l]));
  for (const l of incoming) map.set(`${l.bankId}:${l.productId}`, l);
  return Array.from(map.values());
}

// Our own rates (internal, never scraped) + competitor banks in parallel,
// each read via one real-time TinyFish agent covering all 3 products in
// a single pass. Used by the scheduled GitHub Actions path.
export async function runSweep(): Promise<SweepState> {
  if (!(await acquireLock(LOCK_KEY, LOCK_TTL_SECONDS))) {
    console.log("[sweep] a sweep is already in progress — skipping duplicate trigger");
    return getState();
  }
  try {
    const nextSweepAt = getNextSweepAt();
    const before = await getState();
    await storeSet(STATE_KEY, { ...before, sweepInProgress: true, nextSweepAt });

    console.log(`[sweep] starting — internal feed (1) + ${COMPETITOR_BANKS.length} banks, in batches of 5`);
    const profile = await getProfile();
    const ourListings = getInternalRates();
    const autoApplyPromises: Promise<void>[] = [];
    if (AUTO_APPLY_BANK_IDS.includes(OUR_BANK.id)) {
      const listing = ourListings.find((l) => l.productId === profile.productId);
      autoApplyPromises.push(autoApplyForBank(OUR_BANK, listing));
    }
    const results = await runInBatches(COMPETITOR_BANKS, (b) => fetchBankRates(b, PRODUCTS), 5);
    for (const bank of COMPETITOR_BANKS) {
      if (!AUTO_APPLY_BANK_IDS.includes(bank.id)) continue;
      const bankIndex = COMPETITOR_BANKS.indexOf(bank);
      const listing = results[bankIndex]?.listings.find((l) => l.productId === profile.productId);
      autoApplyPromises.push(autoApplyForBank(bank, listing));
    }
    const listings = mergeListings(before.listings, [...ourListings, ...results.flatMap((r) => r.listings)]);
    const live = results.some((r) => r.live);
    await Promise.all(autoApplyPromises);

    const state: SweepState = {
      listings,
      lastSweepAt: new Date().toISOString(),
      nextSweepAt,
      live,
      sweepInProgress: false
    };
    await storeSet(STATE_KEY, state);
    console.log(`[sweep] complete — ${listings.length} listings, live=${live}`);
    return state;
  } finally {
    await releaseLock(LOCK_KEY);
  }
}

// Same work as runSweep(), but emits a live event AND persists to storage
// as each individual bank finishes — not just when a whole batch-of-5
// completes, and not just once at the very end. That persistence is what
// lets a page refresh mid-sweep still show whatever's already been found,
// instead of going blank until the entire sweep finishes.
export async function runSweepStreaming(
  onEvent: (event: string, data: unknown) => void
): Promise<SweepState> {
  if (!(await acquireLock(LOCK_KEY, LOCK_TTL_SECONDS))) {
    console.log("[sweep] a sweep is already in progress — skipping duplicate trigger");
    onEvent("skipped", {});
    return getState();
  }
  try {
    const total = COMPETITOR_BANKS.length + 1;
    const nextSweepAt = getNextSweepAt();
    onEvent("start", { total });

    let current = await getState();
    current = { ...current, sweepInProgress: true, nextSweepAt };
    await storeSet(STATE_KEY, current);

    const profile = await getProfile();
    const autoApplyPromises: Promise<void>[] = [];

    const ourListings = getInternalRates();
    current = { ...current, listings: mergeListings(current.listings, ourListings) };
    await storeSet(STATE_KEY, current);
    onEvent("bank_done", { bankId: OUR_BANK.id, bankName: OUR_BANK.name, listings: ourListings });
    if (AUTO_APPLY_BANK_IDS.includes(OUR_BANK.id)) {
      const listing = ourListings.find((l) => l.productId === profile.productId);
      autoApplyPromises.push(autoApplyForBank(OUR_BANK, listing));
    }

    let anyLive = false;

    for (let i = 0; i < COMPETITOR_BANKS.length; i += 5) {
      const batch = COMPETITOR_BANKS.slice(i, i + 5);
      batch.forEach((b) => onEvent("bank_status", { bankId: b.id, bankName: b.name, status: "running" }));
      // Each bank persists AND emits the instant IT resolves — not waiting
      // for the rest of the batch, since this is genuinely parallel work.
      await Promise.all(
        batch.map(async (b) => {
          const r = await fetchBankRates(b, PRODUCTS);
          if (r.live) anyLive = true;
          current = { ...current, listings: mergeListings(current.listings, r.listings) };
          await storeSet(STATE_KEY, current);
          onEvent("bank_done", { bankId: b.id, bankName: b.name, listings: r.listings });
          // Fires the instant this bank's own rate is in — genuinely
          // alongside the rate check, not after the whole sweep finishes.
          if (AUTO_APPLY_BANK_IDS.includes(b.id)) {
            const listing = r.listings.find((l) => l.productId === profile.productId);
            autoApplyPromises.push(autoApplyForBank(b, listing));
          }
        })
      );
    }

    await Promise.all(autoApplyPromises);

    const finalState: SweepState = {
      listings: current.listings,
      lastSweepAt: new Date().toISOString(),
      nextSweepAt,
      live: anyLive,
      sweepInProgress: false
    };
    await storeSet(STATE_KEY, finalState);
    onEvent("complete", finalState);
    return finalState;
  } finally {
    await releaseLock(LOCK_KEY);
  }
}

export { getNextSweepAt };
