// Runs INSIDE the GitHub Actions runner — this is the real compute, not
// just a ping to Vercel. It calls runSweep() directly (real TinyFish
// agent calls, batched 5-then-2) and writes straight to Redis, the same
// database Vercel reads from on every request. Vercel's job is purely to
// read what's already there — this script is what actually updates it.
import { runSweep } from "../src/lib/sweep";

async function main() {
  console.log("[sweep-script] starting real sweep inside GitHub Actions runner...");
  const state = await runSweep();
  console.log(`[sweep-script] complete — ${state.listings.length} listings, live=${state.live}`);
}

main().catch((err) => {
  console.error("[sweep-script] failed:", err);
  process.exit(1);
});
