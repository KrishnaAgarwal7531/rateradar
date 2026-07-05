import { NextRequest } from "next/server";
import { runSweepStreaming } from "@/lib/sweep";

export const maxDuration = 300;
export const dynamic = "force-dynamic";

// Used only by the interactive "Refresh rates now" button — streams
// per-bank progress live as each agent actually finishes, so the UI can
// show a real progress bar rather than a spinner + one big jump at the end.
export async function POST(req: NextRequest) {
  const secret = process.env.CRON_SECRET;
  if (secret) {
    const provided = req.headers.get("x-cron-secret");
    if (provided !== secret) {
      return new Response(JSON.stringify({ error: "unauthorized" }), { status: 401 });
    }
  }

  const stream = new ReadableStream({
    async start(controller) {
      const encoder = new TextEncoder();
      const send = (event: string, data: unknown) => {
        controller.enqueue(encoder.encode(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`));
      };
      try {
        await runSweepStreaming(send);
      } catch (err) {
        send("error", { message: err instanceof Error ? err.message : "sweep failed" });
      }
      controller.close();
    }
  });

  return new Response(stream, {
    headers: {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache, no-transform",
      Connection: "keep-alive"
    }
  });
}
