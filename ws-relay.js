import "dotenv/config";
import express from "express";
import { createServer } from "http";
import { WebSocketServer } from "ws";
import { createClient } from "@supabase/supabase-js";

const PORT = process.env.PORT || 3000;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY;

// Supabase client
const supabase =
  SUPABASE_URL && SUPABASE_ANON_KEY
    ? createClient(SUPABASE_URL, SUPABASE_ANON_KEY)
    : null;

// Express app + HTTP server
const app = express();
app.use(express.json());

// Health check (Railway uses this to verify the service is up)
app.get("/health", (req, res) => {
  res.json({ status: "ok", supabase: !!supabase });
});

// ──────────────────────────────────────────────
// HTTP Webhook endpoint for Recall.ai
// Events: transcript.data, transcript.partial_data
// Docs: https://docs.recall.ai/docs/bot-real-time-transcription
// ──────────────────────────────────────────────
app.post("/webhook", async (req, res) => {
  const payload = req.body;
  const event = payload.event;

  console.log(`📩 Webhook [${event}] received`);

  // Only process transcript events
  if (event !== "transcript.data" && event !== "transcript.partial_data") {
    console.log(`ℹ️ Ignoring event: ${event}`);
    return res.json({ success: true, ignored: true });
  }

  // Extract fields per Recall.ai payload format
  const bot_id = payload.data?.bot?.id || "unknown";
  const words = payload.data?.data?.words || [];
  const text = words.map((w) => w.text).join(" ").trim();
  const speaker = payload.data?.data?.participant?.name || null;
  const participant_id = payload.data?.data?.participant?.id || null;
  const is_partial = event === "transcript.partial_data";

  // Timestamps from first and last word
  const start_time = words[0]?.start_timestamp?.relative || null;
  const end_time = words[words.length - 1]?.end_timestamp?.relative || null;

  if (!text) {
    console.warn("⚠️ No transcript text found in webhook payload");
    return res.status(400).json({ error: "No transcript text found" });
  }

  console.log(`➡️ [${bot_id}] ${speaker || "Unknown"}: ${text}${is_partial ? " (partial)" : ""}`);

  // Insert into Supabase (skip partial transcripts to avoid duplicates)
  if (supabase && !is_partial) {
    const { error } = await supabase.from("copilot_transcripts").insert({
      call_id: bot_id,
      text,
      meta: {
        speaker,
        participant_id,
        start_time,
        end_time,
        source: "recall_ai",
        event,
      },
    });

    if (error) {
      console.error("Supabase insert failed:", error.message);
      return res.status(500).json({ error: "Database insert failed" });
    }

    console.log("✅ Inserted into Supabase");
  } else if (!supabase) {
    console.log("ℹ️ Supabase not configured — skipping insert");
  }

  // Broadcast to all connected WebSocket clients (real-time to frontend)
  const wsMessage = JSON.stringify({
    call_id: bot_id,
    text,
    speaker,
    is_partial,
    start_time,
    end_time,
    timestamp: new Date().toISOString(),
  });

  wss.clients.forEach((client) => {
    if (client.readyState === 1) {
      client.send(wsMessage);
    }
  });

  res.json({ success: true });
});

// ──────────────────────────────────────────────
// WebSocket server (for real-time streaming to frontend)
// ──────────────────────────────────────────────
const server = createServer(app);
const wss = new WebSocketServer({ server });

wss.on("connection", (ws) => {
  console.log("🔌 WebSocket client connected");

  ws.on("message", async (data) => {
    let payload;

    try {
      payload = JSON.parse(data.toString());
    } catch {
      console.error("Invalid JSON payload");
      return;
    }

    const { call_id, text, meta } = payload;

    if (!call_id || !text) {
      console.warn("Missing call_id or text");
      return;
    }

    console.log("➡️ WS Received chunk:", text);

    if (supabase) {
      const { error } = await supabase.from("copilot_transcripts").insert({
        call_id,
        text,
        meta: meta || null,
      });

      if (error) {
        console.error("Supabase insert failed:", error.message);
      } else {
        console.log("✅ Inserted into Supabase");
      }
    }
  });

  ws.on("close", () => {
    console.log("Client disconnected");
  });

  ws.on("error", (err) => {
    console.error("WS error:", err.message);
  });
});

// Start server
server.listen(PORT, () => {
  console.log(`✅ Relay server running on port ${PORT}`);
  console.log(`   HTTP webhook: http://localhost:${PORT}/webhook`);
  console.log(`   WebSocket:    ws://localhost:${PORT}`);
  console.log(`   Health check: http://localhost:${PORT}/health`);
  console.log(`   Supabase:     ${supabase ? "connected" : "not configured"}`);
});
