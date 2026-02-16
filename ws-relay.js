import "dotenv/config";
import WebSocket, { WebSocketServer } from "ws";
import { createClient } from "@supabase/supabase-js";

const PORT = process.env.PORT || 3000;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY;

// Supabase client (using anon key)
const supabase =
  SUPABASE_URL && SUPABASE_ANON_KEY
    ? createClient(SUPABASE_URL, SUPABASE_ANON_KEY)
    : null;

// WebSocket Server
const wss = new WebSocketServer({ port: PORT });

console.log(`✅ WebSocket relay running on ws://localhost:${PORT}`);

wss.on("connection", (ws) => {
  console.log("🔌 Client connected");

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

    console.log("➡️ Received chunk:", text);

    // Insert into Supabase if configured
    if (supabase) {
      const { error } = await supabase.from("copilot_transcripts").insert({
        call_id,
        text,
        meta: meta || null,
      });

      if (error) {
        console.error("Supabase insert failed:", error.message);
      } else {
        console.log("Inserted into Supabase");
      }
    } else {
      console.log("ℹ️ Supabase not configured (URL or anon key missing)");
    }
  });

  ws.on("close", () => {
    console.log("Client disconnected");
  });

  ws.on("error", (err) => {
    console.error("WS error:", err.message);
  });
});
