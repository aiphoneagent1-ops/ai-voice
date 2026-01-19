import "dotenv/config";
import express from "express";
import crypto from "node:crypto";
import path from "node:path";
import fs from "node:fs";
import http from "node:http";
import { WebSocketServer } from "ws";
import { OpenAI } from "openai";
import twilio from "twilio";
import multer from "multer";
import ExcelJS from "exceljs";
import { parse as csvParse } from "csv-parse/sync";

import {
  openDb,
  getContactByPhone,
  createOrGetCall,
  incrementTurn,
  addMessage,
  getMessages,
  markDoNotCall,
  getSetting,
  setSetting,
  getKnowledge,
  setKnowledge,
  upsertContact,
  upsertLead,
  listLeads,
  deleteLead,
  fetchNextContactsToDial,
  queueContactForDial,
  markDialResult
} from "./db.js";
import { buildSystemPrompt, buildGreeting, buildSalesAgentSystemPrompt } from "./prompts.js";
import { buildPlayAndHangup, buildRecordTwiML, buildSayAndHangup } from "./twiml.js";
// Gemini TTS removed (we keep only OpenAI STT/LLM + ElevenLabs v3 TTS).
import { renderAdminPage } from "./admin_page.js";
import { normalizePhoneE164IL } from "./phone.js";

const PORT = Number(process.env.PORT || 3000);

function normalizeOpenAIModel(raw) {
  const s = String(raw || "").trim();
  if (!s) return "";
  const low = s.toLowerCase();
  // Accept common human-entered variants from Render/.env UI
  // Examples: "GPT-5.2 Mini", "gpt 5 mini", "gpt-5-mini"
  if (/gpt\s*[-_ ]?\s*5(\.2)?\s*[-_ ]?\s*mini/.test(low)) return "gpt-5-mini";
  if (/gpt\s*[-_ ]?\s*4\.1\b/.test(low)) return "gpt-4.1";
  return s;
}

function isGpt5Family(model) {
  return /^gpt-5\b/i.test(String(model || "").trim());
}

function extractTextFromResponses(resp) {
  // Responses API can return:
  // - resp.output_text (plain text)
  // - resp.output[].content[].text (text parts)
  // - resp.output[].content[].json (structured output for json_schema)
  try {
    const direct = resp?.output_text;
    if (direct && typeof direct === "string") return direct;
  } catch {}

  try {
    const out = resp?.output;
    if (!Array.isArray(out)) return "";
    let acc = "";
    for (const item of out) {
      const content = item?.content;
      if (!Array.isArray(content)) continue;
      for (const part of content) {
        const t = part?.text ?? part?.output_text ?? part?.content;
        if (typeof t === "string" && t) {
          acc += t;
          continue;
        }
        // Some SDKs return structured output under `json` for json_schema formats.
        const j = part?.json;
        if (j && typeof j === "object") {
          acc += JSON.stringify(j);
          continue;
        }
      }
    }
    return acc;
  } catch {
    return "";
  }
}

async function createLlmText({ model, messages, temperature, maxTokens, response_format, stream }) {
  if (!openai) throw new Error("OpenAI client not initialized");
  const m = String(model || "").trim();

  // Prefer Responses API for GPT-5 family models (best compatibility).
  // For streaming, we currently fall back to non-streaming and send one chunk.
  if (isGpt5Family(m) && openai?.responses?.create) {
    // Translate Chat Completions-style `response_format` into Responses `text.format`.
    // Chat: { type:"json_schema", json_schema:{ name, strict, schema } }
    // Responses expects: text: { format: { type:"json_schema", name, strict, schema } }
    let textFormat = undefined;
    try {
      if (response_format && typeof response_format === "object") {
        const rfType = String(response_format.type || "").trim();
        if (rfType === "json_schema" && response_format.json_schema && typeof response_format.json_schema === "object") {
          const js = response_format.json_schema;
          textFormat = {
            type: "json_schema",
            name: String(js.name || "output").trim() || "output",
            strict: Boolean(js.strict),
            schema: js.schema
          };
        }
      }
    } catch {
      textFormat = undefined;
    }

    // Responses API format requirements have changed over time:
    // - some versions required `text.format.type`
    // - some reject `text.format.name`
    // To keep compatibility, for plain text we send the minimal format `{ type:"text" }`.
    const payload = {
      model: m,
      input: messages,
      text: { format: textFormat || { type: "text" } },
      ...(Number.isFinite(maxTokens) ? { max_output_tokens: maxTokens } : {})
    };
    // Avoid passing temperature for GPT-5 unless we explicitly control reasoning settings.
    void temperature;
    void stream;
    try {
      const resp = await openai.responses.create(payload);
      return { api: "responses", rawText: String(extractTextFromResponses(resp) || "").trim(), resp };
    } catch (e) {
      // Fail-safe: if Responses rejects the structured format (or changes requirements),
      // retry once with explicit plain_text output so the call can continue.
      const msg = String(e?.message || e || "");
      console.warn("[llm] responses.create failed; retrying with plain_text", { model: m, err: msg });
      try {
        // Minimal plain-text format: avoid `name` (some API versions reject it).
        const resp2 = await openai.responses.create({
          model: m,
          input: messages,
          text: { format: { type: "text" } },
          ...(Number.isFinite(maxTokens) ? { max_output_tokens: maxTokens } : {})
        });
        return { api: "responses_retry_plain", rawText: String(extractTextFromResponses(resp2) || "").trim(), resp: resp2 };
      } catch {
        throw e;
      }
    }
  }

  // Chat Completions (legacy + still fine for many models).
  if (stream) {
    const resp = await openai.chat.completions.create({
      model: m,
      messages,
      ...(typeof temperature === "number" ? { temperature } : {}),
      ...(Number.isFinite(maxTokens) ? { max_tokens: maxTokens } : {}),
      ...(response_format ? { response_format } : {}),
      stream: true
    });
    return { api: "chat_stream", stream: resp };
  }

  const resp = await openai.chat.completions.create({
    model: m,
    messages,
    ...(typeof temperature === "number" ? { temperature } : {}),
    ...(Number.isFinite(maxTokens) ? { max_tokens: maxTokens } : {}),
    ...(response_format ? { response_format } : {})
  });
  const rawText = String(resp?.choices?.[0]?.message?.content || "").trim();
  return { api: "chat", rawText, resp };
}

// Persistent storage paths:
// - On Render: you typically mount a disk to /var/data
// - On local macOS: /var/data is usually not writable → fallback to ./data
const IS_RENDER = !!process.env.RENDER_EXTERNAL_HOSTNAME;
const RAW_DATA_DIR = String(process.env.DATA_DIR || "./data").trim() || "./data";
const RAW_DB_PATH = String(process.env.DB_PATH || "").trim();

function pickLocalDataDir() {
  // If the user explicitly set a local path, respect it.
  if (RAW_DATA_DIR && RAW_DATA_DIR !== "/var/data") return RAW_DATA_DIR;
  // If the user set /var/data on macOS (common when copying Render env), fallback.
  if (!IS_RENDER && process.platform === "darwin" && RAW_DATA_DIR === "/var/data") return "./data";
  return RAW_DATA_DIR || "./data";
}

const DATA_DIR = pickLocalDataDir();
function pickLocalDbPath() {
  // If DB_PATH is set to a Render mount on macOS, ignore it and use the local data dir.
  if (!IS_RENDER && process.platform === "darwin" && RAW_DB_PATH.startsWith("/var/data")) {
    // Keep the same filename if provided (usually app.db)
    const fname = path.basename(RAW_DB_PATH) || "app.db";
    return path.join(DATA_DIR, fname);
  }
  return RAW_DB_PATH || path.join(DATA_DIR, "app.db");
}
const DB_PATH = pickLocalDbPath();

if (!IS_RENDER && process.platform === "darwin" && (RAW_DATA_DIR === "/var/data" || RAW_DB_PATH.startsWith("/var/data"))) {
  console.warn(
    "[config] Detected Render-style DATA_DIR/DB_PATH on macOS. Falling back to local ./data. " +
      "If you want to override: set DATA_DIR=./data (or any writable path)."
  );
}
// Default to a high-quality model. You can override via OPENAI_MODEL in env.
// IMPORTANT: normalize common human-entered values (e.g. "GPT-5.2 Mini") → valid model id.
const OPENAI_MODEL = normalizeOpenAIModel(process.env.OPENAI_MODEL || "gpt-4.1") || "gpt-4.1";
const OPENAI_STT_MODEL = process.env.OPENAI_STT_MODEL || "whisper-1";
// Optional fallback model for "suspicious" transcriptions (long audio but very short/generic text).
// Example: gpt-4o-mini-transcribe
const OPENAI_STT_MODEL_FALLBACK = String(process.env.OPENAI_STT_MODEL_FALLBACK || "").trim();
const OPENAI_STT_PROMPT =
  process.env.OPENAI_STT_PROMPT ||
  // Keep this generic (white-label) but tuned for Israeli phone Hebrew: short confirmations, polite filler,
  // and common “handoff” phrases that matter for intent detection.
  "תמלול שיחה טלפונית בעברית (ישראל). שמור ניסוח מלא ככל האפשר ואל תחליף משפטים ב'תודה/כן' אם לא נאמר. " +
  "מילים/ביטויים נפוצים: כן, לא, לא תודה, תודה, תודה רבה, סבבה, בסדר, אוקיי, מעולה, מצוין, הכל טוב, מה שלומך, " +
  "מעוניין, מעוניינת, רוצה, אשמח, תעביר, תעבירו, תעביר את הפרטים, תעבירו את הפרטים, פרטים, שיחזרו, לחזור אלי, " +
  "מה השעות שלכם, איזו שעה, מתי, להסיר, אל תתקשרו, תפסיקו להתקשר.";
const OPENAI_TTS_MODEL = process.env.OPENAI_TTS_MODEL || "gpt-4o-mini-tts";
const OPENAI_TTS_VOICE_MALE = process.env.OPENAI_TTS_VOICE_MALE || "alloy";
const OPENAI_TTS_VOICE_FEMALE = process.env.OPENAI_TTS_VOICE_FEMALE || "alloy";
// We keep only ElevenLabs v3 for TTS in this codebase.
// (OpenAI is used for STT + LLM.)
const TTS_PROVIDER = "elevenlabs";
const DEBUG_TTS = process.env.DEBUG_TTS === "1";
// Default OFF: keep answers driven by KB + LLM (more natural). Set USE_FAQ_RULES=1 to re-enable deterministic FAQ replies.
const USE_FAQ_RULES = process.env.USE_FAQ_RULES === "1";
const TTS_POLL_MAX = Number(process.env.TTS_POLL_MAX || 20);
const TTS_POLL_WAIT_SECONDS = Number(process.env.TTS_POLL_WAIT_SECONDS || 1);
// Realtime modes:
// - "1": Twilio ConversationRelay (text in/out; Twilio handles STT/TTS)
// - "2": Twilio Media Streams (audio in/out; our server handles STT+LLM+TTS)
const REALTIME_MODE = String(process.env.REALTIME_MODE || "0").trim();
const CR_MODE = REALTIME_MODE === "1";
const MS_MODE = REALTIME_MODE === "2";
// Twilio <Record> max length per utterance. If too small, callers get cut mid-sentence.
// We enforce a sane minimum; override by setting a larger value.
const RECORD_MAX_LENGTH_SECONDS = Math.max(6, Number(process.env.RECORD_MAX_LENGTH_SECONDS || 6));
const RECORD_TIMEOUT_SECONDS = Number(process.env.RECORD_TIMEOUT_SECONDS || 1);
// Twilio <Record> "timeout" is silence timeout (seconds). Historically we forced a minimum of 2s to avoid premature "לא שמעתי".
// If you want faster turn-taking, set RECORD_TIMEOUT_MIN_SECONDS=1 and RECORD_TIMEOUT_SECONDS=1.
const RECORD_TIMEOUT_MIN_SECONDS = Number(process.env.RECORD_TIMEOUT_MIN_SECONDS || 2);
const NO_SPEECH_MAX_RETRIES = Number(process.env.NO_SPEECH_MAX_RETRIES || 2);

function recordTimeoutSeconds() {
  const min = Number.isFinite(RECORD_TIMEOUT_MIN_SECONDS) ? RECORD_TIMEOUT_MIN_SECONDS : 2;
  const v = Number.isFinite(RECORD_TIMEOUT_SECONDS) ? RECORD_TIMEOUT_SECONDS : 1;
  return Math.max(1, Math.max(min, v));
}

const ELEVENLABS_API_KEY = process.env.ELEVENLABS_API_KEY || "";
// If you have one cloned voice for everything, set ELEVENLABS_VOICE_ID.
// If you want different voices per persona, set ELEVENLABS_VOICE_MALE / ELEVENLABS_VOICE_FEMALE.
const ELEVENLABS_VOICE_ID = process.env.ELEVENLABS_VOICE_ID || process.env.ELEVENLABS_VOICE || "";
const ELEVENLABS_VOICE_MALE = process.env.ELEVENLABS_VOICE_MALE || "";
const ELEVENLABS_VOICE_FEMALE = process.env.ELEVENLABS_VOICE_FEMALE || "";
// This build supports ElevenLabs v3 only.
const ELEVENLABS_MODEL_ID = "eleven_v3";
function parseFiniteEnvNumber(raw, fallback) {
  const v = Number(raw);
  return Number.isFinite(v) ? v : fallback;
}
// Eleven v3 UI exposes only Stability.
const ELEVENLABS_STABILITY = parseFiniteEnvNumber(process.env.ELEVENLABS_STABILITY, 0.5);

// Force a deterministic MP3 output (helps Twilio playback quality; avoids VBR surprises).
// ElevenLabs docs: codec_sample_rate_bitrate (e.g. mp3_44100_128, mp3_22050_32, etc.)
const ELEVENLABS_OUTPUT_FORMAT = String(process.env.ELEVENLABS_OUTPUT_FORMAT || "mp3_44100_128").trim();
// ISO 639-1 (e.g. "he"). Enforces language + normalization.
// Leave empty by default to match ElevenLabs UI behavior; set in env if you want to force Hebrew ("he").
const ELEVENLABS_LANGUAGE_CODE = String(process.env.ELEVENLABS_LANGUAGE_CODE || "").trim();

// Agent voice gender (TTS). Keep grammar-to-callee separate from agent voice.
// Configure via env: AGENT_VOICE_PERSONA=male|female
function normalizeAgentVoicePersona(raw) {
  const v = String(raw || "").trim().toLowerCase();
  return v === "female" ? "female" : "male";
}
const AGENT_VOICE_PERSONA = normalizeAgentVoicePersona(process.env.AGENT_VOICE_PERSONA || "male");

// ConversationRelay (Realtime voice via Twilio)
const CR_ENABLED = CR_MODE;
const CR_LANGUAGE = String(process.env.CR_LANGUAGE || "he-IL").trim();
const CR_TTS_PROVIDER = String(process.env.CR_TTS_PROVIDER || "Google").trim(); // Google | Amazon | ElevenLabs
const CR_VOICE = String(process.env.CR_VOICE || "he-IL-Wavenet-D").trim(); // default: male Hebrew (Google)
// Per Twilio ConversationRelay docs, language can be set via:
// - language (sets both STT + TTS)
// - transcriptionLanguage (STT)
// - ttsLanguage (TTS)
// Some combinations (e.g. ElevenLabs + he-IL) can be rejected by Twilio validation.
const CR_TRANSCRIPTION_LANGUAGE = String(process.env.CR_TRANSCRIPTION_LANGUAGE || "").trim(); // e.g. "he-IL"
const CR_TTS_LANGUAGE = String(process.env.CR_TTS_LANGUAGE || "").trim(); // e.g. "multi"
// STT provider/model: Twilio ConversationRelay validates provider+language+model combinations.
// We default to Google for maximum language coverage (incl. he-IL). You can override via env.
const CR_TRANSCRIPTION_PROVIDER = String(process.env.CR_TRANSCRIPTION_PROVIDER || "Google").trim(); // Google | Deepgram
const CR_SPEECH_MODEL = String(process.env.CR_SPEECH_MODEL || "telephony").trim(); // Google: telephony|long|..., Deepgram: nova-2-general|nova-3-general
const CR_INTERRUPTIBLE = String(process.env.CR_INTERRUPTIBLE || "speech").trim(); // none|dtmf|speech|any
const CR_INTERRUPT_SENSITIVITY = String(process.env.CR_INTERRUPT_SENSITIVITY || "high").trim(); // low|medium|high
const CR_DEBUG = String(process.env.CR_DEBUG || "").trim(); // e.g. "debugging speaker-events tokens-played"
// Twilio ConversationRelay: ElevenLabs-only option. Values: on|auto|off
const CR_ELEVENLABS_TEXT_NORMALIZATION = String(process.env.CR_ELEVENLABS_TEXT_NORMALIZATION || "").trim();

// Media Streams (Realtime audio in/out)
const MS_DEBUG = process.env.MS_DEBUG === "1" || process.env.MS_DEBUG === "true";
const MS_LOG_EVERY_FRAME = process.env.MS_LOG_EVERY_FRAME === "1" || process.env.MS_LOG_EVERY_FRAME === "true";
// VAD sensitivity (RMS on decoded PCM16 @ 8k).
// IMPORTANT: many phone lines have a constant "noise floor" (often ~1000 RMS) that would otherwise look like speech.
// We therefore use an adaptive threshold: max(MS_VAD_MIN_RMS, noiseFloor*mult + margin).
const MS_VAD_MIN_RMS = Number(process.env.MS_VAD_MIN_RMS || 700);
const MS_VAD_NOISE_MULT = Number(process.env.MS_VAD_NOISE_MULT || 1.8);
const MS_VAD_NOISE_MARGIN = Number(process.env.MS_VAD_NOISE_MARGIN || 250);
// If calibration is too long, we miss the beginning of the user's first reply (common right after greeting).
const MS_NOISE_CALIBRATION_MS = Number(process.env.MS_NOISE_CALIBRATION_MS || 120);
// Greeting latency: keep the pickup snappy (avoid 4–6s intros).
const MS_GREETING_MAX_CHARS = Number(process.env.MS_GREETING_MAX_CHARS || 70);
const MS_FORCE_SHORT_GREETING = process.env.MS_FORCE_SHORT_GREETING === "1" || process.env.MS_FORCE_SHORT_GREETING === "true";
// Auto-hangup after final line (after mark ack)
const MS_AUTO_HANGUP = process.env.MS_AUTO_HANGUP !== "0" && process.env.MS_AUTO_HANGUP !== "false";
// Endpointing: how much silence we wait before finalizing an utterance.
// IMPORTANT (Hebrew telephony): if this is too low, we cut sentences mid-phrase and STT becomes unreliable.
// Target: +700–1200ms more silence vs old defaults to capture full phrases.
// You can tune per environment with MS_END_SILENCE_MS.
const MS_END_SILENCE_MS = Number(process.env.MS_END_SILENCE_MS || 1000);
// Faster turn-taking for short utterances (e.g. "כן") without waiting full silence window.
// Fast-end should apply ONLY to single-word acknowledgments.
// Defaults are conservative to avoid cutting real phrases.
const MS_FAST_END_SILENCE_MS = Number(process.env.MS_FAST_END_SILENCE_MS || 380);
const MS_FAST_END_MAX_UTTERANCE_MS = Number(process.env.MS_FAST_END_MAX_UTTERANCE_MS || 550);
// Disable the "thinking" backchannel by default (it can feel interruptive on real calls).
// If you ever want it back: set MS_THINKING_DELAY_MS (e.g. 380).
const MS_THINKING_DELAY_MS = Number(process.env.MS_THINKING_DELAY_MS || 0);
const MS_MIN_UTTERANCE_MS = Number(process.env.MS_MIN_UTTERANCE_MS || 250);
// Force-finalize is a safety hatch for very long utterances.
// Default OFF because it can cut normal phrases mid-sentence and degrade STT.
const MS_ENABLE_FORCE_FINALIZE = process.env.MS_ENABLE_FORCE_FINALIZE === "1" || process.env.MS_ENABLE_FORCE_FINALIZE === "true";
// If you enable it, these control when we force-finalize after a short pause.
const MS_FORCE_FINALIZE_AFTER_MS = Number(process.env.MS_FORCE_FINALIZE_AFTER_MS || 2600);
const MS_FORCE_FINALIZE_PAUSE_MS = Number(process.env.MS_FORCE_FINALIZE_PAUSE_MS || 350);
// Safety: cap utterance length so we don't buffer indefinitely.
// Old default (2500ms) could cut long natural sentences; raise for better STT fidelity.
const MS_MAX_UTTERANCE_MS = Number(process.env.MS_MAX_UTTERANCE_MS || 4500);
const ELEVENLABS_STREAM_OUTPUT_FORMAT = String(process.env.ELEVENLABS_STREAM_OUTPUT_FORMAT || "ulaw_8000").trim();
// When using streaming TTS, we prebuffer a few frames before starting playback to avoid underflow.
// 10 frames * 20ms = 200ms buffer (good tradeoff for low perceived latency).
const MS_TTS_PREBUFFER_FRAMES = Number(process.env.MS_TTS_PREBUFFER_FRAMES || 10);
// If the LLM call hangs, we cut it off and ask a safe handoff question instead of going silent.
const MS_LLM_TIMEOUT_MS = Number(process.env.MS_LLM_TIMEOUT_MS || 2500);
// If the ElevenLabs streaming reader stalls (no bytes arriving), end playback cleanly (avoid infinite silence).
const MS_TTS_STREAM_STALL_MS = Number(process.env.MS_TTS_STREAM_STALL_MS || 1500);
// ElevenLabs low-latency is optional. Some accounts/voices behave differently with this flag.
// If unset, we do NOT send optimize_streaming_latency at all (safe default).
const ELEVENLABS_OPTIMIZE_STREAMING_LATENCY_RAW = String(process.env.ELEVENLABS_OPTIMIZE_STREAMING_LATENCY || "").trim();
const ELEVENLABS_OPTIMIZE_STREAMING_LATENCY = ELEVENLABS_OPTIMIZE_STREAMING_LATENCY_RAW
  ? clampInt(ELEVENLABS_OPTIMIZE_STREAMING_LATENCY_RAW, { min: 0, max: 4, fallback: 0 })
  : null;
const MS_TEST_TONE = process.env.MS_TEST_TONE === "1" || process.env.MS_TEST_TONE === "true";
// Barge-in tuning: avoid clearing agent speech on line noise/echo.
const MS_BARGE_IN_FRAMES = Number(process.env.MS_BARGE_IN_FRAMES || 15); // 15 frames * 20ms = 300ms
const MS_BARGE_IN_GRACE_MS = Number(process.env.MS_BARGE_IN_GRACE_MS || 400); // don't barge-in instantly after agent starts
// Default: turn-taking like a normal call (no interruptions). You can enable barge-in later.
const MS_ENABLE_BARGE_IN = process.env.MS_ENABLE_BARGE_IN === "1" || process.env.MS_ENABLE_BARGE_IN === "true";
// Call recording (Media Streams): record both sides (agent + caller) into a stereo WAV (8k PCM16).
// Enabled via MS_RECORD_CALLS=1 on Render.
const MS_RECORD_CALLS = process.env.MS_RECORD_CALLS === "1" || process.env.MS_RECORD_CALLS === "true";
// Safety: cap recording duration (seconds) to avoid unbounded disk usage in production.
const MS_RECORD_MAX_SECONDS = Number(process.env.MS_RECORD_MAX_SECONDS || 300); // 5 minutes default

// Cache for ElevenLabs ulaw outputs:
// - Memory cache helps within a single process lifetime.
// - Disk cache (Render persistent disk) makes greeting fast even after cold starts/deploys.
const _ulawMemCache = new Map(); // key -> Buffer
const _ulawDiskCacheDir = path.resolve(DATA_DIR, "ulaw-cache");
function ensureUlawCacheDir() {
  try {
    if (!fs.existsSync(_ulawDiskCacheDir)) fs.mkdirSync(_ulawDiskCacheDir, { recursive: true });
  } catch {}
}
function ulawDiskPath(key) {
  return path.join(_ulawDiskCacheDir, `${key}.ulaw`);
}
function getUlawFromDisk(key) {
  try {
    const p = ulawDiskPath(key);
    if (!fs.existsSync(p)) return null;
    const buf = fs.readFileSync(p);
    return buf && buf.length ? buf : null;
  } catch {
    return null;
  }
}
function putUlawToDisk(key, buf) {
  try {
    ensureUlawCacheDir();
    fs.writeFileSync(ulawDiskPath(key), buf);
  } catch {}
}

function computeElevenUlawCacheKey({ text, persona }) {
  const voiceId =
    (persona === "female" ? ELEVENLABS_VOICE_FEMALE : ELEVENLABS_VOICE_MALE) || ELEVENLABS_VOICE_ID;
  return crypto
    .createHash("sha256")
    .update(
      [
        "elevenlabs_ulaw",
        ELEVENLABS_STREAM_OUTPUT_FORMAT || "ulaw_8000",
        voiceId || "",
        ELEVENLABS_MODEL_ID,
        String(ELEVENLABS_LANGUAGE_CODE || ""),
        String(ELEVENLABS_STABILITY),
        String(text || "")
      ].join("::")
    )
    .digest("hex");
}

function primaryLangTag(code) {
  const s = String(code || "").trim();
  if (!s) return "";
  return s.split("-")[0] || s;
}

function normalizeTextTokenLang(raw) {
  const s = String(raw || "").trim();
  if (!s) return "";
  // Prefer BCP-47 like "he-IL". Twilio rejects some values (e.g. plain "he").
  // Accept language-region, otherwise return empty to omit "lang" entirely.
  if (/^[a-z]{2,3}-[a-z]{2}$/i.test(s)) return s;
  // Special-case Hebrew
  if (/^he(\b|-)/i.test(s)) return "he-IL";
  return "";
}

function langForTextTokens() {
  // If using ElevenLabs with Hebrew content, Twilio may block or lack TTS settings for he-IL.
  // In that case we omit token lang entirely and rely on a supported ttsLanguage (set in TwiML).
  if (String(CR_TTS_PROVIDER || "").toLowerCase() === "elevenlabs" && startsWithLang(CR_LANGUAGE, "he")) {
    return "";
  }
  // Optional override: CR_TEXT_LANG
  const explicit = normalizeTextTokenLang(process.env.CR_TEXT_LANG);
  if (explicit) return explicit;
  const fromCr = normalizeTextTokenLang(CR_LANGUAGE);
  if (fromCr) return fromCr;
  return "";
}

function wsSendText(ws, payload) {
  const lang = langForTextTokens();
  const msg = lang ? { ...payload, lang } : payload;
  // Helpful for debugging "it spoke English / nonsense" – see exactly what we sent.
  try {
    if (String(payload?.type || "") === "text") {
      const token = String(payload?.token || "");
      crLogVerbose("send text", { chars: token.length, last: !!payload?.last, snippet: token.slice(0, 120) });
    }
  } catch {}
  wsSendJson(ws, msg);
}

function buildMediaStreamTwiML({ wsUrl, customParameters = {} }) {
  const paramsXml = Object.entries(customParameters)
    .filter(([k, val]) => k && val != null && String(val).length)
    .map(([k, val]) => `<Parameter name="${escapeXmlAttr(k)}" value="${escapeXmlAttr(val)}" />`)
    .join("");

  // Bidirectional Media Stream (audio in/out): <Connect><Stream>
  // Note: for bidirectional streams, Twilio only sends inbound_track to us,
  // but we can send outbound audio back as "media" events.
  const attrs = [`url="${escapeXmlAttr(wsUrl)}"`, `track="inbound_track"`].join(" ");
  return `<?xml version="1.0" encoding="UTF-8"?>
<Response>
  <Connect>
    <Stream ${attrs}>${paramsXml}</Stream>
  </Connect>
</Response>`;
}

// μ-law decode (G.711) to 16-bit PCM
function mulawToPcmSample(u) {
  // u: 0..255
  // Standard G.711 μ-law decode (Twilio Media Streams inbound payload is μ-law @ 8kHz).
  // IMPORTANT: must stay within 16-bit range; otherwise recordings/VAD will sound like noise due to Int16 overflow.
  let x = (~u) & 0xff;
  const sign = x & 0x80;
  const exponent = (x >> 4) & 0x07;
  const mantissa = x & 0x0f;
  let t = ((mantissa << 3) + 0x84) << exponent;
  t -= 0x84;
  return sign ? -t : t;
}

// 16-bit PCM sample to μ-law byte (G.711)
function pcmToMulawSample(pcm) {
  // Standard G.711 μ-law encode.
  const MULAW_BIAS = 0x84;
  const MULAW_CLIP = 32635;
  let sign = 0;
  let x = pcm | 0;
  if (x < 0) {
    sign = 0x80;
    x = -x;
  }
  if (x > MULAW_CLIP) x = MULAW_CLIP;
  x += MULAW_BIAS;

  let exponent = 7;
  for (let expMask = 0x4000; (x & expMask) === 0 && exponent > 0; expMask >>= 1) exponent--;
  const mantissa = (x >> (exponent + 3)) & 0x0f;
  return (~(sign | (exponent << 4) | mantissa)) & 0xff;
}

function pcm16ToUlawBuffer(pcm16) {
  const out = Buffer.alloc(pcm16.length);
  for (let i = 0; i < pcm16.length; i++) out[i] = pcmToMulawSample(pcm16[i]);
  return out;
}

function generateUlawTone({ freqHz = 440, ms = 350, sampleRate = 8000, amp = 0.2 } = {}) {
  const n = Math.max(1, Math.floor((ms / 1000) * sampleRate));
  const pcm = new Int16Array(n);
  const a = Math.max(0, Math.min(1, amp)) * 32767;
  for (let i = 0; i < n; i++) {
    pcm[i] = Math.round(Math.sin((2 * Math.PI * freqHz * i) / sampleRate) * a);
  }
  return pcm16ToUlawBuffer(pcm);
}

function ulawBufferToPcm16(ulawBuf) {
  const out = new Int16Array(ulawBuf.length);
  for (let i = 0; i < ulawBuf.length; i++) out[i] = mulawToPcmSample(ulawBuf[i]);
  return out;
}

function rmsFromPcm16(pcm) {
  let sum = 0;
  for (let i = 0; i < pcm.length; i++) {
    const v = pcm[i];
    sum += v * v;
  }
  return Math.sqrt(sum / Math.max(1, pcm.length));
}

function upsample8kTo16k(pcm8k) {
  // simple linear interpolation (2x)
  const out = new Int16Array(pcm8k.length * 2);
  for (let i = 0; i < pcm8k.length; i++) {
    const s0 = pcm8k[i];
    const s1 = i + 1 < pcm8k.length ? pcm8k[i + 1] : s0;
    out[i * 2] = s0;
    out[i * 2 + 1] = (s0 + s1) >> 1;
  }
  return out;
}

function pcm16ToWavBuffer(pcm16, sampleRate) {
  const numChannels = 1;
  const bitsPerSample = 16;
  const byteRate = (sampleRate * numChannels * bitsPerSample) / 8;
  const blockAlign = (numChannels * bitsPerSample) / 8;
  const dataSize = pcm16.length * 2;
  const buf = Buffer.alloc(44 + dataSize);
  let o = 0;
  buf.write("RIFF", o); o += 4;
  buf.writeUInt32LE(36 + dataSize, o); o += 4;
  buf.write("WAVE", o); o += 4;
  buf.write("fmt ", o); o += 4;
  buf.writeUInt32LE(16, o); o += 4; // PCM
  buf.writeUInt16LE(1, o); o += 2; // format
  buf.writeUInt16LE(numChannels, o); o += 2;
  buf.writeUInt32LE(sampleRate, o); o += 4;
  buf.writeUInt32LE(byteRate, o); o += 4;
  buf.writeUInt16LE(blockAlign, o); o += 2;
  buf.writeUInt16LE(bitsPerSample, o); o += 2;
  buf.write("data", o); o += 4;
  buf.writeUInt32LE(dataSize, o); o += 4;
  for (let i = 0; i < pcm16.length; i++, o += 2) buf.writeInt16LE(pcm16[i], o);
  return buf;
}

function wavHeaderPcm16({ numChannels, sampleRate, dataBytes }) {
  const bitsPerSample = 16;
  const byteRate = (sampleRate * numChannels * bitsPerSample) / 8;
  const blockAlign = (numChannels * bitsPerSample) / 8;
  const buf = Buffer.alloc(44);
  let o = 0;
  buf.write("RIFF", o); o += 4;
  buf.writeUInt32LE(36 + dataBytes, o); o += 4;
  buf.write("WAVE", o); o += 4;
  buf.write("fmt ", o); o += 4;
  buf.writeUInt32LE(16, o); o += 4; // PCM fmt chunk size
  buf.writeUInt16LE(1, o); o += 2; // PCM
  buf.writeUInt16LE(numChannels, o); o += 2;
  buf.writeUInt32LE(sampleRate, o); o += 4;
  buf.writeUInt32LE(byteRate, o); o += 4;
  buf.writeUInt16LE(blockAlign, o); o += 2;
  buf.writeUInt16LE(bitsPerSample, o); o += 2;
  buf.write("data", o); o += 4;
  buf.writeUInt32LE(dataBytes, o); o += 4;
  return buf;
}

function interleaveStereoPcm16(left, right) {
  const n = Math.min(left.length, right.length);
  const out = Buffer.alloc(n * 4); // 2ch * 16-bit
  let o = 0;
  for (let i = 0; i < n; i++) {
    out.writeInt16LE(left[i], o); o += 2;
    out.writeInt16LE(right[i], o); o += 2;
  }
  return out;
}

async function elevenlabsTtsToUlaw8000({ text, persona }) {
  if (!ELEVENLABS_API_KEY) return null;
  const voiceId =
    (persona === "female" ? ELEVENLABS_VOICE_FEMALE : ELEVENLABS_VOICE_MALE) || ELEVENLABS_VOICE_ID;
  if (!voiceId) return null;

  // Hot-path cache (memory)
  const cacheKey = computeElevenUlawCacheKey({ text, persona });
  const cached = _ulawMemCache.get(cacheKey);
  if (cached) return cached;
  // Warm-path cache (disk)
  const disk = getUlawFromDisk(cacheKey);
  if (disk) {
    _ulawMemCache.set(cacheKey, disk);
    return disk;
  }

  const baseUrl = `https://api.elevenlabs.io/v1/text-to-speech/${voiceId}?output_format=${encodeURIComponent(
    ELEVENLABS_STREAM_OUTPUT_FORMAT || "ulaw_8000"
  )}`;

  const urlWithOpt =
    ELEVENLABS_OPTIMIZE_STREAMING_LATENCY == null
      ? null
      : `${baseUrl}&optimize_streaming_latency=${encodeURIComponent(String(ELEVENLABS_OPTIMIZE_STREAMING_LATENCY))}`;

  const bodyJson = JSON.stringify({
    text,
    model_id: ELEVENLABS_MODEL_ID,
    ...(ELEVENLABS_LANGUAGE_CODE ? { language_code: ELEVENLABS_LANGUAGE_CODE } : {}),
    voice_settings: { stability: Math.max(0, Math.min(1, ELEVENLABS_STABILITY)) }
  });

  const doFetch = (u) =>
    fetch(u, {
      method: "POST",
      headers: {
        "xi-api-key": ELEVENLABS_API_KEY,
        "Content-Type": "application/json",
        Accept: "audio/ulaw"
      },
      body: bodyJson
    });

  // Try low-latency only if explicitly configured; if it fails, retry without it (prevents silence).
  let res = null;
  if (urlWithOpt) {
    res = await doFetch(urlWithOpt);
    if (!res.ok) {
      const errText = await res.text().catch(() => "");
      console.warn("ElevenLabs ulaw TTS failed (optimize_streaming_latency):", res.status, errText);
      res = await doFetch(baseUrl);
    }
  } else {
    res = await doFetch(baseUrl);
  }

  if (!res.ok) {
    const errText = await res.text().catch(() => "");
    console.warn("ElevenLabs ulaw TTS failed:", res.status, errText);
    return null;
  }
  const ct = String(res.headers.get("content-type") || "").toLowerCase();
  const buf = Buffer.from(await res.arrayBuffer());
  if (MS_DEBUG) {
    console.log("[ms] elevenlabs ulaw ok", {
      status: res.status,
      contentType: ct,
      bytes: buf.length,
      format: ELEVENLABS_STREAM_OUTPUT_FORMAT || "ulaw_8000"
    });
  }

  // Safety: sometimes providers return a WAV container. Twilio requires raw mulaw bytes (no headers).
  // If we detect RIFF/WAVE, extract the "data" chunk.
  if (buf.length >= 12 && buf.toString("ascii", 0, 4) === "RIFF" && buf.toString("ascii", 8, 12) === "WAVE") {
    // naive chunk scan
    let i = 12;
    while (i + 8 <= buf.length) {
      const tag = buf.toString("ascii", i, i + 4);
      const size = buf.readUInt32LE(i + 4);
      const dataStart = i + 8;
      if (tag === "data" && dataStart + size <= buf.length) {
        const raw = buf.subarray(dataStart, dataStart + size);
        if (MS_DEBUG) console.log("[ms] stripped wav header", { rawBytes: raw.length });
        _ulawMemCache.set(cacheKey, raw);
        putUlawToDisk(cacheKey, raw);
        return raw;
      }
      i = dataStart + size;
    }
  }

  // If content-type looks wrong (json/text), avoid sending garbage to Twilio.
  if (ct && (ct.includes("application/json") || ct.includes("text/"))) {
    console.warn("ElevenLabs ulaw TTS returned non-audio content-type:", ct);
    return null;
  }

  _ulawMemCache.set(cacheKey, buf);
  putUlawToDisk(cacheKey, buf);
  return buf; // raw mulaw/8000 bytes
}

async function elevenlabsTtsToUlaw8000Stream({ text, persona }) {
  if (!ELEVENLABS_API_KEY) return null;
  const voiceId =
    (persona === "female" ? ELEVENLABS_VOICE_FEMALE : ELEVENLABS_VOICE_MALE) || ELEVENLABS_VOICE_ID;
  if (!voiceId) return null;

  const baseUrl = `https://api.elevenlabs.io/v1/text-to-speech/${voiceId}?output_format=${encodeURIComponent(
    ELEVENLABS_STREAM_OUTPUT_FORMAT || "ulaw_8000"
  )}`;
  const urlWithOpt =
    ELEVENLABS_OPTIMIZE_STREAMING_LATENCY == null
      ? null
      : `${baseUrl}&optimize_streaming_latency=${encodeURIComponent(String(ELEVENLABS_OPTIMIZE_STREAMING_LATENCY))}`;

  const bodyJson = JSON.stringify({
    text,
    model_id: ELEVENLABS_MODEL_ID,
    ...(ELEVENLABS_LANGUAGE_CODE ? { language_code: ELEVENLABS_LANGUAGE_CODE } : {}),
    voice_settings: { stability: Math.max(0, Math.min(1, ELEVENLABS_STABILITY)) }
  });

  const doFetch = (u) =>
    fetch(u, {
      method: "POST",
      headers: {
        "xi-api-key": ELEVENLABS_API_KEY,
        "Content-Type": "application/json",
        Accept: "audio/ulaw"
      },
      body: bodyJson
    });

  let res = null;
  if (urlWithOpt) {
    res = await doFetch(urlWithOpt);
    if (!res.ok) {
      const errText = await res.text().catch(() => "");
      console.warn("ElevenLabs ulaw TTS stream failed (optimize_streaming_latency):", res.status, errText);
      res = await doFetch(baseUrl);
    }
  } else {
    res = await doFetch(baseUrl);
  }

  if (!res.ok) {
    const errText = await res.text().catch(() => "");
    console.warn("ElevenLabs ulaw TTS stream failed:", res.status, errText);
    return null;
  }

  const ct = String(res.headers.get("content-type") || "").toLowerCase();
  if (ct && (ct.includes("application/json") || ct.includes("text/"))) {
    const errText = await res.text().catch(() => "");
    console.warn("ElevenLabs ulaw TTS stream returned non-audio:", ct, errText.slice(0, 300));
    return null;
  }

  // Node fetch returns a Web ReadableStream; we can consume it via getReader().
  if (!res.body || typeof res.body.getReader !== "function") {
    // Fallback: buffer fully (should be rare).
    const buf = Buffer.from(await res.arrayBuffer());
    return { buffered: buf };
  }

  const reader = res.body.getReader();
  // Probe one chunk up front. If ElevenLabs returns an empty body (we saw this in logs),
  // return null so callers can fall back to the non-streaming path.
  try {
    const first = await reader.read();
    if (!first?.done && first?.value && first.value.byteLength) {
      return { reader, contentType: ct, firstChunk: Buffer.from(first.value) };
    }
  } catch (e) {
    try { reader.releaseLock(); } catch {}
    if (MS_DEBUG) console.warn("[ms] elevenlabs ulaw stream probe failed:", e?.message || e);
    return null;
  }
  try { reader.releaseLock(); } catch {}
  if (MS_DEBUG) console.warn("[ms] elevenlabs ulaw stream empty body (probe done with no bytes)");
  return null;
}
function startsWithLang(code, prefix) {
  return new RegExp(`^${String(prefix).replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}(?:-|$)`, "i").test(String(code || ""));
}

function normalizeConversationRelaySettings() {
  let transcriptionLanguage = CR_TRANSCRIPTION_LANGUAGE || CR_LANGUAGE;

  // Base attrs (may be overridden by compatibility rules below)
  let languageAttr = CR_LANGUAGE;
  let ttsLanguageAttr = CR_TTS_LANGUAGE;
  let transcriptionProviderAttr = CR_TRANSCRIPTION_PROVIDER;
  let speechModelAttr = CR_SPEECH_MODEL;

  const ttsProviderLower = String(CR_TTS_PROVIDER || "").toLowerCase();
  const sttProviderLower = String(transcriptionProviderAttr || "").toLowerCase();

  // --- TTS rules ---
  // Hebrew + ElevenLabs via Twilio ConversationRelay:
  // Some accounts reject/block ElevenLabs for he-IL at TwiML validation time (64101 block_elevenlabs/he-IL/...),
  // and/or have no TTS settings configured for he-IL (64106).
  // Reliable workaround:
  // - omit any he-IL mapping for TTS (no <Language code="he-IL"...>, no token lang)
  // - omit ttsLanguage (let Twilio pick defaults) and still send Hebrew text tokens; ElevenLabs will read them as-is.
  if (ttsProviderLower === "elevenlabs" && startsWithLang(CR_LANGUAGE, "he")) {
    languageAttr = "";
    ttsLanguageAttr = "";
  }

  // --- STT rules ---
  // Twilio ConversationRelay validation rejects explicit Hebrew locale for STT in some accounts
  // (e.g. google/he-IL/telephony, deepgram/he-IL/nova-2-general). Workaround:
  // use automatic language detection for STT: transcriptionLanguage="multi"
  // which requires Deepgram + nova-2-general (or nova-3-general).
  if (startsWithLang(transcriptionLanguage, "he")) {
    transcriptionLanguage = "multi";
    transcriptionProviderAttr = "Deepgram";
    speechModelAttr = "nova-2-general";
  }

  // Normalize model for provider
  if (String(transcriptionProviderAttr).toLowerCase() === "google") {
    const m = String(speechModelAttr || "").toLowerCase();
    if (m !== "telephony" && m !== "long") speechModelAttr = "telephony";
  }
  if (String(transcriptionProviderAttr).toLowerCase() === "deepgram") {
    const m = String(speechModelAttr || "").toLowerCase();
    if (!m.startsWith("nova-")) speechModelAttr = "nova-2-general";
  }

  return {
    languageAttr,
    ttsLanguageAttr,
    transcriptionLanguage,
    transcriptionProviderAttr,
    speechModelAttr
  };
}

function toWsUrlFromHttpBase(baseHttp) {
  const b = String(baseHttp || "").trim();
  if (b.startsWith("https://")) return `wss://${b.slice("https://".length)}`;
  if (b.startsWith("http://")) return `ws://${b.slice("http://".length)}`;
  return b;
}

function escapeXmlAttr(v) {
  return String(v ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("\"", "&quot;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;");
}

function buildConversationRelayTwiML({
  wsUrl,
  language,
  ttsLanguage,
  ttsProvider,
  voice,
  transcriptionLanguage,
  transcriptionProvider,
  speechModel,
  elevenlabsTextNormalization,
  welcomeGreeting,
  welcomeGreetingInterruptible,
  interruptible,
  interruptSensitivity,
  debug,
  customParameters = {},
  languageElements = []
}) {
  const paramsXml = Object.entries(customParameters)
    .filter(([k, val]) => k && val != null && String(val).length)
    .map(([k, val]) => `<Parameter name="${escapeXmlAttr(k)}" value="${escapeXmlAttr(val)}" />`)
    .join("");

  const languagesXml = Array.isArray(languageElements)
    ? languageElements
        .filter((x) => x && x.code)
        .map((x) => {
          const attrs = [
            `code="${escapeXmlAttr(x.code)}"`,
            x.ttsProvider ? `ttsProvider="${escapeXmlAttr(x.ttsProvider)}"` : "",
            x.voice ? `voice="${escapeXmlAttr(x.voice)}"` : "",
            x.transcriptionProvider ? `transcriptionProvider="${escapeXmlAttr(x.transcriptionProvider)}"` : "",
            x.speechModel ? `speechModel="${escapeXmlAttr(x.speechModel)}"` : ""
          ]
            .filter(Boolean)
            .join(" ");
          return `<Language ${attrs} />`;
        })
        .join("")
    : "";

  const attrs = [
    `url="${escapeXmlAttr(wsUrl)}"`,
    language ? `language="${escapeXmlAttr(language)}"` : "",
    ttsLanguage ? `ttsLanguage="${escapeXmlAttr(ttsLanguage)}"` : "",
    ttsProvider ? `ttsProvider="${escapeXmlAttr(ttsProvider)}"` : "",
    voice ? `voice="${escapeXmlAttr(voice)}"` : "",
    transcriptionLanguage ? `transcriptionLanguage="${escapeXmlAttr(transcriptionLanguage)}"` : "",
    transcriptionProvider ? `transcriptionProvider="${escapeXmlAttr(transcriptionProvider)}"` : "",
    speechModel ? `speechModel="${escapeXmlAttr(speechModel)}"` : "",
    elevenlabsTextNormalization ? `elevenlabsTextNormalization="${escapeXmlAttr(elevenlabsTextNormalization)}"` : "",
    welcomeGreeting ? `welcomeGreeting="${escapeXmlAttr(welcomeGreeting)}"` : "",
    welcomeGreetingInterruptible ? `welcomeGreetingInterruptible="${escapeXmlAttr(welcomeGreetingInterruptible)}"` : "",
    interruptible ? `interruptible="${escapeXmlAttr(interruptible)}"` : "",
    interruptSensitivity ? `interruptSensitivity="${escapeXmlAttr(interruptSensitivity)}"` : "",
    debug ? `debug="${escapeXmlAttr(debug)}"` : ""
  ]
    .filter(Boolean)
    .join(" ");

  return `<?xml version="1.0" encoding="UTF-8"?>
<Response>
  <Connect>
    <ConversationRelay ${attrs}>${paramsXml}${languagesXml}</ConversationRelay>
  </Connect>
</Response>`;
}

const MAX_TURNS = Number(process.env.MAX_TURNS || 6); // 6 סבבים ~ 2 דק' בשיחה קצרה

const db = openDb({ dbPath: DB_PATH });
const app = express();

// Twilio שולח application/x-www-form-urlencoded
app.use(express.urlencoded({ extended: false }));
app.use(express.json({ limit: "2mb" }));

const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 10 * 1024 * 1024 } });

// קבצי TTS (אם משתמשים ב-ElevenLabs)
const ttsDir = path.resolve(DATA_DIR, "tts-cache");
if (!fs.existsSync(ttsDir)) fs.mkdirSync(ttsDir, { recursive: true });
app.use("/tts", express.static(ttsDir, { fallthrough: false }));

// Media Streams call recordings (stereo WAVs).
const msRecordingsDir = path.resolve(DATA_DIR, "ms-recordings");
try {
  if (!fs.existsSync(msRecordingsDir)) fs.mkdirSync(msRecordingsDir, { recursive: true });
} catch {}
// Expose recordings as static files so you can listen from a browser:
// GET /ms-recordings/<filename>.wav
app.use("/ms-recordings", express.static(msRecordingsDir, { fallthrough: false }));

let openai = null;
if (!process.env.OPENAI_API_KEY) {
  console.warn("Missing OPENAI_API_KEY. /twilio/gather will return a friendly message until set.");
} else {
  openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });
}

// דף בית: להפוך את פתיחת ה-URL של ה-tunnel בדפדפן ליותר ברורה
// (אחרת מקבלים "Cannot GET /" וזה מבלבל, למרות שהשרת עובד)
app.get("/", (req, res) => res.redirect("/admin"));

// ---------------------------
// Default agent content (editable from /admin)
// ---------------------------
const DEFAULT_KNOWLEDGE_BASE = `
כללי (תבנית לקמפיין)
- מי אתם ומה אתם מציעים (משפט אחד).
- למי זה מתאים (משפט אחד).
- מה המטרה של השיחה: להגיע לאישור להעביר פרטים כדי שיחזרו לתיאום.
- אם אין פרטים מדויקים (יום/שעה/כתובת/מחיר) — לא להמציא. להגיד שהגורם הרלוונטי יחזור עם כל הפרטים.
- אם מבקשים להסיר/לא להתקשר: לאשר מיד ולסיים בנימוס.

שאלות נפוצות (דוגמאות — תחליף למה שמתאים לקמפיין)
- "במה מדובר?" → "בגדול: [מה מציעים]… רוצה שיחזרו אליך/אלייך עם פרטים מסודרים?"
- "איפה/מתי/כמה עולה?" → "אין לי פה את כל הפרטים המדויקים, יחזרו אליך/אלייך לתיאום."
`.trim();

// Default openers (generic). Admin can override; keep defaults consistent with agent voice persona.
function defaultOpeningForCallee(calleePersona) {
  const verb = AGENT_VOICE_PERSONA === "female" ? "מדברת" : "מדבר";
  if (calleePersona === "female") return `שלום יקרה, ${verb} בנוגע להצעה קצרה—יש לך דקה?`;
  return `שלום אחי, ${verb} בנוגע להצעה קצרה—יש לך דקה?`;
}
const DEFAULT_OPENING_MALE = defaultOpeningForCallee("male");
const DEFAULT_OPENING_FEMALE = defaultOpeningForCallee("female");

// Campaign content is configured via the Admin UI.
// Keep server defaults generic so the codebase is fully white-label.
const DEFAULT_MIDDLE_MALE = `
מטרת האמצע (תבנית לקמפיין):
- להבין מהר אם יש עניין.
- לענות קצר וברור.
- להגיע לאישור להעברת פרטים כדי שיחזרו לתיאום.

דוגמאות תשובה (להחליף למה שמתאים לקמפיין):
- "בגדול זה [תיאור קצר]. רוצה שיחזרו אליך עם פרטים מסודרים?"
- "אין לי פה את כל הפרטים המדויקים, יחזרו אליך לתיאום."

אם מבקשים להסיר/לא להתקשר:
- "בטח, מוריד אותך עכשיו. יום טוב."
`.trim();

const DEFAULT_MIDDLE_FEMALE = `
מטרת האמצע (תבנית לקמפיין):
- להבין מהר אם יש עניין.
- לענות קצר וברור.
- להגיע לאישור להעברת פרטים כדי שיחזרו לתיאום.

דוגמאות תשובה (להחליף למה שמתאים לקמפיין):
- "בגדול זה [תיאור קצר]. רוצה שיחזרו אלייך עם פרטים מסודרים?"
- "אין לי פה את כל הפרטים המדויקים, יחזרו אלייך לתיאום."

אם מבקשים להסיר/לא להתקשר:
- "בטח, מוריד אותך עכשיו. יום טוב."
`.trim();

const DEFAULT_CLOSING_MALE = `
סגירה (תבנית לקמפיין):
- אם יש הסכמה: "מעולה. אני מעביר את הפרטים שלך, ויחזרו אליך לתיאום. יום טוב."
- אם אין עניין: "הבנתי, תודה על הזמן. יום טוב."
`.trim();

const DEFAULT_CLOSING_FEMALE = `
סגירה (תבנית לקמפיין):
- אם יש הסכמה: "מעולה. אני מעביר את הפרטים שלך, ויחזרו אלייך לתיאום. יום טוב."
- אם אין עניין: "הבנתי, תודה על הזמן. יום טוב."
`.trim();

function setDefaultIfEmpty(key, value) {
  const cur = String(getSetting(db, key, "") || "").trim();
  if (!cur) setSetting(db, key, String(value));
}

function appendIfMissing(key, { marker, snippet }) {
  const cur = String(getSetting(db, key, "") || "").trim();
  if (!cur) return;
  if (cur.includes(marker)) return;
  setSetting(db, key, `${cur}\n\n${snippet}`.trim());
}

// Seed defaults (only if empty) so the admin panel starts with rich content.
setDefaultIfEmpty("knowledgeBase", DEFAULT_KNOWLEDGE_BASE);
setDefaultIfEmpty("openingScriptMale", DEFAULT_OPENING_MALE);
setDefaultIfEmpty("openingScriptFemale", DEFAULT_OPENING_FEMALE);
setDefaultIfEmpty("middleScriptMale", DEFAULT_MIDDLE_MALE);
setDefaultIfEmpty("middleScriptFemale", DEFAULT_MIDDLE_FEMALE);
setDefaultIfEmpty("closingScriptMale", DEFAULT_CLOSING_MALE);
setDefaultIfEmpty("closingScriptFemale", DEFAULT_CLOSING_FEMALE);
// White-label / campaign config: these strings control who calls back.
// Keep them as phrases (not just nouns) so Hebrew grammar stays correct across clients.
setDefaultIfEmpty("handoffToPhrase", "לצוות");
setDefaultIfEmpty("handoffFromPhrase", "מהצוות");
// Campaign / flow knobs (no campaign text here; only generic behavior toggles)
setDefaultIfEmpty("campaignMode", "handoff"); // "handoff" | "guided"
setDefaultIfEmpty("femaleOnly", false);
setDefaultIfEmpty("minParticipants", 15);
setDefaultIfEmpty("cooldownMonths", 6);

// If the user already edited scripts, keep them—but add a small helpful "explanation" block once.
// Keep legacy upgrade helpers, but make them generic (white-label).
appendIfMissing("middleScriptMale", {
  marker: "דוגמאות תשובה",
  snippet:
    `דוגמאות תשובה (להחליף למה שמתאים לקמפיין):\n- "בגדול זה [תיאור קצר]. רוצה שיחזרו אליך עם פרטים מסודרים?"\n- "אין לי פה את כל הפרטים המדויקים, יחזרו אליך לתיאום."`
});
appendIfMissing("middleScriptFemale", {
  marker: "דוגמאות תשובה",
  snippet:
    `דוגמאות תשובה (להחליף למה שמתאים לקמפיין):\n- "בגדול זה [תיאור קצר]. רוצה שיחזרו אלייך עם פרטים מסודרים?"\n- "אין לי פה את כל הפרטים המדויקים, יחזרו אלייך לתיאום."`
});

function normalizePhone(raw) {
  if (!raw) return "";
  return String(raw).trim();
}

function sanitizeSayText(text) {
  // Twilio לפעמים נופל על תווים מיוחדים; נשאיר עברית אבל נחליף תווים בעייתיים.
  return String(text || "")
    .replaceAll("—", "-")
    .replaceAll("–", "-")
    .replaceAll("\u202A", "")
    .replaceAll("\u202B", "")
    .replaceAll("\u202C", "")
    .replaceAll("\u200E", "")
    .replaceAll("\u200F", "")
    .trim();
}

async function respondWithPlayAndMaybeHangup(req, res, { text, persona, hangup = true, retry = 0 }) {
  const safe = sanitizeSayText(text);
  const provider = TTS_PROVIDER || "openai";
  const key = computeTtsCacheKey({ provider, text: safe, persona: AGENT_VOICE_PERSONA });
  const cached = findCachedAudioByKey(key);

  if (!cached) {
    kickoffTtsGeneration({ provider, text: safe, persona: AGENT_VOICE_PERSONA }).catch(() => {});
    const redirectUrl = toAbsoluteUrl(
      req,
      `${hangup ? "/twilio/play_end" : "/twilio/play"}?k=${encodeURIComponent(key)}&a=0${
        hangup ? "" : `&callSid=${encodeURIComponent(String(getParam(req, "CallSid") || ""))}`
      }`
    );
    const response = new twilio.twiml.VoiceResponse();
    response.pause({ length: TTS_POLL_WAIT_SECONDS });
    response.redirect({ method: "POST" }, redirectUrl);
    res.type("text/xml").send(response.toString());
    return;
  }

  if (hangup) {
    res.type("text/xml").send(buildPlayAndHangup({ playUrl: toAbsoluteUrl(req, cached.rel) }));
    return;
  }

  const xml = buildRecordTwiML({
    sayText: null,
    playUrl: toAbsoluteUrl(req, cached.rel),
    actionUrl: recordActionUrl(req, retry),
    playBeep: false,
    maxLengthSeconds: RECORD_MAX_LENGTH_SECONDS,
    timeoutSeconds: recordTimeoutSeconds()
  });
  res.type("text/xml").send(xml);
}

function clampInt(n, { min, max, fallback }) {
  const v = Number.parseInt(String(n ?? ""), 10);
  if (!Number.isFinite(v)) return fallback;
  return Math.max(min, Math.min(max, v));
}

function recordActionUrl(req, retry) {
  const r = clampInt(retry, { min: 0, max: 10, fallback: 0 });
  return toAbsoluteUrl(req, `/twilio/record?r=${r}`);
}

function getParam(req, key) {
  // תומך גם ב-POST (body) וגם ב-GET (query) למקרה ש-Twilio/Proxy שולחים אחרת.
  return (req.body && req.body[key]) ?? (req.query && req.query[key]) ?? "";
}

function detectOptOut(text) {
  const t = (text || "").toLowerCase();
  const patterns = [
    "אל תתקשר",
    "אל תתקשרו",
    "תסיר",
    "להסיר",
    "תמחק",
    "מחק",
    "לא רוצה שיחות",
    "תפסיקו להתקשר",
    "דונט קול",
    "do not call"
  ];
  return patterns.some((p) => t.includes(p));
}

function normalizeIntentText(text) {
  // Normalize STT output so "כן." / "כן," / "מעוניים" typos don't break intent detection.
  // - lower
  // - strip punctuation/symbols
  // - collapse whitespace
  const s = String(text || "").toLowerCase();
  // Keep letters/numbers/spaces only (Unicode-aware)
  const cleaned = s.replace(/[^\p{L}\p{N}\s]+/gu, " ");
  return cleaned.replace(/\s+/g, " ").trim();
}

function detectInterested(text) {
  const t = normalizeIntentText(text);
  const patterns = [
    "מעוניין",
    "מעוניינת",
    "מעונין",
    "מעונינת",
    "רוצה לבוא",
    "רוצה להגיע",
    "אני רוצה לבוא",
    "אני רוצה להגיע",
    "אשמח לבוא",
    "תרשמי אותי",
    "תרשמי אותי",
    "תרשמי",
    "להירשם",
    "תעבירי אותי",
    "תעביר",
    "תעבירי",
    "תעבירו"
  ];
  if (patterns.some((p) => t.includes(p))) return true;
  // Extra tolerance for common partials/typos like "מעוניים"
  if (/(^|\s)מעונ[א-ת]*($|\s)/.test(t)) return true;
  return false;
}

function detectNotInterested(text) {
  const t = normalizeIntentText(text);
  const patterns = [
    "לא",
    "לא רוצה",
    "לא מעוניין",
    "לא מעוניינת",
    "לא מעונין",
    "לא מעונינת",
    "לא מתאים",
    "לא תודה",
    "ביי",
    "ביי ביי",
    "יאללה ביי",
    "להתראות",
    "עזוב",
    "עזבי",
    "אין לי זמן",
    "לא עכשיו",
    "לא נראה לי",
    "אולי אחר כך"
  ];
  return patterns.some((p) => t.includes(p));
}

function detectWaitRequest(text) {
  const t = normalizeIntentText(text);
  const patterns = ["תמתין", "תמתין רגע", "רגע", "שנייה", "שניה", "דקה", "חכה", "חכי", "רק רגע"];
  return patterns.some((p) => t.includes(p));
}

function detectRepeatRequest(text) {
  const t = normalizeIntentText(text);
  const patterns = [
    "מה אמרת",
    "מה אמרת לי",
    "מה נאמר",
    "לא הבנתי",
    "לא הבנתי אותך",
    "תחזור",
    "תחזרי",
    "תאמר שוב",
    "תגיד שוב",
    "תגידי שוב",
    "עוד פעם",
    "שוב פעם"
  ];
  return patterns.some((p) => t.includes(p));
}

function detectSmalltalk(text) {
  const t = normalizeIntentText(text);
  // common Israeli smalltalk right after pickup
  const patterns = ["מה נשמע", "מה שלומך", "מה קורה", "מה העניינים", "הכל טוב", "שלום לך", "שלום מה נשמע"];
  return patterns.some((p) => t.includes(p));
}

function looksLikeDateAnswer(text) {
  const t = normalizeIntentText(text);
  if (!t) return false;
  // digits often indicate a date/time
  if (/\d/.test(t)) return true;
  const patterns = [
    "היום",
    "מחר",
    "מחרתיים",
    "עוד",
    "שבוע",
    "שבועיים",
    "חודש",
    "חודשים",
    "תאריך",
    "יום",
    "ראשון",
    "שני",
    "שלישי",
    "רביעי",
    "חמישי",
    "שישי",
    "שבת",
    "בבוקר",
    "בצהריים",
    "בצהרים",
    "בערב",
    "בלילה",
    "אחרי"
  ];
  return patterns.some((p) => t.includes(p));
}

function isGenericAckTranscript(text) {
  const ns = normalizeIntentText(text);
  return (
    ns === "תודה" ||
    ns === "תודה רבה" ||
    ns === "כן" ||
    ns === "כאן" ||
    ns === "אוקיי" ||
    ns === "אוקי" ||
    ns === "סבבה" ||
    ns === "בסדר"
  );
}

function hasHebrewLetters(text) {
  return /[\u0590-\u05FF]/.test(String(text || ""));
}

function looksLikePromptEcho(text) {
  const t = String(text || "").trim();
  if (!t) return false;
  // We never expect the model to output our own prompt; this indicates a bad decode/edge-case.
  return (
    t.startsWith("תמלול שיחה טלפונית בעברית") ||
    t.includes("מילים נפוצות") ||
    (OPENAI_STT_PROMPT && t.includes(String(OPENAI_STT_PROMPT).slice(0, 24)))
  );
}

function isSuspiciousTranscript(text) {
  const s = String(text || "").trim();
  if (!s) return true;
  if (looksLikePromptEcho(s)) return true;
  // If it's mostly non-Hebrew and very short, it's often garbage (e.g. "I'm on the").
  if (!hasHebrewLetters(s) && s.length <= 24) return true;
  return false;
}

function shouldRetryStt({ speech, pcm8kLen }) {
  // If we have meaningful audio duration but the transcript is extremely short/generic,
  // it's often a cut/partial decode. Retry once with a fallback model if configured.
  const audioMs = (Number(pcm8kLen || 0) / 8000) * 1000;
  const s = String(speech || "").trim();
  if (!s) return audioMs >= 700;
  if (audioMs < 800) return false; // keep fast for truly short utterances
  if (s.length <= 4) return true;
  if (isGenericAckTranscript(s) && audioMs >= 900) return true;
  if (looksLikePromptEcho(s) && audioMs >= 700) return true;
  return false;
}

function getThinkingText() {
  // Disabled by product decision (user preference).
  return "";
}

function limitPhoneReply(text, maxChars = 70) {
  const s = sanitizeSayText(String(text || "").trim());
  if (!s) return "";
  if (s.length <= maxChars) return s;
  const cut = s.slice(0, maxChars);
  const lastPunct = Math.max(cut.lastIndexOf("."), cut.lastIndexOf("!"), cut.lastIndexOf("?"), cut.lastIndexOf("…"));
  return sanitizeSayText((lastPunct > 40 ? cut.slice(0, lastPunct + 1) : cut).trim());
}

function detectAffirmativeShort(text) {
  const t = normalizeIntentText(text);
  if (!t) return false;
  // Negation wins: avoid false positives like "כן אבל לא".
  if (detectOptOut(t) || detectNotInterested(t)) return false;
  // If "כן" appears anywhere, treat as affirmative (unless a clear negation is also present).
  if (/\bכן\b/.test(t)) return true;
  const patterns = [
    "בטח",
    "ברור",
    "אוקיי",
    "אוקי",
    "סבבה",
    "בהחלט",
    "יאללה",
    "קדימה",
    "סגור",
    "סבבה אחי",
    "נראה לי",
    "אפשר",
    "בוא נעשה",
    "יאללה תעביר",
    "תעביר",
    "תעבירו"
  ];
  return patterns.some((p) => t === p || t.startsWith(p + " ") || t.includes(" " + p + " "));
}

function detectTransferConsent(text) {
  const t = normalizeIntentText(text);
  const patterns = [
    "תעביר את הפרטים",
    "תעביר פרטים",
    "תעבירו את הפרטים",
    "תעבירו פרטים",
    "מאשר להעביר",
    "מאשרת להעביר",
    "כן תעביר",
    "כן תעבירו",
    "סבבה תעביר",
    "אוקיי תעביר",
    "תרשום אותי",
    "תרשמי אותי",
    "אני מאשר",
    "אני מאשרת"
  ];
  return patterns.some((p) => t.includes(p));
}

function detectNoMoreHelp(text) {
  const t = String(text || "").toLowerCase().trim();
  const patterns = ["לא", "לא תודה", "זהו", "זה הכל", "אין", "אין עוד", "לא צריך", "סיימנו"];
  return patterns.some((p) => t === p || t.includes(p));
}

function serverVersion() {
  // Render provides RENDER_GIT_COMMIT in many setups; allow manual override too.
  return (
    String(process.env.SERVER_VERSION || "").trim() ||
    String(process.env.RENDER_GIT_COMMIT || "").trim() ||
    String(process.env.GIT_COMMIT || "").trim() ||
    "dev"
  );
}

function shortGreetingByPersona(persona) {
  return persona === "female"
    ? "היי, מדבר בנוגע להצעה קצרה—זה רלוונטי לך?"
    : "היי, מדבר בנוגע להצעה קצרה—זה רלוונטי לך?";
}

function normalizeGreetingForLatency({ greeting, persona }) {
  const g = sanitizeSayText(String(greeting || "").trim());
  if (!g) return sanitizeSayText(buildGreeting({ persona }));
  if (MS_FORCE_SHORT_GREETING) return sanitizeSayText(shortGreetingByPersona(persona));
  if (g.length > MS_GREETING_MAX_CHARS) return sanitizeSayText(shortGreetingByPersona(persona));
  return g;
}

function extractClosingLine(script, { interested }) {
  const s = String(script || "").trim();
  if (!s) return "";
  const lower = s.toLowerCase();
  const wants = interested
    ? ["רוצה", "מסכים", "מעוניין", "מעוניינת"]
    : ["לא רוצה", "לא מעוניין", "לא מעוניינת", "לא מתאים"];
  const idx = wants
    .map((w) => lower.indexOf(w))
    .filter((i) => i >= 0)
    .sort((a, b) => a - b)[0];
  const slice = idx >= 0 ? s.slice(idx) : s;
  const m = slice.match(/"([^"]+)"/);
  return m?.[1]?.trim() || "";
}

function detectFaq(text) {
  const t = String(text || "").toLowerCase();
  const hasQWord =
    t.includes("מי") || t.includes("מה") || t.includes("איפה") || t.includes("מתי") || t.includes("כמה");
  return {
    hasQWord,
    who: t.includes("מי זה") || t.includes("מאיפה") || t.includes("מאיפה יש"),
    what: t.includes("מה זה") || t.includes("מה עושים") || t.includes("על מה") || t.includes("איך זה עובד"),
    where: t.includes("איפה") || t.includes("כתובת"),
    when: t.includes("מתי") || t.includes("שעה") || t.includes("יום"),
    cost: t.includes("כמה עולה") || t.includes("עולה") || t.includes("מחיר") || t.includes("חינם"),
    howLong: t.includes("כמה זמן") || t.includes("אורך") || t.includes("משך"),
    repeat: t.includes("לא הבנתי") || t.includes("תחזרי") || t.includes("תאמרי שוב") || t.includes("עוד פעם")
  };
}

function quickReplyByRules({ speech, persona }) {
  const s = String(speech || "").trim();
  if (!s) return null;
  const faq = detectFaq(s);

  // כדי לא "לחטוף" שיחה טבעית, מפעילים חוקים רק כשזה באמת נשמע כמו שאלה/בקשה שחוזרים עליה.
  const shouldUse =
    faq.repeat || (faq.hasQWord && (faq.who || faq.what || faq.where || faq.when || faq.cost || faq.howLong));
  if (!shouldUse) return null;

  // תשובות קצרות וקבועות. הסוכן תמיד מדבר בלשון זכר; הדקדוק ללקוח/ה לפי persona.
  if (faq.repeat) {
    return { text: "ברור, אני מסביר שוב בקצרה. יש לך דקה רגע?", end: false };
  }
  if (faq.who) {
    return {
      text:
        "אני מתקשר בנוגע להצעה קצרה. המספר אצלנו ברשימה של אנשים שאישרו לקבל עדכון. אם לא מתאים—אני מוריד אותך מיד.",
      end: false
    };
  }
  if (faq.what) {
    return {
      text:
        persona === "female"
          ? "בגדול זו הצעה/שירות קצר. אין לי את כל הפרטים כאן—רוצה שיחזרו אלייך עם פרטים מסודרים?"
          : "בגדול זו הצעה/שירות קצר. אין לי את כל הפרטים כאן—רוצה שיחזרו אליך עם פרטים מסודרים?",
      end: false
    };
  }
  if (faq.where || faq.when || faq.cost || faq.howLong) {
    const toYou = persona === "female" ? "אלייך" : "אליך";
    return {
      text:
        `את הפרטים המדויקים—שעה, מקום ואם יש עלות—הגורם הרלוונטי נותן בשיחה. רוצה שיחזרו ${toYou} עם כל הפרטים המסודרים?`,
      end: false
    };
  }
  return null;
}

function tokenizeHe(text) {
  const stop = new Set([
    "אני",
    "את",
    "אתה",
    "הוא",
    "היא",
    "אנחנו",
    "זה",
    "של",
    "על",
    "מה",
    "מי",
    "איך",
    "למה",
    "כן",
    "לא",
    "עם",
    "אם",
    "יש",
    "אין",
    "היה",
    "היית",
    "הייתי",
    "זהו",
    "פה",
    "שם",
    "רגע",
    "טוב",
    "באמת"
  ]);
  return String(text || "")
    .toLowerCase()
    .replace(/[^\p{L}\p{N}\s]/gu, " ")
    .split(/\s+/)
    .filter(Boolean)
    .filter((w) => w.length >= 2)
    .filter((w) => !stop.has(w));
}

function extractFaqPairsFromKnowledgeBase(kbRaw) {
  const kb = String(kbRaw || "");
  if (!kb) return [];
  const lines = kb.split(/\r?\n/);
  const out = [];
  for (const raw of lines) {
    const line = String(raw || "").trim();
    if (!line) continue;
    // Support "question → answer" (recommended) and "question -> answer"
    const arrowIdx = line.includes("→") ? line.indexOf("→") : line.indexOf("->");
    if (arrowIdx <= 0) continue;
    const left = line.slice(0, arrowIdx).trim();
    const right = line.slice(arrowIdx + (line[arrowIdx] === "→" ? 1 : 2)).trim();
    if (!left || !right) continue;

    // Allow multiple variants on the left: separated by | or /
    const variants = left
      .split(/[|/]/g)
      .map((s) => s.trim().replace(/^"|"$/g, ""))
      .filter(Boolean);
    if (!variants.length) continue;

    out.push({
      variants,
      answer: right.replace(/^"|"$/g, "").trim()
    });
  }
  return out;
}

function matchFaqAnswerFromKb({ kb, userText, minScore = 2 }) {
  const qa = extractFaqPairsFromKnowledgeBase(kb);
  if (!qa.length) return null;
  const uTokens = new Set(tokenizeHe(userText));
  if (!uTokens.size) return null;

  let best = null;
  for (const item of qa) {
    let bestScoreForItem = 0;
    for (const v of item.variants) {
      const vTokens = tokenizeHe(v);
      let score = 0;
      for (const t of vTokens) if (uTokens.has(t)) score++;
      if (score > bestScoreForItem) bestScoreForItem = score;
    }
    if (!best || bestScoreForItem > best.score) best = { score: bestScoreForItem, answer: item.answer };
  }
  if (!best || best.score < minScore) return null;
  return best.answer;
}

function extractFlowLinesFromKnowledgeBase(kbRaw) {
  const kb = String(kbRaw || "");
  if (!kb) return {};
  // We support two notations:
  // 1) Under a [FLOW] section: KEY=VALUE
  // 2) Anywhere in KB: FLOW_KEY=VALUE
  const lines = kb.split(/\r?\n/);
  let inFlow = false;
  const out = {};
  for (const raw of lines) {
    const line = String(raw || "").trim();
    if (!line) continue;
    if (/^\[flow\]$/i.test(line)) {
      inFlow = true;
      continue;
    }
    if (/^\[\/flow\]$/i.test(line)) {
      inFlow = false;
      continue;
    }
    const m = line.match(/^([A-Z0-9_]+)\s*=\s*(.+)$/);
    if (!m) continue;
    const k = String(m[1] || "").trim();
    const v = String(m[2] || "").trim();
    if (!k || !v) continue;
    if (inFlow) out[k] = v;
    else if (k.startsWith("FLOW_")) out[k] = v;
  }
  return out;
}

function extractApprovedNamesFromKnowledgeBase(kbRaw) {
  const kb = String(kbRaw || "");
  if (!kb) return [];
  const lines = kb.split(/\r?\n/);
  const out = [];
  let inNames = false;
  for (const raw of lines) {
    const line = String(raw || "").trim();
    if (!line) {
      // blank line ends the section
      if (inNames) break;
      continue;
    }
    if (/^רשימת\s+שמות\s+מאושרים/i.test(line)) {
      inNames = true;
      continue;
    }
    if (!inNames) continue;
    // bullet items: "- ___"
    const m = line.match(/^-+\s*(.+)$/);
    if (!m) {
      // stop if we reached another heading
      if (/^\[.+\]$/.test(line) || /:$/.test(line)) break;
      continue;
    }
    const name = String(m[1] || "").trim().replace(/^"|"$/g, "");
    if (!name) continue;
    if (name === "___" || name === "__" || name === "_") continue;
    out.push(name);
  }
  // de-dupe + keep short reasonable names only
  const uniq = Array.from(new Set(out.map((s) => s.trim()).filter(Boolean)));
  return uniq.filter((n) => n.length >= 2 && n.length <= 30);
}

function detectRabbinicalInquiry(text) {
  const t = normalizeIntentText(text);
  if (!t) return false;
  // "רבנית X", "מדריכה X", "הרבנית מגיעה?", "מי הרבנית?"
  const triggers = ["רבנית", "רב", "מדריכה", "מדריכות", "רבניות", "שם הרבנית", "מי הרבנית"];
  if (triggers.some((p) => t.includes(p))) return true;
  // Common question forms about names
  const q = ["מי", "האם", "יש", "קוראים", "איך קוראים", "מגיעה", "מגיע"];
  return q.some((p) => t.includes(p));
}

function matchApprovedNameInText({ kb, userText }) {
  const names = extractApprovedNamesFromKnowledgeBase(kb);
  if (!names.length) return null;
  const raw = String(userText || "");
  for (const n of names) {
    if (!n) continue;
    if (raw.includes(n)) return n;
  }
  return null;
}

function guidedFlowTextFromKb(kb, { minParticipants, cooldownMonths }) {
  const flow = extractFlowLinesFromKnowledgeBase(kb);
  // Provide safe GENERIC defaults (white-label; can be overridden in KB).
  // Important: these defaults must NOT mention any specific campaign (religion/city/organization/etc.).
  const defaults = {
    FLOW_ASK_PURPOSE: "בגדול—מה המטרה/הבקשה שלך?",
    FLOW_ASK_DATE: "מתי נוח לך?",
    FLOW_ASK_PARTICIPANTS: "כמה משתתפים צפויים בערך?",
    FLOW_PARTICIPANTS_OK: "מצוין. נציגה תחזור אלייך בהקדם עם כל הפרטים.",
    FLOW_PARTICIPANTS_LOW: `כדי שזה יעבוד אנחנו צריכים מינימום ${minParticipants} משתתפים. תצליחי להגיע ל־${minParticipants}?`,
    FLOW_PARTICIPANTS_LOW_FALLBACK: "מבין. בכל מקרה נציגה תחזור אלייך בהקדם ותנסה לעזור לגבי הפרטים.",
    FLOW_COOLDOWN_RULE: `בדרך כלל אפשר לקבוע שוב רק אחרי מינימום ${cooldownMonths} חודשים.`,
    FLOW_WOMEN_ONLY: "שלום, כרגע השיחה מיועדת לנשים בלבד. יום טוב.",
    // Optional: acknowledgments to make the conversation feel like active listening (override in KB if desired)
    FLOW_ACK_PURPOSE: "הבנתי.",
    FLOW_ACK_DATE: "מעולה.",
    FLOW_ACK_PARTICIPANTS: "סבבה.",
    FLOW_ACK_GENERAL: "סבבה.",
    // Optional: extra confirmation line after the date (matches the PDF style).
    FLOW_DATE_CONFIRM: "מצוין. אנחנו נבדוק את התאריך ביומן ונציגה תחזור אלייך בהקדם עם כל הפרטים.",
    // Optional: what to do when user asks about time/when; keep it short and promise a callback.
    FLOW_WHEN_UNKNOWN: "כרגע אין לי את כל השעות המדויקות. נציגה תחזור אלייך לתיאום עם כל הפרטים.",
    // Names policy (rabbinical/instructor names):
    // - Never list names proactively.
    // - If asked about a specific name and it's in the approved list -> confirm.
    // - Otherwise -> say we'll check and call back.
    FLOW_NAME_CONFIRMED: "כן, השם הזה מוכר אצלנו. נבדוק מול היומן ויחזרו אלייך עם כל הפרטים.",
    FLOW_NAME_UNKNOWN: "אקח את השם ואבדוק מול היומן, ויחזרו אלייך."
  };
  return { ...defaults, ...flow };
}

function extractHeNumber(text) {
  const s = normalizeIntentText(text);
  if (!s) return null;
  // Digits
  const dm = s.match(/\b(\d{1,3})\b/);
  if (dm) {
    const n = Number(dm[1]);
    if (Number.isFinite(n)) return n;
  }
  // Hebrew words (minimal set for this campaign)
  const map = new Map([
    ["אחת", 1],
    ["אחד", 1],
    ["שתיים", 2],
    ["שניים", 2],
    ["שתי", 2],
    ["שלוש", 3],
    ["ארבע", 4],
    ["חמש", 5],
    ["שש", 6],
    ["שבע", 7],
    ["שמונה", 8],
    ["תשע", 9],
    ["עשר", 10],
    ["עשרה", 10],
    ["אחת עשרה", 11],
    ["שתים עשרה", 12],
    ["שתים-עשרה", 12],
    ["שלוש עשרה", 13],
    ["ארבע עשרה", 14],
    ["חמש עשרה", 15],
    ["חמשעשרה", 15],
    ["חמש-עשרה", 15],
    ["חמש עשר", 15]
  ]);
  // Try multi-word matches first
  for (const [k, v] of map.entries()) {
    if (k.includes(" ") && s.includes(k)) return v;
  }
  // Single-word
  for (const [k, v] of map.entries()) {
    if (!k.includes(" ") && s.split(" ").includes(k)) return v;
  }
  return null;
}

function selectRelevantKnowledge({ knowledgeBase, query, maxChars = 1800, maxChunks = 8 }) {
  const kb = String(knowledgeBase || "").trim();
  const q = String(query || "").trim();
  if (!kb) return "";
  if (!q) return kb.slice(0, maxChars);

  // split knowledge into chunks by blank lines
  const chunks = kb
    .split(/\n\s*\n+/g)
    .map((c) => c.trim())
    .filter(Boolean);

  const qTokens = new Set(tokenizeHe(q));
  if (qTokens.size === 0) return kb.slice(0, maxChars);

  const scored = chunks
    .map((chunk) => {
      const cTokens = tokenizeHe(chunk);
      let score = 0;
      for (const t of cTokens) if (qTokens.has(t)) score++;
      // small boost if chunk contains explicit Q/A arrows
      if (chunk.includes("→")) score += 1;
      return { chunk, score };
    })
    .filter((x) => x.score > 0)
    .sort((a, b) => b.score - a.score)
    .slice(0, maxChunks)
    .map((x) => x.chunk);

  const picked = scored.length ? scored.join("\n\n") : chunks.slice(0, maxChunks).join("\n\n");
  return picked.length > maxChars ? picked.slice(0, maxChars) : picked;
}

function pickPersona(contact) {
  if (contact?.gender === "female") return "female";
  return "male";
}

function getConversationPhone(req) {
  // Outbound (calls.create): To=recipient, From=our Twilio number
  // Inbound (we call Twilio number): From=caller, To=our Twilio number
  const direction = String(getParam(req, "Direction") || "").toLowerCase();
  const to = normalizePhone(getParam(req, "To"));
  const from = normalizePhone(getParam(req, "From"));
  return direction.startsWith("inbound") ? from : to;
}

function parseGender(raw) {
  const v = String(raw ?? "").trim().toLowerCase();
  if (!v) return null;
  if (v === "male" || v === "m" || v === "זכר") return "male";
  if (v === "female" || v === "f" || v === "נקבה") return "female";
  return null;
}

function normalizeShortPhrase(raw, { fallback, maxChars = 24, maxWords = 3 } = {}) {
  const fb = String(fallback || "").trim();
  let s = String(raw ?? "").trim();
  if (!s) return fb;
  // collapse whitespace + remove newlines (admin UI sometimes pastes multi-line text by mistake)
  s = s.replace(/\s+/g, " ").trim();
  // If this contains sentence punctuation or looks like a full sentence/question, reject.
  if (/[?!。！？]/.test(s) || s.includes("\n")) return fb;
  // If user pasted a full script, it will typically be long and multi-word.
  if (s.length > maxChars) return fb;
  const words = s.split(" ").filter(Boolean);
  if (words.length > maxWords) return fb;
  return s || fb;
}

function settingsSnapshot() {
  const knowledgeBase = getSetting(db, "knowledgeBase", "");

  // legacy keys (kept for backward-compat)
  const openingScript = getSetting(db, "openingScript", "");
  const middleScript = getSetting(db, "middleScript", "");
  const closingScript = getSetting(db, "closingScript", "");

  // persona-specific keys (preferred)
  const openingScriptMale = getSetting(db, "openingScriptMale", "");
  const openingScriptFemale = getSetting(db, "openingScriptFemale", "");
  const middleScriptMale = getSetting(db, "middleScriptMale", "");
  const middleScriptFemale = getSetting(db, "middleScriptFemale", "");
  const closingScriptMale = getSetting(db, "closingScriptMale", "");
  const closingScriptFemale = getSetting(db, "closingScriptFemale", "");

  const autoDialEnabled = !!getSetting(db, "autoDialEnabled", false);
  const autoDialBatchSize = Number(getSetting(db, "autoDialBatchSize", 5));
  const autoDialIntervalSeconds = Number(getSetting(db, "autoDialIntervalSeconds", 30));

  // White-label phrases MUST be short (examples: "לצוות", "מהצוות", "למוקד", "מהמוקד").
  // If someone pastes the whole greeting here, it creates a repetition loop like:
  // "רוצה שיחזרו אליך <greeting> עם פרטים מסודרים?"
  const handoffToPhrase = normalizeShortPhrase(getSetting(db, "handoffToPhrase", "לצוות"), {
    fallback: "לצוות",
    maxChars: 24,
    maxWords: 3
  });
  const handoffFromPhrase = normalizeShortPhrase(getSetting(db, "handoffFromPhrase", "מהצוות"), {
    fallback: "מהצוות",
    maxChars: 24,
    maxWords: 3
  });

  const campaignMode = String(getSetting(db, "campaignMode", "handoff") || "handoff").trim() || "handoff";
  const femaleOnly = !!getSetting(db, "femaleOnly", false);
  const minParticipants = Math.max(1, Math.min(200, Number(getSetting(db, "minParticipants", 15)) || 15));
  const cooldownMonths = Math.max(0, Math.min(60, Number(getSetting(db, "cooldownMonths", 6)) || 6));

  return {
    knowledgeBase,
    openingScript,
    middleScript,
    closingScript,
    openingScriptMale,
    openingScriptFemale,
    middleScriptMale,
    middleScriptFemale,
    closingScriptMale,
    closingScriptFemale,
    autoDialEnabled,
    autoDialBatchSize,
    autoDialIntervalSeconds,
    handoffToPhrase,
    handoffFromPhrase,
    campaignMode,
    femaleOnly,
    minParticipants,
    cooldownMonths
  };
}

function handoffQuestionTextForPersona(persona) {
  const { handoffFromPhrase } = settingsSnapshot();
  const from = String(handoffFromPhrase || "מהצוות").trim() || "מהצוות";
  return persona === "female"
    ? `סבבה. רוצה שיחזרו אלייך ${from} עם פרטים מסודרים?`
    : `סבבה. רוצה שיחזרו אליך ${from} עם פרטים מסודרים?`;
}

function handoffConfirmCloseText({ persona }) {
  const { handoffToPhrase, handoffFromPhrase } = settingsSnapshot();
  const toPhrase = String(handoffToPhrase || "לצוות").trim() || "לצוות";
  const fromPhrase = String(handoffFromPhrase || "מהצוות").trim() || "מהצוות";
  return persona === "female"
    ? `מעולה. אין בעיה — העברתי ${toPhrase} ויחזרו אלייך ${fromPhrase} בהקדם. יום טוב ובשורות טובות.`
    : `מעולה. אין בעיה — העברתי ${toPhrase} ויחזרו אליך ${fromPhrase} בהקדם. יום טוב ובשורות טובות.`;
}

async function elevenlabsTtsToFile({ text, persona }) {
  if (!ELEVENLABS_API_KEY) return null;
  const voiceId =
    (persona === "female" ? ELEVENLABS_VOICE_FEMALE : ELEVENLABS_VOICE_MALE) || ELEVENLABS_VOICE_ID;
  if (!voiceId) return null;

  // IMPORTANT: must match computeTtsCacheKey() so polling + static /tts lookup work.
  const key = computeTtsCacheKey({ provider: "elevenlabs", text, persona });
  const filename = `${key}.mp3`;
  const outPath = path.join(ttsDir, filename);
  // ה-URL הציבורי נבנה מה-request בפועל, לכן כאן נחזיר path יחסי.
  if (fs.existsSync(outPath)) return `/tts/${filename}`;

  const url = `https://api.elevenlabs.io/v1/text-to-speech/${voiceId}?output_format=${encodeURIComponent(
    ELEVENLABS_OUTPUT_FORMAT || "mp3_44100_128"
  )}`;
  const res = await fetch(url, {
    method: "POST",
    headers: {
      "xi-api-key": ELEVENLABS_API_KEY,
      "Content-Type": "application/json",
      Accept: "audio/mpeg"
    },
    body: JSON.stringify({
      text,
      model_id: ELEVENLABS_MODEL_ID,
      ...(ELEVENLABS_LANGUAGE_CODE ? { language_code: ELEVENLABS_LANGUAGE_CODE } : {}),
      voice_settings: { stability: Math.max(0, Math.min(1, ELEVENLABS_STABILITY)) }
    })
  });

  if (!res.ok) {
    const errText = await res.text().catch(() => "");
    console.warn("ElevenLabs TTS failed:", res.status, errText);
    return null;
  }

  const buf = Buffer.from(await res.arrayBuffer());
  fs.writeFileSync(outPath, buf);
  return `/tts/${filename}`;
}

async function openaiTtsToFile({ text, persona }) {
  if (!openai) return null;

  const voice = persona === "female" ? OPENAI_TTS_VOICE_FEMALE : OPENAI_TTS_VOICE_MALE;
  const key = crypto
    .createHash("sha256")
    .update(`openai::${OPENAI_TTS_MODEL}::${voice}::${text}`)
    .digest("hex");
  const filename = `${key}.mp3`;
  const outPath = path.join(ttsDir, filename);
  if (fs.existsSync(outPath)) return `/tts/${filename}`;

  const resp = await openai.audio.speech.create({
    model: OPENAI_TTS_MODEL,
    voice,
    input: text,
    format: "mp3"
  });

  const buf = Buffer.from(await resp.arrayBuffer());
  fs.writeFileSync(outPath, buf);
  return `/tts/${filename}`;
}

function computeTtsCacheKey({ provider, text, persona }) {
  const p = String(provider || "").toLowerCase();
  if (p === "elevenlabs") {
    const voiceId =
      (persona === "female" ? ELEVENLABS_VOICE_FEMALE : ELEVENLABS_VOICE_MALE) || ELEVENLABS_VOICE_ID;
    // Include model + stability so changing env doesn't keep serving stale cached audio.
    const settingsSig = [ELEVENLABS_MODEL_ID, ELEVENLABS_OUTPUT_FORMAT || "", ELEVENLABS_LANGUAGE_CODE || "", ELEVENLABS_STABILITY].join("|");
    return crypto.createHash("sha256").update(`${voiceId}::${settingsSig}::${text}`).digest("hex");
  }
  // openai
  const voice = persona === "female" ? OPENAI_TTS_VOICE_FEMALE : OPENAI_TTS_VOICE_MALE;
  return crypto
    .createHash("sha256")
    .update(`openai::${OPENAI_TTS_MODEL}::${voice}::${text}`)
    .digest("hex");
}

function findCachedAudioByKey(key) {
  if (!key) return null;
  const mp3 = path.join(ttsDir, `${key}.mp3`);
  if (fs.existsSync(mp3)) return { rel: `/tts/${key}.mp3`, ext: "mp3" };
  const wav = path.join(ttsDir, `${key}.wav`);
  if (fs.existsSync(wav)) return { rel: `/tts/${key}.wav`, ext: "wav" };
  return null;
}

async function kickoffTtsGeneration({ provider, text, persona }) {
  const p = String(provider || "").toLowerCase();
  try {
    if (p === "elevenlabs") {
      await elevenlabsTtsToFile({ text, persona });
      return;
    }
    await openaiTtsToFile({ text, persona });
  } catch {}
}

async function ttsToPath({ text, persona }) {
  const started = Date.now();
  const debug = (provider, result) => {
    if (!DEBUG_TTS) return;
    const ms = Date.now() - started;
    console.log(`[tts] provider=${provider} persona=${persona ?? "n/a"} ms=${ms} result=${result ?? "null"}`);
  };
  // Only ElevenLabs in this build (keep OpenAI TTS as a last-resort safety fallback).
  const out = await elevenlabsTtsToFile({ text, persona });
  debug("elevenlabs", out);
  if (out) return out;
  const fb = await openaiTtsToFile({ text, persona });
  debug("openai(fallback)", fb);
  return fb;
}

function getPublicBaseUrl(req) {
  // עובד מאחורי reverse proxies/tunnels (cloudflared/ngrok) וגם בלוקאלי.
  const xfProto = req.headers["x-forwarded-proto"];
  const xfHost = req.headers["x-forwarded-host"];
  const host = (Array.isArray(xfHost) ? xfHost[0] : xfHost) || req.headers.host;
  const proto =
    (Array.isArray(xfProto) ? xfProto[0] : xfProto) ||
    (req.secure ? "https" : "http");
  if (!host) return "";
  return `${proto}://${host}`;
}

function toAbsoluteUrl(req, pathname) {
  const base = getPublicBaseUrl(req);
  if (!base) return pathname;
  return `${base}${pathname}`;
}

app.get("/health", (req, res) =>
  res.json({
    ok: true,
    version: serverVersion(),
    realtimeMode: REALTIME_MODE,
    msMode: MS_MODE,
    crMode: CR_MODE
  })
);

// Admin mini-site
app.get("/admin", (req, res) => {
  res.type("text/html").send(renderAdminPage());
});

app.get("/api/admin/state", (req, res) => {
  const s = settingsSnapshot();
  res.json({ ...s, updatedAt: new Date().toISOString() });
});

app.post("/api/admin/settings", (req, res) => {
  const {
    knowledgeBase = "",
    // legacy
    openingScript = "",
    middleScript = "",
    closingScript = "",
    // persona-specific
    openingScriptMale = "",
    openingScriptFemale = "",
    middleScriptMale = "",
    middleScriptFemale = "",
    closingScriptMale = "",
    closingScriptFemale = "",
    handoffToPhrase = "",
    handoffFromPhrase = "",
    campaignMode = undefined,
    femaleOnly = undefined,
    minParticipants = undefined,
    cooldownMonths = undefined
  } = req.body || {};
  setSetting(db, "knowledgeBase", String(knowledgeBase));
  setSetting(db, "openingScript", String(openingScript));
  setSetting(db, "middleScript", String(middleScript));
  setSetting(db, "closingScript", String(closingScript));
  setSetting(db, "openingScriptMale", String(openingScriptMale));
  setSetting(db, "openingScriptFemale", String(openingScriptFemale));
  setSetting(db, "middleScriptMale", String(middleScriptMale));
  setSetting(db, "middleScriptFemale", String(middleScriptFemale));
  setSetting(db, "closingScriptMale", String(closingScriptMale));
  setSetting(db, "closingScriptFemale", String(closingScriptFemale));
  if (handoffToPhrase != null) setSetting(db, "handoffToPhrase", String(handoffToPhrase));
  if (handoffFromPhrase != null) setSetting(db, "handoffFromPhrase", String(handoffFromPhrase));
  if (campaignMode != null) setSetting(db, "campaignMode", String(campaignMode));
  if (femaleOnly != null) setSetting(db, "femaleOnly", !!femaleOnly);
  if (minParticipants != null) setSetting(db, "minParticipants", Math.max(1, Math.min(200, Number(minParticipants) || 15)));
  if (cooldownMonths != null) setSetting(db, "cooldownMonths", Math.max(0, Math.min(60, Number(cooldownMonths) || 6)));
  res.json({ ok: true });
});

// Admin: view recent calls + transcripts (from call_messages).
app.get("/api/calls/recent", (req, res) => {
  const limit = Math.max(1, Math.min(50, Number(req.query.limit || 15)));
  const calls = db
    .prepare(
      `
      SELECT
        c.call_sid AS callSid,
        c.phone AS phone,
        c.persona AS persona,
        c.turn_count AS turnCount,
        c.started_at AS startedAt,
        c.updated_at AS updatedAt,
        COALESCE(ct.first_name, '') AS firstName
      FROM calls c
      LEFT JOIN contacts ct ON ct.phone = c.phone
      ORDER BY c.updated_at DESC
      LIMIT ?
    `
    )
    .all(limit);
  res.json({ calls });
});

app.get("/api/calls/messages", (req, res) => {
  const callSid = String(req.query.callSid || "").trim();
  if (!callSid) return res.status(400).json({ error: "missing callSid" });
  const limit = Math.max(1, Math.min(200, Number(req.query.limit || 80)));
  const messages = db
    .prepare(
      `
      SELECT role, content, created_at AS createdAt
      FROM call_messages
      WHERE call_sid = ?
      ORDER BY id ASC
      LIMIT ?
    `
    )
    .all(callSid, limit);
  res.json({ callSid, messages });
});

app.post("/api/admin/dialer", (req, res) => {
  const {
    autoDialEnabled = false,
    autoDialBatchSize = 5,
    autoDialIntervalSeconds = 30
  } = req.body || {};
  setSetting(db, "autoDialEnabled", !!autoDialEnabled);
  setSetting(db, "autoDialBatchSize", Math.max(1, Math.min(50, Number(autoDialBatchSize) || 5)));
  setSetting(db, "autoDialIntervalSeconds", Math.max(5, Math.min(3600, Number(autoDialIntervalSeconds) || 30)));
  res.json({ ok: true });
});

// Import contacts: XLSX upload
app.post("/api/contacts/import-xlsx", upload.single("file"), (req, res) => {
  if (!req.file) return res.status(400).json({ error: "missing file" });
  const filename = String(req.file.originalname || "").toLowerCase();
  const isCsv = filename.endsWith(".csv");

  let imported = 0;
  if (isCsv) {
    const csv = req.file.buffer.toString("utf8");
    const records = csvParse(csv, { columns: true, skip_empty_lines: true });
    for (const row of records) {
      const phone = String(row.phone || row.Phone || row.PHONE || "").trim();
      if (!phone) continue;
      const gender = parseGender(row.gender || row.Gender || "");
      const firstName = String(row.first_name || row.firstName || row.name || row.Name || "").trim() || null;
      upsertContact(db, { phone, gender, firstName });
      imported++;
    }
    res.json({ ok: true, imported, type: "csv" });
    return;
  }

  // NOTE: We intentionally do NOT use the `xlsx` package due to unresolved security advisories.
  // ExcelJS parses XLSX safely enough for our use-case (simple header row + scalar values).
  (async () => {
    function normalizeCellValue(v) {
      if (v == null) return "";
      if (v instanceof Date) return v.toISOString();
      if (typeof v === "object") {
        // ExcelJS can return rich objects for formulas, hyperlinks, etc.
        if ("text" in v && typeof v.text === "string") return v.text;
        if ("richText" in v && Array.isArray(v.richText)) return v.richText.map((p) => p?.text || "").join("");
        if ("result" in v) return normalizeCellValue(v.result);
        if ("hyperlink" in v && typeof v.hyperlink === "string") return v.hyperlink;
        if ("formula" in v && "result" in v) return normalizeCellValue(v.result);
      }
      return String(v);
    }

    const wb = new ExcelJS.Workbook();
    await wb.xlsx.load(req.file.buffer);
    const ws = wb.worksheets[0];
    if (!ws) return res.status(400).json({ error: "no sheets found" });

    const headerRow = ws.getRow(1);
    const headers = (headerRow?.values || []).slice(1).map((h) => String(h || "").trim());
    if (!headers.some(Boolean)) return res.status(400).json({ error: "missing header row" });

    for (let i = 2; i <= ws.rowCount; i++) {
      const row = ws.getRow(i);
      if (!row || !row.hasValues) continue;
      const r = {};
      for (let c = 0; c < headers.length; c++) {
        const key = headers[c];
        if (!key) continue;
        r[key] = normalizeCellValue(row.getCell(c + 1).value);
      }

    const phone = String(r.phone || r.Phone || r.PHONE || "").trim();
    if (!phone) continue;
    const gender = parseGender(r.gender || r.Gender || r.GENDER || "");
    const firstName = String(r.first_name || r.firstName || r.name || r.Name || "").trim() || null;
    upsertContact(db, { phone, gender, firstName });
    imported++;
  }

  res.json({ ok: true, imported, type: "xlsx" });
  })().catch((err) => {
    console.error("[import-xlsx] failed:", err);
    res.status(400).json({ error: "failed to parse xlsx" });
  });
});

function toGoogleCsvUrl(url) {
  const u = String(url || "").trim();
  if (!u) return "";
  if (u.includes("export?format=csv")) return u;
  const m = u.match(/\/spreadsheets\/d\/([a-zA-Z0-9-_]+)/);
  if (!m) return u;
  const sheetId = m[1];
  return `https://docs.google.com/spreadsheets/d/${sheetId}/export?format=csv`;
}

// Import contacts: Google Sheets public CSV
app.post("/api/contacts/import-sheet", async (req, res) => {
  const url = toGoogleCsvUrl(req.body?.url);
  if (!url) return res.status(400).json({ error: "missing url" });

  const r = await fetch(url);
  if (!r.ok) return res.status(400).json({ error: `failed to fetch sheet: ${r.status}` });
  const csv = await r.text();

  const records = csvParse(csv, { columns: true, skip_empty_lines: true });
  let imported = 0;
  for (const row of records) {
    const phone = String(row.phone || row.Phone || row.PHONE || "").trim();
    if (!phone) continue;
    const gender = parseGender(row.gender || row.Gender || "");
    const firstName = String(row.first_name || row.firstName || row.name || row.Name || "").trim() || null;
    upsertContact(db, { phone, gender, firstName });
    imported++;
  }

  res.json({ ok: true, imported });
});

app.get("/api/contacts/stats", (req, res) => {
  const total = db.prepare(`SELECT COUNT(1) AS c FROM contacts`).get()?.c ?? 0;
  const newCount = db.prepare(`SELECT COUNT(1) AS c FROM contacts WHERE dial_status = 'new' AND do_not_call = 0`).get()?.c ?? 0;
  const failed = db.prepare(`SELECT COUNT(1) AS c FROM contacts WHERE dial_status = 'failed' AND do_not_call = 0`).get()?.c ?? 0;
  const dnc = db.prepare(`SELECT COUNT(1) AS c FROM contacts WHERE do_not_call = 1`).get()?.c ?? 0;
  res.json({ total, new: newCount, failed, dnc });
});

app.get("/api/contacts/list", (req, res) => {
  const limit = Math.max(1, Math.min(1000, Number(req.query.limit || 200)));
  const offset = Math.max(0, Number(req.query.offset || 0));
  const rows = db
    .prepare(
      `SELECT id, first_name, phone, gender, do_not_call, dial_status, dial_attempts, last_dial_at, last_dial_error
       FROM contacts
       ORDER BY id DESC
       LIMIT ? OFFSET ?`
    )
    .all(limit, offset);
  res.json({ rows, limit, offset });
});

// Leads (waiting/not_interested)
app.get("/api/leads/list", (req, res) => {
  const status = String(req.query.status || "all");
  const limit = Math.max(1, Math.min(1000, Number(req.query.limit || 200)));
  const offset = Math.max(0, Number(req.query.offset || 0));
  const out = listLeads(db, { status, limit, offset });
  res.json(out);
});

app.post("/api/leads/delete", (req, res) => {
  const rawPhone = String(req.body?.phone ?? "").trim();
  if (!rawPhone) return res.status(400).json({ error: "missing phone" });
  const out = deleteLead(db, rawPhone);
  res.json({ ok: true, ...out });
});

// Add single contact (used by admin "test dial" modal)
app.post("/api/contacts/add", (req, res) => {
  const firstName = String(req.body?.first_name ?? req.body?.firstName ?? req.body?.name ?? "").trim();
  const rawPhone = String(req.body?.phone ?? "").trim();
  const gender = parseGender(req.body?.gender);

  const phone = normalizePhoneE164IL(rawPhone);
  if (!phone) return res.status(400).json({ error: "מספר לא תקין. השתמש ב-05XXXXXXXX או +972..." });

  upsertContact(db, { phone, gender, firstName: firstName || null });
  const c = getContactByPhone(db, phone);
  res.json({ ok: true, contact: c });
});

// Remove contact by phone (admin delete)
app.post("/api/contacts/remove", (req, res) => {
  const rawPhone = String(req.body?.phone ?? "").trim();
  const phone = normalizePhoneE164IL(rawPhone);
  if (!phone) return res.status(400).json({ error: "מספר לא תקין" });

  const info = db.prepare(`DELETE FROM contacts WHERE phone = ?`).run(phone);
  res.json({ ok: true, deleted: Number(info?.changes || 0) });
});

function ensureVoicePath(url) {
  if (!url) return "";
  const u = String(url).trim().replace(/\/$/, "");
  if (!u.includes("/twilio/voice")) return `${u}/twilio/voice`;
  return u;
}

// Dial a specific number (admin "phone icon") — similar to `npm run dial`
app.post("/api/contacts/dial", async (req, res) => {
  if (!process.env.TWILIO_ACCOUNT_SID || !process.env.TWILIO_AUTH_TOKEN || !process.env.TWILIO_FROM_NUMBER) {
    return res
      .status(400)
      .json({ error: "חסר Twilio env: TWILIO_ACCOUNT_SID / TWILIO_AUTH_TOKEN / TWILIO_FROM_NUMBER" });
  }
  const callUrl = ensureVoicePath(String(process.env.VOICE_WEBHOOK_URL || "").trim());
  if (!callUrl) return res.status(400).json({ error: "חסר VOICE_WEBHOOK_URL (צריך URL ציבורי של ה-tunnel)" });

  const rawPhone = String(req.body?.phone ?? "").trim();
  const phone = normalizePhoneE164IL(rawPhone);
  if (!phone) return res.status(400).json({ error: "מספר לא תקין" });

  const contact = getContactByPhone(db, phone);
  if (contact?.do_not_call) return res.status(400).json({ error: "המספר מסומן DNC (לא להתקשר)" });

  const client = twilio(process.env.TWILIO_ACCOUNT_SID, process.env.TWILIO_AUTH_TOKEN);
  try {
    queueContactForDial(db, phone);
    const out = await client.calls.create({
      to: phone,
      from: process.env.TWILIO_FROM_NUMBER,
      url: callUrl,
      method: "POST"
    });
    res.json({ ok: true, callSid: out?.sid || null, to: phone, url: callUrl });
  } catch (e) {
    markDialResult(db, phone, { status: "failed", error: String(e?.message || e) });
    res.status(500).json({ error: String(e?.message || e) });
  }
});

async function handleTwilioVoice(req, res) {
  const callSid = String(getParam(req, "CallSid") || "");
  const phone = getConversationPhone(req);

  const contact = getContactByPhone(db, phone);
  if (contact?.do_not_call) {
    await respondWithPlayAndMaybeHangup(req, res, { text: "בסדר גמור. יום טוב.", persona: "female", hangup: true });
    return;
  }

  const persona = pickPersona(contact);
  createOrGetCall(db, { callSid, phone, persona });

  const { openingScript, openingScriptMale, openingScriptFemale } = settingsSnapshot();
  // אם לא הוגדר פתיח, ניפול לברירת מחדל טכנית (כדי שלא יהיה שקט).
  const personaOpening =
    persona === "female" ? (openingScriptFemale || openingScript) : (openingScriptMale || openingScript);
  const greeting = sanitizeSayText(String(personaOpening || "").trim() || buildGreeting({ persona }));

  // קריטי: לשמור את הפתיח בהיסטוריה כדי שהמודל לא יחזור עליו אחרי שהלקוח אומר "כן יש לי דקה".
  // עושים את זה פעם אחת לכל שיחה (idempotent).
  try {
    const c = db.prepare(`SELECT COUNT(1) AS c FROM call_messages WHERE call_sid = ?`).get(callSid)?.c ?? 0;
    if (Number(c) === 0) {
      addMessage(db, { callSid, role: "assistant", content: greeting });
    }
  } catch {}

  // חשוב: לא לחכות ל-TTS בתוך webhook של Twilio.
  // אבל גם חשוב שהשיחה *תמיד* תתחיל בפתיח (ולא רק beep+הקלטה).
  // לכן נשתמש באותו polling endpoint (/twilio/play) כמו בתשובות באמצע שיחה.
  const provider = TTS_PROVIDER || "openai";
  const key = computeTtsCacheKey({ provider, text: greeting, persona: AGENT_VOICE_PERSONA });
  const cached = findCachedAudioByKey(key);
  if (!cached) {
    kickoffTtsGeneration({ provider, text: greeting, persona: AGENT_VOICE_PERSONA }).catch(() => {});
    const redirectUrl = toAbsoluteUrl(
      req,
      `/twilio/play?callSid=${encodeURIComponent(callSid)}&k=${encodeURIComponent(key)}&a=0`
    );
    const response = new twilio.twiml.VoiceResponse();
    response.pause({ length: TTS_POLL_WAIT_SECONDS });
    response.redirect({ method: "POST" }, redirectUrl);
    res.type("text/xml").send(response.toString());
    return;
  }

  // אחרי שהפתיח הושמע, לתת קצת זמן לענות (מינימום 2 שניות) כדי שלא "יפול" ישר ל-"לא שמעתי".
  const xml = buildRecordTwiML({
    sayText: null,
    playUrl: toAbsoluteUrl(req, cached.rel),
    actionUrl: recordActionUrl(req, 0),
    playBeep: false,
    maxLengthSeconds: RECORD_MAX_LENGTH_SECONDS,
    timeoutSeconds: recordTimeoutSeconds()
  });

  res.type("text/xml").send(xml);
}

// נקודת התחלה לשיחה (משמשת גם ב-url של calls.create)
app.all("/twilio/voice", async (req, res) => {
  try {
    if (MS_MODE) {
      const callSid = String(getParam(req, "CallSid") || "");
      const phone = getConversationPhone(req);
      const contact = getContactByPhone(db, phone);
      if (contact?.do_not_call) {
        await respondWithPlayAndMaybeHangup(req, res, { text: "בסדר גמור. יום טוב.", persona: "female", hangup: true });
        return;
      }

      const persona = pickPersona(contact);
      createOrGetCall(db, { callSid, phone, persona });

      const { openingScript, openingScriptMale, openingScriptFemale } = settingsSnapshot();
      const personaOpening =
        persona === "female" ? (openingScriptFemale || openingScript) : (openingScriptMale || openingScript);
      const greeting = normalizeGreetingForLatency({
        greeting: String(personaOpening || "").trim() || buildGreeting({ persona }),
        persona
      });

      // Save greeting once so the LLM won't repeat it.
      try {
        const c = db.prepare(`SELECT COUNT(1) AS c FROM call_messages WHERE call_sid = ?`).get(callSid)?.c ?? 0;
        if (Number(c) === 0) addMessage(db, { callSid, role: "assistant", content: greeting });
      } catch {}

      const httpBase = getPublicBaseUrl(req);
      const wsBase = toWsUrlFromHttpBase(httpBase);
      const wsUrl = `${wsBase}/twilio/mediastream`;

      const xml = buildMediaStreamTwiML({
        wsUrl,
        customParameters: {
          callSid,
          phone,
          persona,
          greeting
        }
      });

      if (MS_DEBUG) {
        console.log("[ms] twiml served", { callSid, wsUrl });
      }
      res.type("text/xml").send(xml);
      return;
    }

    if (CR_ENABLED) {
      const callSid = String(getParam(req, "CallSid") || "");
      const phone = getConversationPhone(req);
      const contact = getContactByPhone(db, phone);
      if (contact?.do_not_call) {
        await respondWithPlayAndMaybeHangup(req, res, { text: "בסדר גמור. יום טוב.", persona: "female", hangup: true });
        return;
      }

      const persona = pickPersona(contact);
      createOrGetCall(db, { callSid, phone, persona });

      const { openingScript, openingScriptMale, openingScriptFemale } = settingsSnapshot();
      const personaOpening =
        persona === "female" ? (openingScriptFemale || openingScript) : (openingScriptMale || openingScript);
      const greeting = sanitizeSayText(String(personaOpening || "").trim() || buildGreeting({ persona }));

      // Save greeting once so the LLM won't repeat it.
      try {
        const c = db.prepare(`SELECT COUNT(1) AS c FROM call_messages WHERE call_sid = ?`).get(callSid)?.c ?? 0;
        if (Number(c) === 0) addMessage(db, { callSid, role: "assistant", content: greeting });
      } catch {}

      const httpBase = getPublicBaseUrl(req);
      const wsBase = toWsUrlFromHttpBase(httpBase);
      const wsUrl = `${wsBase}/twilio/conversationrelay`;

      const {
        languageAttr,
        ttsLanguageAttr,
        transcriptionLanguage,
        transcriptionProviderAttr,
        speechModelAttr
      } = normalizeConversationRelaySettings();

      const xml = buildConversationRelayTwiML({
        wsUrl,
        language: languageAttr,
        ttsLanguage: ttsLanguageAttr,
        ttsProvider: CR_TTS_PROVIDER,
        voice: CR_VOICE,
        transcriptionLanguage,
        transcriptionProvider: transcriptionProviderAttr,
        speechModel: speechModelAttr,
        elevenlabsTextNormalization:
          String(CR_TTS_PROVIDER).toLowerCase() === "elevenlabs" ? CR_ELEVENLABS_TEXT_NORMALIZATION : "",
        // Avoid TwiML welcomeGreeting to prevent "silent greeting" cases.
        // We'll send the greeting as the first "text" token from our WebSocket handler on setup.
        welcomeGreeting: "",
        welcomeGreetingInterruptible: "",
        interruptible: CR_INTERRUPTIBLE,
        interruptSensitivity: CR_INTERRUPT_SENSITIVITY,
        debug: CR_DEBUG,
        customParameters: {
          callSid,
          phone,
          persona,
          greeting
        },
        // IMPORTANT: Do NOT add <Language code="he-IL"...> for ElevenLabs.
        // Some accounts get 64101: block_elevenlabs/he-IL/<voiceId> when that mapping is present.
        languageElements: []
      });

      // Breadcrumb for correlating Twilio /twilio/voice request with WS logs.
      crLogAlways("twiml served", {
        callSid,
        wsUrl,
        ttsProvider: CR_TTS_PROVIDER,
        voice: CR_VOICE,
        transcriptionLanguage,
        transcriptionProvider: transcriptionProviderAttr,
        speechModel: speechModelAttr
      });

      res.type("text/xml").send(xml);
      return;
    }

    await handleTwilioVoice(req, res);
  } catch (err) {
    console.error(err);
    await respondWithPlayAndMaybeHangup(req, res, {
      text: "סליחה, הייתה תקלה קטנה. יום טוב.",
      persona: "female",
      hangup: true
    });
  }
});

app.all("/twilio/record", async (req, res) => {
  try {
    const callSid = String(getParam(req, "CallSid") || "");
    const phone = getConversationPhone(req);
    const recordingUrl = String(getParam(req, "RecordingUrl") || "").trim();
    const retry = clampInt(req.query?.r, { min: 0, max: 10, fallback: 0 });

    const contact = getContactByPhone(db, phone);
    if (contact?.do_not_call) {
      await respondWithPlayAndMaybeHangup(req, res, { text: "בסדר גמור. יום טוב.", persona: "female", hangup: true });
      return;
    }

    const persona = pickPersona(contact);
    const callRow = createOrGetCall(db, { callSid, phone, persona });

    if (!recordingUrl) {
      // Don't hang up immediately if the user is still speaking / recording failed.
      if (retry < Math.max(0, NO_SPEECH_MAX_RETRIES)) {
      await respondWithPlayAndMaybeHangup(req, res, {
          text: handoffQuestionTextForPersona(persona),
        persona,
          hangup: false,
          retry: retry + 1
      });
        return;
      }
      await respondWithPlayAndMaybeHangup(req, res, { text: "סבבה, נסיים כאן. יום טוב ובשורות טובות.", persona, hangup: true });
      return;
    }

    if (!openai) {
      await respondWithPlayAndMaybeHangup(req, res, {
        text: "בשביל להמשיך את השיחה החכמה צריך להגדיר מפתח מערכת. תודה רבה ויום טוב.",
        persona,
        hangup: true
      });
      return;
    }

    // מורידים את ההקלטה מ-Twilio (דורש Basic Auth)
    if (!process.env.TWILIO_ACCOUNT_SID || !process.env.TWILIO_AUTH_TOKEN) {
      await respondWithPlayAndMaybeHangup(req, res, {
        text: "חסר חיבור טלפוניה במערכת. תודה רבה ויום טוב.",
        persona,
        hangup: true
      });
      return;
    }

    const downloadUrl = `${recordingUrl}.wav`;
    const tmpDir = path.resolve(DATA_DIR, "recordings");
    if (!fs.existsSync(tmpDir)) fs.mkdirSync(tmpDir, { recursive: true });
    const tmpPath = path.join(tmpDir, `${callSid}-${Date.now()}.wav`);

    const auth = Buffer.from(`${process.env.TWILIO_ACCOUNT_SID}:${process.env.TWILIO_AUTH_TOKEN}`).toString("base64");
    const recRes = await fetch(downloadUrl, {
      headers: { Authorization: `Basic ${auth}` }
    });
    if (!recRes.ok) {
      if (retry < Math.max(0, NO_SPEECH_MAX_RETRIES)) {
      await respondWithPlayAndMaybeHangup(req, res, {
          text: handoffQuestionTextForPersona(persona),
        persona,
          hangup: false,
          retry: retry + 1
      });
        return;
      }
      await respondWithPlayAndMaybeHangup(req, res, { text: "סבבה, נסיים כאן. יום טוב ובשורות טובות.", persona, hangup: true });
      return;
    }
    fs.writeFileSync(tmpPath, Buffer.from(await recRes.arrayBuffer()));

    // תמלול עברית
    const transcription = await openai.audio.transcriptions.create({
      model: OPENAI_STT_MODEL,
      file: fs.createReadStream(tmpPath),
      language: "he",
      prompt: OPENAI_STT_PROMPT
    });
    const speech = String(transcription.text || "").trim();

    // מנקים קובץ זמני (best-effort)
    try {
      fs.unlinkSync(tmpPath);
    } catch {}

    if (!speech) {
      if (retry < Math.max(0, NO_SPEECH_MAX_RETRIES)) {
      await respondWithPlayAndMaybeHangup(req, res, {
          text: handoffQuestionTextForPersona(persona),
        persona,
          hangup: false,
          retry: retry + 1
      });
        return;
      }
      await respondWithPlayAndMaybeHangup(req, res, { text: "סבבה, נסיים כאן. יום טוב ובשורות טובות.", persona, hangup: true });
      return;
    }

    if (detectOptOut(speech)) {
      markDoNotCall(db, phone);
      // Track as "not_interested" lead (they explicitly asked to stop).
      try {
        upsertLead(db, { phone, status: "not_interested", callSid, persona });
      } catch {}
      await respondWithPlayAndMaybeHangup(req, res, {
        text: "אין בעיה, הסרתי אותך. סליחה על ההפרעה ויום טוב.",
        persona,
        hangup: true
      });
      return;
    }

    addMessage(db, { callSid, role: "user", content: speech });
    const updated = incrementTurn(db, callSid);

    if (updated.turn_count >= MAX_TURNS) {
      await respondWithPlayAndMaybeHangup(req, res, { text: "תודה רבה על הזמן. יום טוב!", persona, hangup: true });
      return;
    }

    const { knowledgeBase } = settingsSnapshot();

    // "ידע עצום" עובד טוב רק אם מכניסים בכל פעם רק את החלק הרלוונטי למה שהלקוח אמר (RAG פשוט).
    const knowledgeForThisTurn = selectRelevantKnowledge({
      knowledgeBase,
      query: speech,
      maxChars: 1800,
      maxChunks: 8
    });

    // Optional deterministic FAQ rules. Default is OFF to keep everything driven by the KB + LLM.
    if (USE_FAQ_RULES) {
      const ruleReply = quickReplyByRules({ speech, persona });
      if (ruleReply?.text) {
        const safe = sanitizeSayText(ruleReply.text);
        addMessage(db, { callSid, role: "assistant", content: safe });

        const provider = TTS_PROVIDER || "openai";
        const key = computeTtsCacheKey({ provider, text: safe, persona: AGENT_VOICE_PERSONA });
        const cached = findCachedAudioByKey(key);
        if (!cached) {
          kickoffTtsGeneration({ provider, text: safe, persona: AGENT_VOICE_PERSONA }).catch(() => {});
          const redirectUrl = toAbsoluteUrl(
            req,
            `/twilio/play?callSid=${encodeURIComponent(callSid)}&k=${encodeURIComponent(key)}&a=0`
          );
          const response = new twilio.twiml.VoiceResponse();
          response.pause({ length: TTS_POLL_WAIT_SECONDS });
          response.redirect({ method: "POST" }, redirectUrl);
          res.type("text/xml").send(response.toString());
          return;
        }

        const xml = buildRecordTwiML({
          sayText: null,
          playUrl: toAbsoluteUrl(req, cached.rel),
          actionUrl: recordActionUrl(req, 0),
          playBeep: false,
          maxLengthSeconds: RECORD_MAX_LENGTH_SECONDS,
          timeoutSeconds: recordTimeoutSeconds()
        });
        res.type("text/xml").send(xml);
        return;
      }
    }

    // סגירה דטרמיניסטית: אם המשתמש אומר במפורש שהוא/היא רוצה/מעוניין/ת — נסגור נכון.
    // זה מונע: "אני רוצה לבוא" -> "הבנתי, יום טוב".
    const interested = detectInterested(speech) && !detectNotInterested(speech);
    const notInterested = detectNotInterested(speech) && !interested;
    if (interested || notInterested) {
      try {
        upsertLead(db, { phone, status: interested ? "waiting" : "not_interested", callSid, persona });
      } catch {}
      const picked =
        (interested
          ? handoffConfirmCloseText({ persona })
          : "הבנתי, אין בעיה. תודה על הזמן, יום טוב.");
      const safe = sanitizeSayText(picked);

      const provider = TTS_PROVIDER || "openai";
      const key = computeTtsCacheKey({ provider, text: safe, persona: AGENT_VOICE_PERSONA });
      const cached = findCachedAudioByKey(key);
      if (!cached) {
        kickoffTtsGeneration({ provider, text: safe, persona: AGENT_VOICE_PERSONA }).catch(() => {});
        const redirectUrl = toAbsoluteUrl(req, `/twilio/play_end?k=${encodeURIComponent(key)}&a=0`);
        const response = new twilio.twiml.VoiceResponse();
        response.pause({ length: TTS_POLL_WAIT_SECONDS });
        response.redirect({ method: "POST" }, redirectUrl);
        res.type("text/xml").send(response.toString());
        return;
      }

      const xml = buildPlayAndHangup({ playUrl: toAbsoluteUrl(req, cached.rel) });
      res.type("text/xml").send(xml);
      return;
    }

    const { handoffToPhrase, handoffFromPhrase } = settingsSnapshot();
    const system = buildSystemPrompt({
      persona,
      knowledgeBase: knowledgeForThisTurn,
      handoffToPhrase,
      handoffFromPhrase
    });
    const history = getMessages(db, callSid, { limit: 10 });
    const messages = [{ role: "system", content: system }, ...history];

    const llm = await createLlmText({ model: OPENAI_MODEL, messages, temperature: 0.3, maxTokens: 120 });
    const answer = sanitizeSayText(String(llm.rawText || "").trim() || "תודה רבה, יום טוב.");
    addMessage(db, { callSid, role: "assistant", content: answer });

    if (detectOptOut(answer)) {
      markDoNotCall(db, phone);
      await respondWithPlayAndMaybeHangup(req, res, { text: "אין בעיה, הסרתי אותך. יום טוב.", persona, hangup: true });
      return;
    }

    // לא נחכה ל-TTS בתוך webhook של Twilio. נשתמש בקאש אם קיים, אחרת נתחיל יצירה ברקע ונעשה poll.
    const key = computeTtsCacheKey({ provider: TTS_PROVIDER || "openai", text: answer, persona: AGENT_VOICE_PERSONA });
    const cached = findCachedAudioByKey(key);
    if (!cached) {
      kickoffTtsGeneration({ provider: TTS_PROVIDER || "openai", text: answer, persona: AGENT_VOICE_PERSONA }).catch(
        () => {}
      );
      const redirectUrl = toAbsoluteUrl(
        req,
        `/twilio/play?callSid=${encodeURIComponent(callSid)}&k=${encodeURIComponent(key)}&a=0`
      );
      const response = new twilio.twiml.VoiceResponse();
      response.pause({ length: TTS_POLL_WAIT_SECONDS });
      response.redirect({ method: "POST" }, redirectUrl);
      res.type("text/xml").send(response.toString());
      return;
    }

    const xml = buildRecordTwiML({
      sayText: null,
      playUrl: toAbsoluteUrl(req, cached.rel),
      actionUrl: recordActionUrl(req, 0),
      playBeep: false,
      maxLengthSeconds: RECORD_MAX_LENGTH_SECONDS,
      timeoutSeconds: recordTimeoutSeconds()
    });

    res.type("text/xml").send(xml);
  } catch (err) {
    console.error(err);
    await respondWithPlayAndMaybeHangup(req, res, {
      text: "סליחה, הייתה תקלה קטנה. יום טוב.",
      persona: "female",
      hangup: true
    });
  }
});

// Auto dialer (simple background loop)
let dialerTimer = null;
async function runDialerOnce() {
  const { autoDialEnabled, autoDialBatchSize } = settingsSnapshot();
  if (!autoDialEnabled) return;
  if (!process.env.TWILIO_ACCOUNT_SID || !process.env.TWILIO_AUTH_TOKEN || !process.env.TWILIO_FROM_NUMBER) return;
  const voiceBase = String(process.env.VOICE_WEBHOOK_URL || "").trim().replace(/\/$/, "");
  if (!voiceBase) return;
  const callUrl = `${voiceBase}/twilio/voice`;

  const client = twilio(process.env.TWILIO_ACCOUNT_SID, process.env.TWILIO_AUTH_TOKEN);
  const batch = fetchNextContactsToDial(db, autoDialBatchSize);
  for (const c of batch) {
    try {
      queueContactForDial(db, c.phone);
      await client.calls.create({
        to: c.phone,
        from: process.env.TWILIO_FROM_NUMBER,
        url: callUrl,
        method: "POST"
      });
      markDialResult(db, c.phone, { status: "called" });
    } catch (e) {
      markDialResult(db, c.phone, { status: "failed", error: e?.message || String(e) });
    }
  }
}

function ensureDialerRunning() {
  const { autoDialEnabled, autoDialIntervalSeconds } = settingsSnapshot();
  if (dialerTimer) {
    clearInterval(dialerTimer);
    dialerTimer = null;
  }
  if (!autoDialEnabled) return;
  dialerTimer = setInterval(() => {
    runDialerOnce().catch(() => {});
  }, Math.max(5, autoDialIntervalSeconds) * 1000);
}

// Poll endpoint: wait until the audio file exists, then play it and continue recording.
app.all("/twilio/play", async (req, res) => {
  try {
    const callSid = String(getParam(req, "callSid") || getParam(req, "CallSid") || "");
    const key = String(getParam(req, "k") || "").trim();
    const aRaw = String(getParam(req, "a") || "0");
    const attempt = Number.parseInt(aRaw, 10) || 0;

    const cached = findCachedAudioByKey(key);
    if (!cached) {
      if (attempt >= TTS_POLL_MAX) {
        // Never leave the caller in silence. Fallback to Twilio <Say> (Polly.Carmit) and continue.
        const xml = buildRecordTwiML({
          sayText: handoffQuestionTextForPersona("male"),
          playUrl: null,
          actionUrl: recordActionUrl(req, 0),
          playBeep: false,
          maxLengthSeconds: RECORD_MAX_LENGTH_SECONDS,
          timeoutSeconds: recordTimeoutSeconds()
        });
        res.type("text/xml").send(xml);
        return;
      }
      const redirectUrl = toAbsoluteUrl(
        req,
        `/twilio/play?callSid=${encodeURIComponent(callSid)}&k=${encodeURIComponent(key)}&a=${attempt + 1}`
      );
      const response = new twilio.twiml.VoiceResponse();
      response.pause({ length: TTS_POLL_WAIT_SECONDS });
      response.redirect({ method: "POST" }, redirectUrl);
      res.type("text/xml").send(response.toString());
      return;
    }

    const xml = buildRecordTwiML({
      sayText: null,
      playUrl: toAbsoluteUrl(req, cached.rel),
      actionUrl: recordActionUrl(req, 0),
      playBeep: false,
      maxLengthSeconds: RECORD_MAX_LENGTH_SECONDS,
      // מינימום זמן לענות אחרי שהושמע האודיו (פתיח/תשובה), כדי לא ליפול מהר ל-"לא שמעתי".
      timeoutSeconds: recordTimeoutSeconds()
    });
    res.type("text/xml").send(xml);
  } catch (err) {
    console.error(err);
    // Safety: never return "nothing". Continue the conversation even on errors.
    const xml = buildRecordTwiML({
      sayText: handoffQuestionTextForPersona("male"),
      playUrl: null,
      actionUrl: recordActionUrl(req, 0),
      playBeep: false,
      maxLengthSeconds: RECORD_MAX_LENGTH_SECONDS,
      timeoutSeconds: recordTimeoutSeconds()
    });
    res.type("text/xml").send(xml);
  }
});

// Poll endpoint: wait until the audio file exists, then play it and hang up (used for deterministic closings).
app.all("/twilio/play_end", async (req, res) => {
  try {
    const key = String(getParam(req, "k") || "").trim();
    const aRaw = String(getParam(req, "a") || "0");
    const attempt = Number.parseInt(aRaw, 10) || 0;

    const cached = findCachedAudioByKey(key);
    if (!cached) {
      if (attempt >= TTS_POLL_MAX) {
        // End-call fallback: say a short goodbye (avoid silent hangup).
        res.type("text/xml").send(buildSayAndHangup({ sayText: "תודה רבה ויום טוב." }));
        return;
      }
      const redirectUrl = toAbsoluteUrl(req, `/twilio/play_end?k=${encodeURIComponent(key)}&a=${attempt + 1}`);
      const response = new twilio.twiml.VoiceResponse();
      response.pause({ length: TTS_POLL_WAIT_SECONDS });
      response.redirect({ method: "POST" }, redirectUrl);
      res.type("text/xml").send(response.toString());
      return;
    }

    const xml = buildPlayAndHangup({ playUrl: toAbsoluteUrl(req, cached.rel) });
    res.type("text/xml").send(xml);
  } catch (err) {
    console.error(err);
    res.type("text/xml").send(buildSayAndHangup({ sayText: "תודה רבה ויום טוב." }));
  }
});

// ---------------------------
// ConversationRelay WebSocket (Realtime mode)
// ---------------------------

function extractCalleePhoneFromSetup(msg) {
  // Setup message includes from/to and direction.
  const direction = String(msg?.direction || "").toLowerCase();
  const to = normalizePhoneE164IL(msg?.to || "");
  const from = normalizePhoneE164IL(msg?.from || "");
  // Inbound: caller is "from". Outbound: callee is "to".
  return direction === "inbound" ? from : to;
}

function wsSendJson(ws, obj) {
  try {
    if (ws.readyState === ws.OPEN) ws.send(JSON.stringify(obj));
  } catch {}
}

function crServerVerboseEnabled() {
  // Verbose logging can be enabled explicitly. Always-on logs (connect/setup/errors) do not depend on this.
  return Boolean(CR_DEBUG) || process.env.CR_SERVER_DEBUG === "1" || process.env.CR_SERVER_DEBUG === "true";
}

function crLogAlways(...args) {
  console.log("[cr]", ...args);
}

function crLogVerbose(...args) {
  if (!crServerVerboseEnabled()) return;
  console.log("[cr]", ...args);
}

async function streamAssistantToConversationRelay({
  ws,
  callSid,
  persona,
  userText
}) {
  if (!openai) {
    wsSendText(ws, { type: "text", token: handoffQuestionTextForPersona(persona), last: true });
    return;
  }

  const { knowledgeBase } = settingsSnapshot();
  const knowledgeForThisTurn = selectRelevantKnowledge({ knowledgeBase, query: userText });
  const { handoffToPhrase, handoffFromPhrase } = settingsSnapshot();
  const system = buildSystemPrompt({
    persona,
    knowledgeBase: knowledgeForThisTurn,
    handoffToPhrase,
    handoffFromPhrase
  });

  // Persist user message
  addMessage(db, { callSid, role: "user", content: userText });

  const history = getMessages(db, callSid, { limit: 10 });
  const messages = [{ role: "system", content: system }, ...history];

  // Stream tokens when supported; otherwise send a single chunk (GPT-5 via Responses API).
  let full = "";
  try {
    const llm = await createLlmText({
      model: OPENAI_MODEL,
      messages,
      temperature: 0.3,
      maxTokens: 180,
      stream: !isGpt5Family(OPENAI_MODEL)
    });
    if (llm.api === "chat_stream" && llm.stream) {
      let pending = "";
      for await (const chunk of llm.stream) {
        const delta = chunk?.choices?.[0]?.delta?.content ?? "";
        if (!delta) continue;
        full += delta;
        // Hold back one token chunk so we can mark the last chunk with last:true.
        if (pending) {
          wsSendText(ws, { type: "text", token: pending, last: false, interruptible: true, preemptible: true });
        }
        pending = delta;
      }

      if (!pending && !full) {
        crLogAlways("llm empty stream output", { callSid, model: OPENAI_MODEL, userText: String(userText || "").slice(0, 120) });
        pending = handoffQuestionTextForPersona(persona);
        full = pending;
      }

      wsSendText(ws, { type: "text", token: pending, last: true, interruptible: true, preemptible: true });
    } else {
      full = String(llm.rawText || "").trim();
      if (!full) {
        crLogAlways("llm empty output", { callSid, api: llm.api, model: OPENAI_MODEL, userText: String(userText || "").slice(0, 120) });
        full = handoffQuestionTextForPersona(persona);
      }
      wsSendText(ws, { type: "text", token: full, last: true, interruptible: true, preemptible: true });
    }
  } catch {
    full = handoffQuestionTextForPersona(persona);
    wsSendText(ws, { type: "text", token: full, last: true, interruptible: true, preemptible: true });
  }

  const answer = sanitizeSayText(String(full || "").trim());
  addMessage(db, { callSid, role: "assistant", content: answer });
}

// Render (וגם רוב ספקי ה-hosting) דורשים bind ל-0.0.0.0 כדי שה-Service יהיה נגיש מבחוץ.
// בלוקאלי אפשר להישאר על 127.0.0.1.
const HOST = process.env.HOST || (process.env.RENDER ? "0.0.0.0" : "127.0.0.1");

const server = http.createServer(app);

// IMPORTANT:
// Do NOT attach multiple ws servers with { server, path } — the first one will abortHandshake(400) for other paths.
// We use noServer=true and route upgrades ourselves.
const wssConversationRelay = new WebSocketServer({ noServer: true });
wssConversationRelay.on("error", (e) => {
  crLogAlways("wss error", e?.message || e);
});
wssConversationRelay.on("connection", (ws, req) => {
  // Session state per socket
  let callSid = "";
  let phone = "";
  let persona = "male";
  let inFlight = null; // Promise
  let closed = false;
  let greetingSent = false;

  crLogAlways("ws connected", {
    path: req?.url,
    ua: req?.headers?.["user-agent"],
    ip:
      req?.headers?.["x-forwarded-for"] ||
      req?.headers?.["cf-connecting-ip"] ||
      req?.socket?.remoteAddress ||
      ""
  });

  ws.on("close", () => {
    closed = true;
    crLogAlways("ws closed", { callSid, phone });
  });

  ws.on("error", (e) => {
    crLogAlways("ws error", { callSid, err: e?.message || String(e) });
  });

  ws.on("message", (data) => {
    if (closed) return;
    let msg = null;
    try {
      msg = JSON.parse(String(data || ""));
    } catch {
      wsSendJson(ws, { type: "error", description: "Invalid JSON" });
      return;
    }

    const t = String(msg?.type || "");
    // Always surface Twilio-side errors/warnings + playback/speaker signals.
    if (
      t === "error" ||
      t === "warning" ||
      t === "info" ||
      t === "tokensPlayed" ||
      t === "agentSpeaking" ||
      t === "clientSpeaking" ||
      t === "debugging"
    ) {
      // Keep logs readable: include common fields + a truncated raw JSON payload.
      let raw = "";
      try {
        raw = JSON.stringify(msg);
        if (raw.length > 1200) raw = `${raw.slice(0, 1200)}…`;
      } catch {}
      crLogAlways("ws event", {
        type: t,
        callSid: String(msg?.callSid || callSid || ""),
        sessionId: String(msg?.sessionId || ""),
        // common fields (Twilio uses different shapes across events)
        description: msg?.description || msg?.msg || msg?.message || "",
        data: msg?.data || "",
        last: typeof msg?.last === "boolean" ? msg.last : undefined,
        raw
      });
    } else {
      crLogVerbose("ws message", { type: t });
    }
    if (t === "setup") {
      callSid = String(msg?.callSid || "");
      const cp = msg?.customParameters || {};
      persona = String(cp?.persona || "male");
      phone = String(cp?.phone || "") || extractCalleePhoneFromSetup(msg) || "";
      if (callSid && phone) {
        try {
          createOrGetCall(db, { callSid, phone, persona });
        } catch {}
      }

      // Send greeting immediately after setup (instead of TwiML welcomeGreeting).
      // This also doubles as a health check that TTS is working at all.
      const greeting = sanitizeSayText(String(cp?.greeting || "").trim());
      if (!greetingSent && greeting) {
        greetingSent = true;
        wsSendText(ws, { type: "text", token: greeting, last: true, interruptible: true, preemptible: true });
        crLogAlways("sent greeting token", { callSid, chars: greeting.length });
      }
      return;
    }

    // Prompt: caller speech (may arrive as partials with last=false).
    if (t === "prompt") {
      const last = !!msg?.last;
      const voicePrompt = String(msg?.voicePrompt || "").trim();
      if (!voicePrompt) return;
      if (!last) return;
      if (!callSid) callSid = String(msg?.callSid || "");

      // Barge-in behavior: if we are still streaming a response, we let Twilio interrupt playback,
      // and we simply start generating a new response.
      inFlight = (async () => {
        // Update lead table deterministically when explicit.
        try {
          const interested = detectInterested(voicePrompt) && !detectNotInterested(voicePrompt);
          const notInterested = detectNotInterested(voicePrompt) && !interested;
          if (callSid && phone && (interested || notInterested)) {
            upsertLead(db, { phone, status: interested ? "waiting" : "not_interested", callSid, persona });
          }
        } catch {}

        // Handle opt-out early
        if (detectOptOut(voicePrompt)) {
          if (phone) markDoNotCall(db, phone);
          wsSendJson(ws, { type: "text", token: "אין בעיה, הסרתי אותך. יום טוב.", last: true, interruptible: true });
          return;
        }

        await streamAssistantToConversationRelay({ ws, callSid, persona, userText: voicePrompt });
      })().catch((e) => {
        wsSendJson(ws, { type: "text", token: handoffQuestionTextForPersona(persona), last: true });
        if (DEBUG_TTS) console.warn("[cr] handler error:", e?.message || e);
      });
    }
  });
});

// Media Streams WebSocket server (used when REALTIME_MODE=2)
const wssMediaStream = new WebSocketServer({ noServer: true });
wssMediaStream.on("error", (e) => {
  console.warn("[ms] wss error", e?.message || e);
});
wssMediaStream.on("connection", (ws, req) => {
  let streamSid = "";
  let callSid = "";
  let phone = "";
  let persona = "male";
  let greeting = "";
  let closed = false;
  let leadWaiting = false;

  // We want a "normal call":
  // - Greeting plays fully (no barge-in, no listening)
  // - After greeting ends, enable listening + barge-in
  // We always CAPTURE inbound audio, but only TRANSCRIBE/RESPOND after the greeting is done.
  let allowListen = false; // "process speech" (STT+LLM+TTS)
  let allowBargeIn = false; // interrupt agent speech
  // Echo-safe turn-taking: while agent audio is playing, ignore inbound audio entirely.
  let agentSpeaking = false;
  let pendingEnableListenOnMark = false;
  let lastMarkName = "";

  // Outbound playback state
  let playTimer = null;
  let playing = false;
  let playId = 0;
  /** @type {{ id: number, resolve?: (v:any)=>void, label?: string } | null} */
  let currentPlay = null;
  let playingSince = 0;

  // Call recording (Media Streams): record both sides to stereo WAV
  // - Left channel: agent (what we send to Twilio)
  // - Right channel: caller (what Twilio sends to us)
  const recordEnabled = MS_RECORD_CALLS;
  const ULaw_FRAME_BYTES = 160; // 20ms @ 8kHz ulaw
  const ulawSilenceFrame = Buffer.alloc(ULaw_FRAME_BYTES, 0xff);
  /** @type {Buffer[]} */
  let recInUlawQ = [];
  /** @type {Buffer[]} */
  let recOutUlawQ = [];
  let recFd = null;
  let recPath = "";
  let recDataBytes = 0;
  let recTimer = null;
  let recStartedAt = 0;

  function startRecorderIfNeeded() {
    if (!recordEnabled) return;
    if (recFd) return;
    if (!callSid) return;
    try {
      const ts = Date.now();
      recPath = path.join(msRecordingsDir, `ms-${callSid}-${ts}-stereo.wav`);
      recFd = fs.openSync(recPath, "w");
      // Placeholder header; we'll overwrite sizes on finalize.
      fs.writeSync(recFd, wavHeaderPcm16({ numChannels: 2, sampleRate: 8000, dataBytes: 0 }), 0, 44, 0);
      recDataBytes = 0;
      recStartedAt = Date.now();

      recTimer = setInterval(() => {
        try {
          if (!recFd) return;
          // Safety: cap duration to avoid runaway disk usage.
          if (MS_RECORD_MAX_SECONDS > 0 && Date.now() - recStartedAt > MS_RECORD_MAX_SECONDS * 1000) {
            finalizeRecorder();
            return;
          }
          const inU = recInUlawQ.length ? recInUlawQ.shift() : ulawSilenceFrame;
          const outU = recOutUlawQ.length ? recOutUlawQ.shift() : ulawSilenceFrame;
          const inP = ulawBufferToPcm16(inU);
          const outP = ulawBufferToPcm16(outU);
          const stereo = interleaveStereoPcm16(outP, inP);
          fs.writeSync(recFd, stereo);
          recDataBytes += stereo.length;
        } catch {}
      }, 20);

      msLog("recording started", { callSid, path: `/ms-recordings/${path.basename(recPath)}` });
    } catch (e) {
      msLog("recording start failed", e?.message || e);
      try {
        if (recTimer) clearInterval(recTimer);
      } catch {}
      recTimer = null;
      try {
        if (recFd) fs.closeSync(recFd);
      } catch {}
      recFd = null;
      recPath = "";
      recDataBytes = 0;
    }
  }

  function finalizeRecorder() {
    if (!recFd) return;
    try {
      if (recTimer) clearInterval(recTimer);
    } catch {}
    recTimer = null;
    try {
      // Overwrite header with final sizes.
      const header = wavHeaderPcm16({ numChannels: 2, sampleRate: 8000, dataBytes: recDataBytes });
      fs.writeSync(recFd, header, 0, 44, 0);
    } catch {}
    try {
      fs.closeSync(recFd);
    } catch {}
    const url = recPath ? `/ms-recordings/${path.basename(recPath)}` : "";
    msLog("recording finalized", { callSid, bytes: recDataBytes, url });
    recFd = null;
    recPath = "";
    recDataBytes = 0;
    recInUlawQ = [];
    recOutUlawQ = [];
  }

  // Inbound VAD / utterance state
  let speechActive = false;
  let lastVoiceAt = 0;
  let bargeInFrames = 0;
  let inboundFrames = 0;
  let inboundVoicedFrames = 0;
  let inboundMaxRms = 0;
  let noiseFloor = 0;
  let calibrateUntil = 0;
  /** @type {Int16Array[]} */
  let utterancePcm8kChunks = [];
  let utteranceStartAt = 0;
  let utteranceVoicedFrames = 0;
  let inFlight = null; // Promise

  // Sales/flow state (kept per call connection)
  /** @type {"CHECK_INTEREST"|"PITCH"|"CLOSE"|"POST_CLOSE"|"HANDLE_OBJECTION"|"END"} */
  let conversationState = "CHECK_INTEREST";
  // Reprompt control: never loop "repeat yourself" style prompts.
  let confirmCount = 0;
  let persuasionAttempted = false;

  // Guided flow state (campaign-specific logic driven by Admin KB + knobs)
  /** @type {"ASK_PURPOSE"|"ASK_DATE"|"ASK_PARTICIPANTS"|"PARTICIPANTS_PERSUADE"|"CLOSE"} */
  let guidedStep = "ASK_PURPOSE";
  let guidedPurpose = "";
  let guidedAskedPurposeQ = false;
  let guidedDateText = "";
  let guidedParticipants = null;
  let participantsPersuadeAsked = false;
  let guidedAskedCooldownRule = false;
  let guidedLastQuestionAt = 0;

  function confirmQuestionText() {
    // No-apology mode: never say "לא שמעתי/לא קלטתי/תחזור".
    return handoffQuestionTextForPersona(persona);
  }

  async function sayFinalAndHangup({ text, outcome }) {
    if (closed) return;
    const finalText = sanitizeSayText(String(text || "").trim());
    if (!finalText) return;
    try {
      if (callSid && phone) addMessage(db, { callSid, role: "assistant", content: finalText });
    } catch {}

    // Persist lead outcome before hangup.
    try {
      if (outcome === "interested") {
        if (callSid && phone) upsertLead(db, { phone, status: "waiting", callSid, persona });
        leadWaiting = true;
        lastOutcome = "interested";
      } else if (outcome === "not_interested") {
        if (callSid && phone) upsertLead(db, { phone, status: "not_interested", callSid, persona });
        lastOutcome = "not_interested";
      } else if (outcome === "do_not_call") {
        if (phone) markDoNotCall(db, phone);
        if (callSid && phone) upsertLead(db, { phone, status: "not_interested", callSid, persona });
        lastOutcome = "do_not_call";
      }
    } catch {}

    const r = await sayText(finalText, { label: "reply" });
    if (r?.markName) {
      pendingHangupOnMark = true;
      hangupMarkName = String(r.markName || "");
    } else {
      await hangupCallNow();
    }
    conversationState = "END";
  }

  async function handleUnclearOrEmptySpeech(reason) {
    if (closed) return;
    // Only allow one confirm question; then end politely to avoid loops.
    if (confirmCount >= 1) {
      const finalText = "סבבה, נסיים כאן. יום טוב ובשורות טובות.";
      try {
        if (callSid && phone) addMessage(db, { callSid, role: "assistant", content: finalText });
      } catch {}
      const r = await sayText(finalText, { label: "reply" });
      if (MS_DEBUG) msLog("confirm->end", { callSid, reason, confirmCount });
      if (r?.markName) {
        pendingHangupOnMark = true;
        hangupMarkName = String(r.markName || "");
      } else {
        await hangupCallNow();
      }
      conversationState = "END";
      lastOutcome = "not_interested";
      try {
        if (callSid && phone) upsertLead(db, { phone, status: "not_interested", callSid, persona });
      } catch {}
      return;
    }
    confirmCount++;
    const q = confirmQuestionText();
    try {
      if (callSid && phone) addMessage(db, { callSid, role: "assistant", content: q });
    } catch {}
    if (MS_DEBUG) msLog("confirm question", { callSid, reason, confirmCount, state: conversationState });
    await sayText(q, { label: "reply" });
  }

  // Hangup control (only after Twilio acks playback completion via mark)
  let pendingHangupOnMark = false;
  let hangupMarkName = "";
  let lastOutcome = "none";

  // Dead-air backchannel ("thinking") control
  let thinkingTimer = null;
  let awaitingReply = false;
  let lastUtteranceEndAt = 0;
  let thinkingPlayed = false;

  function clearThinkingTimer() {
    if (thinkingTimer) {
      try { clearTimeout(thinkingTimer); } catch {}
      thinkingTimer = null;
    }
  }

  function scheduleThinkingOnce() {
    if (closed) return;
    if (!awaitingReply) return;
    if (thinkingPlayed) return;
    if (!(MS_THINKING_DELAY_MS > 0)) return;
    clearThinkingTimer();
    thinkingTimer = setTimeout(() => {
      if (closed) return;
      if (!awaitingReply) return;
      if (thinkingPlayed) return;
      if (agentSpeaking || playing) return;
      const think = sanitizeSayText(getThinkingText());
      if (!think) return;
      // Mark as played immediately to prevent duplicates even if TTS is slow.
      thinkingPlayed = true;
      elevenlabsTtsToUlaw8000({ text: think, persona: AGENT_VOICE_PERSONA })
        .then((ulaw) => {
          if (!awaitingReply) return;
          if (ulaw) playUlaw(ulaw, { label: "thinking" });
        })
        .catch(() => {});
    }, MS_THINKING_DELAY_MS);
  }

  async function hangupCallNow() {
    if (!MS_AUTO_HANGUP) return;
    if (!callSid) return;
    if (!process.env.TWILIO_ACCOUNT_SID || !process.env.TWILIO_AUTH_TOKEN) return;
    try {
      const client = twilio(process.env.TWILIO_ACCOUNT_SID, process.env.TWILIO_AUTH_TOKEN);
      await client.calls(callSid).update({ status: "completed" });
      msLog("hangup requested", { callSid });
    } catch (e) {
      msLog("hangup failed", e?.message || e);
    }
  }

  function msLog(...args) {
    if (MS_DEBUG) console.log("[ms]", ...args);
  }

  function wsSendToTwilio(obj) {
    if (closed) return;
    try {
      ws.send(JSON.stringify(obj));
    } catch {}
  }

  function stopPlayback({ clear = false } = {}) {
    if (MS_DEBUG && currentPlay) {
      // Snapshot where we stopped (proves whether we streamed multiple frames or only 1).
      const meta = currentPlay?.meta || {};
      msLog("play stop", {
        label: currentPlay?.label || "",
        reason: clear ? "clear" : "stop",
        framesSent: Number(meta.framesSent || 0),
        offset: Number(meta.offset || 0),
        totalBytes: Number(meta.totalBytes || 0)
      });
    }
    if (playTimer) clearInterval(playTimer);
    playTimer = null;
    playing = false;
    playingSince = 0;
    agentSpeaking = false;
    pendingEnableListenOnMark = false;
    if (currentPlay?.resolve) {
      try {
        currentPlay.resolve({ ok: false, reason: "stopped", label: currentPlay.label });
      } catch {}
    }
    currentPlay = null;
    if (clear && streamSid) {
      wsSendToTwilio({ event: "clear", streamSid });
    }
  }

  function playUlaw(ulawBuf, { label = "tts" } = {}) {
    if (!ulawBuf || !ulawBuf.length || !streamSid) return;
    // Any outbound audio cancels pending "thinking" timer.
    if (thinkingTimer) {
      try { clearTimeout(thinkingTimer); } catch {}
      thinkingTimer = null;
    }
    stopPlayback({ clear: true });
    playing = true;
    playingSince = Date.now();
    agentSpeaking = true;
    pendingEnableListenOnMark = true;
    // While we are speaking, do not listen (echo will look like "speech").
    allowListen = false;
    const CHUNK_BYTES = 160; // 20ms @ 8kHz, 1 byte/sample (mulaw)
    let offset = 0;
    let sent = 0;
    const myId = ++playId;

    const playStartAt = Date.now();
    return new Promise((resolve) => {
      currentPlay = {
        id: myId,
        resolve,
        label,
        meta: { framesSent: 0, offset: 0, totalBytes: ulawBuf.length, firstChunkAt: 0, playStartAt }
      };
      msLog("play start", { callSid, streamSid, label, bytes: ulawBuf.length });

      const tick = () => {
        if (closed || !streamSid || ws.readyState !== ws.OPEN) {
          // This will resolve via stopPlayback().
          stopPlayback({ clear: false });
          return;
        }
        // If a newer play started, abort this one.
        if (currentPlay?.id !== myId) return;

        const end = Math.min(offset + CHUNK_BYTES, ulawBuf.length);
        const chunk = ulawBuf.subarray(offset, end);
        offset = end;
        // Recording: capture what we send out (agent channel).
        if (recordEnabled && chunk && chunk.length) {
          // Ensure exactly 20ms frames where possible; pad short final chunks with μ-law silence.
          const b =
            chunk.length === ULaw_FRAME_BYTES
              ? Buffer.from(chunk)
              : Buffer.concat([Buffer.from(chunk), Buffer.alloc(Math.max(0, ULaw_FRAME_BYTES - chunk.length), 0xff)]);
          recOutUlawQ.push(b);
        }
        wsSendToTwilio({
          event: "media",
          streamSid,
          media: { payload: Buffer.from(chunk).toString("base64") }
        });
        sent++;
        // update stop/diagnostic snapshot
        if (currentPlay?.meta) {
          currentPlay.meta.framesSent = sent;
          currentPlay.meta.offset = offset;
          currentPlay.meta.totalBytes = ulawBuf.length;
        }
        if (MS_LOG_EVERY_FRAME) {
          msLog("frame", {
            label,
            chunk: sent,
            offset,
            end: offset >= ulawBuf.length
          });
        }
        if (sent === 1) {
          if (currentPlay?.meta) currentPlay.meta.firstChunkAt = Date.now();
          msLog("sent first audio chunk", { callSid, streamSid, label, bytes: ulawBuf.length });
        }
        if (sent % 25 === 0) msLog("sent audio chunks", { label, chunks: sent });

        if (offset >= ulawBuf.length) {
          const markName = `done_${label}_${myId}_${Date.now()}`;
          lastMarkName = markName;
          msLog("tx mark", { name: markName });
          wsSendToTwilio({ event: "mark", streamSid, mark: { name: markName } });
          // Resolve before stopPlayback clears currentPlay.
          const done = currentPlay;
          currentPlay = null;
          playing = false;
          if (playTimer) clearInterval(playTimer);
          playTimer = null;
          try {
            const meta = done?.meta || {};
            const firstChunkDelayMs =
              meta.firstChunkAt && meta.playStartAt ? Number(meta.firstChunkAt) - Number(meta.playStartAt) : null;
            const playbackMs = meta.playStartAt ? Date.now() - Number(meta.playStartAt) : null;
            resolve({
              ok: true,
              label,
              bytes: ulawBuf.length,
              chunks: sent,
              markName,
              firstChunkDelayMs,
              playbackMs,
              firstChunkAt: meta.firstChunkAt || null,
              playStartAt: meta.playStartAt || null
            });
          } catch {}
        }
      };

      // Send the first frame immediately (do not wait 20ms) so short calls still hear something.
      tick();
      if (!currentPlay || currentPlay.id !== myId) return;
      if (offset >= ulawBuf.length) return;

      playTimer = setInterval(tick, 20);
    });
  }

  async function sayText(text, { label = "elevenlabs" } = {}) {
    const safe = sanitizeSayText(String(text || "").trim());
    if (!safe) return;
    const tGen0 = Date.now();

    // Fast path: cache hit (memory/disk) → play immediately
    const cacheKey = computeElevenUlawCacheKey({ text: safe, persona: AGENT_VOICE_PERSONA });
    const cached = _ulawMemCache.get(cacheKey) || getUlawFromDisk(cacheKey);
    if (cached && cached.length) {
      if (!_ulawMemCache.get(cacheKey)) _ulawMemCache.set(cacheKey, cached);
      const pr = await playUlaw(cached, { label });
      return { ...pr, genMs: 0, cached: true };
    }

    // Streaming path: start playback while ElevenLabs is still producing audio.
    const stream = await elevenlabsTtsToUlaw8000Stream({ text: safe, persona: AGENT_VOICE_PERSONA });
    const genMs = Date.now() - tGen0;
    if (!stream) {
      // Fallback: if streaming fails (or returns empty body), generate full ulaw and play normally.
      const ulaw = await elevenlabsTtsToUlaw8000({ text: safe, persona: AGENT_VOICE_PERSONA });
      if (!ulaw) return;
      _ulawMemCache.set(cacheKey, ulaw);
      putUlawToDisk(cacheKey, ulaw);
      const pr = await playUlaw(ulaw, { label });
      return { ...pr, genMs: Date.now() - tGen0, streamed: false, streamFallback: true };
    }

    // If we had to buffer (no reader), just play normally.
    if (stream.buffered) {
      _ulawMemCache.set(cacheKey, stream.buffered);
      putUlawToDisk(cacheKey, stream.buffered);
      const pr = await playUlaw(stream.buffered, { label });
      return { ...pr, genMs, streamed: false };
    }

    const reader = stream.reader;
    if (!reader) return;

    // Initialize playback state (similar to playUlaw) but with a streaming queue.
    if (thinkingTimer) {
      try { clearTimeout(thinkingTimer); } catch {}
      thinkingTimer = null;
    }
    stopPlayback({ clear: true });
    playing = true;
    playingSince = Date.now();
    agentSpeaking = true;
    pendingEnableListenOnMark = true;
    allowListen = false;

    const CHUNK_BYTES = 160;
    const myId = ++playId;
    const playStartAt = Date.now();
    // For replies we prefer starting faster (avoid perceived silence on slow streams).
    // For greetings we can prebuffer more for smoothness.
    const basePrebuffer = Math.max(1, Math.min(60, Number(MS_TTS_PREBUFFER_FRAMES) || 10));
    const prebufferFrames = label === "reply" ? Math.min(basePrebuffer, 3) : basePrebuffer;

    // Queue and buffering (seed with the probed first chunk so we never "play" an empty stream)
    let queued = Buffer.from(stream.firstChunk || Buffer.alloc(0));
    let streamDone = false;
    let streamErr = "";
    let totalBytes = queued.length;
    let framesSent = 0;
    let offsetBytes = 0;
    let lastByteAt = Date.now();

    return await new Promise((resolve) => {
      currentPlay = {
        id: myId,
        resolve,
        label,
        meta: { framesSent: 0, offset: 0, totalBytes: 0, firstChunkAt: 0, playStartAt }
      };
      msLog("play start (streaming)", { callSid, streamSid, label, prebufferFrames });

      // Read loop (pull bytes from ElevenLabs).
      // IMPORTANT: start this only AFTER currentPlay is set; otherwise `currentPlay?.id === myId`
      // is false and the loop exits immediately, causing playback to cut off after 1 frame.
      (async () => {
        try {
          while (!closed && ws.readyState === ws.OPEN) {
            if (currentPlay?.id !== myId) break; // newer playback started or we stopped
            const { value, done } = await reader.read();
            if (done) break;
            if (value && value.byteLength) {
              const b = Buffer.from(value);
              totalBytes += b.length;
              queued = queued.length ? Buffer.concat([queued, b]) : b;
              lastByteAt = Date.now();
            }
          }
        } catch (e) {
          streamErr = String(e?.message || e || "");
        } finally {
          streamDone = true;
          try {
            reader.releaseLock();
          } catch {}
        }
      })().catch(() => {});

      let started = false;
      const tick = () => {
        if (closed || !streamSid || ws.readyState !== ws.OPEN) {
          stopPlayback({ clear: false });
          return;
        }
        if (currentPlay?.id !== myId) return;

        // Wait until we have enough to start (avoid underflow silence).
        if (!started) {
          const haveFrames = Math.floor(queued.length / CHUNK_BYTES);
          if (haveFrames < prebufferFrames) {
            // If streaming stalls, start with whatever we have (better than indefinite silence).
            if (!streamDone && haveFrames >= 1 && Date.now() - lastByteAt > MS_TTS_STREAM_STALL_MS) {
              msLog("tts stream stall (start anyway)", { callSid, streamSid, label, haveFrames, prebufferFrames });
              started = true;
            } else {
            if (streamDone && queued.length < CHUNK_BYTES) {
              // Nothing meaningful arrived; end cleanly.
              const markName = `done_${label}_${myId}_${Date.now()}`;
              lastMarkName = markName;
              msLog("tx mark", { name: markName });
              wsSendToTwilio({ event: "mark", streamSid, mark: { name: markName } });
              const done = currentPlay;
              currentPlay = null;
              playing = false;
              if (playTimer) clearInterval(playTimer);
              playTimer = null;
              agentSpeaking = true; // until mark ack
              resolve({ ok: true, label, bytes: totalBytes, chunks: framesSent, markName, genMs, streamed: true, empty: true, streamErr });
              return;
            }
            return;
            }
          }
          started = true;
        }

        if (queued.length < CHUNK_BYTES) {
          // If the stream stalls after start, treat it as done and flush/pad.
          if (!streamDone && Date.now() - lastByteAt > MS_TTS_STREAM_STALL_MS) {
            msLog("tts stream stall (flush)", { callSid, streamSid, label, queued: queued.length });
            streamDone = true;
          }
          if (streamDone) {
            // Flush any remainder by padding to a full frame (Twilio expects 160 bytes).
            if (queued.length > 0) {
              const pad = Buffer.alloc(CHUNK_BYTES - queued.length, 0xff); // μlaw silence-ish
              queued = Buffer.concat([queued, pad]);
            } else {
              const markName = `done_${label}_${myId}_${Date.now()}`;
              lastMarkName = markName;
              msLog("tx mark", { name: markName });
              wsSendToTwilio({ event: "mark", streamSid, mark: { name: markName } });
              const done = currentPlay;
              currentPlay = null;
              playing = false;
              if (playTimer) clearInterval(playTimer);
              playTimer = null;
              // Cache what we got (best-effort)
              try {
                if (totalBytes > 0 && totalBytes <= 2_000_000) {
                  // We didn't persist the full byte stream; keep only if we managed to accumulate it (not guaranteed).
                }
              } catch {}
              resolve({
                ok: true,
                label,
                bytes: totalBytes,
                chunks: framesSent,
                markName,
                genMs,
                streamed: true,
                streamErr
              });
              return;
            }
          } else {
            return; // wait for more bytes
          }
        }

        const chunk = queued.subarray(0, CHUNK_BYTES);
        queued = queued.subarray(CHUNK_BYTES);
        // Recording: capture what we send out (agent channel).
        if (recordEnabled && chunk && chunk.length) {
          recOutUlawQ.push(Buffer.from(chunk));
        }
        wsSendToTwilio({
          event: "media",
          streamSid,
          media: { payload: Buffer.from(chunk).toString("base64") }
        });
        framesSent++;
        offsetBytes += CHUNK_BYTES;
        if (currentPlay?.meta) {
          currentPlay.meta.framesSent = framesSent;
          currentPlay.meta.offset = offsetBytes;
          currentPlay.meta.totalBytes = totalBytes;
        }
        if (framesSent === 1) {
          if (currentPlay?.meta) currentPlay.meta.firstChunkAt = Date.now();
          msLog("sent first audio chunk (streaming)", { callSid, streamSid, label });
        }
      };

      tick();
      if (!currentPlay || currentPlay.id !== myId) return;
      playTimer = setInterval(tick, 20);
    });
  }

  function handoffQuestionText() {
    const { handoffFromPhrase } = settingsSnapshot();
    const from = String(handoffFromPhrase || "מהצוות").trim() || "מהצוות";
    return persona === "female"
      ? `רוצה שיחזרו אלייך ${from} עם פרטים מסודרים?`
      : `רוצה שיחזרו אליך ${from} עם פרטים מסודרים?`;
  }

  function buildHandoffSystemPrompt({ persona, knowledgeBase }) {
    const toYou = persona === "female" ? "אלייך" : "אליך";
    // Keep it short, natural, and aligned to product mode.
    const agentNoun = AGENT_VOICE_PERSONA === "female" ? "נציגת" : "נציג";
    const agentPronoun = AGENT_VOICE_PERSONA === "female" ? "את" : "אתה";
    return [
      `${agentPronoun} ${agentNoun} אנושי/ת, טבעי/ת וקצר/ה שמתקשר/ת ללקוח/ה בשיחה טלפונית.`,
      `המטרה: מענה ראשוני קצר ואז להגיע לאישור להעברת פרטים כדי שיחזרו ${toYou} לתיאום. לא מנהלים שיחה ארוכה.`,
      `כללי טון: קצר, רגוע, לא לוחץ. בלי "כן או לא עכשיו".`,
      `אסור להמציא פרטים (שעה/מיקום/עלות). אם חסר—תגיד שיחזרו ${toYou} עם כל הפרטים לתיאום.`,
      `תענה בעברית בלבד, 1–2 משפטים.`,
      `בסוף התשובה, הוסף שאלה קצרה שמקדמת: "${handoffQuestionText()}".`,
      knowledgeBase ? `מידע לסוכן (רק אם רלוונטי):\n${knowledgeBase}` : ""
    ]
      .filter(Boolean)
      .join("\n");
  }

  async function transcribeAndRespond(pcm8kAll) {
    if (closed) return;
    if (!openai) return;
    if (!pcm8kAll || !pcm8kAll.length) return;

    const t0 = Date.now();
    msLog("stt start", { callSid, samples8k: pcm8kAll.length });
    // Convert to 16k wav for Whisper (better quality than 8k).
    const pcm16k = upsample8kTo16k(pcm8kAll);
    const wav = pcm16ToWavBuffer(pcm16k, 16000);
    const tmpDir = path.resolve(DATA_DIR, "recordings");
    if (!fs.existsSync(tmpDir)) fs.mkdirSync(tmpDir, { recursive: true });
    const tmpPath = path.join(tmpDir, `ms-${callSid || "call"}-${Date.now()}.wav`);
    fs.writeFileSync(tmpPath, wav);

    let speech = "";
    try {
      const tStt0 = Date.now();
      const transcription = await openai.audio.transcriptions.create({
        model: OPENAI_STT_MODEL,
        file: fs.createReadStream(tmpPath),
        language: "he",
        prompt: OPENAI_STT_PROMPT
      });
      speech = String(transcription.text || "").trim();
      msLog("timing", { callSid, sttMs: Date.now() - tStt0 });
    } catch (e) {
      console.warn("[ms] transcription failed", e?.message || e);
    }

    // Smart retry: long audio but short/generic transcript.
    // This is critical at scale (many accents/noise/line conditions).
    if (openai && shouldRetryStt({ speech, pcm8kLen: pcm8kAll.length })) {
      const fallbackModel = OPENAI_STT_MODEL_FALLBACK || OPENAI_STT_MODEL;
      if (fallbackModel) {
        try {
          const tStt1 = Date.now();
          const prompt2 =
            "תמלול שיחה טלפונית בעברית. חשוב: אל תחליף משפטים ב'תודה' אם לא נאמר. נסה לשמר ניסוח מלא. " +
            "מילים נפוצות: כן, אני מעוניין/ת, במה מדובר, מה זה, לא בטוח/ה, אולי, תעבירו את הפרטים, אל תתקשרו.";
          const transcription2 = await openai.audio.transcriptions.create({
            model: fallbackModel,
            file: fs.createReadStream(tmpPath),
            language: "he",
            prompt: prompt2
          });
          const speech2 = String(transcription2.text || "").trim();
          msLog("timing", { callSid, sttRetryMs: Date.now() - tStt1, model: fallbackModel });
          // Prefer the retry if it fixes known failure modes (prompt-echo / garbage),
          // even when the retry is shorter.
          const origLooksEcho = looksLikePromptEcho(speech);
          const retryLooksEcho = looksLikePromptEcho(speech2);
          const origSuspicious = isSuspiciousTranscript(speech);
          const retrySuspicious = isSuspiciousTranscript(speech2);

          const shouldUseRetry =
            // Hard override: original is prompt-echo, retry is not
            (origLooksEcho && speech2 && !retryLooksEcho) ||
            // General: original suspicious, retry not suspicious
            (origSuspicious && speech2 && !retrySuspicious) ||
            // Old heuristic: retry is clearly more informative OR original was generic ack on long audio
            (speech2 && (speech2.length > speech.length + 2 || isGenericAckTranscript(speech)));

          if (shouldUseRetry) {
            speech = speech2;
            msLog("stt retry used", { callSid, chars: speech.length });
          } else {
            msLog("stt retry skipped", { callSid, origChars: speech.length, retryChars: speech2.length });
          }
        } catch (e) {
          msLog("stt retry failed", { callSid, err: e?.message || String(e) });
        }
      }
    }
    try {
      fs.unlinkSync(tmpPath);
    } catch {}

    if (!speech) {
      msLog("stt empty", { callSid });
      await handleUnclearOrEmptySpeech("stt_empty");
      return;
    }
    msLog("stt", { callSid, chars: speech.length });
    // Debug: show what Whisper heard (helps diagnose why "כן/מעוניין" isn't triggering deterministic logic).
    msLog("stt text", {
      callSid,
      text: String(speech || "").slice(0, 120),
      norm: normalizeIntentText(speech).slice(0, 120),
      state: conversationState
    });

    // Campaign mode
    const snap = settingsSnapshot();
    const campaignMode = String(snap.campaignMode || "handoff").trim() || "handoff"; // "handoff" | "guided"
    const femaleOnly = !!snap.femaleOnly;
    const minParticipants = Number(snap.minParticipants || 15) || 15;
    const cooldownMonths = Number(snap.cooldownMonths || 6) || 6;

    // Guided flow: deterministic multi-step conversation driven by Admin KB.
    if (campaignMode === "guided") {
      const flowText = guidedFlowTextFromKb(snap.knowledgeBase, { minParticipants, cooldownMonths });

      // If STT returned garbage/non-Hebrew/prompt-echo, do not advance the flow. Just repeat the current question.
      if (isSuspiciousTranscript(speech)) {
        msLog("guided", { callSid, kind: "stt_suspicious", step: guidedStep, text: String(speech || "").slice(0, 60) });
        // Cooldown to avoid spamming the same question if STT keeps returning garbage.
        if (Date.now() - guidedLastQuestionAt < 1400) return;
        let q0 = "";
        if (guidedStep === "ASK_PURPOSE") q0 = flowText.FLOW_ASK_PURPOSE;
        else if (guidedStep === "ASK_DATE") q0 = flowText.FLOW_ASK_DATE;
        else if (guidedStep === "ASK_PARTICIPANTS") q0 = flowText.FLOW_ASK_PARTICIPANTS;
        else if (guidedStep === "PARTICIPANTS_PERSUADE") q0 = flowText.FLOW_PARTICIPANTS_LOW;
        else if (guidedStep === "CLOSE") q0 = handoffQuestionText();
        q0 = limitPhoneReply(q0 || handoffQuestionText(), 200);
        try {
          if (callSid && phone) addMessage(db, { callSid, role: "assistant", content: q0 });
        } catch {}
        guidedLastQuestionAt = Date.now();
        await sayText(q0, { label: "reply" });
        return;
      }

      // Women-only enforcement (optional)
      if (femaleOnly && persona !== "female") {
        msLog("guided", { callSid, kind: "women_only_block" });
        await sayFinalAndHangup({ text: flowText.FLOW_WOMEN_ONLY, outcome: "not_interested" });
        return;
      }

      // Opt-out fast path
      if (detectOptOut(speech)) {
        confirmCount = 0;
        msLog("guided", { callSid, kind: "opt_out" });
        await sayFinalAndHangup({ text: "אין בעיה, הסרתי אותך. יום טוב.", outcome: "do_not_call" });
        return;
      }

      // Explicit consent to transfer details should close immediately (regardless of step).
      if (detectTransferConsent(speech)) {
        msLog("guided", { callSid, kind: "handoff_consent_direct" });
        await sayFinalAndHangup({ text: handoffConfirmCloseText({ persona }), outcome: "interested" });
        return;
      }

      // Persist user message (after critical guards so we don't create confusing transcripts)
      try {
        if (callSid && phone) addMessage(db, { callSid, role: "user", content: speech });
      } catch {}

      // Rabbinical/instructor names policy enforcement:
      // - Do NOT list names proactively.
      // - If user asks about a specific name: confirm only if it's in the approved list in KB,
      //   otherwise say we'll check and call back.
      if (detectRabbinicalInquiry(speech)) {
        const askedName = matchApprovedNameInText({ kb: snap.knowledgeBase, userText: speech });
        const base = askedName ? flowText.FLOW_NAME_CONFIRMED : flowText.FLOW_NAME_UNKNOWN;
        let q0 = "";
        if (guidedStep === "ASK_PURPOSE") q0 = flowText.FLOW_ASK_PURPOSE;
        else if (guidedStep === "ASK_DATE") q0 = flowText.FLOW_ASK_DATE;
        else if (guidedStep === "ASK_PARTICIPANTS") q0 = flowText.FLOW_ASK_PARTICIPANTS;
        else if (guidedStep === "PARTICIPANTS_PERSUADE") q0 = flowText.FLOW_PARTICIPANTS_LOW;
        else if (guidedStep === "CLOSE") q0 = handoffQuestionText();
        const msg = limitPhoneReply(`${sanitizeSayText(base)} ${q0}`.trim(), 260);
        msLog("guided", { callSid, kind: askedName ? "name_confirmed" : "name_unknown", step: guidedStep });
        try {
          if (callSid && phone) addMessage(db, { callSid, role: "assistant", content: msg });
        } catch {}
        guidedLastQuestionAt = Date.now();
        await sayText(msg, { label: "reply" });
        return;
      }

      // Smalltalk/clarification layer: keep the call natural without breaking the deterministic flow.
      // - If user says "מה נשמע/מה שלומך" → respond politely and re-ask the current step question.
      // - If user says "מה אמרת/מה נאמר/לא הבנתי" → repeat the current step question.
      const ns0 = normalizeIntentText(speech);
      if (detectRepeatRequest(ns0) || detectSmalltalk(ns0)) {
        let prefix = "";
        if (detectSmalltalk(ns0)) {
          // Optional override in KB (white-label friendly):
          // FLOW_SMALLTALK_REPLY=הכל טוב, תודה. :)
          prefix = String(flowText.FLOW_SMALLTALK_REPLY || "הכל טוב, תודה.").trim();
        }
        let q0 = "";
        if (guidedStep === "ASK_PURPOSE") q0 = flowText.FLOW_ASK_PURPOSE;
        else if (guidedStep === "ASK_DATE") q0 = flowText.FLOW_ASK_DATE;
        else if (guidedStep === "ASK_PARTICIPANTS") q0 = flowText.FLOW_ASK_PARTICIPANTS;
        else if (guidedStep === "PARTICIPANTS_PERSUADE") q0 = flowText.FLOW_PARTICIPANTS_LOW;
        else if (guidedStep === "CLOSE") q0 = handoffQuestionText();
        const msg = limitPhoneReply(`${prefix ? prefix + " " : ""}${q0 || ""}`.trim() || q0, 240);
        msLog("guided", { callSid, kind: detectSmalltalk(ns0) ? "smalltalk" : "repeat_request", step: guidedStep });
        try {
          if (callSid && phone) addMessage(db, { callSid, role: "assistant", content: msg });
        } catch {}
        guidedLastQuestionAt = Date.now();
        await sayText(msg, { label: "reply" });
        return;
      }

      // FAQ layer (from KB "question → answer" lines):
      // If user asks a known question, answer it and then continue the current guided step.
      // This is how we support "not exact words" matching.
      // For short question-style utterances ("מה השעות שלכם", "מתי זה"), we allow a looser match.
      const nsFaq = normalizeIntentText(speech);
      const looksLikeShortQuestion =
        nsFaq.includes("מתי") ||
        nsFaq.includes("שעה") ||
        nsFaq.includes("שעות") ||
        nsFaq.includes("איפה") ||
        nsFaq.includes("כמה") ||
        nsFaq.includes("מחיר") ||
        nsFaq.includes("עלות");
      const faqAnswer = matchFaqAnswerFromKb({ kb: snap.knowledgeBase, userText: speech, minScore: looksLikeShortQuestion ? 1 : 2 });
      if (faqAnswer) {
        const a = limitPhoneReply(sanitizeSayText(faqAnswer), 220);
        let followUp = "";
        if (guidedStep === "ASK_PURPOSE") followUp = flowText.FLOW_ASK_PURPOSE;
        else if (guidedStep === "ASK_DATE") followUp = flowText.FLOW_ASK_DATE;
        else if (guidedStep === "ASK_PARTICIPANTS") followUp = flowText.FLOW_ASK_PARTICIPANTS;
        else if (guidedStep === "PARTICIPANTS_PERSUADE") followUp = flowText.FLOW_PARTICIPANTS_LOW;
        else if (guidedStep === "CLOSE") followUp = handoffQuestionText();
        const msg = followUp ? limitPhoneReply(`${a} ${followUp}`.trim(), 260) : a;
        msLog("guided", { callSid, kind: "faq", step: guidedStep });
        try {
          if (callSid && phone) addMessage(db, { callSid, role: "assistant", content: msg });
        } catch {}
        await sayText(msg, { label: "reply" });
        return;
      }

      // If the user mentions a recent event, enforce cooldown rule (only when it comes up).
      if (!guidedAskedCooldownRule) {
        const ns = normalizeIntentText(speech);
        if (ns.includes("לפני") && (ns.includes("חודש") || ns.includes("חודשים") || ns.includes("שבוע") || ns.includes("שבועות"))) {
          guidedAskedCooldownRule = true;
          const t = limitPhoneReply(flowText.FLOW_COOLDOWN_RULE, 160);
          try {
            if (callSid && phone) addMessage(db, { callSid, role: "assistant", content: t });
          } catch {}
          await sayText(t, { label: "reply" });
          return;
        }
      }

      // Handle hard NO / goodbye
      if (detectNotInterested(speech)) {
        msLog("guided", { callSid, kind: "not_interested" });
        await sayFinalAndHangup({ text: "אין בעיה, תודה על הזמן. יום טוב ובשורות טובות.", outcome: "not_interested" });
        return;
      }

      // Handle CLOSE (handoff confirmation)
      if (guidedStep === "CLOSE") {
        if (detectAffirmativeShort(speech) || detectInterested(speech) || detectTransferConsent(speech)) {
          msLog("guided", { callSid, kind: "handoff_confirm" });
          await sayFinalAndHangup({ text: handoffConfirmCloseText({ persona }), outcome: "interested" });
          return;
        }
        // If user didn't confirm, ask once more, then end politely.
        msLog("guided", { callSid, kind: "handoff_not_confirmed" });
        await sayFinalAndHangup({ text: "סבבה, תודה על הזמן. יום טוב.", outcome: "not_interested" });
        return;
      }

      // Step machine
      if (guidedStep === "ASK_PURPOSE") {
        const s = String(speech || "").trim();
        const ns = normalizeIntentText(s);
        const ackLike =
          ns === "תודה" ||
          ns === "תודה רבה" ||
          ns === "בסדר" ||
          ns === "סבבה" ||
          ns === "אוקיי" ||
          ns === "אוקי" ||
          ns === "כן" ||
          ns === "כאן";
        const isYesLike = detectAffirmativeShort(s) || detectInterested(s) || ns === "כאן" || ns === "כן";
        // Important: purpose answers are often short single words (e.g. "בריאות", "פרנסה").
        // We should NOT treat "short length" as ack; only explicit acknowledgements/yes-like.
        const shouldAskPurposeFirst = !guidedPurpose && !guidedAskedPurposeQ && (isYesLike || ackLike);

        // If the user just said "כן"/short acknowledgment, ask the purpose question first.
        if (shouldAskPurposeFirst) {
          guidedAskedPurposeQ = true;
          const q0 = limitPhoneReply(flowText.FLOW_ASK_PURPOSE, 160);
          try {
            if (callSid && phone) addMessage(db, { callSid, role: "assistant", content: q0 });
          } catch {}
          guidedLastQuestionAt = Date.now();
          await sayText(q0, { label: "reply" });
          return;
        }

        // If we already asked and still got an ack-like response, re-ask (avoid advancing on "תודה").
        if (!guidedPurpose && guidedAskedPurposeQ && ackLike) {
          const q0 = limitPhoneReply(flowText.FLOW_ASK_PURPOSE, 160);
          try {
            if (callSid && phone) addMessage(db, { callSid, role: "assistant", content: q0 });
          } catch {}
          guidedLastQuestionAt = Date.now();
          await sayText(q0, { label: "reply" });
          return;
        }

        // Otherwise, treat this response as the purpose and move on.
        guidedPurpose = guidedPurpose || s;
        guidedAskedPurposeQ = true;
        guidedStep = "ASK_DATE";
        const ack = limitPhoneReply(flowText.FLOW_ACK_PURPOSE || flowText.FLOW_ACK_GENERAL || "", 40);
        const q = limitPhoneReply(`${ack ? ack + " " : ""}${flowText.FLOW_ASK_DATE}`.trim(), 200);
        try {
          if (callSid && phone) addMessage(db, { callSid, role: "assistant", content: q });
        } catch {}
        guidedLastQuestionAt = Date.now();
        await sayText(q, { label: "reply" });
        return;
      }

      if (guidedStep === "ASK_DATE") {
        const s = String(speech || "").trim();
        const ns = normalizeIntentText(s);
        const ackLike =
          ns === "תודה" ||
          ns === "תודה רבה" ||
          ns === "בסדר" ||
          ns === "סבבה" ||
          ns === "אוקיי" ||
          ns === "אוקי" ||
          ns === "כן" ||
          ns === "כאן";
        // Don't advance on acknowledgments; re-ask current question.
        // (Do NOT use length gating here; real date answers can be short like "מחר", "רביעי", "בערב".)
        if (ackLike || !looksLikeDateAnswer(ns)) {
          const q0 = limitPhoneReply(flowText.FLOW_ASK_DATE, 160);
          try {
            if (callSid && phone) addMessage(db, { callSid, role: "assistant", content: q0 });
          } catch {}
          guidedLastQuestionAt = Date.now();
          await sayText(q0, { label: "reply" });
          return;
        }

        guidedDateText = guidedDateText || s;
        guidedStep = "ASK_PARTICIPANTS";
        const ack = limitPhoneReply(flowText.FLOW_ACK_DATE || flowText.FLOW_ACK_GENERAL || "", 40);
        const confirm = limitPhoneReply(flowText.FLOW_DATE_CONFIRM || "", 180);
        const combined = `${ack ? ack + " " : ""}${confirm ? confirm + " " : ""}${flowText.FLOW_ASK_PARTICIPANTS}`.trim();
        const q = limitPhoneReply(combined, 260);
        try {
          if (callSid && phone) addMessage(db, { callSid, role: "assistant", content: q });
        } catch {}
        guidedLastQuestionAt = Date.now();
        await sayText(q, { label: "reply" });
        return;
      }

      if (guidedStep === "ASK_PARTICIPANTS") {
        const n = extractHeNumber(speech);
        guidedParticipants = n;
        const ns = normalizeIntentText(speech);
        const ackLike =
          ns === "תודה" ||
          ns === "תודה רבה" ||
          ns === "בסדר" ||
          ns === "סבבה" ||
          ns === "אוקיי" ||
          ns === "אוקי" ||
          ns === "כן" ||
          ns === "כאן";
        if (n == null && ackLike) {
          const q0 = limitPhoneReply(flowText.FLOW_ASK_PARTICIPANTS, 160);
          try {
            if (callSid && phone) addMessage(db, { callSid, role: "assistant", content: q0 });
          } catch {}
          guidedLastQuestionAt = Date.now();
          await sayText(q0, { label: "reply" });
          return;
        }
        if (n != null && n >= minParticipants) {
          msLog("guided", { callSid, kind: "participants_ok", n });
          // Ask handoff question (rep will call back)
          guidedStep = "CLOSE";
          const ack = limitPhoneReply(flowText.FLOW_ACK_PARTICIPANTS || flowText.FLOW_ACK_GENERAL || "", 40);
          const q = limitPhoneReply(`${ack ? ack + " " : ""}${handoffQuestionText()}`.trim(), 220);
          try {
            if (callSid && phone) addMessage(db, { callSid, role: "assistant", content: q });
          } catch {}
          await sayText(q, { label: "reply" });
          return;
        }
        msLog("guided", { callSid, kind: "participants_low_or_unknown", n });
        guidedStep = "PARTICIPANTS_PERSUADE";
        participantsPersuadeAsked = true;
        const q = limitPhoneReply(flowText.FLOW_PARTICIPANTS_LOW, 200);
        try {
          if (callSid && phone) addMessage(db, { callSid, role: "assistant", content: q });
        } catch {}
        await sayText(q, { label: "reply" });
        return;
      }

      if (guidedStep === "PARTICIPANTS_PERSUADE") {
        if (detectAffirmativeShort(speech) || detectInterested(speech)) {
          msLog("guided", { callSid, kind: "participants_persuade_yes" });
          guidedStep = "CLOSE";
          const q = handoffQuestionText();
          try {
            if (callSid && phone) addMessage(db, { callSid, role: "assistant", content: q });
          } catch {}
          await sayText(q, { label: "reply" });
          return;
        }
        // Not sure / no → fallback, then still offer a call back for help.
        msLog("guided", { callSid, kind: "participants_persuade_no_or_unsure" });
        guidedStep = "CLOSE";
        const t = limitPhoneReply(flowText.FLOW_PARTICIPANTS_LOW_FALLBACK, 200);
        const q = handoffQuestionText();
        const msg = limitPhoneReply(`${t} ${q}`.trim(), 240);
        try {
          if (callSid && phone) addMessage(db, { callSid, role: "assistant", content: msg });
        } catch {}
        await sayText(msg, { label: "reply" });
        return;
      }
    }

    // Hebrew phone STT quirk:
    // Whisper often mishears a short "כן" as "כאן." over μ-law phone audio.
    // We accept this ONLY in the early/closing confirmation steps to keep the flow moving.
    const normSpeech = normalizeIntentText(speech);
    const yesMisheardAsKan =
      (conversationState === "CHECK_INTEREST" || conversationState === "CLOSE") &&
      normSpeech === "כאן" &&
      String(speech || "").trim().length <= 4;

    // If STT returns a very short / ambiguous single-word answer in the early stage,
    // do NOT call the LLM (it can timeout and cause perceived silence).
    // Instead, deterministically ask the handoff question to move the flow forward.
    const sttVeryShort = String(speech || "").trim().length <= 5;

    // Early short-utterance shortcut (CHECK_INTEREST):
    // If the user likely answered (but STT is noisy), ask the handoff question directly.
    // This avoids LLM timeouts on calls where Whisper returns 3–5 chars like "כאן"/"ביי".
    if (
      conversationState === "CHECK_INTEREST" &&
      sttVeryShort &&
      !detectOptOut(speech) &&
      !detectTransferConsent(speech)
    ) {
      // If it's clearly a goodbye, end politely and mark not_interested.
      if (detectNotInterested(speech)) {
        msLog("deterministic", { callSid, kind: "short_goodbye_end", text: normSpeech });
        await sayFinalAndHangup({ text: "אין בעיה, תודה על הזמן. יום טוב ובשורות טובות.", outcome: "not_interested" });
        return;
      }
      msLog("deterministic", { callSid, kind: "short_ambiguous_ask_handoff", text: normSpeech });
      conversationState = "CLOSE";
      const q = handoffQuestionText();
      try {
        if (callSid && phone) addMessage(db, { callSid, role: "assistant", content: q });
      } catch {}
      await sayText(q, { label: "reply" });
      return;
    }

    // YES/INTEREST handling (two-step close):
    // - If user explicitly says "transfer details" -> close immediately.
    // - Otherwise: first "yes/interest" -> ask the handoff question (CLOSE state),
    //   second "yes" in CLOSE -> confirm + mark lead waiting + hang up.
    if (!detectNotInterested(speech) && !detectOptOut(speech)) {
      if (detectTransferConsent(speech)) {
        confirmCount = 0;
        persuasionAttempted = false;
        msLog("deterministic", { callSid, kind: "handoff_consent_direct" });
        await sayFinalAndHangup({ text: handoffConfirmCloseText({ persona }), outcome: "interested" });
        return;
      }

      if (detectAffirmativeShort(speech) || detectInterested(speech) || yesMisheardAsKan) {
        confirmCount = 0;
        persuasionAttempted = false;

        if (conversationState === "CLOSE") {
          msLog("deterministic", { callSid, kind: "handoff_yes_confirm" });
          await sayFinalAndHangup({ text: handoffConfirmCloseText({ persona }), outcome: "interested" });
          return;
        }

        msLog("deterministic", { callSid, kind: "handoff_yes_ask" });
        conversationState = "CLOSE";
        const q = handoffQuestionText();
        try {
          if (callSid && phone) addMessage(db, { callSid, role: "assistant", content: q });
        } catch {}
        await sayText(q, { label: "reply" });
        return;
      }
    }

    // Hard NO handling: try to persuade ONCE, then end politely.
    if (detectNotInterested(speech) && !detectOptOut(speech)) {
      confirmCount = 0;
      if (!persuasionAttempted) {
        persuasionAttempted = true;
        msLog("deterministic", { callSid, kind: "no_first_try" });
        const oneTry =
          persona === "female"
            ? `מבין לגמרי. לפני שמסיימים — ${handoffQuestionText()}`
            : `מבין לגמרי. לפני שמסיימים — ${handoffQuestionText()}`;
        await sayText(oneTry, { label: "reply" });
        return;
      }
      msLog("deterministic", { callSid, kind: "no_second_end" });
      await sayFinalAndHangup({ text: "אין בעיה, תודה על הזמן. יום טוב ובשורות טובות.", outcome: "not_interested" });
      return;
    }

    // If caller asks us to wait, don't end the call. Keep it natural.
    if (detectWaitRequest(speech)) {
      confirmCount = 0;
      persuasionAttempted = false;
      msLog("deterministic", { callSid, kind: "wait_request", state: conversationState });
      const waitText = "ברור, אני איתך. תגיד לי מתי נוח.";
      try {
        if (callSid && phone) addMessage(db, { callSid, role: "assistant", content: waitText });
      } catch {}
      await sayText(waitText, { label: "reply" });
      return;
    }

    // Barge-in: if caller starts talking mid-playback, we already clear buffered audio.
    // Now handle business logic + LLM response.
    try {
      if (callSid && phone) addMessage(db, { callSid, role: "user", content: speech });
    } catch {}

    // Lead tracking is handled by the LLM structured decision (outcome).
    // We avoid heuristic writes here because short/partial transcriptions can flip-flop status.

    // Opt-out fast path
    if (detectOptOut(speech)) {
      confirmCount = 0;
      msLog("deterministic", { callSid, kind: "opt_out" });
      await sayFinalAndHangup({ text: "אין בעיה, הסרתי אותך. יום טוב.", outcome: "do_not_call" });
      return;
    }

    // STT noise gating: if Whisper returns something tiny, ignore it unless we are in a closing phase.
    if (speech.length < 4) {
      msLog("stt gated (too short)", { callSid, chars: speech.length, state: conversationState });
      await handleUnclearOrEmptySpeech("stt_too_short");
      return;
    }

    // LLM fallback: answer naturally (like chat), but keep it short and always return to handoff.
    try {
      const { knowledgeBase } = settingsSnapshot();
      const knowledgeForThisTurn = selectRelevantKnowledge({
        knowledgeBase,
        query: speech,
        maxChars: 1600,
        maxChunks: 8
      });
      const system = buildHandoffSystemPrompt({ persona, knowledgeBase: knowledgeForThisTurn });
      const history = callSid ? getMessages(db, callSid, { limit: 10 }) : [];
      const messages = [{ role: "system", content: system }, ...history, { role: "user", content: speech }];

      const tLlm0 = Date.now();
      msLog("llm start (fallback)", { callSid, timeoutMs: MS_LLM_TIMEOUT_MS, model: OPENAI_MODEL });
      const llmPromise = createLlmText({
        model: OPENAI_MODEL,
        messages,
        temperature: 0.2,
        maxTokens: 140
      });
      const timeoutPromise = new Promise((resolve) =>
        setTimeout(() => resolve({ __timeout: true }), Math.max(300, MS_LLM_TIMEOUT_MS))
      );
      const raced = await Promise.race([llmPromise, timeoutPromise]);
      const llmTimedOut = !!raced && typeof raced === "object" && raced.__timeout === true;
      if (llmTimedOut) {
        msLog("llm timeout (fallback)", { callSid, ms: Date.now() - tLlm0 });
        const q = handoffQuestionText();
        try {
          if (callSid && phone) addMessage(db, { callSid, role: "assistant", content: q });
        } catch {}
        await sayText(q, { label: "reply" });
        return;
      }
      const llm = raced;
      msLog("timing", { callSid, llmMs: Date.now() - tLlm0 });
      let answer = sanitizeSayText(String(llm?.rawText || "").trim());
      if (!answer) answer = handoffQuestionText();
      // Safety: always end with the handoff question, even if the model forgets.
      if (!answer.includes("רוצה שיחזרו") && !answer.includes("שיחזרו")) {
        answer = `${answer} ${handoffQuestionText()}`.trim();
      }
      answer = limitPhoneReply(answer, 140);

      try {
        if (callSid && phone) addMessage(db, { callSid, role: "assistant", content: answer });
      } catch {}
      await sayText(answer, { label: "reply" });
    } catch (e) {
      msLog("llm fallback failed", e?.message || e);
      await handleUnclearOrEmptySpeech("llm_fallback_failed");
    } finally {
      awaitingReply = false;
      clearThinkingTimer();
    }
    return;
  }

  msLog("ws connected", {
    path: req?.url,
    ua: req?.headers?.["user-agent"],
    ip:
      req?.headers?.["x-forwarded-for"] ||
      req?.headers?.["cf-connecting-ip"] ||
      req?.socket?.remoteAddress ||
      ""
  });

  ws.on("close", () => {
    closed = true;
    stopPlayback({ clear: false });
    // Finalize recording file (if enabled)
    try { finalizeRecorder(); } catch {}
    // If the call ended (hangup mid-call) and we never got explicit consent,
    // mark it as not_interested per product requirement.
    try {
      if (callSid && phone && !leadWaiting) {
        upsertLead(db, { phone, status: "not_interested", callSid, persona });
      }
    } catch {}
    msLog("ws closed", { callSid, streamSid, phone });
  });

  ws.on("error", (e) => {
    msLog("ws error", e?.message || e);
  });

  ws.on("message", (data) => {
    if (closed) return;
    let msg = null;
    try {
      msg = JSON.parse(String(data || ""));
    } catch {
      return;
    }

    const ev = String(msg?.event || "");
    if (ev === "connected") {
      msLog("connected", { protocol: msg?.protocol, version: msg?.version });
      return;
    }

    if (ev === "start") {
      streamSid = String(msg?.start?.streamSid || msg?.streamSid || "");
      callSid = String(msg?.start?.callSid || "");
      const cp = msg?.start?.customParameters || {};
      phone = String(cp?.phone || "");
      persona = String(cp?.persona || "male");
      greeting = String(cp?.greeting || "");
      msLog("start", { callSid, streamSid, phone });

      // Start recording as early as possible (captures the whole call).
      startRecorderIfNeeded();

      try {
        if (callSid && phone) createOrGetCall(db, { callSid, phone, persona });
      } catch {}

      // Speak greeting immediately
      inFlight = (async () => {
        // During greeting: do NOT listen and do NOT barge-in (prevents echo/noise from cutting speech).
        // We will enable listening only after Twilio confirms playback finished via the mark ack.
        allowListen = false;
        allowBargeIn = false;
        pendingEnableListenOnMark = true;

        // Debug: prove audio-out works even without ElevenLabs/OpenAI (beep tone)
        if (MS_TEST_TONE) {
          try {
            const tone = generateUlawTone({ freqHz: 440, ms: 350, amp: 0.25 });
            msLog("test tone", { bytes: tone.length });
            await playUlaw(tone, { label: "testTone" });
          } catch (e) {
            msLog("test tone failed", e?.message || e);
          }
        }

        // Prefer the greeting saved in DB (avoid relying on XML param parsing/normalization).
        let g = sanitizeSayText(String(greeting || "").trim());
        try {
          if (callSid) {
            const row = db
              .prepare(
                `SELECT content FROM call_messages WHERE call_sid = ? AND role = 'assistant' ORDER BY id ASC LIMIT 1`
              )
              .get(callSid);
            const fromDb = sanitizeSayText(String(row?.content || "").trim());
            if (fromDb) g = fromDb;
          }
        } catch {}
        if (!g) g = sanitizeSayText(buildGreeting({ persona }));

        // Persist greeting as the first assistant message once per callSid.
        // This prevents the LLM from "restarting" the call and repeating the intro later.
        try {
          if (callSid) {
            const existing = db
              .prepare(
                `SELECT id FROM call_messages WHERE call_sid = ? AND role = 'assistant' ORDER BY id ASC LIMIT 1`
              )
              .get(callSid);
            if (!existing && g) addMessage(db, { callSid, role: "assistant", content: g });
          }
        } catch {}
        msLog("greeting", { chars: g.length });
        await sayText(g, { label: "greeting" });
        msLog("greeting sent (awaiting mark to enable listening)");

        // If the user already spoke during greeting, and we're already past end-of-speech,
        // finalize immediately (so they don't wait).
        try {
          const now = Date.now();
          if (speechActive && lastVoiceAt && now - lastVoiceAt >= MS_END_SILENCE_MS) {
            const durMs = now - utteranceStartAt;
            const chunks = utterancePcm8kChunks;
            speechActive = false;
            utterancePcm8kChunks = [];
            utteranceStartAt = 0;
            lastVoiceAt = 0;
            if (durMs >= MS_MIN_UTTERANCE_MS) {
              let total = 0;
              for (const c of chunks) total += c.length;
              const pcmAll = new Int16Array(total);
              let o = 0;
              for (const c of chunks) {
                pcmAll.set(c, o);
                o += c.length;
              }
              msLog("utterance finalize after greeting", { callSid, ms: durMs });
              inFlight = (inFlight || Promise.resolve())
                .then(() => transcribeAndRespond(pcmAll))
                .catch(() => {});
            }
          }
        } catch {}
      })().catch((e) => {
        console.warn("[ms] greeting failed", e?.message || e);
        // Fail-open: if greeting fails, still allow the call to proceed.
        allowListen = true;
        allowBargeIn = false;
        agentSpeaking = false;
        msLog("listening enabled (after greeting failure)");
      });
      return;
    }

    if (ev === "media") {
      const track = String(msg?.media?.track || "");
      if (track && track !== "inbound") return;
      const b64 = String(msg?.media?.payload || "");
      if (!b64) return;

      const ulawBuf = Buffer.from(b64, "base64");
      // Recording: capture inbound (caller channel) even while agent is speaking.
      if (recordEnabled && ulawBuf && ulawBuf.length) {
        const b =
          ulawBuf.length === ULaw_FRAME_BYTES
            ? ulawBuf
            : Buffer.concat([ulawBuf, Buffer.alloc(Math.max(0, ULaw_FRAME_BYTES - ulawBuf.length), 0xff)]);
        recInUlawQ.push(b);
      }

      // Echo-safe: while agent is speaking (Twilio still playing our audio), ignore inbound for VAD/STT.
      // This prevents echo from being treated as "user speech" and causing 6–8s waits.
      if (agentSpeaking) return;

      const pcm16 = ulawBufferToPcm16(ulawBuf);
      const rms = rmsFromPcm16(pcm16);
      const now = Date.now();

      inboundFrames++;
      if (rms > inboundMaxRms) inboundMaxRms = rms;

      const adaptiveThr = Math.max(MS_VAD_MIN_RMS, noiseFloor * MS_VAD_NOISE_MULT + MS_VAD_NOISE_MARGIN);
      const calibrating = allowListen && calibrateUntil && now < calibrateUntil;
      const voiced = !calibrating && rms >= adaptiveThr;
      if (voiced) {
        inboundVoicedFrames++;
        lastVoiceAt = now;
        if (!speechActive) {
          // New user utterance begins: cancel any pending "thinking" (it means our end-of-speech detection was early)
          // and reset awaiting flags for the new turn.
          awaitingReply = false;
          thinkingPlayed = false;
          clearThinkingTimer();
          speechActive = true;
          utterancePcm8kChunks = [];
          utteranceStartAt = now;
          utteranceVoicedFrames = 0;
          msLog("speech start", { callSid, rms: Math.round(rms), thr: Math.round(adaptiveThr) });
        }
        utteranceVoicedFrames++;
        utterancePcm8kChunks.push(pcm16);

        // Optional barge-in (disabled by default)
        if (MS_ENABLE_BARGE_IN && playing && allowBargeIn) {
          const inGrace = playingSince && now - playingSince < MS_BARGE_IN_GRACE_MS;
          if (!inGrace) {
            bargeInFrames++;
            if (bargeInFrames >= Math.max(1, MS_BARGE_IN_FRAMES)) {
              if (MS_DEBUG) {
                msLog("barge-in: clear", {
                  label: currentPlay?.label || "",
                  rms: Math.round(rms),
                  frames: bargeInFrames
                });
              }
              bargeInFrames = 0;
              stopPlayback({ clear: true });
            }
          }
        }
      } else if (speechActive) {
        // Keep short trailing silence to help Whisper; but don't grow unbounded.
        if (utterancePcm8kChunks.length < 400) utterancePcm8kChunks.push(pcm16);
      }
      if (!voiced && bargeInFrames > 0) {
        // decay quickly so random noise doesn't accumulate
        bargeInFrames = Math.max(0, bargeInFrames - 1);
      }

      // Update noise floor (EMA) when we think it's silence and we're allowed to listen.
      if (allowListen && !voiced) {
        const alpha = 0.06;
        noiseFloor = noiseFloor ? noiseFloor * (1 - alpha) + rms * alpha : rms;
      }

      if (MS_DEBUG && inboundFrames % 100 === 0) {
        msLog("inbound stats", {
          frames: inboundFrames,
          voiced: inboundVoicedFrames,
          maxRms: Math.round(inboundMaxRms),
          thr: Math.round(adaptiveThr),
          noise: Math.round(noiseFloor),
          calibrating,
          allowListen
        });
        // decay max rms so log stays meaningful over time
        inboundMaxRms = 0;
      }

      // End-of-speech: normal silence threshold OR faster threshold for short utterances ("כן", "אוקיי")
      // Fast end is ONLY for truly short utterances ("כן", "אוקיי"), not for multi-word phrases.
      // Otherwise we risk cutting mid-sentence and degrading STT.
      const fastSilenceOk =
        utteranceStartAt &&
        // Only if it's very short overall (single-word vibe)
        now - utteranceStartAt <= Math.min(MS_FAST_END_MAX_UTTERANCE_MS, 550) &&
        utteranceVoicedFrames > 0 &&
        utteranceVoicedFrames <= 12 &&
        lastVoiceAt &&
        now - lastVoiceAt >= MS_FAST_END_SILENCE_MS;
      const normalSilenceOk = lastVoiceAt && now - lastVoiceAt >= MS_END_SILENCE_MS;
      if (allowListen && speechActive && (fastSilenceOk || normalSilenceOk)) {
        const durMs = now - utteranceStartAt;
        const chunks = utterancePcm8kChunks;
        speechActive = false;
        utterancePcm8kChunks = [];
        utteranceStartAt = 0;
        utteranceVoicedFrames = 0;
        lastVoiceAt = 0;

        // In Guided mode we require longer utterances to avoid "I'm th..." partials from fast endpointing.
        let minMs = MS_MIN_UTTERANCE_MS;
        try {
          const mode = String(settingsSnapshot()?.campaignMode || "handoff").trim() || "handoff";
          if (mode === "guided") minMs = Math.max(minMs, 450);
        } catch {}
        if (durMs < minMs) return;
        // mark end-of-speech moment for latency measurement + conditional backchannel
        lastUtteranceEndAt = Date.now();
        awaitingReply = true;
        thinkingPlayed = false;
        scheduleThinkingOnce();
        msLog("utterance end", { callSid, ms: durMs });

        // Concatenate
        let total = 0;
        for (const c of chunks) total += c.length;
        const pcmAll = new Int16Array(total);
        let o = 0;
        for (const c of chunks) {
          pcmAll.set(c, o);
          o += c.length;
        }

      // Only one pipeline at a time; newer utterances can wait.
        inFlight = (inFlight || Promise.resolve())
          .then(() => transcribeAndRespond(pcmAll))
          .catch(() => {});
      }

      // Fallback: if we have a long utterance and a short pause, force-finalize for faster turn-taking.
      if (
        allowListen &&
        speechActive &&
        utteranceStartAt &&
        MS_ENABLE_FORCE_FINALIZE &&
        now - utteranceStartAt >= MS_FORCE_FINALIZE_AFTER_MS &&
        lastVoiceAt &&
        now - lastVoiceAt >= MS_FORCE_FINALIZE_PAUSE_MS
      ) {
        const durMs = now - utteranceStartAt;
        if (durMs >= MS_MIN_UTTERANCE_MS) {
          const chunks = utterancePcm8kChunks;
          speechActive = false;
          utterancePcm8kChunks = [];
          utteranceStartAt = 0;
          utteranceVoicedFrames = 0;
          lastVoiceAt = 0;
          let total = 0;
          for (const c of chunks) total += c.length;
          const pcmAll = new Int16Array(total);
          let o = 0;
          for (const c of chunks) {
            pcmAll.set(c, o);
            o += c.length;
          }
          msLog("utterance force finalize", { callSid, ms: durMs });
          lastUtteranceEndAt = Date.now();
          awaitingReply = true;
          thinkingPlayed = false;
          scheduleThinkingOnce();
          inFlight = (inFlight || Promise.resolve())
            .then(() => transcribeAndRespond(pcmAll))
            .catch(() => {});
        }
      }

      // Safety: cap utterance length.
      if (allowListen && speechActive && utteranceStartAt && now - utteranceStartAt >= MS_MAX_UTTERANCE_MS) {
        const durMs = now - utteranceStartAt;
        if (durMs < MS_MIN_UTTERANCE_MS) return;
        const chunks = utterancePcm8kChunks;
        speechActive = false;
        utterancePcm8kChunks = [];
        utteranceStartAt = 0;
        utteranceVoicedFrames = 0;
        lastVoiceAt = 0;
        let total = 0;
        for (const c of chunks) total += c.length;
        const pcmAll = new Int16Array(total);
        let o = 0;
        for (const c of chunks) {
          pcmAll.set(c, o);
          o += c.length;
        }
        msLog("utterance max finalize", { callSid, ms: durMs });
        lastUtteranceEndAt = Date.now();
        awaitingReply = true;
        thinkingPlayed = false;
        scheduleThinkingOnce();
        inFlight = (inFlight || Promise.resolve())
          .then(() => transcribeAndRespond(pcmAll))
          .catch(() => {});
      }
      return;
    }

    if (ev === "stop") {
      msLog("stop", { callSid, streamSid });
      return;
    }

    if (ev === "mark") {
      // Twilio acks our marks; this is the moment playback is complete.
      const name = String(msg?.mark?.name || "");
      msLog("rx mark", { name });
      if (pendingEnableListenOnMark && name && name === lastMarkName) {
        pendingEnableListenOnMark = false;
        agentSpeaking = false;
        allowListen = true;
        allowBargeIn = MS_ENABLE_BARGE_IN;
        calibrateUntil = Date.now() + MS_NOISE_CALIBRATION_MS;
        msLog("listening enabled");
      }
      if (pendingHangupOnMark && name && name === hangupMarkName) {
        pendingHangupOnMark = false;
        hangupMarkName = "";
        hangupCallNow().catch(() => {});
      }
      return;
    }
  });
});

// Route WS upgrades by path.
server.on("upgrade", (req, socket, head) => {
  const rawUrl = String(req?.url || "");
  let pathname = rawUrl;
  try {
    pathname = new URL(rawUrl, "http://localhost").pathname;
  } catch {}

  // Logging (helps debug when Twilio can't connect)
  try {
    if (pathname === "/twilio/conversationrelay") {
      crLogAlways("ws upgrade", {
        url: rawUrl,
        ua: req?.headers?.["user-agent"],
        ip:
          req?.headers?.["x-forwarded-for"] ||
          req?.headers?.["cf-connecting-ip"] ||
          req?.socket?.remoteAddress ||
          ""
      });
    }
    if (pathname === "/twilio/mediastream" && MS_DEBUG) {
      console.log("[ms] ws upgrade", {
        url: rawUrl,
        ua: req?.headers?.["user-agent"],
        ip:
          req?.headers?.["x-forwarded-for"] ||
          req?.headers?.["cf-connecting-ip"] ||
          req?.socket?.remoteAddress ||
          ""
      });
    }
  } catch {}

  if (pathname === "/twilio/conversationrelay") {
    wssConversationRelay.handleUpgrade(req, socket, head, (ws) => {
      wssConversationRelay.emit("connection", ws, req);
    });
    return;
  }
  if (pathname === "/twilio/mediastream") {
    wssMediaStream.handleUpgrade(req, socket, head, (ws) => {
      wssMediaStream.emit("connection", ws, req);
    });
    return;
  }

  // Unknown WS path
  try {
    socket.destroy();
  } catch {}
});

server.listen(PORT, HOST, () => {
  console.log(`Server listening on ${HOST}:${PORT}`);

  // Pre-warm common TTS outputs to avoid a long first-call silence.
  // This runs in the background and fills the cache (data/tts-cache).
  (async () => {
    try {
      const maleGreeting = sanitizeSayText(buildGreeting({ persona: "male" }));
      const femaleGreeting = sanitizeSayText(buildGreeting({ persona: "female" }));
      await Promise.all([
        // Prewarm both greeting variants in the configured agent voice.
        ttsToPath({ text: maleGreeting, persona: AGENT_VOICE_PERSONA }),
        ttsToPath({ text: femaleGreeting, persona: AGENT_VOICE_PERSONA })
      ]);
      if (DEBUG_TTS) console.log("[tts] prewarm complete");
    } catch (e) {
      console.warn("[tts] prewarm failed:", e?.message || e);
    }
  })();

  // Prewarm ElevenLabs ulaw greeting (Media Streams) to reduce first-call latency after deploy.
  (async () => {
    try {
      const { openingScript, openingScriptMale, openingScriptFemale } = settingsSnapshot();
      const gMale = sanitizeSayText(String(openingScriptMale || openingScript || "").trim() || buildGreeting({ persona: "male" }));
      const gFemale = sanitizeSayText(
        String(openingScriptFemale || openingScript || "").trim() || buildGreeting({ persona: "female" })
      );
      const fixed = [
        "ברור, אני איתך. תגיד לי מתי נוח.",
        "שנייה אני איתך.",
        "אין בעיה, הסרתי אותך. סליחה על ההפרעה ויום טוב.",
        handoffConfirmCloseText({ persona: "male" })
      ];
      await Promise.all([
        elevenlabsTtsToUlaw8000({ text: gMale, persona: AGENT_VOICE_PERSONA }),
        elevenlabsTtsToUlaw8000({ text: gFemale, persona: AGENT_VOICE_PERSONA }),
        ...fixed.map((t) => elevenlabsTtsToUlaw8000({ text: sanitizeSayText(t), persona: AGENT_VOICE_PERSONA }))
      ]);
      if (MS_DEBUG) console.log("[ms] ulaw prewarm complete");
    } catch (e) {
      if (MS_DEBUG) console.warn("[ms] ulaw prewarm failed:", e?.message || e);
    }
  })();

  ensureDialerRunning();
});


