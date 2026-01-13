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
import { buildSystemPrompt, buildGreeting } from "./prompts.js";
import { buildPlayAndHangup, buildRecordTwiML, buildSayAndHangup } from "./twiml.js";
import { geminiTtsToFile } from "./gemini_tts.js";
import { renderAdminPage } from "./admin_page.js";
import { normalizePhoneE164IL } from "./phone.js";

const PORT = Number(process.env.PORT || 3000);

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
// Default to a high-quality chat model. You can override via OPENAI_MODEL in env.
const OPENAI_MODEL = process.env.OPENAI_MODEL || "gpt-4.1";
const OPENAI_STT_MODEL = process.env.OPENAI_STT_MODEL || "whisper-1";
const OPENAI_STT_PROMPT =
  process.env.OPENAI_STT_PROMPT ||
  "תמלול שיחה טלפונית בעברית. מילים נפוצות: חדרה, קהילה, לשכה, שיעור תורה, הפרשת חלה, רישום, כתובת, יום, שעה, עלות, תרומה, תודה.";
const OPENAI_TTS_MODEL = process.env.OPENAI_TTS_MODEL || "gpt-4o-mini-tts";
const OPENAI_TTS_VOICE_MALE = process.env.OPENAI_TTS_VOICE_MALE || "alloy";
const OPENAI_TTS_VOICE_FEMALE = process.env.OPENAI_TTS_VOICE_FEMALE || "alloy";
const TTS_PROVIDER = (process.env.TTS_PROVIDER || "").toLowerCase(); // openai | elevenlabs | google
const GOOGLE_TTS_API_KEY = process.env.GOOGLE_TTS_API_KEY || "";
const GOOGLE_TTS_VOICE_NAME = process.env.GOOGLE_TTS_VOICE_NAME || "he-IL-Wavenet-A"; // typically female
const GEMINI_API_KEY = process.env.GEMINI_API_KEY || "";
const GEMINI_TTS_MODEL = process.env.GEMINI_TTS_MODEL || "gemini-2.5-pro-preview-tts";
const GEMINI_TTS_VOICE_NAME = process.env.GEMINI_TTS_VOICE_NAME || "Zephyr";
const DEBUG_TTS = process.env.DEBUG_TTS === "1";
const GEMINI_TTS_TIMEOUT_MS = Number(process.env.GEMINI_TTS_TIMEOUT_MS || 8000);
// Default OFF: keep answers driven by KB + LLM (more natural). Set USE_FAQ_RULES=1 to re-enable deterministic FAQ replies.
const USE_FAQ_RULES = process.env.USE_FAQ_RULES === "1";
const TTS_POLL_MAX = Number(process.env.TTS_POLL_MAX || 20);
const TTS_POLL_WAIT_SECONDS = Number(process.env.TTS_POLL_WAIT_SECONDS || 1);
const REALTIME_MODE = process.env.REALTIME_MODE === "1";
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
const ELEVENLABS_MODEL_ID = process.env.ELEVENLABS_MODEL_ID || "eleven_multilingual_v2";
const IS_ELEVEN_V3 = String(ELEVENLABS_MODEL_ID).toLowerCase() === "eleven_v3";
function parseFiniteEnvNumber(raw, fallback) {
  const v = Number(raw);
  return Number.isFinite(v) ? v : fallback;
}
// Match UI defaults:
// - Multilingual v2: stability 0.4, similarity 0.8, style 0
// - Eleven v3 (alpha): stability 0.5 (UI shows only stability)
const ELEVENLABS_STABILITY = parseFiniteEnvNumber(
  process.env.ELEVENLABS_STABILITY,
  IS_ELEVEN_V3 ? 0.5 : 0.4
);
const ELEVENLABS_SIMILARITY_BOOST = parseFiniteEnvNumber(process.env.ELEVENLABS_SIMILARITY_BOOST, 0.8);
const ELEVENLABS_STYLE = parseFiniteEnvNumber(process.env.ELEVENLABS_STYLE, 0); // 0..1 (optional)
const ELEVENLABS_SPEAKER_BOOST =
  process.env.ELEVENLABS_SPEAKER_BOOST === "1" || process.env.ELEVENLABS_SPEAKER_BOOST === "true";

// Force a deterministic MP3 output (helps Twilio playback quality; avoids VBR surprises).
// ElevenLabs docs: codec_sample_rate_bitrate (e.g. mp3_44100_128, mp3_22050_32, etc.)
const ELEVENLABS_OUTPUT_FORMAT = String(process.env.ELEVENLABS_OUTPUT_FORMAT || "mp3_44100_128").trim();
// ISO 639-1 (e.g. "he"). Enforces language + normalization.
// Leave empty by default to match ElevenLabs UI behavior; set in env if you want to force Hebrew ("he").
const ELEVENLABS_LANGUAGE_CODE = String(process.env.ELEVENLABS_LANGUAGE_CODE || "").trim();

// Product requirement: agent voice is always male. We still adapt grammar to the callee's gender.
const AGENT_VOICE_PERSONA = "male";

// ConversationRelay (Realtime voice via Twilio)
const CR_ENABLED = REALTIME_MODE;
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
// STT provider/model: for some languages (e.g. he-IL) Twilio requires these to be explicitly set
// or it will fail with: "Incomplete value set in TwiML for language ...".
// Defaults are chosen to be robust for Hebrew.
const CR_TRANSCRIPTION_PROVIDER = String(process.env.CR_TRANSCRIPTION_PROVIDER || "Deepgram").trim(); // Google | Deepgram
const CR_SPEECH_MODEL = String(process.env.CR_SPEECH_MODEL || "nova-2-general").trim(); // Deepgram: nova-2-general|nova-3-general, Google: telephony|long|...
const CR_INTERRUPTIBLE = String(process.env.CR_INTERRUPTIBLE || "speech").trim(); // none|dtmf|speech|any
const CR_INTERRUPT_SENSITIVITY = String(process.env.CR_INTERRUPT_SENSITIVITY || "high").trim(); // low|medium|high
const CR_DEBUG = String(process.env.CR_DEBUG || "").trim(); // e.g. "debugging speaker-events tokens-played"

function primaryLangTag(code) {
  const s = String(code || "").trim();
  if (!s) return "";
  return s.split("-")[0] || s;
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
  welcomeGreeting,
  welcomeGreetingInterruptible,
  interruptible,
  interruptSensitivity,
  debug,
  customParameters = {}
}) {
  const paramsXml = Object.entries(customParameters)
    .filter(([k, val]) => k && val != null && String(val).length)
    .map(([k, val]) => `<Parameter name="${escapeXmlAttr(k)}" value="${escapeXmlAttr(val)}" />`)
    .join("");

  const attrs = [
    `url="${escapeXmlAttr(wsUrl)}"`,
    language ? `language="${escapeXmlAttr(language)}"` : "",
    ttsLanguage ? `ttsLanguage="${escapeXmlAttr(ttsLanguage)}"` : "",
    ttsProvider ? `ttsProvider="${escapeXmlAttr(ttsProvider)}"` : "",
    voice ? `voice="${escapeXmlAttr(voice)}"` : "",
    transcriptionLanguage ? `transcriptionLanguage="${escapeXmlAttr(transcriptionLanguage)}"` : "",
    transcriptionProvider ? `transcriptionProvider="${escapeXmlAttr(transcriptionProvider)}"` : "",
    speechModel ? `speechModel="${escapeXmlAttr(speechModel)}"` : "",
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
    <ConversationRelay ${attrs}>${paramsXml}</ConversationRelay>
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
כללי
- אנחנו מתקשרים ממשרד הקהילה בחדרה לאנשים שאישרו מראש לקבל שיחה.
- המטרה: להזמין גברים לשיעור תורה בחדרה, ונשים להפרשת חלה בחדרה.
- אם אין פרטים מדויקים (יום/שעה/כתובת) — לא להמציא. להגיד: "הלשכה שלנו תתן את כל הפרטים בהרשמה".
- אם מבקשים להסיר/לא להתקשר: לאשר מיד ולסיים בנימוס.

הפרשת חלה — הסבר קצר ופשוט
- הפרשת חלה זו מצווה יפה של נשים (וגם גברים שמכינים בצק), כשאופים/מכינים בצק בכמות מסוימת.
- ברעיון של "אירוע הפרשת חלה": מתכנסות נשים, שומעות כמה דקות חיזוק, מתפללות יחד, מפרישות חלה ומבקשות ישועות.
- זה מתאים גם למי שלא "חרדית" — אווירה טובה, פשוטה, מחברת.

שאלות נפוצות על הפרשת חלה (תשובות קצרות)
- "מה עושים שם?" → "מתכנסות בחדרה, קצת חיזוק, הפרשת חלה ותפילה קצרה. הכל באווירה נעימה."
- "זה חובה?" → "לא חובה, זה הזמנה. מי שרוצה באה, מי שלא—הכל בסדר."
- "צריך להביא משהו?" → "בדרך כלל לא. אם צריך משהו מיוחד, הלשכה תעדכן בהרשמה."
- "זה רק דתיות?" → "ממש לא. באות נשים מכל הסוגים."
- "כמה זמן זה?" → "בדרך כלל סביב שעה–שעה וחצי. הלשכה תדייק לפי האירוע."
- "כמה זה עולה?" → "אם יש עלות/תרומה—הלשכה תגיד לך בהרשמה. אני לא רוצה להמציא."
- "איפה בחדרה?" → "הלשכה נותנת כתובת מדויקת בהרשמה."

שיעור תורה לגברים — הסבר קצר ופשוט
- שיעור תורה הוא מפגש לגברים בחדרה: לימוד קצר וברור, שאלות ותשובות, אווירה טובה.
- מתאים גם למי שלא למד הרבה שנים — מסבירים פשוט, בלי לחץ.

שאלות נפוצות על שיעור תורה (תשובות קצרות)
- "על מה השיעור?" → "דברים פרקטיים לחיים מהתורה וההלכה, בשפה פשוטה. הלשכה תיתן את הנושא המדויק."
- "אני לא דתי" → "הכל טוב אחי, זה פתוח לכולם. באים לשמוע, להתחזק קצת, בלי התחייבות."
- "אין לי זמן" → "מבין לגמרי. זה קצר ונעים—אם תרצה, תבוא פעם אחת לנסות."
- "איפה זה בחדרה?" → "הלשכה תיתן כתובת ושעה בהרשמה."
- "כמה זמן זה?" → "בד״כ 45–60 דק'. הלשכה תדייק."
- "מי הרב/מי מעביר?" → "הלשכה תמסור את השם והפרטים המדויקים בהרשמה."

כללי התנהלות בשיחה
- לדבר בעברית יום-יומית, לא גבוהה מדי, אבל מכבדת.
- לשאול שאלת סגירה קצרה: "בא לך להצטרף פעם אחת ולנסות?"
- אם יש עניין: להעביר ללשכה לרישום (ולסיים את השיחה).
`.trim();

// הסוכן תמיד בקול גברי; הדקדוק ללקוח/ה מותאם לפי המין שלהם/שלה.
const DEFAULT_OPENING_MALE = `שלום אחי, מדבר ממשרד הקהילה בחדרה. רציתי להזמין אותך לשיעור תורה קרוב—יש לך דקה?`;
const DEFAULT_OPENING_FEMALE = `שלום יקרה, מדבר ממשרד הקהילה בחדרה. רציתי להזמין אותך להפרשת חלה קרובה—יש לך דקה?`;

const DEFAULT_MIDDLE_MALE = `
מטרת האמצע: להבין מהר אם יש עניין, לענות קצר, ולהציע לבוא פעם אחת לנסות.

הסבר קצר על השיעור (אם שואלים "מה זה בדיוק?" או "על מה מדברים?"):
- "זה שיעור תורה קצר וברור בחדרה, באווירה טובה. מדברים על דברים פרקטיים לחיים—אמונה, זוגיות, פרנסה, הלכה פשוטה—וגם יש זמן לשאלות. לא חייבים ידע קודם."
- "בדרך כלל זה 45–60 דקות, יש כיבוד קל, ואפשר לבוא פעם אחת רק לנסות. אם תרצה—הלשכה תתן שעה/מקום מדויק."

אם שואלים "מי זה?/מאיפה יש לכם את המספר?"
- "אני ממשרד הקהילה בחדרה. המספר אצלנו ברשימה של אנשים שאישרו לקבל עדכון. אם לא מתאים—אני מוריד אותך."

אם אומרים "אין לי זמן"
- "מבין. זה קצר ונעים. רוצה לנסות פעם אחת, רק לראות אם זה מתאים?"

אם אומרים "אני לא דתי"
- "הכל טוב אחי, זה פתוח לכולם. באים לשמוע ולהתחזק קצת, בלי התחייבות."

אם שואלים "איפה/מתי?"
- "הפרטים המדויקים אצל הלשכה—אני יכולה להעביר אותך לרישום והם יגידו שעה וכתובת."

אם יש הסכמה/עניין
- "מעולה. אז אני מעביר עכשיו את הפרטים שלך ללשכה שלנו לרישום ופרטים, בסדר?"

אם מבקשים להסיר/לא להתקשר
- "בטח, מוריד אותך עכשיו. תודה רבה ויום טוב."
`.trim();

const DEFAULT_MIDDLE_FEMALE = `
מטרת האמצע: להסביר בקצרה מה זה, להרגיע חששות, ולהזמין לבוא פעם אחת.

הסבר קצר על האירוע (אם שואלים "מה עושים שם?" או "מה זה בכלל?"):
- "זה מפגש נשים נעים בחדרה: כמה דקות חיזוק, הפרשת חלה ותפילה קצרה. אווירה טובה ומכבדת, לא מלחיץ—באמת באות נשים מכל הסוגים."
- "בדרך כלל זה בערך שעה–שעה וחצי, יש אווירה משפחתית וקצת כיבוד. אם תרצי—הלשכה תתן שעה/מקום מדויק."

אם שואלים "מי זה?/מאיפה יש לכם את המספר?"
- "אני ממשרד הקהילה בחדרה. המספר אצלנו ברשימה של אנשים שאישרו לקבל עדכון. אם לא מתאים—אני מוריד אותך."

אם שואלים "מה זה הפרשת חלה?"
- "מפגש נשים בחדרה, קצת חיזוק, הפרשת חלה ותפילה קצרה. אווירה טובה, באמת."

אם אומרים "אני לא דתייה"
- "זה בסדר גמור, זה פתוח לכולן. באות נשים מכל הסוגים."

אם אומרים "אין לי זמן"
- "מבין. זה לא ארוך. ואם תרצי—תבואי פעם אחת רק לראות."

אם שואלים "איפה/מתי?/כמה עולה?"
- "אני לא רוצה להמציא. הלשכה נותנת את כל הפרטים בהרשמה—שעה, כתובת ואם יש עלות."

אם יש הסכמה/עניין
- "מעולה יקרה. אז להעביר אותך עכשיו ללשכה שלנו לרישום?"

אם מבקשים להסיר/לא להתקשר
- "בטח, מוריד אותך עכשיו. תודה רבה ויום טוב."
`.trim();

const DEFAULT_CLOSING_MALE = `
אם הבן אדם רוצה/מסכים:
- "מעולה אחי. אני מעביר עכשיו את הפרטים שלך ללשכה שלנו לרישום ופרטים, והם יחזרו אליך. תודה רבה!"
- ואז לסיים שיחה.

אם הבן אדם לא רוצה:
- "הבנתי, אין בעיה. תודה על הזמן, יום טוב."
- ואז לסיים שיחה.
`.trim();

const DEFAULT_CLOSING_FEMALE = `
אם היא רוצה/מסכימה:
- "מדהים יקרה. אני מעביר עכשיו את הפרטים שלך ללשכה שלנו לרישום ופרטים, והם יחזרו אלייך. תודה רבה!"
- ואז לסיים שיחה.

אם היא לא רוצה:
- "הבנתי, אין בעיה. תודה על הזמן, יום טוב."
- ואז לסיים שיחה.
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

// If the user already edited scripts, keep them—but add a small helpful "explanation" block once.
appendIfMissing("middleScriptMale", {
  marker: "הסבר קצר על השיעור",
  snippet: `הסבר קצר על השיעור (אם שואלים "מה זה בדיוק?" או "על מה מדברים?"):\n- "זה שיעור תורה קצר וברור בחדרה, באווירה טובה. מדברים על דברים פרקטיים לחיים—אמונה, זוגיות, פרנסה, הלכה פשוטה—וגם יש זמן לשאלות. לא חייבים ידע קודם."\n- "בדרך כלל זה 45–60 דקות, יש כיבוד קל, ואפשר לבוא פעם אחת רק לנסות. אם תרצה—הלשכה תתן שעה/מקום מדויק."`
});
appendIfMissing("middleScriptFemale", {
  marker: "הסבר קצר על האירוע",
  snippet: `הסבר קצר על האירוע (אם שואלים "מה עושים שם?" או "מה זה בכלל?"):\n- "זה מפגש נשים נעים בחדרה: כמה דקות חיזוק, הפרשת חלה ותפילה קצרה. אווירה טובה ומכבדת, לא מלחיץ—באמת באות נשים מכל הסוגים."\n- "בדרך כלל זה בערך שעה–שעה וחצי, יש אווירה משפחתית וקצת כיבוד. אם תרצי—הלשכה תתן שעה/מקום מדויק."`
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

function detectInterested(text) {
  const t = String(text || "").toLowerCase();
  const patterns = [
    "מעוניין",
    "מעוניינת",
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
    "תעבירי ללשכה",
    "תעבירו ללשכה"
  ];
  return patterns.some((p) => t.includes(p));
}

function detectNotInterested(text) {
  const t = String(text || "").toLowerCase();
  const patterns = [
    "לא רוצה",
    "לא מעוניין",
    "לא מעוניינת",
    "לא מתאים",
    "לא תודה",
    "עזוב",
    "עזבי",
    "אין לי זמן",
    "לא עכשיו",
    "אולי אחר כך"
  ];
  return patterns.some((p) => t.includes(p));
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
        "אני מהלשכה של הקהילה בחדרה. המספר אצלנו ברשימה של אנשים שאישרו לקבל עדכון. אם לא מתאים לך—אני מוריד אותך מיד, בסדר?",
      end: false
    };
  }
  if (faq.what) {
    if (persona === "female") {
      return {
        text:
          "זה מפגש נשים נעים בחדרה: כמה דקות חיזוק, הפרשת חלה ותפילה קצרה. אווירה טובה ומכבדת. רוצה שאעביר את הפרטים שלך ללשכה שיחזרו אלייך עם שעה ומקום?",
        end: false
      };
    }
    return {
      text:
        "זה שיעור תורה קצר וברור בחדרה, באווירה טובה, עם זמן לשאלות. לא צריך ידע קודם. רוצה שאעביר את הפרטים שלך ללשכה שיחזרו אליך עם שעה ומקום?",
      end: false
    };
  }
  if (faq.where || faq.when || faq.cost || faq.howLong) {
    const toYou = persona === "female" ? "אלייך" : "אליך";
    return {
      text:
        `את הפרטים המדויקים—שעה, מקום ואם יש עלות—הלשכה נותנת בהרשמה. רוצה שאעביר עכשיו את הפרטים שלך ללשכה שלנו שיחזרו ${toYou}?`,
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
    autoDialIntervalSeconds
  };
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
      // Eleven v3 (alpha) UI exposes only Stability; keep request minimal to match it closely.
      voice_settings: IS_ELEVEN_V3
        ? {
            stability: Math.max(0, Math.min(1, ELEVENLABS_STABILITY))
          }
        : {
            stability: Math.max(0, Math.min(1, ELEVENLABS_STABILITY)),
            similarity_boost: Math.max(0, Math.min(1, ELEVENLABS_SIMILARITY_BOOST)),
            // Optional knobs (some voices/models benefit from these):
            ...(Number.isFinite(ELEVENLABS_STYLE) ? { style: Math.max(0, Math.min(1, ELEVENLABS_STYLE)) } : {}),
            ...(ELEVENLABS_SPEAKER_BOOST ? { use_speaker_boost: true } : {})
          }
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

async function googleTtsToFile({ text }) {
  if (!GOOGLE_TTS_API_KEY) return null;

  const key = crypto
    .createHash("sha256")
    .update(`google::${GOOGLE_TTS_VOICE_NAME}::${text}`)
    .digest("hex");
  const filename = `${key}.mp3`;
  const outPath = path.join(ttsDir, filename);
  if (fs.existsSync(outPath)) return `/tts/${filename}`;

  const url = `https://texttospeech.googleapis.com/v1/text:synthesize?key=${encodeURIComponent(
    GOOGLE_TTS_API_KEY
  )}`;
  const payload = {
    input: { text },
    voice: { languageCode: "he-IL", name: GOOGLE_TTS_VOICE_NAME },
    audioConfig: { audioEncoding: "MP3" }
  };

  const res = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload)
  });

  if (!res.ok) {
    const errText = await res.text().catch(() => "");
    console.warn("Google TTS failed:", res.status, errText);
    return null;
  }

  const json = await res.json();
  const audioContent = json?.audioContent;
  if (!audioContent) return null;

  fs.writeFileSync(outPath, Buffer.from(audioContent, "base64"));
  return `/tts/${filename}`;
}

function computeTtsCacheKey({ provider, text, persona }) {
  const p = String(provider || "").toLowerCase();
  if (p === "gemini" || p === "google_ai_studio") {
    return crypto
      .createHash("sha256")
      .update(`gemini::${GEMINI_TTS_MODEL}::${GEMINI_TTS_VOICE_NAME}::${text}`)
      .digest("hex");
  }
  if (p === "google") {
    return crypto
      .createHash("sha256")
      .update(`google::${GOOGLE_TTS_VOICE_NAME}::${text}`)
      .digest("hex");
  }
  if (p === "elevenlabs") {
    const voiceId =
      (persona === "female" ? ELEVENLABS_VOICE_FEMALE : ELEVENLABS_VOICE_MALE) || ELEVENLABS_VOICE_ID;
    // Include model + settings so changing env doesn't keep serving stale cached audio.
    const settingsSig = IS_ELEVEN_V3
      ? [ELEVENLABS_MODEL_ID, ELEVENLABS_OUTPUT_FORMAT || "", ELEVENLABS_LANGUAGE_CODE || "", ELEVENLABS_STABILITY].join(
          "|"
        )
      : [
          ELEVENLABS_MODEL_ID,
          ELEVENLABS_OUTPUT_FORMAT || "",
          ELEVENLABS_LANGUAGE_CODE || "",
          Number.isFinite(ELEVENLABS_STABILITY) ? ELEVENLABS_STABILITY : "",
          Number.isFinite(ELEVENLABS_SIMILARITY_BOOST) ? ELEVENLABS_SIMILARITY_BOOST : "",
          Number.isFinite(ELEVENLABS_STYLE) ? ELEVENLABS_STYLE : "",
          ELEVENLABS_SPEAKER_BOOST ? "boost" : "no-boost"
        ].join("|");
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
    if (p === "gemini" || p === "google_ai_studio") {
      await geminiTtsToFile({
        apiKey: GEMINI_API_KEY,
        model: GEMINI_TTS_MODEL,
        voiceName: GEMINI_TTS_VOICE_NAME,
        text,
        outDir: ttsDir
      });
      return;
    }
    if (p === "google") {
      await googleTtsToFile({ text });
      return;
    }
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

  // בחירת ספק מפורשת (מומלץ כדי להימנע מהפתעות)
  if (TTS_PROVIDER === "gemini" || TTS_PROVIDER === "google_ai_studio") {
    const geminiPromise = geminiTtsToFile({
      apiKey: GEMINI_API_KEY,
      model: GEMINI_TTS_MODEL,
      voiceName: GEMINI_TTS_VOICE_NAME,
      text,
      outDir: ttsDir
    });
    const timeoutPromise = new Promise((resolve) =>
      setTimeout(() => resolve(null), GEMINI_TTS_TIMEOUT_MS)
    );
    const out = await Promise.race([geminiPromise, timeoutPromise]);
    debug("gemini", out);
    if (out) return out;
    // fallback מהיר כדי שלא יהיה "שקט" / timeout של Twilio
    const fb = await openaiTtsToFile({ text, persona });
    debug("openai(fallback_from_gemini)", fb);
    return fb;
  }
  if (TTS_PROVIDER === "google") {
    const out = await googleTtsToFile({ text });
    debug("google", out);
    return out;
  }
  if (TTS_PROVIDER === "elevenlabs") {
    const out = await elevenlabsTtsToFile({ text, persona });
    debug("elevenlabs", out);
    return out;
  }
  if (TTS_PROVIDER === "openai") {
    const out = await openaiTtsToFile({ text, persona });
    debug("openai", out);
    return out;
  }

  // ברירת מחדל: ElevenLabs אם הוגדר, אחרת OpenAI
  const eleven = await elevenlabsTtsToFile({ text, persona });
  if (eleven) {
    debug("elevenlabs(fallback)", eleven);
    return eleven;
  }
  const out = await openaiTtsToFile({ text, persona });
  debug("openai(fallback)", out);
  return out;
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

app.get("/health", (req, res) => res.json({ ok: true }));

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
    closingScriptFemale = ""
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

// Leads (won/lost)
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

      // Twilio validation may reject ElevenLabs + language="he-IL".
      // Workaround:
      // - Set transcriptionLanguage for STT explicitly (he-IL)
      // - Set ttsLanguage="multi" for ElevenLabs (so ElevenLabs can detect language from tokens)
      // - Omit "language" to avoid the invalid combination validation.
      const transcriptionLanguage = CR_TRANSCRIPTION_LANGUAGE || CR_LANGUAGE;
      let languageAttr = CR_LANGUAGE;
      let ttsLanguageAttr = CR_TTS_LANGUAGE;
      if (String(CR_TTS_PROVIDER).toLowerCase() === "elevenlabs" && /^he(-|$)/i.test(transcriptionLanguage)) {
        languageAttr = "";
        if (!ttsLanguageAttr) ttsLanguageAttr = "multi";
      }

      const xml = buildConversationRelayTwiML({
        wsUrl,
        language: languageAttr,
        ttsLanguage: ttsLanguageAttr,
        ttsProvider: CR_TTS_PROVIDER,
        voice: CR_VOICE,
        transcriptionLanguage,
        transcriptionProvider: CR_TRANSCRIPTION_PROVIDER,
        speechModel: CR_SPEECH_MODEL,
        welcomeGreeting: greeting,
        welcomeGreetingInterruptible: CR_INTERRUPTIBLE,
        interruptible: CR_INTERRUPTIBLE,
        interruptSensitivity: CR_INTERRUPT_SENSITIVITY,
        debug: CR_DEBUG,
        customParameters: {
          callSid,
          phone,
          persona
        }
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
          text: "לא שמעתי אותך טוב. אפשר להגיד שוב? אני מקשיב.",
          persona,
          hangup: false,
          retry: retry + 1
        });
        return;
      }
      await respondWithPlayAndMaybeHangup(req, res, { text: "לא שמעתי אותך טוב. תודה רבה ויום טוב.", persona, hangup: true });
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
          text: "סליחה, לא הצלחתי לשמוע אותך. אפשר להגיד שוב?",
          persona,
          hangup: false,
          retry: retry + 1
        });
        return;
      }
      await respondWithPlayAndMaybeHangup(req, res, { text: "סליחה, לא הצלחתי לשמוע אותך. יום טוב.", persona, hangup: true });
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
          text: "לא הבנתי אותך טוב. אפשר לחזור שוב במשפט שלם? אני מקשיב.",
          persona,
          hangup: false,
          retry: retry + 1
        });
        return;
      }
      await respondWithPlayAndMaybeHangup(req, res, { text: "לא שמעתי אותך טוב. תודה רבה ויום טוב.", persona, hangup: true });
      return;
    }

    if (detectOptOut(speech)) {
      markDoNotCall(db, phone);
      // Track as "lost" lead (they explicitly asked to stop).
      try {
        upsertLead(db, { phone, status: "lost", callSid, persona });
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
        upsertLead(db, { phone, status: interested ? "won" : "lost", callSid, persona });
      } catch {}
      const toYou = persona === "female" ? "אלייך" : "אליך";
      const picked =
        (interested
          ? `מעולה. אני מעביר עכשיו את הפרטים שלך ללשכה שלנו, והם יחזרו ${toYou} בהקדם עם רישום ופרטים. תודה רבה!`
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

    const system = buildSystemPrompt({ persona, knowledgeBase: knowledgeForThisTurn });
    const history = getMessages(db, callSid, { limit: 10 });
    const messages = [{ role: "system", content: system }, ...history];

    const completion = await openai.chat.completions.create({
      model: OPENAI_MODEL,
      messages,
      temperature: 0.3,
      max_tokens: 120
    });

    const answer = sanitizeSayText((completion.choices?.[0]?.message?.content || "").trim() || "תודה רבה, יום טוב.");
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
          sayText: "רגע אחד, אני בודקת... אפשר להגיד שוב?",
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
      sayText: "סליחה, הייתה תקלה קטנה. אפשר להגיד שוב?",
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

async function streamAssistantToConversationRelay({
  ws,
  callSid,
  persona,
  userText
}) {
  if (!openai) {
    const lang = primaryLangTag(CR_TRANSCRIPTION_LANGUAGE || CR_LANGUAGE) || "he";
    wsSendJson(ws, { type: "text", token: "בשביל להמשיך את השיחה החכמה צריך להגדיר מפתח מערכת.", lang, last: true });
    return;
  }

  const { knowledgeBase } = settingsSnapshot();
  const knowledgeForThisTurn = selectRelevantKnowledge({ knowledgeBase, query: userText });
  const system = buildSystemPrompt({ persona, knowledgeBase: knowledgeForThisTurn });

  // Persist user message
  addMessage(db, { callSid, role: "user", content: userText });

  const history = getMessages(db, callSid, { limit: 10 });
  const messages = [{ role: "system", content: system }, ...history];

  // Stream tokens to Twilio as soon as they arrive.
  const stream = await openai.chat.completions.create({
    model: OPENAI_MODEL,
    messages,
    temperature: 0.3,
    max_tokens: 180,
    stream: true
  });

  let full = "";
  let pending = "";
  for await (const chunk of stream) {
    const delta = chunk?.choices?.[0]?.delta?.content ?? "";
    if (!delta) continue;
    full += delta;
    // Hold back one token chunk so we can mark the last chunk with last:true.
    if (pending) {
      const lang = primaryLangTag(CR_TRANSCRIPTION_LANGUAGE || CR_LANGUAGE) || "he";
      wsSendJson(ws, { type: "text", token: pending, lang, last: false, interruptible: true, preemptible: true });
    }
    pending = delta;
  }

  if (!pending && !full) {
    pending = "תודה רבה, יום טוב.";
    full = pending;
  }

  {
    const lang = primaryLangTag(CR_TRANSCRIPTION_LANGUAGE || CR_LANGUAGE) || "he";
    wsSendJson(ws, { type: "text", token: pending, lang, last: true, interruptible: true, preemptible: true });
  }

  const answer = sanitizeSayText(full.trim());
  addMessage(db, { callSid, role: "assistant", content: answer });
}

// Render (וגם רוב ספקי ה-hosting) דורשים bind ל-0.0.0.0 כדי שה-Service יהיה נגיש מבחוץ.
// בלוקאלי אפשר להישאר על 127.0.0.1.
const HOST = process.env.HOST || (process.env.RENDER ? "0.0.0.0" : "127.0.0.1");

const server = http.createServer(app);

// ConversationRelay WebSocket server (only used when REALTIME_MODE=1)
const wssConversationRelay = new WebSocketServer({ server, path: "/twilio/conversationrelay" });
wssConversationRelay.on("connection", (ws) => {
  // Session state per socket
  let callSid = "";
  let phone = "";
  let persona = "male";
  let inFlight = null; // Promise
  let closed = false;

  ws.on("close", () => {
    closed = true;
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
            upsertLead(db, { phone, status: interested ? "won" : "lost", callSid, persona });
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
        wsSendJson(ws, { type: "text", token: "סליחה, הייתה תקלה קטנה. אפשר להגיד שוב?", last: true });
        if (DEBUG_TTS) console.warn("[cr] handler error:", e?.message || e);
      });
    }
  });
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
        // Prewarm both greeting variants, but always in the agent's (male) voice.
        ttsToPath({ text: maleGreeting, persona: AGENT_VOICE_PERSONA }),
        ttsToPath({ text: femaleGreeting, persona: AGENT_VOICE_PERSONA })
      ]);
      if (DEBUG_TTS) console.log("[tts] prewarm complete");
    } catch (e) {
      console.warn("[tts] prewarm failed:", e?.message || e);
    }
  })();

  ensureDialerRunning();
});


