import "dotenv/config";
import twilio from "twilio";
import { openDb } from "../src/db.js";
import { normalizePhoneE164IL } from "../src/phone.js";

const DB_PATH = process.env.DB_PATH || "./data/app.db";
const BASE_URL = process.env.BASE_URL || "";
const VOICE_WEBHOOK_URL = process.env.VOICE_WEBHOOK_URL || "";
const FROM = process.env.TWILIO_FROM_NUMBER || "";
const ACCOUNT_SID = process.env.TWILIO_ACCOUNT_SID || "";
const AUTH_TOKEN = process.env.TWILIO_AUTH_TOKEN || "";

const LIMIT = Number(process.env.DIAL_LIMIT || 1000000);
const BATCH_DELAY_MS = Number(process.env.DIAL_DELAY_MS || 250);
const DIAL_TO = process.env.DIAL_TO || "";

function ensureVoicePath(url) {
  if (!url) return "";
  const u = String(url).trim().replace(/\/$/, "");
  // אם המשתמש שם רק דומיין/שורש, נוסיף את הנתיב
  if (!u.includes("/twilio/voice")) return `${u}/twilio/voice`;
  return u;
}

const callUrl = ensureVoicePath(VOICE_WEBHOOK_URL || BASE_URL);

if (!callUrl || !FROM || !ACCOUNT_SID || !AUTH_TOKEN) {
  console.error(
    "Missing env. Need VOICE_WEBHOOK_URL (or BASE_URL) plus TWILIO_FROM_NUMBER, TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN"
  );
  process.exit(1);
}

const client = twilio(ACCOUNT_SID, AUTH_TOKEN);
const db = openDb({ dbPath: DB_PATH });

const explicitTo = normalizePhoneE164IL(DIAL_TO);
if (DIAL_TO && !explicitTo) {
  console.error(`Invalid DIAL_TO: ${DIAL_TO}. Use Israeli format like 05XXXXXXXX or +972...`);
  process.exit(1);
}

const rawContacts = explicitTo
  ? [{ phone: explicitTo }]
  : db
      .prepare(
        `SELECT phone FROM contacts WHERE do_not_call = 0 ORDER BY id DESC LIMIT ?`
      )
      .all(LIMIT);

const contacts = rawContacts.filter((c) => /^\+\d{8,15}$/.test(String(c.phone || "")));

console.log(`Dialing ${contacts.length} contacts...`);
console.log(`Using webhook url: ${callUrl}`);

let ok = 0;
let fail = 0;

for (const c of contacts) {
  const to = c.phone;
  try {
    await client.calls.create({
      to,
      from: FROM,
      url: callUrl,
      method: "POST"
    });
    ok++;
  } catch (e) {
    fail++;
    console.warn("Dial failed:", to, e?.message || e);
  }

  if (BATCH_DELAY_MS) {
    await new Promise((r) => setTimeout(r, BATCH_DELAY_MS));
  }
}

console.log(`Done. ok=${ok}, fail=${fail}`);


