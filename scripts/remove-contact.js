import "dotenv/config";
import { openDb } from "../src/db.js";
import { normalizePhoneE164IL } from "../src/phone.js";

const DB_PATH = process.env.DB_PATH || "./data/app.db";

function usage() {
  console.error("Usage: node scripts/remove-contact.js PHONE");
  process.exit(1);
}

const raw = process.argv[2];
if (!raw) usage();

const phone = normalizePhoneE164IL(raw) || String(raw).trim();
if (!phone) usage();

const db = openDb({ dbPath: DB_PATH });
const info = db.prepare("DELETE FROM contacts WHERE phone = ?").run(phone);
console.log(`Removed ${info.changes} contacts for ${phone}`);


