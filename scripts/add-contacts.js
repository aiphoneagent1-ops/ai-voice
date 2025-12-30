import "dotenv/config";
import { openDb, upsertContact } from "../src/db.js";

const DB_PATH = process.env.DB_PATH || "./data/app.db";

function usage() {
  console.error(
    'Usage:\n  node scripts/add-contacts.js --contact "PHONE|NAME|GENDER" [--contact "..."]\n\nGENDER: male|female (optional)\nExample:\n  node scripts/add-contacts.js --contact "0549050710|מיכה צור|male" --contact "0506204852|ורד צור|female"'
  );
  process.exit(1);
}

const args = process.argv.slice(2);
const contacts = [];
for (let i = 0; i < args.length; i++) {
  if (args[i] === "--contact") {
    const v = args[i + 1];
    if (!v) usage();
    contacts.push(v);
    i++;
  }
}

if (!contacts.length) usage();

const db = openDb({ dbPath: DB_PATH });

let count = 0;
for (const c of contacts) {
  const [phone, name, genderRaw] = String(c).split("|").map((x) => (x ?? "").trim());
  const g = (genderRaw || "").trim().toLowerCase();
  const gender =
    g === "female" || g === "f" || genderRaw === "נקבה"
      ? "female"
      : g === "male" || g === "m" || genderRaw === "זכר"
        ? "male"
        : null;
  upsertContact(db, { phone, gender, firstName: name || null });
  count++;
}

console.log(`Added/updated ${count} contacts.`);


