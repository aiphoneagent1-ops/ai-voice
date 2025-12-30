import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import xlsx from "xlsx";
import { openDb, upsertContact } from "../src/db.js";
import { normalizePhoneE164IL } from "../src/phone.js";

const DB_PATH = process.env.DB_PATH || "./data/app.db";

function usage() {
  console.error("Usage: node scripts/import-xlsx.js /absolute/path/to/list.xlsx");
  process.exit(1);
}

const filePath = process.argv[2];
if (!filePath) usage();
if (!fs.existsSync(filePath)) {
  console.error("File not found:", filePath);
  process.exit(1);
}

const wb = xlsx.readFile(filePath);
const sheetName = wb.SheetNames[0];
if (!sheetName) {
  console.error("No sheets found in file");
  process.exit(1);
}

const sheet = wb.Sheets[sheetName];
const rows = xlsx.utils.sheet_to_json(sheet, { defval: "" });

const db = openDb({ dbPath: DB_PATH });

let count = 0;
for (const r of rows) {
  const phoneRaw = String(r.phone || r.Phone || r.PHONE || "").trim();
  const phone = normalizePhoneE164IL(phoneRaw);
  if (!phone) continue;

  const gRaw = String(r.gender || r.Gender || r.GENDER || "").trim().toLowerCase();
  const gender =
    gRaw === "female" || gRaw === "f" || gRaw === "נקבה"
      ? "female"
      : gRaw === "male" || gRaw === "m" || gRaw === "זכר"
        ? "male"
        : null;

  const firstName = String(r.first_name || r.firstName || r.name || "").trim() || null;

  upsertContact(db, { phone, gender, firstName });
  count++;
}

console.log(`Imported/updated ${count} contacts from ${path.basename(filePath)} (${sheetName})`);


