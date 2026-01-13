import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import ExcelJS from "exceljs";
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

function normalizeCellValue(v) {
  if (v == null) return "";
  if (v instanceof Date) return v.toISOString();
  if (typeof v === "object") {
    if ("text" in v && typeof v.text === "string") return v.text;
    if ("richText" in v && Array.isArray(v.richText)) return v.richText.map((p) => p?.text || "").join("");
    if ("result" in v) return normalizeCellValue(v.result);
    if ("hyperlink" in v && typeof v.hyperlink === "string") return v.hyperlink;
    if ("formula" in v && "result" in v) return normalizeCellValue(v.result);
  }
  return String(v);
}

const wb = new ExcelJS.Workbook();
await wb.xlsx.readFile(filePath);
const ws = wb.worksheets[0];
if (!ws) {
  console.error("No sheets found in file");
  process.exit(1);
}

const headerRow = ws.getRow(1);
const headers = (headerRow?.values || []).slice(1).map((h) => String(h || "").trim());
if (!headers.some(Boolean)) {
  console.error("Missing header row (row 1)");
  process.exit(1);
}

const rows = [];
for (let i = 2; i <= ws.rowCount; i++) {
  const row = ws.getRow(i);
  if (!row || !row.hasValues) continue;
  const r = {};
  for (let c = 0; c < headers.length; c++) {
    const key = headers[c];
    if (!key) continue;
    r[key] = normalizeCellValue(row.getCell(c + 1).value);
  }
  rows.push(r);
}

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

console.log(`Imported/updated ${count} contacts from ${path.basename(filePath)} (${ws.name})`);


