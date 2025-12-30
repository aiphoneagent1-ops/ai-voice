import fs from "node:fs";
import path from "node:path";
import Database from "better-sqlite3";
import { normalizePhoneE164IL } from "./phone.js";

function ensureColumns(db, table, columns) {
  const existing = db.prepare(`PRAGMA table_info(${table})`).all().map((c) => c.name);
  for (const col of columns) {
    if (!existing.includes(col.name)) {
      db.exec(`ALTER TABLE ${table} ADD COLUMN ${col.name} ${col.type}`);
    }
  }
}

export function openDb({ dbPath }) {
  const dir = path.dirname(dbPath);
  if (dir && dir !== "." && !fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });

  const db = new Database(dbPath);
  db.pragma("journal_mode = WAL");

  db.exec(`
    CREATE TABLE IF NOT EXISTS contacts (
      id INTEGER PRIMARY KEY,
      phone TEXT UNIQUE NOT NULL,
      gender TEXT CHECK(gender IN ('male','female')) NULL,
      first_name TEXT NULL,
      do_not_call INTEGER NOT NULL DEFAULT 0,
      created_at TEXT NOT NULL DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS calls (
      call_sid TEXT PRIMARY KEY,
      phone TEXT NOT NULL,
      persona TEXT CHECK(persona IN ('male','female')) NOT NULL,
      turn_count INTEGER NOT NULL DEFAULT 0,
      started_at TEXT NOT NULL DEFAULT (datetime('now')),
      updated_at TEXT NOT NULL DEFAULT (datetime('now')),
      status TEXT NOT NULL DEFAULT 'active'
    );

    CREATE TABLE IF NOT EXISTS call_messages (
      id INTEGER PRIMARY KEY,
      call_sid TEXT NOT NULL,
      role TEXT NOT NULL,
      content TEXT NOT NULL,
      created_at TEXT NOT NULL DEFAULT (datetime('now')),
      FOREIGN KEY(call_sid) REFERENCES calls(call_sid)
    );

    CREATE INDEX IF NOT EXISTS idx_call_messages_call_sid ON call_messages(call_sid);

    CREATE TABLE IF NOT EXISTS agent_settings (
      key TEXT PRIMARY KEY,
      value TEXT NOT NULL,
      updated_at TEXT NOT NULL DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS agent_knowledge (
      id INTEGER PRIMARY KEY,
      title TEXT NOT NULL,
      content TEXT NOT NULL,
      enabled INTEGER NOT NULL DEFAULT 1,
      updated_at TEXT NOT NULL DEFAULT (datetime('now'))
    );
  `);

  // Add dialing columns if missing (migration)
  ensureColumns(db, "contacts", [
    { name: "dial_status", type: "TEXT NOT NULL DEFAULT 'new'" }, // new|queued|called|failed
    { name: "dial_attempts", type: "INTEGER NOT NULL DEFAULT 0" },
    { name: "last_dial_at", type: "TEXT NULL" },
    { name: "last_dial_error", type: "TEXT NULL" }
  ]);

  // No default knowledge seeding: controlled from the admin scripts.

  return db;
}

export function upsertContact(db, { phone, gender, firstName }) {
  const normalized = normalizePhoneE164IL(phone);
  if (!normalized) return;
  const stmt = db.prepare(`
    INSERT INTO contacts (phone, gender, first_name)
    VALUES (@phone, @gender, @firstName)
    ON CONFLICT(phone) DO UPDATE SET
      gender = COALESCE(excluded.gender, contacts.gender),
      first_name = COALESCE(excluded.first_name, contacts.first_name)
  `);
  stmt.run({ phone: normalized, gender: gender ?? null, firstName: firstName ?? null });
}

export function getContactByPhone(db, phone) {
  const normalized = normalizePhoneE164IL(phone);
  return db.prepare(`SELECT * FROM contacts WHERE phone = ?`).get(normalized);
}

export function markDoNotCall(db, phone) {
  const normalized = normalizePhoneE164IL(phone);
  db.prepare(`UPDATE contacts SET do_not_call = 1 WHERE phone = ?`).run(normalized);
}

export function createOrGetCall(db, { callSid, phone, persona }) {
  const existing = db.prepare(`SELECT * FROM calls WHERE call_sid = ?`).get(callSid);
  if (existing) return existing;

  db.prepare(
    `INSERT INTO calls (call_sid, phone, persona) VALUES (?, ?, ?)`
  ).run(callSid, phone, persona);
  return db.prepare(`SELECT * FROM calls WHERE call_sid = ?`).get(callSid);
}

export function incrementTurn(db, callSid) {
  db.prepare(
    `UPDATE calls SET turn_count = turn_count + 1, updated_at = datetime('now') WHERE call_sid = ?`
  ).run(callSid);
  return db.prepare(`SELECT * FROM calls WHERE call_sid = ?`).get(callSid);
}

export function addMessage(db, { callSid, role, content }) {
  db.prepare(
    `INSERT INTO call_messages (call_sid, role, content) VALUES (?, ?, ?)`
  ).run(callSid, role, content);
}

export function getMessages(db, callSid, { limit = 10 } = {}) {
  return db
    .prepare(
      `SELECT role, content FROM call_messages WHERE call_sid = ? ORDER BY id DESC LIMIT ?`
    )
    .all(callSid, limit)
    .reverse();
}

export function getSetting(db, key, defaultValue = null) {
  const row = db.prepare(`SELECT value FROM agent_settings WHERE key = ?`).get(key);
  if (!row) return defaultValue;
  try {
    return JSON.parse(row.value);
  } catch {
    return defaultValue;
  }
}

export function setSetting(db, key, value) {
  db.prepare(
    `INSERT INTO agent_settings (key, value, updated_at) VALUES (?, ?, datetime('now'))
     ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = datetime('now')`
  ).run(key, JSON.stringify(value));
}

export function getKnowledge(db) {
  return db
    .prepare(`SELECT id, title, content, enabled, updated_at FROM agent_knowledge ORDER BY id ASC`)
    .all();
}

export function setKnowledge(db, { id, title, content, enabled }) {
  if (id) {
    db.prepare(
      `UPDATE agent_knowledge SET title = ?, content = ?, enabled = ?, updated_at = datetime('now') WHERE id = ?`
    ).run(title, content, enabled ? 1 : 0, id);
    return;
  }
  db.prepare(
    `INSERT INTO agent_knowledge (title, content, enabled) VALUES (?, ?, ?)`
  ).run(title, content, enabled ? 1 : 0);
}

export function queueContactForDial(db, phone, { error = null } = {}) {
  const normalized = normalizePhoneE164IL(phone);
  if (!normalized) return;
  db.prepare(
    `UPDATE contacts
     SET dial_status = 'queued', dial_attempts = dial_attempts + 1, last_dial_at = datetime('now'), last_dial_error = ?
     WHERE phone = ?`
  ).run(error, normalized);
}

export function markDialResult(db, phone, { status, error = null } = {}) {
  const normalized = normalizePhoneE164IL(phone);
  if (!normalized) return;
  db.prepare(
    `UPDATE contacts SET dial_status = ?, last_dial_error = ? WHERE phone = ?`
  ).run(status, error, normalized);
}

export function fetchNextContactsToDial(db, limit = 10) {
  return db
    .prepare(
      `SELECT phone, gender, first_name
       FROM contacts
       WHERE do_not_call = 0 AND dial_status = 'new'
       ORDER BY id ASC
       LIMIT ?`
    )
    .all(limit);
}


