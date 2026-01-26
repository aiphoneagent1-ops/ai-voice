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

    CREATE TABLE IF NOT EXISTS contact_lists (
      id INTEGER PRIMARY KEY,
      name TEXT NOT NULL,
      source TEXT NULL,
      created_at TEXT NOT NULL DEFAULT (datetime('now')),
      updated_at TEXT NOT NULL DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS contact_list_members (
      list_id INTEGER NOT NULL,
      phone TEXT NOT NULL,
      created_at TEXT NOT NULL DEFAULT (datetime('now')),
      PRIMARY KEY(list_id, phone),
      FOREIGN KEY(list_id) REFERENCES contact_lists(id)
    );

    CREATE INDEX IF NOT EXISTS idx_contact_list_members_list_id ON contact_list_members(list_id);
    CREATE INDEX IF NOT EXISTS idx_contact_list_members_phone ON contact_list_members(phone);

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

    CREATE TABLE IF NOT EXISTS leads (
      id INTEGER PRIMARY KEY,
      phone TEXT UNIQUE NOT NULL,
      status TEXT NOT NULL CHECK(status IN ('waiting','not_interested')),
      call_sid TEXT NULL,
      persona TEXT NULL CHECK(persona IN ('male','female')),
      created_at TEXT NOT NULL DEFAULT (datetime('now')),
      updated_at TEXT NOT NULL DEFAULT (datetime('now'))
    );

    CREATE INDEX IF NOT EXISTS idx_leads_status ON leads(status);
  `);

  // Migration: old leads status values were ('won','lost'). New: ('waiting','not_interested').
  // We migrate in-place by rebuilding the table (SQLite can't ALTER CHECK constraints).
  try {
    const row = db
      .prepare(`SELECT sql FROM sqlite_master WHERE type='table' AND name='leads' LIMIT 1`)
      .get();
    const sql = String(row?.sql || "");
    if (sql.includes("status IN ('won','lost')")) {
      db.exec(`
        BEGIN;
        DROP INDEX IF EXISTS idx_leads_status;
        CREATE TABLE IF NOT EXISTS leads_new (
          id INTEGER PRIMARY KEY,
          phone TEXT UNIQUE NOT NULL,
          status TEXT NOT NULL CHECK(status IN ('waiting','not_interested')),
          call_sid TEXT NULL,
          persona TEXT NULL CHECK(persona IN ('male','female')),
          created_at TEXT NOT NULL DEFAULT (datetime('now')),
          updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        INSERT INTO leads_new (id, phone, status, call_sid, persona, created_at, updated_at)
        SELECT
          id,
          phone,
          CASE status WHEN 'won' THEN 'waiting' ELSE 'not_interested' END,
          call_sid,
          persona,
          created_at,
          updated_at
        FROM leads;
        DROP TABLE leads;
        ALTER TABLE leads_new RENAME TO leads;
        CREATE INDEX IF NOT EXISTS idx_leads_status ON leads(status);
        COMMIT;
      `);
    }
  } catch {
    // Fail-open: if migration fails, app still boots. (Worst case: leads table keeps old values.)
  }

  // Add dialing columns if missing (migration)
  ensureColumns(db, "contacts", [
    { name: "dial_status", type: "TEXT NOT NULL DEFAULT 'new'" }, // new|queued|called|failed
    { name: "dial_attempts", type: "INTEGER NOT NULL DEFAULT 0" },
    { name: "last_dial_at", type: "TEXT NULL" },
    { name: "last_dial_error", type: "TEXT NULL" },
    // Twilio status callback enrichment (optional)
    { name: "last_call_sid", type: "TEXT NULL" },
    { name: "last_call_status", type: "TEXT NULL" },
    { name: "last_call_duration", type: "INTEGER NULL" },
    { name: "last_call_at", type: "TEXT NULL" }
  ]);

  // No default knowledge seeding: controlled from the admin scripts.

  return db;
}

export function upsertLead(db, { phone, status, callSid = null, persona = null } = {}) {
  const normalized = normalizePhoneE164IL(phone);
  if (!normalized) return;
  // Backward-compatible mapping:
  // - won -> waiting
  // - lost -> not_interested
  const s = String(status || "").toLowerCase();
  const st = s === "waiting" || s === "won" ? "waiting" : "not_interested";
  db.prepare(
    `
    INSERT INTO leads (phone, status, call_sid, persona, created_at, updated_at)
    VALUES (@phone, @status, @callSid, @persona, datetime('now'), datetime('now'))
    ON CONFLICT(phone) DO UPDATE SET
      status = excluded.status,
      call_sid = COALESCE(excluded.call_sid, leads.call_sid),
      persona = COALESCE(excluded.persona, leads.persona),
      updated_at = datetime('now')
  `
  ).run({ phone: normalized, status: st, callSid: callSid || null, persona: persona || null });
}

export function deleteLead(db, phone) {
  const normalized = normalizePhoneE164IL(phone);
  if (!normalized) return { deleted: 0 };
  const info = db.prepare(`DELETE FROM leads WHERE phone = ?`).run(normalized);
  return { deleted: Number(info?.changes || 0) };
}

export function listLeads(db, { status = "all", limit = 200, offset = 0 } = {}) {
  const lim = Math.max(1, Math.min(1000, Number(limit || 200)));
  const off = Math.max(0, Number(offset || 0));
  const st = String(status || "all").toLowerCase();
  const where = st === "waiting" || st === "not_interested" ? "WHERE l.status = ?" : "";
  const params = st === "waiting" || st === "not_interested" ? [st, lim, off] : [lim, off];

  const rows = db
    .prepare(
      `
      SELECT
        l.id,
        l.phone,
        l.status,
        l.call_sid AS callSid,
        l.persona,
        l.created_at AS createdAt,
        l.updated_at AS updatedAt,
        COALESCE(c.first_name, '') AS firstName
      FROM leads l
      LEFT JOIN contacts c ON c.phone = l.phone
      ${where}
      ORDER BY l.updated_at DESC
      LIMIT ? OFFSET ?
    `
    )
    .all(...params);

  return { rows, limit: lim, offset: off };
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

export function setDoNotCall(db, phone, doNotCall) {
  const normalized = normalizePhoneE164IL(phone);
  if (!normalized) return;
  db.prepare(`UPDATE contacts SET do_not_call = ? WHERE phone = ?`).run(doNotCall ? 1 : 0, normalized);
}

export function updateContactName(db, phone, firstName) {
  const normalized = normalizePhoneE164IL(phone);
  if (!normalized) return;
  db.prepare(`UPDATE contacts SET first_name = ? WHERE phone = ?`).run(firstName ?? null, normalized);
}

export function updateLeadStatus(db, phone, status) {
  const normalized = normalizePhoneE164IL(phone);
  if (!normalized) return;
  const s = String(status || "").toLowerCase();
  const st = s === "waiting" || s === "won" ? "waiting" : "not_interested";
  db.prepare(`UPDATE leads SET status = ?, updated_at = datetime('now') WHERE phone = ?`).run(st, normalized);
}

export function renamePhoneEverywhere(db, { oldPhone, newPhone }) {
  const oldN = normalizePhoneE164IL(oldPhone);
  const newN = normalizePhoneE164IL(newPhone);
  if (!oldN || !newN) return { ok: false, error: "invalid_phone" };
  if (oldN === newN) return { ok: true, oldPhone: oldN, newPhone: newN };
  const tx = db.transaction(() => {
    // Contacts (unique)
    const existing = db.prepare(`SELECT phone FROM contacts WHERE phone = ?`).get(newN);
    if (existing) throw new Error("phone_exists");
    db.prepare(`UPDATE contacts SET phone = ? WHERE phone = ?`).run(newN, oldN);
    db.prepare(`UPDATE leads SET phone = ? WHERE phone = ?`).run(newN, oldN);
    db.prepare(`UPDATE calls SET phone = ? WHERE phone = ?`).run(newN, oldN);
  });
  try {
    tx();
    return { ok: true, oldPhone: oldN, newPhone: newN };
  } catch (e) {
    return { ok: false, error: String(e?.message || e) };
  }
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

export function upsertContactList(db, { name, source = null } = {}) {
  const n = String(name || "").trim();
  if (!n) return null;
  const info = db
    .prepare(
      `INSERT INTO contact_lists (name, source, created_at, updated_at)
       VALUES (?, ?, datetime('now'), datetime('now'))`
    )
    .run(n, source ? String(source) : null);
  const id = Number(info?.lastInsertRowid || 0);
  return id || null;
}

export function renameContactList(db, { id, name } = {}) {
  const listId = Number(id || 0);
  const n = String(name || "").trim();
  if (!listId || !n) return { ok: false };
  const info = db
    .prepare(`UPDATE contact_lists SET name = ?, updated_at = datetime('now') WHERE id = ?`)
    .run(n, listId);
  return { ok: true, updated: Number(info?.changes || 0) };
}

export function deleteContactList(db, { id } = {}) {
  const listId = Number(id || 0);
  if (!listId) return { ok: false };
  const tx = db.transaction(() => {
    db.prepare(`DELETE FROM contact_list_members WHERE list_id = ?`).run(listId);
    const info = db.prepare(`DELETE FROM contact_lists WHERE id = ?`).run(listId);
    return Number(info?.changes || 0);
  });
  try {
    const deleted = tx();
    return { ok: true, deleted };
  } catch (e) {
    return { ok: false, error: String(e?.message || e) };
  }
}

export function addContactsToList(db, { listId, phones } = {}) {
  const id = Number(listId || 0);
  const arr = Array.isArray(phones) ? phones : [];
  if (!id || !arr.length) return { ok: true, added: 0 };
  const stmt = db.prepare(
    `INSERT OR IGNORE INTO contact_list_members (list_id, phone, created_at)
     VALUES (?, ?, datetime('now'))`
  );
  const tx = db.transaction((ps) => {
    let added = 0;
    for (const p of ps) {
      const normalized = normalizePhoneE164IL(p);
      if (!normalized) continue;
      const info = stmt.run(id, normalized);
      if (Number(info?.changes || 0) > 0) added++;
    }
    return added;
  });
  const added = tx(arr);
  return { ok: true, added };
}

export function listContactLists(db) {
  return db
    .prepare(
      `SELECT id, name, source, created_at AS createdAt, updated_at AS updatedAt
       FROM contact_lists
       ORDER BY id DESC`
    )
    .all();
}

export function listContactsByList(db, { listId, limit = 200, offset = 0 } = {}) {
  const id = Number(listId || 0);
  const lim = Math.max(1, Math.min(1000, Number(limit || 200)));
  const off = Math.max(0, Number(offset || 0));
  if (!id) return { rows: [], limit: lim, offset: off };
  const rows = db
    .prepare(
      `SELECT c.id, c.first_name, c.phone, c.gender, c.do_not_call, c.dial_status, c.dial_attempts, c.last_dial_at, c.last_dial_error,
              c.last_call_status, c.last_call_duration, c.last_call_at
       FROM contact_list_members m
       JOIN contacts c ON c.phone = m.phone
       WHERE m.list_id = ?
       ORDER BY c.id DESC
       LIMIT ? OFFSET ?`
    )
    .all(id, lim, off);
  return { rows, limit: lim, offset: off };
}

export function computeListStats(db, { listId } = {}) {
  const id = Number(listId || 0);
  if (!id) return null;
  const row = db
    .prepare(
      `
      SELECT
        COUNT(1) AS total,
        SUM(CASE WHEN c.do_not_call = 1 THEN 1 ELSE 0 END) AS dnc,
        SUM(CASE WHEN c.do_not_call = 0 AND c.dial_status = 'new' THEN 1 ELSE 0 END) AS remaining,
        SUM(CASE WHEN c.dial_status = 'called' THEN 1 ELSE 0 END) AS called,
        SUM(CASE WHEN c.dial_status = 'failed' THEN 1 ELSE 0 END) AS failed,
        SUM(CASE WHEN c.last_call_status = 'no-answer' THEN 1 ELSE 0 END) AS noAnswer,
        SUM(CASE WHEN c.last_call_status IN ('busy','failed','canceled') THEN 1 ELSE 0 END) AS notAvailable,
        SUM(CASE WHEN c.last_call_status = 'completed' AND COALESCE(c.last_call_duration, 0) < 5 THEN 1 ELSE 0 END) AS answeredUnder5,
        SUM(CASE WHEN l.status = 'waiting' THEN 1 ELSE 0 END) AS interested,
        SUM(CASE WHEN l.status = 'not_interested' THEN 1 ELSE 0 END) AS notInterested
      FROM contact_list_members m
      JOIN contacts c ON c.phone = m.phone
      LEFT JOIN leads l ON l.phone = c.phone
      WHERE m.list_id = ?
      `
    )
    .get(id);
  // Normalize to numbers
  const out = {};
  for (const [k, v] of Object.entries(row || {})) out[k] = Number(v || 0);
  return out;
}

export function listContactsForListExport(db, { listId, bucket = "all" } = {}) {
  const id = Number(listId || 0);
  if (!id) return [];
  const b = String(bucket || "all").trim().toLowerCase() || "all";

  let extraWhere = "";
  if (b === "no_answer") {
    extraWhere = ` AND c.last_call_status = 'no-answer'`;
  } else if (b === "not_available") {
    // "לא זמין": busy/canceled. (We keep "failed" for erroneous.)
    extraWhere = ` AND c.last_call_status IN ('busy','canceled')`;
  } else if (b === "completed_disconnected") {
    // "בוצע והיה ניתוק"
    extraWhere = ` AND c.last_call_status = 'completed' AND COALESCE(c.last_call_duration, 0) < 5`;
  } else if (b === "completed_progressed") {
    // "בוצע והתקדם" => lead created as waiting
    extraWhere = ` AND l.status = 'waiting'`;
  } else if (b === "not_completed") {
    // "לא בוצע": no call attempt completed yet (new/queued/unknown status)
    extraWhere = ` AND (c.dial_status IN ('new','queued') OR c.last_call_status IS NULL OR c.last_call_status IN ('initiated','ringing','answered'))`;
  } else if (b === "erroneous") {
    // "שגוי": internal dial failed or Twilio reports failed
    extraWhere = ` AND (c.dial_status = 'failed' OR c.last_call_status = 'failed')`;
  } else if (b !== "all") {
    return [];
  }

  return db
    .prepare(
      `
      SELECT
        c.first_name,
        c.phone,
        c.gender,
        c.do_not_call,
        c.dial_status,
        c.dial_attempts,
        c.last_dial_at,
        c.last_dial_error,
        c.last_call_status,
        c.last_call_duration,
        c.last_call_at,
        COALESCE(l.status, '') AS lead_status,
        COALESCE(l.updated_at, '') AS lead_updated_at
      FROM contact_list_members m
      JOIN contacts c ON c.phone = m.phone
      LEFT JOIN leads l ON l.phone = c.phone
      WHERE m.list_id = ?
      ${extraWhere}
      ORDER BY c.id DESC
      `
    )
    .all(id);
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


