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

    -- Import errors per list (invalid phones, parsing issues, etc.)
    CREATE TABLE IF NOT EXISTS contact_import_errors (
      id INTEGER PRIMARY KEY,
      list_id INTEGER NOT NULL,
      raw_phone TEXT NULL,
      reason TEXT NULL,
      row_json TEXT NULL,
      created_at TEXT NOT NULL DEFAULT (datetime('now')),
      FOREIGN KEY(list_id) REFERENCES contact_lists(id)
    );

    CREATE INDEX IF NOT EXISTS idx_contact_import_errors_list_id ON contact_import_errors(list_id);

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

  // Per-list dialing columns (migration): dialing state must be tracked per import list, not globally per contact.
  ensureColumns(db, "contact_list_members", [
    { name: "dial_status", type: "TEXT NOT NULL DEFAULT 'new'" }, // new|queued|called|failed
    { name: "dial_attempts", type: "INTEGER NOT NULL DEFAULT 0" },
    { name: "last_dial_at", type: "TEXT NULL" },
    { name: "last_dial_error", type: "TEXT NULL" },
    // Twilio status callback enrichment (per list)
    { name: "last_call_sid", type: "TEXT NULL" },
    { name: "last_call_status", type: "TEXT NULL" },
    { name: "last_call_duration", type: "INTEGER NULL" },
    { name: "last_call_at", type: "TEXT NULL" }
  ]);

  // Best-effort backfill: if list member status is NULL (older data), copy from contacts.
  try {
    db.exec(`
      UPDATE contact_list_members
      SET
        dial_status = COALESCE(dial_status, (SELECT c.dial_status FROM contacts c WHERE c.phone = contact_list_members.phone)),
        dial_attempts = COALESCE(dial_attempts, (SELECT c.dial_attempts FROM contacts c WHERE c.phone = contact_list_members.phone)),
        last_dial_at = COALESCE(last_dial_at, (SELECT c.last_dial_at FROM contacts c WHERE c.phone = contact_list_members.phone)),
        last_dial_error = COALESCE(last_dial_error, (SELECT c.last_dial_error FROM contacts c WHERE c.phone = contact_list_members.phone)),
        last_call_sid = COALESCE(last_call_sid, (SELECT c.last_call_sid FROM contacts c WHERE c.phone = contact_list_members.phone)),
        last_call_status = COALESCE(last_call_status, (SELECT c.last_call_status FROM contacts c WHERE c.phone = contact_list_members.phone)),
        last_call_duration = COALESCE(last_call_duration, (SELECT c.last_call_duration FROM contacts c WHERE c.phone = contact_list_members.phone)),
        last_call_at = COALESCE(last_call_at, (SELECT c.last_call_at FROM contacts c WHERE c.phone = contact_list_members.phone))
      WHERE dial_status IS NULL OR dial_attempts IS NULL OR last_call_status IS NULL;
    `);
  } catch {}

  // Safety / conversation guard columns (migration)
  ensureColumns(db, "calls", [
    { name: "off_topic_strikes", type: "INTEGER NOT NULL DEFAULT 0" },
    { name: "last_relevant_at", type: "TEXT NULL" }
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
    // Capture phones that belong to this list BEFORE deleting the membership rows.
    const phones = db
      .prepare(`SELECT phone FROM contact_list_members WHERE list_id = ?`)
      .all(listId)
      .map((r) => String(r?.phone || "").trim())
      .filter(Boolean);

    db.prepare(`DELETE FROM contact_import_errors WHERE list_id = ?`).run(listId);
    db.prepare(`DELETE FROM contact_list_members WHERE list_id = ?`).run(listId);
    const info = db.prepare(`DELETE FROM contact_lists WHERE id = ?`).run(listId);

    // Delete contacts that are now "orphaned" (not in any remaining list).
    // Important: if a phone exists in another list, it stays in the system.
    if (phones.length) {
      const stmtDeleteContacts = db.prepare(
        `
        DELETE FROM contacts
        WHERE phone = ?
          AND NOT EXISTS (SELECT 1 FROM contact_list_members m2 WHERE m2.phone = contacts.phone)
      `
      );
      const stmtDeleteLeads = db.prepare(
        `
        DELETE FROM leads
        WHERE phone = ?
          AND NOT EXISTS (SELECT 1 FROM contact_list_members m2 WHERE m2.phone = leads.phone)
      `
      );
      const tx2 = db.transaction((ps) => {
        let deletedContacts = 0;
        let deletedLeads = 0;
        for (const p of ps) {
          const a = stmtDeleteContacts.run(p);
          deletedContacts += Number(a?.changes || 0);
          const b = stmtDeleteLeads.run(p);
          deletedLeads += Number(b?.changes || 0);
        }
        return { deletedContacts, deletedLeads };
      });
      const r = tx2(phones);
      return { deletedList: Number(info?.changes || 0), ...r };
    }

    return { deletedList: Number(info?.changes || 0), deletedContacts: 0, deletedLeads: 0 };
  });
  try {
    const out = tx();
    return { ok: true, ...out };
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

export function addImportErrors(db, { listId, errors } = {}) {
  const id = Number(listId || 0);
  const arr = Array.isArray(errors) ? errors : [];
  if (!id || !arr.length) return { ok: true, added: 0 };
  const stmt = db.prepare(
    `INSERT INTO contact_import_errors (list_id, raw_phone, reason, row_json, created_at)
     VALUES (?, ?, ?, ?, datetime('now'))`
  );
  const tx = db.transaction((es) => {
    let added = 0;
    for (const e of es) {
      const rawPhone = e?.rawPhone != null ? String(e.rawPhone).trim() : null;
      const reason = e?.reason != null ? String(e.reason).trim() : null;
      let rowJson = null;
      try {
        if (e?.row != null) rowJson = JSON.stringify(e.row);
      } catch {
        rowJson = null;
      }
      stmt.run(id, rawPhone || null, reason || null, rowJson || null);
      added++;
    }
    return added;
  });
  const added = tx(arr);
  return { ok: true, added };
}

export function listImportErrorsByList(db, { listId, limit = 5000, offset = 0 } = {}) {
  const id = Number(listId || 0);
  const lim = Math.max(1, Math.min(50000, Number(limit || 5000)));
  const off = Math.max(0, Number(offset || 0));
  if (!id) return { rows: [], limit: lim, offset: off };
  const rows = db
    .prepare(
      `SELECT id, raw_phone AS rawPhone, reason, row_json AS rowJson, created_at AS createdAt
       FROM contact_import_errors
       WHERE list_id = ?
       ORDER BY id ASC
       LIMIT ? OFFSET ?`
    )
    .all(id, lim, off);
  return { rows, limit: lim, offset: off };
}

export function listContactsByList(db, { listId, limit = 200, offset = 0 } = {}) {
  const id = Number(listId || 0);
  const lim = Math.max(1, Math.min(1000, Number(limit || 200)));
  const off = Math.max(0, Number(offset || 0));
  if (!id) return { rows: [], limit: lim, offset: off };
  const rows = db
    .prepare(
      `SELECT c.id, c.first_name, c.phone, c.gender, c.do_not_call,
              m.dial_status, m.dial_attempts, m.last_dial_at, m.last_dial_error,
              m.last_call_status, m.last_call_duration, m.last_call_at
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
        SUM(CASE WHEN c.do_not_call = 0 AND COALESCE(m.dial_status,'new') = 'new' THEN 1 ELSE 0 END) AS remaining,
        SUM(CASE WHEN c.do_not_call = 0 AND COALESCE(m.dial_status,'') = 'queued' THEN 1 ELSE 0 END) AS queued,
        SUM(CASE WHEN COALESCE(m.dial_status,'') = 'called' THEN 1 ELSE 0 END) AS called,
        SUM(CASE WHEN COALESCE(m.dial_status,'') = 'failed' THEN 1 ELSE 0 END) AS failed,
        SUM(CASE WHEN COALESCE(m.last_call_status,'') = 'no-answer' THEN 1 ELSE 0 END) AS noAnswer,
        SUM(CASE WHEN COALESCE(m.last_call_status,'') IN ('busy','failed','canceled') THEN 1 ELSE 0 END) AS notAvailable,
        SUM(CASE WHEN COALESCE(m.last_call_status,'') = 'completed' AND COALESCE(m.last_call_duration, 0) < 5 THEN 1 ELSE 0 END) AS answeredUnder5,
        SUM(CASE WHEN l.status = 'waiting' THEN 1 ELSE 0 END) AS interested,
        SUM(CASE WHEN l.status = 'not_interested' THEN 1 ELSE 0 END) AS notInterested
      FROM contact_list_members m
      JOIN contacts c ON c.phone = m.phone
      LEFT JOIN leads l ON l.phone = c.phone
      WHERE m.list_id = ?
      `
    )
    .get(id);
  const invalid = db
    .prepare(`SELECT COUNT(1) AS c FROM contact_import_errors WHERE list_id = ?`)
    .get(id)?.c;
  // Normalize to numbers
  const out = {};
  for (const [k, v] of Object.entries(row || {})) out[k] = Number(v || 0);
  out.invalid = Number(invalid || 0);
  out.notDone = Number(out.remaining || 0) + Number(out.queued || 0);
  // "בוצע" = attempts that actually happened (anything not pending), excluding DNC phones.
  // We consider a number "done" once it left the pending pool (new/queued).
  out.done = Math.max(0, Number(out.total || 0) - Number(out.dnc || 0) - Number(out.notDone || 0));
  return out;
}

export function fetchNextListMembersToDial(db, { limit = 10, listIds = [] } = {}) {
  const lim = Math.max(1, Math.min(200, Number(limit || 10)));
  const ids = Array.isArray(listIds) ? listIds.map((x) => Number(x || 0)).filter((n) => Number.isFinite(n) && n > 0) : [];

  // If ids is empty => all lists.
  if (!ids.length) {
    return db
      .prepare(
        `
        SELECT
          m.list_id AS listId,
          m.phone AS phone,
          c.gender AS gender,
          c.first_name AS firstName
        FROM contact_list_members m
        JOIN contacts c ON c.phone = m.phone
        WHERE c.do_not_call = 0
          AND COALESCE(m.dial_status,'new') = 'new'
        ORDER BY m.created_at ASC
        LIMIT ?
        `
      )
      .all(lim);
  }

  const placeholders = ids.map(() => "?").join(",");
  return db
    .prepare(
      `
      SELECT
        m.list_id AS listId,
        m.phone AS phone,
        c.gender AS gender,
        c.first_name AS firstName
      FROM contact_list_members m
      JOIN contacts c ON c.phone = m.phone
      WHERE c.do_not_call = 0
        AND COALESCE(m.dial_status,'new') = 'new'
        AND m.list_id IN (${placeholders})
      ORDER BY m.created_at ASC
      LIMIT ?
      `
    )
    .all(...ids, lim);
}

export function queueListMemberForDial(db, { listId, phone, error = null } = {}) {
  const id = Number(listId || 0);
  const normalized = normalizePhoneE164IL(phone);
  if (!id || !normalized) return;
  db.prepare(
    `UPDATE contact_list_members
     SET dial_status = 'queued',
         dial_attempts = COALESCE(dial_attempts, 0) + 1,
         last_dial_at = datetime('now'),
         last_dial_error = ?
     WHERE list_id = ? AND phone = ?`
  ).run(error, id, normalized);
}

export function markListMemberDialResult(db, { listId, phone, status, error = null } = {}) {
  const id = Number(listId || 0);
  const normalized = normalizePhoneE164IL(phone);
  if (!id || !normalized) return;
  const st = String(status || "").trim() || "failed";
  db.prepare(
    `UPDATE contact_list_members
     SET dial_status = ?,
         last_dial_error = ?
     WHERE list_id = ? AND phone = ?`
  ).run(st, error, id, normalized);
}

export function updateListMemberCallStatus(db, { listId, phone, callSid, callStatus, duration } = {}) {
  const id = Number(listId || 0);
  const normalized = normalizePhoneE164IL(phone);
  if (!id || !normalized) return;
  const dur = Number(duration || 0);
  db.prepare(
    `UPDATE contact_list_members
     SET last_call_sid = COALESCE(?, last_call_sid),
         last_call_status = ?,
         last_call_duration = CASE WHEN ? > 0 THEN ? ELSE last_call_duration END,
         last_call_at = datetime('now')
     WHERE list_id = ? AND phone = ?`
  ).run(callSid || null, callStatus || null, dur, dur, id, normalized);
}

export function getCallOffTopicStrikes(db, callSid) {
  const sid = String(callSid || "").trim();
  if (!sid) return 0;
  const row = db.prepare(`SELECT off_topic_strikes AS s FROM calls WHERE call_sid = ?`).get(sid);
  return Number(row?.s || 0);
}

export function incrementCallOffTopicStrikes(db, callSid) {
  const sid = String(callSid || "").trim();
  if (!sid) return 0;
  db.prepare(
    `UPDATE calls SET off_topic_strikes = COALESCE(off_topic_strikes, 0) + 1, updated_at = datetime('now') WHERE call_sid = ?`
  ).run(sid);
  return getCallOffTopicStrikes(db, sid);
}

export function resetCallOffTopicStrikes(db, callSid) {
  const sid = String(callSid || "").trim();
  if (!sid) return;
  db.prepare(
    `UPDATE calls SET off_topic_strikes = 0, last_relevant_at = datetime('now'), updated_at = datetime('now') WHERE call_sid = ?`
  ).run(sid);
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


