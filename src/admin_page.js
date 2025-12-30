export function renderAdminPage() {
  return `<!doctype html>
<html lang="he" dir="rtl">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>סוכן AI טלפוני לשירותי דת</title>
    <style>
      :root{
        --bg:#070a12;
        --panel:#0f1426;
        --panel2:#0b1020;
        --border:#273155;
        --text:#ffffff;
        --muted:rgba(255,255,255,.75);
        --muted2:rgba(255,255,255,.6);
        --brand:#6d8cff;
        --brand2:#7cf3ff;
        --good:#7CFFB2;
        --bad:#FF7C7C;
        --shadow: 0 10px 30px rgba(0,0,0,.35);
      }
      *{ box-sizing:border-box; }
      body { font-family: system-ui, -apple-system, Arial; margin: 0; background: radial-gradient(1200px 800px at 20% 0%, rgba(109,140,255,.18), transparent 50%), radial-gradient(900px 700px at 80% 10%, rgba(124,243,255,.12), transparent 55%), var(--bg); color: var(--text); }
      header { padding: 22px 24px; border-bottom: 1px solid rgba(255,255,255,.06); background: rgba(10,12,20,.6); backdrop-filter: blur(8px); position: sticky; top:0; z-index: 10; }
      .wrap { max-width: 1100px; margin: 0 auto; }
      .top { display:flex; justify-content: space-between; align-items: center; gap: 16px; flex-wrap: wrap; }
      h1 { margin: 0; font-size: 18px; letter-spacing: .2px; }
      .sub { font-size: 12px; color: var(--muted2); margin-top: 4px; }
      main { padding: 24px; }
      .grid { display: grid; grid-template-columns: 1fr; gap: 16px; }
      @media (min-width: 980px){ .grid { grid-template-columns: 1.05fr .95fr; } }
      .card { background: linear-gradient(180deg, rgba(255,255,255,.04), rgba(255,255,255,.02)); border: 1px solid rgba(255,255,255,.07); border-radius: 16px; padding: 16px; box-shadow: var(--shadow); }
      .card h2 { margin: 0 0 10px; font-size: 15px; }
      .pill { display:inline-flex; gap:8px; align-items:center; padding: 6px 10px; border-radius: 999px; border:1px solid rgba(255,255,255,.10); background: rgba(255,255,255,.03); color: var(--muted); font-size: 12px; }
      .stats { display:flex; gap:10px; flex-wrap:wrap; }
      .stat { min-width: 120px; padding: 10px 12px; border-radius: 12px; border:1px solid rgba(255,255,255,.08); background: rgba(0,0,0,.18); }
      .stat .k { font-size: 11px; color: var(--muted2); }
      .stat .v { font-size: 16px; margin-top: 2px; }
      label { display:block; font-size: 12px; color: var(--muted); margin: 10px 0 6px; }
      textarea, input { width: 100%; padding: 11px 12px; border-radius: 12px; border: 1px solid rgba(255,255,255,.10); background: rgba(0,0,0,.20); color: var(--text); outline: none; }
      textarea:focus, input:focus { border-color: rgba(109,140,255,.6); box-shadow: 0 0 0 4px rgba(109,140,255,.12); }
      textarea { min-height: 220px; resize: vertical; }
      button { cursor: pointer; padding: 10px 12px; border-radius: 12px; border: 1px solid rgba(255,255,255,.12); background: linear-gradient(180deg, rgba(109,140,255,.35), rgba(109,140,255,.18)); color: #fff; }
      button:hover { filter: brightness(1.07); }
      button.secondary { background: rgba(255,255,255,.04); }
      .row { display:flex; gap: 10px; flex-wrap: wrap; align-items: center; }
      .status { font-size: 12px; color: var(--muted2); }
      .ok { color: var(--good); }
      .err { color: var(--bad); }
      code { background: rgba(0,0,0,.25); padding: 2px 6px; border-radius: 8px; border:1px solid rgba(255,255,255,.10); }
      a { color: #b9c7ff; }
      hr { border:0; border-top:1px solid rgba(255,255,255,.07); margin:16px 0; }
      .tabs{ display:flex; gap:8px; flex-wrap:wrap; margin-top: 8px; }
      .tab{ padding: 8px 10px; border-radius: 999px; border:1px solid rgba(255,255,255,.10); background: rgba(255,255,255,.03); color: var(--muted); font-size: 12px; cursor:pointer; }
      .tab.active{ border-color: rgba(109,140,255,.55); background: rgba(109,140,255,.18); color: #fff; }
      .tableWrap{ overflow:auto; border-radius: 14px; border:1px solid rgba(255,255,255,.08); background: rgba(0,0,0,.16); }
      table{ width:100%; border-collapse: collapse; min-width: 820px; }
      th, td{ text-align:right; padding: 10px 12px; border-bottom:1px solid rgba(255,255,255,.06); font-size: 12px; color: var(--muted); }
      th{ color:#fff; font-weight:600; background: rgba(255,255,255,.03); position: sticky; top:0; }
      td strong{ color:#fff; font-weight:600; }
      .badge{ display:inline-flex; padding: 3px 8px; border-radius: 999px; border:1px solid rgba(255,255,255,.12); background: rgba(255,255,255,.03); font-size: 11px; }
      .badge.good{ border-color: rgba(124,255,178,.35); color: var(--good); }
      .badge.bad{ border-color: rgba(255,124,124,.35); color: var(--bad); }
      .drop {
        border: 1px dashed rgba(255,255,255,.22);
        background: rgba(0,0,0,.16);
        border-radius: 14px;
        padding: 14px;
        display:flex;
        gap: 12px;
        align-items: center;
        justify-content: space-between;
      }
      .drop strong { font-size: 13px; }
      .drop span { font-size: 12px; color: var(--muted2); display:block; margin-top:4px; }
      .toast {
        position: fixed; left: 16px; bottom: 16px; z-index: 9999;
        background: rgba(10,12,20,.9); border:1px solid rgba(255,255,255,.10);
        border-radius: 14px; padding: 12px 14px; min-width: 260px; max-width: 420px;
        box-shadow: var(--shadow); display:none;
      }
      .toast .t { font-size: 12px; color: var(--muted2); }
      .toast .m { margin-top: 4px; }

      .iconBtn{
        display:inline-flex; align-items:center; justify-content:center;
        width: 34px; height: 34px;
        padding: 0;
        border-radius: 10px;
        border: 1px solid rgba(255,255,255,.14);
        background: rgba(255,255,255,.04);
      }
      .iconBtn:hover{ filter: brightness(1.12); }
      .iconBtn svg{ width: 18px; height: 18px; opacity: .95; }

      /* Modal */
      .modalOverlay{
        position: fixed; inset:0; z-index: 9998;
        background: rgba(0,0,0,.55);
        display:none;
        padding: 16px;
      }
      .modal{
        max-width: 520px;
        margin: 64px auto;
        background: rgba(10,12,20,.92);
        border: 1px solid rgba(255,255,255,.10);
        border-radius: 16px;
        box-shadow: var(--shadow);
        padding: 14px;
      }
      .modal h3{ margin: 0 0 8px; font-size: 14px; }
      select{
        width: 100%;
        padding: 11px 12px;
        border-radius: 12px;
        border: 1px solid rgba(255,255,255,.10);
        background: rgba(0,0,0,.20);
        color: var(--text);
        outline: none;
      }
    </style>
  </head>
  <body>
    <header>
      <div class="wrap top">
        <div>
          <h1>סוכן AI טלפוני לשירותי דת</h1>
          <div class="sub">ניהול אנשי קשר, ידע ותסריטים — ומשם חיוג אוטומטי.</div>
        </div>
        <div class="stats" id="stats"></div>
      </div>
      <div class="wrap status" id="topStatus" style="margin-top:10px;"></div>
    </header>
    <main>
      <div class="wrap grid">
        <div class="card">
          <h2>מאגר טלפונים</h2>
          <div class="pill">עמודות נדרשות: <code>phone</code> | אופציונלי: <code>first_name</code>, <code>gender</code> (זכר/נקבה או male/female)</div>
          <div style="height:10px;"></div>
          <div class="drop" id="dropzone">
            <div>
              <strong>גרור לכאן XLSX/CSV או בחר קובץ</strong>
              <span>קבצים נתמכים: .xlsx או .csv</span>
            </div>
            <div class="row">
              <input type="file" id="xlsxFile" accept=".xlsx,.csv" style="max-width:260px;" />
              <button id="uploadXlsxBtn">העלה</button>
            </div>
          </div>
          <div class="row" style="margin-top:10px;">
            <span class="status" id="importStatus"></span>
          </div>
          <hr />
          <label>Google Sheets (קישור שיתוף או קישור CSV)</label>
          <input id="sheetUrl" placeholder="הדבק קישור ל-Google Sheet (מומלץ שיהיה 'Anyone with the link')"/>
          <div class="row" style="margin-top:10px;">
            <button id="importSheetBtn">ייבוא מ-Google Sheets</button>
            <span class="status" id="sheetStatus"></span>
          </div>
          <hr />
          <div class="row" style="justify-content:space-between;">
            <div class="pill">הצגת כל אנשי הקשר</div>
            <div class="row" style="gap:8px;">
              <button class="secondary" id="addTestBtn">הוסף מספר בדיקה</button>
              <button class="secondary" id="showListBtn">לרשימה המלאה</button>
            </div>
          </div>
          <div id="listSection" style="display:none; margin-top:12px;">
            <div class="row" style="justify-content:space-between;">
              <div class="row" style="gap:8px;">
                <input id="search" placeholder="חיפוש (שם/טלפון)..." style="max-width:260px;" />
                <button class="secondary" id="refreshListBtn">רענון</button>
              </div>
              <span class="status" id="listStatus"></span>
            </div>
            <div style="height:10px;"></div>
            <div class="tableWrap">
              <table>
                <thead>
                  <tr>
                    <th>פעולות</th>
                    <th>שם</th>
                    <th>טלפון</th>
                    <th>מין</th>
                    <th>סטטוס חיוג</th>
                    <th>DNC</th>
                    <th>ניסיון</th>
                  </tr>
                </thead>
                <tbody id="contactsBody"></tbody>
              </table>
            </div>
          </div>
          <hr />
          <h3 style="margin:0 0 8px; font-size:14px;">חיוג אוטומטי</h3>
          <div class="row">
            <label style="margin:0; display:flex; gap:8px; align-items:center;">
              <input type="checkbox" id="autoDialEnabled" style="width:auto;" />
              הפעל חיוג אוטומטי למספרים חדשים
            </label>
          </div>
          <div class="row">
            <div style="flex:1; min-width:220px;">
              <label>קצב (כמה מספרים בכל ריצה)</label>
              <input id="autoDialBatch" type="number" min="1" max="50" value="5" />
            </div>
            <div style="flex:1; min-width:220px;">
              <label>הפרש בין ריצות (שניות)</label>
              <input id="autoDialInterval" type="number" min="5" max="3600" value="30" />
            </div>
          </div>
          <div class="row" style="margin-top:10px;">
            <button id="saveDialerBtn">שמור הגדרות חיוג</button>
            <span class="status" id="dialerStatus"></span>
          </div>
        </div>

        <div class="card">
          <h2>הנדסת שיחה (מה הסוכן אומר בפועל)</h2>
          <p class="status">מה שתכתוב כאן נשמר ומשפיע מיד על השיחות הבאות.</p>

          <div style="height:6px;"></div>
          <h3 style="margin:0 0 8px; font-size:14px;">מידע לסוכן (Knowledge Base)</h3>
          <p class="status" style="margin-top:0;">זה המידע שהסוכן משתמש בו כדי לענות על שאלות על הפרשת חלה / שיעור תורה. אם אין לך פרטים מדויקים (תאריך/כתובת), כתוב שהלשכה תיתן את הפרטים בהרשמה.</p>
          <textarea id="knowledgeBase" placeholder="כתוב כאן את כל הידע של הסוכן..."></textarea>

          <div style="height:12px;"></div>
          <label>פתיח קבוע (מה אומרים ישר כשעונים)</label>
          <div class="row" style="gap:12px; align-items:flex-start;">
            <div style="flex:1; min-width:240px;">
              <label style="margin-top:0;">לגברים (שיעור תורה)</label>
              <textarea id="openingScriptMale" placeholder="פתיח לגברים..."></textarea>
            </div>
            <div style="flex:1; min-width:240px;">
              <label style="margin-top:0;">לנשים (הפרשת חלה)</label>
              <textarea id="openingScriptFemale" placeholder="פתיח לנשים..."></textarea>
            </div>
          </div>
          <div class="row" style="margin-top:10px;">
            <button id="saveKbBtn">שמור</button>
            <button class="secondary" id="showCallsBtn">שיחות אחרונות</button>
            <button class="secondary" id="reloadBtn">רענן</button>
            <span class="status" id="kbStatus"></span>
          </div>
        </div>
      </div>
    </main>
    <div class="toast" id="toast"><div class="t" id="toastTitle"></div><div class="m" id="toastMsg"></div></div>

    <div class="modalOverlay" id="testModal">
      <div class="modal">
        <div class="row" style="justify-content:space-between; align-items:center;">
          <h3>הוספת מספר בדיקה</h3>
          <button class="secondary" id="closeTestModalBtn">סגור</button>
        </div>
        <div class="status" style="margin-top:4px;">ממלאים שם, מספר ומין — וזה נכנס לאנשי הקשר. אחרי זה אפשר ללחוץ על אייקון החיוג בטבלה.</div>
        <label>שם</label>
        <input id="testName" placeholder="למשל: מיכה צור" />
        <label>מספר טלפון</label>
        <input id="testPhone" placeholder="למשל: 0549050710 או +9725..." />
        <label>מין</label>
        <select id="testGender">
          <option value="">לא יודע</option>
          <option value="male">זכר</option>
          <option value="female">נקבה</option>
        </select>
        <div class="row" style="margin-top:12px;">
          <button id="saveTestBtn">שמור לאנשי קשר</button>
          <span class="status" id="testStatus"></span>
        </div>
      </div>
    </div>

    <div class="modalOverlay" id="callsModal">
      <div class="modal" style="max-width: 820px;">
        <div class="row" style="justify-content:space-between; align-items:center;">
          <h3>שיחות אחרונות — תמלול ותגובות</h3>
          <button class="secondary" id="closeCallsModalBtn">סגור</button>
        </div>
        <div class="status" style="margin-top:4px;">
          כאן רואים מה הלקוח אמר (תמלול) ומה הסוכן ענה בפועל. זה מגיע מהמסד נתונים (<code>call_messages</code>).
        </div>

        <div style="height:10px;"></div>
        <div class="row" style="gap:10px;">
          <div style="flex:1; min-width:260px;">
            <label style="margin-top:0;">בחר שיחה</label>
            <select id="callsSelect"></select>
          </div>
          <div class="row" style="align-items:flex-end; gap:8px;">
            <button class="secondary" id="refreshCallsBtn">רענן שיחות</button>
          </div>
        </div>
        <div class="status" id="callsMeta" style="margin-top:8px;"></div>

        <label>תמלול</label>
        <textarea id="callsTranscript" readonly placeholder="בחר שיחה כדי לראות את התמלול..." style="min-height: 300px;"></textarea>
      </div>
    </div>

    <script>
      const $ = (id) => document.getElementById(id);
      function setStatus(el, msg, ok=true){ el.textContent = msg; el.className = "status " + (ok ? "ok" : "err"); }
      function toast(title, msg, ok=true){
        const t = $("toast"); if(!t) return;
        $("toastTitle").textContent = title;
        $("toastMsg").textContent = msg;
        t.style.display = "block";
        t.style.borderColor = ok ? "rgba(124,255,178,.35)" : "rgba(255,124,124,.35)";
        clearTimeout(window.__toastTimer);
        window.__toastTimer = setTimeout(() => { t.style.display = "none"; }, 3200);
      }
      async function api(path, opts){
        const res = await fetch(path, opts);
        const text = await res.text();
        let json = null;
        try { json = JSON.parse(text); } catch {}
        if(!res.ok) throw new Error(json?.error || text || ("HTTP " + res.status));
        return json ?? text;
      }

      async function loadAll(){
        try{
          const data = await api("/api/admin/state");
          $("knowledgeBase").value = data.knowledgeBase || "";
          $("openingScriptMale").value = data.openingScriptMale || "";
          $("openingScriptFemale").value = data.openingScriptFemale || "";
          $("autoDialEnabled").checked = !!data.autoDialEnabled;
          $("autoDialBatch").value = data.autoDialBatchSize ?? 5;
          $("autoDialInterval").value = data.autoDialIntervalSeconds ?? 30;
          setStatus($("topStatus"), "מחובר. עודכן: " + (data.updatedAt || "—"), true);
          try{
            const s = await api("/api/contacts/stats");
            const el = $("stats");
            el.innerHTML = "";
            const items = [
              ['סה"כ אנשי קשר', s.total],
              ["חדשים לחיוג", s.new],
              ["נכשלו", s.failed],
              ["DNC", s.dnc],
            ];
            for(const [k,v] of items){
              const d = document.createElement("div");
              d.className = "stat";
              d.innerHTML = '<div class="k">'+k+'</div><div class="v">'+v+'</div>';
              el.appendChild(d);
            }
          }catch{}
        }catch(e){
          setStatus($("topStatus"), "שגיאה בטעינה: " + e.message, false);
        }
      }

      $("uploadXlsxBtn").addEventListener("click", async () => {
        const f = $("xlsxFile").files?.[0];
        if(!f) return setStatus($("importStatus"), "בחר קובץ קודם", false);
        const fd = new FormData();
        fd.append("file", f);
        try{
          const out = await api("/api/contacts/import-xlsx", { method:"POST", body: fd });
          setStatus($("importStatus"), "יובאו " + out.imported + " אנשי קשר (" + (out.type || "file") + ")", true);
          toast("ייבוא הושלם", "יובאו " + out.imported + " אנשי קשר", true);
          await loadAll();
        }catch(e){
          setStatus($("importStatus"), "שגיאה: " + e.message, false);
          toast("שגיאה בייבוא", e.message, false);
        }
      });

      $("importSheetBtn").addEventListener("click", async () => {
        const url = $("sheetUrl").value.trim();
        if(!url) return setStatus($("sheetStatus"), "הדבק קישור", false);
        try{
          const out = await api("/api/contacts/import-sheet", { method:"POST", headers: {"Content-Type":"application/json"}, body: JSON.stringify({ url }) });
          setStatus($("sheetStatus"), "יובאו " + out.imported + " אנשי קשר", true);
          toast("ייבוא הושלם", "יובאו " + out.imported + " אנשי קשר", true);
          await loadAll();
        }catch(e){
          setStatus($("sheetStatus"), "שגיאה: " + e.message, false);
          toast("שגיאה בייבוא", e.message, false);
        }
      });

      $("saveKbBtn").addEventListener("click", async () => {
        try{
          await api("/api/admin/settings", {
            method:"POST",
            headers: {"Content-Type":"application/json"},
            body: JSON.stringify({
              knowledgeBase: $("knowledgeBase").value,
              openingScriptMale: $("openingScriptMale").value,
              openingScriptFemale: $("openingScriptFemale").value
            })
          });
          setStatus($("kbStatus"), "נשמר", true);
          toast("נשמר", "המידע עודכן וישפיע על השיחות הבאות", true);
          await loadAll();
        }catch(e){
          setStatus($("kbStatus"), "שגיאה: " + e.message, false);
          toast("שגיאה", e.message, false);
        }
      });

      $("reloadBtn").addEventListener("click", async () => {
        await loadAll();
        setStatus($("kbStatus"), "רוענן", true);
      });

      $("saveDialerBtn").addEventListener("click", async () => {
        try{
          await api("/api/admin/dialer", {
            method:"POST",
            headers: {"Content-Type":"application/json"},
            body: JSON.stringify({
              autoDialEnabled: $("autoDialEnabled").checked,
              autoDialBatchSize: Number($("autoDialBatch").value || 5),
              autoDialIntervalSeconds: Number($("autoDialInterval").value || 30)
            })
          });
          setStatus($("dialerStatus"), "נשמר", true);
          toast("נשמר", "הגדרות החיוג עודכנו", true);
          await loadAll();
        }catch(e){
          setStatus($("dialerStatus"), "שגיאה: " + e.message, false);
          toast("שגיאה", e.message, false);
        }
      });

      // Dropzone UX
      const dz = $("dropzone");
      dz.addEventListener("dragover", (e) => { e.preventDefault(); dz.style.borderColor = "rgba(109,140,255,.7)"; });
      dz.addEventListener("dragleave", () => { dz.style.borderColor = "rgba(255,255,255,.22)"; });
      dz.addEventListener("drop", (e) => {
        e.preventDefault();
        dz.style.borderColor = "rgba(255,255,255,.22)";
        const f = e.dataTransfer.files?.[0];
        if(!f) return;
        $("xlsxFile").files = e.dataTransfer.files;
        $("uploadXlsxBtn").click();
      });

      // Calls modal (transcripts)
      const callsModal = $("callsModal");
      function openCallsModal(){
        callsModal.style.display = "block";
        $("callsMeta").textContent = "";
        $("callsTranscript").value = "";
        loadRecentCalls().catch((e) => {
          $("callsMeta").textContent = "שגיאה בטעינת שיחות: " + e.message;
        });
      }
      function closeCallsModal(){
        callsModal.style.display = "none";
      }
      $("showCallsBtn").addEventListener("click", openCallsModal);
      $("closeCallsModalBtn").addEventListener("click", closeCallsModal);
      callsModal.addEventListener("click", (e) => { if (e.target === callsModal) closeCallsModal(); });

      async function loadRecentCalls(){
        const out = await api("/api/calls/recent?limit=20");
        const select = $("callsSelect");
        const calls = out.calls || [];
        select.innerHTML = "";
        if(!calls.length){
          const opt = document.createElement("option");
          opt.value = "";
          opt.textContent = "אין שיחות עדיין";
          select.appendChild(opt);
          $("callsMeta").textContent = "";
          $("callsTranscript").value = "";
          return;
        }
        for(const c of calls){
          const opt = document.createElement("option");
          opt.value = c.callSid;
          const who = (c.firstName ? (c.firstName + " • ") : "") + (c.phone || "");
          const when = c.updatedAt ? (" • " + String(c.updatedAt).replace("T"," ").replace("Z","")) : "";
          opt.textContent = who + when;
          select.appendChild(opt);
        }
        await loadCallMessages(select.value);
      }

      async function loadCallMessages(callSid){
        if(!callSid){
          $("callsMeta").textContent = "";
          $("callsTranscript").value = "";
          return;
        }
        const out = await api("/api/calls/messages?callSid=" + encodeURIComponent(callSid) + "&limit=120");
        const msgs = out.messages || [];
        $("callsMeta").textContent = "CallSid: " + callSid + " • הודעות: " + msgs.length;
        const lines = msgs.map((m) => {
          const role = m.role === "user" ? "לקוח" : m.role === "assistant" ? "סוכן" : m.role;
          const ts = m.createdAt ? ("[" + m.createdAt + "] ") : "";
          return ts + role + ": " + (m.content || "");
        });
        $("callsTranscript").value = lines.join("\n\n");
      }

      $("refreshCallsBtn").addEventListener("click", () => loadRecentCalls().catch((e) => {
        $("callsMeta").textContent = "שגיאה: " + e.message;
      }));
      $("callsSelect").addEventListener("change", () => loadCallMessages($("callsSelect").value).catch((e) => {
        $("callsMeta").textContent = "שגיאה: " + e.message;
      }));

      async function loadContacts(){
        try{
          const out = await api("/api/contacts/list?limit=500&offset=0");
          const q = ($("search").value || "").trim();
          const rows = (out.rows || []).filter((r) => {
            if(!q) return true;
            const s = ((r.first_name||"") + " " + (r.phone||"") + " " + (r.gender||"")).toLowerCase();
            return s.includes(q.toLowerCase());
          });
          const body = $("contactsBody");
          body.innerHTML = "";
          for(const r of rows){
            const tr = document.createElement("tr");
            const gender = r.gender === "female" ? "נקבה" : r.gender === "male" ? "זכר" : "—";
            const dnc = r.do_not_call ? '<span class="badge bad">כן</span>' : '<span class="badge good">לא</span>';
            const status = '<span class="badge">'+(r.dial_status || "—")+'</span>';
            const dialBtn = r.do_not_call
              ? '<span class="badge bad">DNC</span>'
              : '<button class="iconBtn dialBtn" data-phone="'+(r.phone||"")+'" title="חייג עכשיו">' +
                '<svg viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">' +
                '<path d="M7.2 2.9c.6-.5 1.4-.5 2 0l2 1.9c.6.6.7 1.5.2 2.2l-1 1.4c-.3.4-.3 1 .1 1.4l3.9 3.9c.4.4 1 .4 1.4.1l1.4-1c.7-.5 1.6-.4 2.2.2l1.9 2c.5.6.5 1.4 0 2-1.2 1.4-2.8 2-4.6 1.6-3.2-.7-6.5-3.1-9.2-5.8C5.1 12.4 2.7 9.1 2 5.9c-.4-1.8.2-3.4 1.6-4.6Z" stroke="rgba(255,255,255,.9)" stroke-width="1.6" stroke-linejoin="round"/>' +
                '</svg></button>';

            const delBtn =
              '<button class="iconBtn delBtn" data-phone="'+(r.phone||"")+'" title="מחק איש קשר">' +
              '<svg viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">' +
              '<path d="M9 3h6m-8 4h10m-9 0 1 14h6l1-14M10 11v7M14 11v7" stroke="rgba(255,255,255,.9)" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"/>' +
              '</svg></button>';

            tr.innerHTML =
              '<td><div class="row" style="gap:6px;">'+ dialBtn + delBtn +'</div></td>' +
              '<td><strong>'+(r.first_name || "—")+'</strong></td>' +
              '<td>'+ (r.phone || "—") +'</td>' +
              '<td>'+ gender +'</td>' +
              '<td>'+ status +'</td>' +
              '<td>'+ dnc +'</td>' +
              '<td>'+ (r.dial_attempts ?? 0) +'</td>';
            body.appendChild(tr);
          }
          setStatus($("listStatus"), "מוצגים " + rows.length + " מתוך " + (out.rows?.length || 0), true);
        }catch(e){
          setStatus($("listStatus"), "שגיאה: " + e.message, false);
        }
      }

      // Test modal open/close
      const testModal = $("testModal");
      function openTestModal(){
        testModal.style.display = "block";
        $("testStatus").textContent = "";
        $("testName").focus();
      }
      function closeTestModal(){
        testModal.style.display = "none";
      }
      $("addTestBtn").addEventListener("click", openTestModal);
      $("closeTestModalBtn").addEventListener("click", closeTestModal);
      testModal.addEventListener("click", (e) => { if (e.target === testModal) closeTestModal(); });

      $("saveTestBtn").addEventListener("click", async () => {
        const name = ($("testName").value || "").trim();
        const phone = ($("testPhone").value || "").trim();
        const gender = ($("testGender").value || "").trim();
        if(!phone) return setStatus($("testStatus"), "חסר מספר טלפון", false);
        try{
          const out = await api("/api/contacts/add", { method:"POST", headers: {"Content-Type":"application/json"}, body: JSON.stringify({ first_name: name, phone, gender }) });
          setStatus($("testStatus"), "נשמר: " + (out.contact?.phone || ""), true);
          toast("נשמר", "המספר נוסף לאנשי הקשר", true);
          // clear for next entry
          $("testPhone").value = "";
          // refresh list if open
          if($("listSection").style.display !== "none") await loadContacts();
          await loadAll();
        }catch(e){
          setStatus($("testStatus"), "שגיאה: " + e.message, false);
          toast("שגיאה", e.message, false);
        }
      });

      // Dial click (event delegation)
      $("contactsBody").addEventListener("click", async (e) => {
        const btn = e.target.closest?.(".dialBtn");
        const del = e.target.closest?.(".delBtn");

        if(btn){
          const phone = btn.getAttribute("data-phone");
          if(!phone) return;
          btn.disabled = true;
          try{
            const out = await api("/api/contacts/dial", { method:"POST", headers: {"Content-Type":"application/json"}, body: JSON.stringify({ phone }) });
            toast("מחייג...", "יצאה שיחה ל-" + (out.to || phone), true);
            if($("listSection").style.display !== "none") await loadContacts();
          }catch(err){
            toast("שגיאת חיוג", err.message || String(err), false);
          } finally {
            btn.disabled = false;
          }
          return;
        }

        if(del){
          const phone = del.getAttribute("data-phone");
          if(!phone) return;
          const ok = window.confirm("למחוק את איש הקשר הזה? (" + phone + ")");
          if(!ok) return;
          del.disabled = true;
          try{
            const out = await api("/api/contacts/remove", { method:"POST", headers: {"Content-Type":"application/json"}, body: JSON.stringify({ phone }) });
            toast("נמחק", "נמחקו " + (out.deleted ?? 0) + " רשומות", true);
            if($("listSection").style.display !== "none") await loadContacts();
            await loadAll();
          }catch(err){
            toast("שגיאת מחיקה", err.message || String(err), false);
          } finally {
            del.disabled = false;
          }
        }
      });

      $("showListBtn").addEventListener("click", async () => {
        const sec = $("listSection");
        sec.style.display = sec.style.display === "none" ? "block" : "none";
        if(sec.style.display === "block") await loadContacts();
      });
      $("refreshListBtn").addEventListener("click", loadContacts);
      $("search").addEventListener("input", () => { if($("listSection").style.display !== "none") loadContacts(); });

      loadAll();
    </script>
  </body>
</html>`;
}


