export function renderAdminPage({ adminToken = "" } = {}) {
  return `<!doctype html>
<html lang="he" dir="rtl">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>סוכן AI טלפוני</title>
    <style>
      :root{
        /* Techelet (light cyan) dominant theme */
        --bg:#bfeeff;
        --panel:#d8f6ff;
        --panel2:#b3ecff;
        --border:#49bfe0;
        --text:#071a2a;
        --muted:rgba(7,26,42,.78);
        --muted2:rgba(7,26,42,.62);
        --brand:#0ea5c6;
        --brand2:#35c6ea;
        --good:#0b7a43;
        --bad:#b32626;
        --shadow: 0 16px 40px rgba(2,30,55,.18);
      }
      *{ box-sizing:border-box; }
      body { font-family: system-ui, -apple-system, Arial; margin: 0; background:
        radial-gradient(1100px 760px at 18% 0%, rgba(53,198,234,.55), transparent 62%),
        radial-gradient(900px 700px at 78% 10%, rgba(14,165,198,.32), transparent 60%),
        radial-gradient(900px 700px at 50% 120%, rgba(53,198,234,.40), transparent 65%),
        var(--bg);
        color: var(--text);
      }
      header { padding: 22px 24px; border-bottom: 1px solid rgba(7,26,42,.16); background: rgba(216,246,255,.92); backdrop-filter: blur(10px); position: sticky; top:0; z-index: 10; }
      .wrap { max-width: 1100px; margin: 0 auto; }
      .top { display:flex; justify-content: space-between; align-items: center; gap: 16px; flex-wrap: wrap; }
      h1 { margin: 0; font-size: 18px; letter-spacing: .2px; }
      .sub { font-size: 12px; color: var(--muted2); margin-top: 4px; }
      main { padding: 24px; }
      .grid { display: grid; grid-template-columns: 1fr; gap: 16px; }
      @media (min-width: 980px){ .grid { grid-template-columns: 1.05fr .95fr; } }
      .card { background: linear-gradient(180deg, rgba(216,246,255,.98), rgba(179,236,255,.92)); border: 1px solid rgba(7,26,42,.16); border-radius: 16px; padding: 16px; box-shadow: var(--shadow); }
      .card h2 { margin: 0 0 10px; font-size: 15px; }
      .pill { display:inline-flex; gap:8px; align-items:center; padding: 6px 10px; border-radius: 999px; border:1px solid rgba(14,165,198,.42); background: rgba(53,198,234,.30); color: var(--muted); font-size: 12px; }
      .stats { display:flex; gap:10px; flex-wrap:wrap; }
      .stat { min-width: 120px; padding: 10px 12px; border-radius: 12px; border:1px solid rgba(7,26,42,.18); background: rgba(216,246,255,.92); }
      .stat .k { font-size: 11px; color: var(--muted2); }
      .stat .v { font-size: 16px; margin-top: 2px; }
      label { display:block; font-size: 12px; color: var(--muted); margin: 10px 0 6px; }
      textarea, input { width: 100%; padding: 11px 12px; border-radius: 12px; border: 1px solid rgba(7,26,42,.20); background: rgba(216,246,255,.95); color: var(--text); outline: none; }
      textarea:focus, input:focus { border-color: rgba(14,165,198,.70); box-shadow: 0 0 0 4px rgba(53,198,234,.28); }
      textarea { min-height: 220px; resize: vertical; }
      button { cursor: pointer; padding: 10px 12px; border-radius: 12px; border: 1px solid rgba(14,165,198,.45); background: linear-gradient(180deg, rgba(14,165,198,.98), rgba(11,139,169,.90)); color: #fff; }
      button:hover { filter: brightness(1.07); }
      button.secondary { background: rgba(216,246,255,.96); color: var(--text); border-color: rgba(7,26,42,.22); }
      .row { display:flex; gap: 10px; flex-wrap: wrap; align-items: center; }
      .status { font-size: 12px; color: var(--muted2); }
      .ok { color: var(--good); }
      .err { color: var(--bad); }
      code { background: rgba(53,198,234,.30); padding: 2px 6px; border-radius: 8px; border:1px solid rgba(14,165,198,.38); }
      a { color: #0a7fa0; }
      hr { border:0; border-top:1px solid rgba(7,26,42,.12); margin:16px 0; }
      .tabs{ display:flex; gap:8px; flex-wrap:wrap; margin-top: 8px; }
      .tab{ padding: 8px 10px; border-radius: 999px; border:1px solid rgba(7,26,42,.18); background: rgba(216,246,255,.92); color: var(--muted); font-size: 12px; cursor:pointer; }
      .tab.active{ border-color: rgba(14,165,198,.52); background: rgba(53,198,234,.36); color: var(--text); }
      .tableWrap{ overflow:auto; border-radius: 14px; border:1px solid rgba(7,26,42,.18); background: rgba(216,246,255,.92); }
      .tableWrap.fixed5 { max-height: 320px; } /* ~5 rows + header; scroll inside */
      table{ width:100%; border-collapse: collapse; min-width: 820px; }
      th, td{ text-align:right; padding: 10px 12px; border-bottom:1px solid rgba(7,26,42,.12); font-size: 12px; color: var(--muted); }
      th{ color:var(--text); font-weight:600; background: rgba(53,198,234,.40); position: sticky; top:0; }
      td strong{ color:var(--text); font-weight:600; }
      .badge{ display:inline-flex; padding: 3px 8px; border-radius: 999px; border:1px solid rgba(7,26,42,.14); background: rgba(241,252,255,.92); font-size: 11px; }
      .badge.good{ border-color: rgba(11,122,67,.25); color: var(--good); }
      .badge.bad{ border-color: rgba(179,38,38,.25); color: var(--bad); }
      tr.dncRow td{ background: rgba(179,38,38,.16); }
      tr.dncRow td strong, tr.dncRow td{ color: rgba(179,38,38,.96); }
      tr.dncRow .badge.good{ border-color: rgba(179,38,38,.22); color: rgba(179,38,38,.96); }
      .drop {
        border: 1px dashed rgba(14,165,198,.70);
        background: rgba(216,246,255,.92);
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
        background: rgba(216,246,255,.98); border:1px solid rgba(7,26,42,.18);
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
        border: 1px solid rgba(7,26,42,.22);
        background: rgba(216,246,255,.96);
      }
      .iconBtn:hover{ filter: brightness(1.12); }
      .iconBtn svg{ width: 18px; height: 18px; opacity: .95; }
      /* Make action icons clearly visible on light background */
      .iconBtn svg *{ stroke: rgba(7,26,42,.92) !important; }
      .delBtn{ background: rgba(179,38,38,.12); border-color: rgba(179,38,38,.45); }
      .delBtn:hover{ filter: brightness(1.08); }
      .delBtn svg *{ stroke: rgba(179,38,38,.92) !important; }
      .dncBtn{ background: rgba(7,26,42,.10); border-color: rgba(7,26,42,.35); }
      .dncBtn:hover{ filter: brightness(1.08); }

      /* Modal */
      .modalOverlay{
        position: fixed; inset:0; z-index: 9998;
        background: rgba(7,26,42,.32);
        display:none;
        padding: 16px;
      }
      .modal{
        max-width: 520px;
        margin: 64px auto;
        background: rgba(216,246,255,.98);
        border: 1px solid rgba(7,26,42,.20);
        border-radius: 16px;
        box-shadow: var(--shadow);
        padding: 14px;
      }
      .modal h3{ margin: 0 0 8px; font-size: 14px; }
      select{
        width: 100%;
        padding: 11px 12px;
        border-radius: 12px;
        border: 1px solid rgba(7,26,42,.20);
        background: rgba(216,246,255,.96);
        color: var(--text);
        outline: none;
      }
    </style>
  </head>
  <body>
    <header>
      <div class="wrap top">
        <div>
          <h1>סוכן AI טלפוני</h1>
          <div class="sub">ניהול אנשי קשר, קמפיין (תפקיד), ידע ותסריטים — ומשם חיוג אוטומטי.</div>
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
              <input id="importListName" placeholder="שם רשימה (אופציונלי) — למשל: גוגל שיטס ינואר" style="max-width:260px;" />
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
              <button class="secondary" id="showLeadsBtn">לידים</button>
            </div>
          </div>
          <div id="listSection" style="display:none; margin-top:12px;">
            <div class="row" style="justify-content:space-between; align-items:center;">
              <div class="pill">רשימות (כל ייבוא נהיה רשימה)</div>
              <div class="row" style="gap:8px;">
                <button class="secondary" id="refreshListsBtn">רענון רשימות</button>
              </div>
            </div>
            <div style="height:10px;"></div>
            <div class="tableWrap fixed5">
              <table style="min-width: 920px;">
                <thead>
                  <tr>
                    <th>פעולות</th>
                    <th>שם רשימה</th>
                    <th>סה״כ</th>
                    <th>לא בוצע</th>
                    <th>בוצע</th>
                    <th>אין מענה</th>
                    <th>לא זמין</th>
                    <th>ניתוק &lt;5ש׳</th>
                    <th>מעוניין</th>
                    <th>לא מעוניין</th>
                    <th>לא להתקשר יותר</th>
                    <th>שגוי</th>
                  </tr>
                </thead>
                <tbody id="listsBody"></tbody>
              </table>
            </div>
            <div style="height:14px;"></div>
            <div class="row" style="justify-content:space-between;">
              <div class="row" style="gap:8px;">
                <select id="listSelect" style="max-width:260px;">
                  <option value="0">רשימה: הכל</option>
                </select>
                <input id="search" placeholder="חיפוש (שם/טלפון)..." style="max-width:260px;" />
                <select id="dialFilter" style="max-width:200px;">
                  <option value="all">סינון: הכל</option>
                  <option value="new">לא בוצעה שיחה</option>
                  <option value="called">בוצעה שיחה</option>
                </select>
                <button class="secondary" id="refreshListBtn">רענון</button>
              </div>
              <span class="status" id="listStatus"></span>
            </div>
            <div style="height:10px;"></div>
            <div class="tableWrap fixed5">
              <table>
                <thead>
                  <tr>
                    <th style="width:42px; text-align:center;">
                      <input type="checkbox" id="selectAllContacts" title="בחר הכל" />
                    </th>
                    <th>פעולות</th>
                    <th>שם</th>
                    <th>טלפון</th>
                    <th>סינון</th>
                    <th>ניסיון</th>
                  </tr>
                </thead>
                <tbody id="contactsBody"></tbody>
              </table>
            </div>
            <div class="row" style="margin-top:10px; justify-content:space-between;">
              <div class="row" style="gap:8px;">
                <button class="secondary" id="deleteSelectedBtn">מחק נבחרים</button>
                <button class="secondary" id="deleteAllBtn" style="border-color: rgba(255,124,124,.35);">מחק הכל</button>
              </div>
              <span class="status" id="bulkStatus"></span>
            </div>
          </div>

          <div id="leadsSection" style="display:none; margin-top:12px;">
            <div class="row" style="justify-content:space-between;">
              <div class="row" style="gap:8px; align-items:center;">
                <select id="leadsFilter" style="max-width:220px;">
                  <option value="all">הכל</option>
                  <option value="waiting">מחכה לשיחה</option>
                  <option value="not_interested">לא מעוניין</option>
                </select>
                <button class="secondary" id="refreshLeadsBtn">רענון</button>
              </div>
              <span class="status" id="leadsStatus"></span>
            </div>
            <div style="height:10px;"></div>
            <div class="tableWrap">
              <table style="min-width: 760px;">
                <thead>
                  <tr>
                    <th>פעולות</th>
                    <th>שם</th>
                    <th>טלפון</th>
                    <th>סטטוס</th>
                    <th>עודכן</th>
                  </tr>
                </thead>
                <tbody id="leadsBody"></tbody>
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
          <div class="row" style="margin-top:8px;">
            <label style="margin:0; display:flex; gap:8px; align-items:center;">
              <input type="checkbox" id="autoDialHoursEnabled" style="width:auto;" />
              הגבל לשעות פעילות (שעון ישראל)
            </label>
          </div>
          <div class="row" style="margin-top:8px;">
            <label style="margin:0; display:flex; gap:8px; align-items:center;">
              <input type="checkbox" id="autoDialSkipFriSat" style="width:auto;" />
              אל תחייג בשישי/שבת (קריטי)
            </label>
          </div>
          <div class="row">
            <div style="flex:1; min-width:220px;">
              <label>משעה</label>
              <input id="autoDialStartTime" type="time" value="09:00" />
            </div>
            <div style="flex:1; min-width:220px;">
              <label>עד שעה</label>
              <input id="autoDialEndTime" type="time" value="17:00" />
            </div>
          </div>
          <div class="row">
            <div style="flex:1; min-width:220px;">
              <label>קצב (כמה שיחות במקביל)</label>
              <select id="autoDialBatch">
                <option value="1">1</option>
                <option value="3">3</option>
                <option value="5">5</option>
              </select>
            </div>
            <div style="flex:1; min-width:220px;">
              <label>הפרש בין ריצות</label>
              <select id="autoDialInterval">
                <option value="120">כל 2 דקות</option>
                <option value="300">כל 5 דקות</option>
                <option value="600">כל 10 דקות</option>
              </select>
            </div>
          </div>
          <div class="row">
            <div style="flex:1; min-width:220px;">
              <label>על אילו רשימות לרוץ (אפשר לבחור כמה, או “כולם”)</label>
              <select id="autoDialLists" multiple size="6"></select>
              <div class="hint">אם לא בוחרים כלום — זה מתנהג כמו “כולם”.</div>
            </div>
          </div>
          <div class="row" style="margin-top:10px;">
            <button id="saveDialerBtn">שמור הגדרות חיוג</button>
            <span class="status" id="dialerStatus"></span>
          </div>
        </div>

        <div class="card">
          <h2>קמפיין / תפקיד הסוכן (מה הסוכן אומר בפועל)</h2>
          <p class="status">מה שתכתוב כאן נשמר ומשפיע מיד על השיחות הבאות.</p>

          <div style="height:6px;"></div>
          <h3 style="margin:0 0 8px; font-size:14px;">מידע לסוכן (Knowledge Base)</h3>
          <p class="status" style="margin-top:0;">כאן אתה כותב מי הסוכן, מה מציעים, מחירים/שעות/כתובות וכל מה שצריך. אם אין לך פרטים מדויקים — כתוב שהגורם הרלוונטי יחזור עם כל הפרטים לתיאום.</p>
          <textarea id="knowledgeBase" placeholder="כתוב כאן את כל הידע של הסוכן..."></textarea>

          <div style="height:12px;"></div>
          <h3 style="margin:0 0 8px; font-size:14px;">מי חוזר ללקוח (White‑label)</h3>
          <p class="status" style="margin-top:0;">כאן מגדירים את הניסוח של החזרה אל הלקוח. לדוגמה: לעמותה/מהעמותה, לעסק/מהעסק.</p>
          <div class="row" style="gap:12px; align-items:flex-start;">
            <div style="flex:1; min-width:240px;">
              <label style="margin-top:0;">ניסוח “העברתי …”</label>
              <input id="handoffToPhrase" placeholder='למשל: לעמותה / לעסק / לבעל העסק' />
            </div>
            <div style="flex:1; min-width:240px;">
              <label style="margin-top:0;">ניסוח “יחזרו …”</label>
              <input id="handoffFromPhrase" placeholder='למשל: מהעמותה / מהעסק / מבעל העסק' />
            </div>
          </div>

          <div style="height:12px;"></div>
          <h3 style="margin:0 0 8px; font-size:14px;">מצב שיחה (Flow)</h3>
          <p class="status" style="margin-top:0;">
            <b>Guided</b>: מתאים לתסריט “שלבים” (למשל לפי PDF). <b>Handoff</b>: מצב פשוט ויציב שמטרתו להגיע מהר לאישור להעברת פרטים.
          </p>
          <div class="row" style="gap:12px; align-items:flex-start;">
            <div style="flex:1; min-width:240px;">
              <label style="margin-top:0;">מצב</label>
              <select id="campaignMode">
                <option value="handoff">Handoff (פשוט/יציב)</option>
                <option value="guided">Guided (תסריט שלבים)</option>
              </select>
            </div>
          </div>
          <div class="row" style="gap:12px; align-items:flex-start;">
            <div style="flex:1; min-width:240px;">
              <label style="margin-top:0;">מינימום משתתפות (למשל 15)</label>
              <input id="minParticipants" type="number" min="1" max="200" value="15" />
            </div>
            <div style="flex:1; min-width:240px;">
              <label style="margin-top:0;">מינימום חודשים לאירוע חוזר (למשל 6)</label>
              <input id="cooldownMonths" type="number" min="0" max="60" value="6" />
            </div>
          </div>

          <div style="height:12px;"></div>
          <label>פתיח קבוע (מה אומרים ישר כשעונים)</label>
          <div class="row" style="gap:12px; align-items:flex-start;">
            <div style="flex:1; min-width:240px;">
              <label style="margin-top:0;">פתיח</label>
              <textarea id="openingScriptFemale" placeholder="פתיח (נשים בלבד)..."></textarea>
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
        <div class="status" style="margin-top:4px;">ממלאים שם ומספר — וזה נכנס לאנשי הקשר. אחרי זה אפשר ללחוץ על אייקון החיוג בטבלה.</div>
        <label>שם</label>
        <input id="testName" placeholder="למשל: מיכה צור" />
        <label>מספר טלפון</label>
        <input id="testPhone" placeholder="למשל: 0549050710 או +9725..." />
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

    <div class="modalOverlay" id="leadEditModal">
      <div class="modal">
        <div class="row" style="justify-content:space-between; align-items:center;">
          <h3>עריכת ליד</h3>
          <button class="secondary" id="closeLeadEditBtn">סגור</button>
        </div>
        <div class="status" style="margin-top:4px;">אפשר לעדכן שם/טלפון/סטטוס ידנית.</div>
        <input type="hidden" id="leadOldPhone" />
        <label>שם</label>
        <input id="leadFirstName" placeholder="שם" />
        <label>טלפון</label>
        <input id="leadPhone" placeholder="05XXXXXXXX או +972..." />
        <label>סטטוס</label>
        <select id="leadStatus">
          <option value="waiting">מעוניינת / מחכה לשיחה</option>
          <option value="not_interested">לא מעוניינת</option>
        </select>
        <div class="row" style="margin-top:12px;">
          <button id="saveLeadEditBtn">שמור</button>
          <span class="status" id="leadEditStatus"></span>
        </div>
      </div>
    </div>

    <script>
      // Injected by the server (GET /admin/app). This token lives only in JS memory.
      // If you refresh, it disappears and you'll be redirected back to /admin (login).
      window.__ADMIN_TOKEN = ${JSON.stringify(String(adminToken || ""))};

      const $ = (id) => document.getElementById(id);
      // Basic HTML escaping for rendering user-provided list names safely.
      function escapeHtml(s){
        return String(s ?? "")
          .replace(/&/g, "&amp;")
          .replace(/</g, "&lt;")
          .replace(/>/g, "&gt;")
          .replace(/"/g, "&quot;")
          .replace(/'/g, "&#39;");
      }
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
        const tok = String(window.__ADMIN_TOKEN || "").trim();
        const base = Object.assign({ cache: "no-store" }, (opts || {}));
        const headers = Object.assign({}, (base.headers || {}));
        if(tok) headers["X-Admin-Token"] = tok;
        base.headers = headers;
        const res = await fetch(path, base);
        const text = await res.text();
        let json = null;
        try { json = JSON.parse(text); } catch {}
        if(!res.ok){
          // If we get 401 (expired/missing token), force a redirect to the login screen.
          if(res.status === 401){
            try{
              setStatus($("topStatus"), "צריך להתחבר מחדש.", false);
            }catch{}
            try{
              window.location.href = "/admin?ts=" + Date.now();
            }catch{}
          }
          throw new Error(json?.error || text || ("HTTP " + res.status));
        }
        return json ?? text;
      }

      async function loadAll(){
        try{
          const data = await api("/api/admin/state");
          $("knowledgeBase").value = data.knowledgeBase || "";
          $("openingScriptFemale").value = data.openingScriptFemale || "";
          if($("handoffToPhrase")) $("handoffToPhrase").value = data.handoffToPhrase || "לעמותה";
          if($("handoffFromPhrase")) $("handoffFromPhrase").value = data.handoffFromPhrase || "מהעמותה";
          if($("campaignMode")) $("campaignMode").value = data.campaignMode || "handoff";
          if($("minParticipants")) $("minParticipants").value = data.minParticipants ?? 15;
          if($("cooldownMonths")) $("cooldownMonths").value = data.cooldownMonths ?? 6;
          $("autoDialEnabled").checked = !!data.autoDialEnabled;
          $("autoDialBatch").value = String(data.autoDialBatchSize ?? 5);
          $("autoDialInterval").value = String(data.autoDialIntervalSeconds ?? 300);
          if($("autoDialHoursEnabled")) $("autoDialHoursEnabled").checked = data.autoDialHoursEnabled !== false;
          if($("autoDialStartTime")) $("autoDialStartTime").value = String(data.autoDialStartTime || "09:00");
          if($("autoDialEndTime")) $("autoDialEndTime").value = String(data.autoDialEndTime || "17:00");
          if($("autoDialSkipFriSat")) $("autoDialSkipFriSat").checked = data.autoDialSkipFriSat !== false;
          // Auto-dial target lists (multi-select)
          try{
            await loadAutoDialLists(Array.isArray(data.autoDialListIds) ? data.autoDialListIds : []);
          }catch{}
          setStatus($("topStatus"), "מחובר. עודכן: " + (data.updatedAt || "—"), true);
          try{
            const s = await api("/api/contacts/stats");
            const el = $("stats");
            el.innerHTML = "";
            const items = [
              ['סה"כ אנשי קשר', s.total],
              ["חדשים לחיוג", s.new],
              ["נכשלו", s.failed],
              ["אל תתקשרו יותר", s.dnc],
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

      async function loadAutoDialLists(selectedIds){
        const sel = $("autoDialLists");
        if(!sel) return;
        const out = await api("/api/contact-lists");
        const rows = out?.rows || [];
        const chosen = new Set((Array.isArray(selectedIds)?selectedIds:[]).map((x)=>Number(x||0)).filter((n)=>n>0));
        sel.innerHTML =
          '<option value="0">כולם</option>' +
          rows.map((r)=>{
            const nm = escapeHtml(r.name || ("רשימה " + r.id));
            return '<option value="' + String(r.id) + '">' + nm + "</option>";
          }).join("");
        // If none selected => select "all" for clarity
        if(!chosen.size){
          Array.from(sel.options).forEach(o => o.selected = String(o.value)==="0");
        } else {
          Array.from(sel.options).forEach(o => o.selected = chosen.has(Number(o.value||0)));
        }
        // UX rule: selecting "all" clears others; selecting others clears "all".
        sel.addEventListener("change", () => {
          const vals = Array.from(sel.selectedOptions).map(o => Number(o.value||0));
          if(vals.includes(0)){
            Array.from(sel.options).forEach(o => o.selected = String(o.value)==="0");
          } else {
            const any = vals.some(v => v>0);
            if(any){
              const allOpt = Array.from(sel.options).find(o => String(o.value)==="0");
              if(allOpt) allOpt.selected = false;
            }
          }
        }, { once:false });
      }

      $("uploadXlsxBtn").addEventListener("click", async () => {
        const f = $("xlsxFile").files?.[0];
        if(!f) return setStatus($("importStatus"), "בחר קובץ קודם", false);
        // If the user didn't explicitly edit the name, default to the uploaded filename.
        try{
          const el = $("importListName");
          const touched = el && el.getAttribute("data-touched") === "1";
          if(el && !touched && f?.name) el.value = f.name;
        }catch{}
        const fd = new FormData();
        fd.append("file", f);
        fd.append("listName", String($("importListName")?.value || "").trim());
        try{
          const out = await api("/api/contacts/import-xlsx", { method:"POST", body: fd });
          const extra = out?.listId ? (" • רשימה #" + out.listId) : "";
          setStatus($("importStatus"), "יובאו " + out.imported + " אנשי קשר (" + (out.type || "file") + ")" + extra, true);
          toast("ייבוא הושלם", "יובאו " + out.imported + " אנשי קשר" + extra, true);
          // Reset for next import (avoid reusing the previous list name/file).
          try{ if($("importListName")) { $("importListName").value = ""; $("importListName").setAttribute("data-touched","0"); } }catch{}
          try{ if($("xlsxFile")) $("xlsxFile").value = ""; }catch{}
          await loadAll();
          if($("listSection")?.style.display !== "none"){
            await loadLists();
            await loadContacts();
          }
        }catch(e){
          setStatus($("importStatus"), "שגיאה: " + e.message, false);
          toast("שגיאה בייבוא", e.message, false);
        }
      });

      $("importSheetBtn").addEventListener("click", async () => {
        const url = $("sheetUrl").value.trim();
        if(!url) return setStatus($("sheetStatus"), "הדבק קישור", false);
        try{
          const listName = String($("importListName")?.value || "").trim();
          const out = await api("/api/contacts/import-sheet", { method:"POST", headers: {"Content-Type":"application/json"}, body: JSON.stringify({ url, listName }) });
          const extra = out?.listId ? (" • רשימה #" + out.listId) : "";
          setStatus($("sheetStatus"), "יובאו " + out.imported + " אנשי קשר" + extra, true);
          toast("ייבוא הושלם", "יובאו " + out.imported + " אנשי קשר" + extra, true);
          // Reset for next import.
          try{ if($("importListName")) { $("importListName").value = ""; $("importListName").setAttribute("data-touched","0"); } }catch{}
          await loadAll();
          if($("listSection")?.style.display !== "none"){
            await loadLists();
            await loadContacts();
          }
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
              openingScriptFemale: $("openingScriptFemale").value,
              handoffToPhrase: $("handoffToPhrase") ? $("handoffToPhrase").value : undefined,
              handoffFromPhrase: $("handoffFromPhrase") ? $("handoffFromPhrase").value : undefined,
              campaignMode: $("campaignMode") ? $("campaignMode").value : undefined,
              minParticipants: $("minParticipants") ? Number($("minParticipants").value || 15) : undefined,
              cooldownMonths: $("cooldownMonths") ? Number($("cooldownMonths").value || 6) : undefined
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
          const listSel = $("autoDialLists");
          const selected = listSel ? Array.from(listSel.selectedOptions).map(o => Number(o.value||0)) : [];
          const autoDialListIds = selected.filter(n => Number.isFinite(n) && n > 0);
          await api("/api/admin/dialer", {
            method:"POST",
            headers: {"Content-Type":"application/json"},
            body: JSON.stringify({
              autoDialEnabled: $("autoDialEnabled").checked,
              autoDialBatchSize: Number($("autoDialBatch").value || 5),
              autoDialIntervalSeconds: Number($("autoDialInterval").value || 300),
              autoDialHoursEnabled: $("autoDialHoursEnabled") ? $("autoDialHoursEnabled").checked : true,
              autoDialStartTime: $("autoDialStartTime") ? String($("autoDialStartTime").value || "09:00") : "09:00",
              autoDialEndTime: $("autoDialEndTime") ? String($("autoDialEndTime").value || "17:00") : "17:00",
              autoDialSkipFriSat: $("autoDialSkipFriSat") ? $("autoDialSkipFriSat").checked : true,
              autoDialListIds
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
        try{
          const el = $("importListName");
          const touched = el && el.getAttribute("data-touched") === "1";
          if(el && !touched && f?.name) el.value = f.name;
        }catch{}
        $("uploadXlsxBtn").click();
      });

      // Import list name UX: only override from filename if user didn't type manually.
      try{
        const nameEl = $("importListName");
        if(nameEl){
          nameEl.setAttribute("data-touched","0");
          nameEl.addEventListener("input", () => {
            nameEl.setAttribute("data-touched", String((nameEl.value || "").trim() ? "1" : "0"));
          });
        }
        const fileEl = $("xlsxFile");
        if(fileEl){
          fileEl.addEventListener("change", () => {
            const f = fileEl.files?.[0];
            const el = $("importListName");
            const touched = el && el.getAttribute("data-touched") === "1";
            if(f?.name && el && !touched) el.value = f.name;
          });
        }
      }catch{}

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
        // NOTE: This JS is embedded inside a template literal in Node.
        // We must escape backslashes so the browser receives a valid JS string literal.
        $("callsTranscript").value = lines.join("\\n\\n");
      }

      $("refreshCallsBtn").addEventListener("click", () => loadRecentCalls().catch((e) => {
        $("callsMeta").textContent = "שגיאה: " + e.message;
      }));
      $("callsSelect").addEventListener("change", () => loadCallMessages($("callsSelect").value).catch((e) => {
        $("callsMeta").textContent = "שגיאה: " + e.message;
      }));

      async function loadContacts(){
        try{
          const listId = Number(($("listSelect")?.value || "0") || 0);
          const out = await api("/api/contacts/list?limit=500&offset=0" + (listId ? ("&listId=" + encodeURIComponent(listId)) : ""));
          const dialFilter = ($("dialFilter")?.value || "all").trim();
          const q = ($("search").value || "").trim();
          const rows = (out.rows || []).filter((r) => {
            if(!q) return true;
            const s = ((r.first_name||"") + " " + (r.phone||"") + " " + (r.gender||"")).toLowerCase();
            return s.includes(q.toLowerCase());
          }).filter((r) => {
            if(dialFilter === "all") return true;
            const isNew = (r.dial_status || "new") === "new";
            return dialFilter === "new" ? isNew : !isNew;
          });
          const body = $("contactsBody");
          body.innerHTML = "";
          if($("selectAllContacts")) $("selectAllContacts").checked = false;
          for(const r of rows){
            const tr = document.createElement("tr");
            const isNew = (r.dial_status || "new") === "new";
            const status = isNew ? '<span class="badge good">לא בוצעה שיחה</span>' : '<span class="badge bad">בוצעה שיחה</span>';
            const dncBadge = r.do_not_call ? '<span class="badge bad">לא להתקשר</span>' : '<span class="badge good">אפשר להתקשר</span>';
            if(r.do_not_call) tr.className = "dncRow";
            const dncBtn =
              '<button class="iconBtn dncBtn" data-phone="'+(r.phone||"")+'" data-dnc="'+(r.do_not_call?1:0)+'" title="שנה סטטוס התקשרות">' +
              '<svg viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">' +
              '<path d="M12 6v6l4 2" stroke="rgba(255,255,255,.9)" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"/>' +
              '<path d="M21 12a9 9 0 1 1-3-6.7" stroke="rgba(255,255,255,.9)" stroke-width="1.6" stroke-linecap="round"/>' +
              '</svg></button>';
            const dialBtn = r.do_not_call
              ? '<span class="badge bad">אל תתקשרו יותר</span>'
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
              '<td style="text-align:center;"><input type="checkbox" class="contactChk" data-phone="'+(r.phone||"")+'"/></td>' +
              '<td><div class="row" style="gap:6px;">'+ dialBtn + dncBtn + delBtn +'</div></td>' +
              '<td><strong>'+(r.first_name || "—")+'</strong></td>' +
              '<td>'+ (r.phone || "—") +'</td>' +
              '<td>'+ status +'</td>' +
              '<td>'+ (r.dial_attempts ?? 0) +'</td>';
            body.appendChild(tr);
          }
          setStatus($("listStatus"), "מוצגים " + rows.length + " מתוך " + (out.rows?.length || 0), true);
        }catch(e){
          setStatus($("listStatus"), "שגיאה: " + e.message, false);
        }
      }

      async function loadLeads(){
        try{
          const status = $("leadsFilter").value || "all";
          const out = await api("/api/leads/list?limit=500&offset=0&status=" + encodeURIComponent(status));
          const rows = out.rows || [];
          const body = $("leadsBody");
          body.innerHTML = "";
          for(const r of rows){
            const tr = document.createElement("tr");
            const st = r.status === "waiting"
              ? '<span class="badge good">מחכה לשיחה</span>'
              : '<span class="badge bad">לא מעוניין</span>';
            const editBtn =
              '<button class="iconBtn leadEditBtn" data-phone="'+(r.phone||"")+'" data-name="'+(r.firstName||"")+'" data-status="'+(r.status||"")+'" title="ערוך ליד">' +
              '<svg viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">' +
              '<path d="M4 20h4l10.5-10.5a2 2 0 0 0 0-3L16.5 4a2 2 0 0 0-3 0L3 14.5V20Z" stroke="rgba(255,255,255,.9)" stroke-width="1.6" stroke-linejoin="round"/>' +
              '<path d="M13.5 6.5 17.5 10.5" stroke="rgba(255,255,255,.9)" stroke-width="1.6" stroke-linecap="round"/>' +
              '</svg></button>';
            const delBtn =
              '<button class="iconBtn leadDelBtn" data-phone="'+(r.phone||"")+'" title="מחק ליד">' +
              '<svg viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">' +
              '<path d="M9 3h6m-8 4h10m-9 0 1 14h6l1-14M10 11v7M14 11v7" stroke="rgba(255,255,255,.9)" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"/>' +
              '</svg></button>';
            const when = r.updatedAt ? String(r.updatedAt).replace("T"," ").replace("Z","") : "—";
            tr.innerHTML =
              '<td><div class="row" style="gap:6px;">'+ editBtn + delBtn +'</div></td>' +
              '<td><strong>'+(r.firstName || "—")+'</strong></td>' +
              '<td>'+ (r.phone || "—") +'</td>' +
              '<td>'+ st +'</td>' +
              '<td>'+ when +'</td>';
            body.appendChild(tr);
          }
          setStatus($("leadsStatus"), "מוצגים " + rows.length + " לידים", true);
        }catch(e){
          setStatus($("leadsStatus"), "שגיאה: " + e.message, false);
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
        if(!phone) return setStatus($("testStatus"), "חסר מספר טלפון", false);
        try{
          const out = await api("/api/contacts/add", { method:"POST", headers: {"Content-Type":"application/json"}, body: JSON.stringify({ first_name: name, phone }) });
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
        const dnc = e.target.closest?.(".dncBtn");
        const del = e.target.closest?.(".delBtn");

        if(dnc){
          const phone = dnc.getAttribute("data-phone");
          const cur = Number(dnc.getAttribute("data-dnc") || 0);
          if(!phone) return;
          dnc.disabled = true;
          try{
            await api("/api/contacts/set-dnc", { method:"POST", headers: {"Content-Type":"application/json"}, body: JSON.stringify({ phone, doNotCall: cur ? 0 : 1 }) });
            toast("עודכן", (cur ? "אפשר להתקשר" : "לא להתקשר יותר") + " • " + phone, true);
            if($("listSection").style.display !== "none") await loadContacts();
            await loadAll();
          }catch(err){
            toast("שגיאה", err.message || String(err), false);
          } finally {
            dnc.disabled = false;
          }
          return;
        }

        if(btn){
          const phone = btn.getAttribute("data-phone");
          if(!phone) return;
          btn.disabled = true;
          try{
            const listId = Number(($("listSelect") && $("listSelect").value) ? $("listSelect").value : 0) || 0;
            const out = await api("/api/contacts/dial", { method:"POST", headers: {"Content-Type":"application/json"}, body: JSON.stringify({ phone, listId }) });
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

      $("leadsBody").addEventListener("click", async (e) => {
        const edit = e.target.closest?.(".leadEditBtn");
        const del = e.target.closest?.(".leadDelBtn");

        if (edit) {
          const phone = edit.getAttribute("data-phone") || "";
          const name = edit.getAttribute("data-name") || "";
          const status = edit.getAttribute("data-status") || "waiting";
          $("leadOldPhone").value = phone;
          $("leadPhone").value = phone;
          $("leadFirstName").value = name;
          $("leadStatus").value = status === "not_interested" ? "not_interested" : "waiting";
          $("leadEditStatus").textContent = "";
          $("leadEditModal").style.display = "block";
          return;
        }

        if(!del) return;
        const phone = del.getAttribute("data-phone");
        if(!phone) return;
        const ok = window.confirm("למחוק את הליד הזה? (" + phone + ")");
        if(!ok) return;
        del.disabled = true;
        try{
          const out = await api("/api/leads/delete", { method:"POST", headers: {"Content-Type":"application/json"}, body: JSON.stringify({ phone }) });
          toast("נמחק", "נמחקו " + (out.deleted ?? 0) + " רשומות", true);
          if($("leadsSection").style.display !== "none") await loadLeads();
        }catch(err){
          toast("שגיאת מחיקה", err.message || String(err), false);
        } finally {
          del.disabled = false;
        }
      });

      // Lead edit modal
      const leadEditModal = $("leadEditModal");
      function closeLeadEdit(){
        leadEditModal.style.display = "none";
      }
      $("closeLeadEditBtn").addEventListener("click", closeLeadEdit);
      leadEditModal.addEventListener("click", (e) => { if (e.target === leadEditModal) closeLeadEdit(); });
      $("saveLeadEditBtn").addEventListener("click", async () => {
        const oldPhone = ($("leadOldPhone").value || "").trim();
        const newPhone = ($("leadPhone").value || "").trim();
        const firstName = ($("leadFirstName").value || "").trim();
        const status = ($("leadStatus").value || "waiting").trim();
        if(!oldPhone) return setStatus($("leadEditStatus"), "חסר טלפון", false);
        try{
          await api("/api/leads/update", {
            method:"POST",
            headers: {"Content-Type":"application/json"},
            body: JSON.stringify({ oldPhone, newPhone, firstName, status })
          });
          setStatus($("leadEditStatus"), "נשמר", true);
          toast("נשמר", "הליד עודכן", true);
          closeLeadEdit();
          if($("leadsSection").style.display !== "none") await loadLeads();
          if($("listSection").style.display !== "none") await loadContacts();
          await loadAll();
        }catch(err){
          setStatus($("leadEditStatus"), "שגיאה: " + (err.message || String(err)), false);
          toast("שגיאה", err.message || String(err), false);
        }
      });

      $("showListBtn").addEventListener("click", async () => {
        const sec = $("listSection");
        sec.style.display = sec.style.display === "none" ? "block" : "none";
        if(sec.style.display === "block"){
          await loadLists();
          await loadContacts();
        }
      });
      $("refreshListBtn").addEventListener("click", loadContacts);
      $("search").addEventListener("input", () => { if($("listSection").style.display !== "none") loadContacts(); });
      if($("dialFilter")) $("dialFilter").addEventListener("change", () => { if($("listSection").style.display !== "none") loadContacts(); });
      if($("listSelect")) $("listSelect").addEventListener("change", () => { if($("listSection").style.display !== "none") loadContacts(); });

      async function loadLists(){
        const out = await api("/api/contact-lists");
        const rows = out?.rows || [];
        const sel = $("listSelect");
        if(sel){
          const current = String(sel.value || "0");
          sel.innerHTML =
            '<option value="0">רשימה: הכל</option>' +
            rows
              .map((r) => {
                const nm = escapeHtml(r.name || ("רשימה " + r.id));
                return '<option value="' + String(r.id) + '">' + nm + "</option>";
              })
              .join("");
          const stillExists = rows.some(r => String(r.id) === current);
          sel.value = stillExists ? current : "0";
        }
        const tb = $("listsBody");
        if(tb){
          tb.innerHTML = rows
            .map((r) => {
              const s = r.stats || {};
              const name = escapeHtml(r.name || ("רשימה " + r.id));
              return (
                "<tr>" +
                "<td>" +
                '<button class="secondary" data-open-list="' +
                String(r.id) +
                '" style="padding:6px 10px;">פתח</button> ' +
                '<button class="secondary" data-export-list="' +
                String(r.id) +
                '" style="padding:6px 10px;">CSV</button> ' +
                '<button class="secondary" data-rename-list="' +
                String(r.id) +
                '" style="padding:6px 10px;">ערוך</button> ' +
                '<button class="secondary" data-delete-list="' +
                String(r.id) +
                '" style="padding:6px 10px; border-color: rgba(179,38,38,.35);">מחק</button>' +
                "</td>" +
                "<td><strong>" +
                name +
                "</strong></td>" +
                "<td>" +
                (s.total ?? 0) +
                "</td>" +
                "<td>" +
                (s.notDone ?? s.remaining ?? 0) +
                "</td>" +
                "<td>" +
                (s.done ?? 0) +
                "</td>" +
                "<td>" +
                (s.noAnswer ?? 0) +
                "</td>" +
                "<td>" +
                (s.notAvailable ?? 0) +
                "</td>" +
                "<td>" +
                (s.answeredUnder5 ?? 0) +
                "</td>" +
                "<td>" +
                (s.interested ?? 0) +
                "</td>" +
                "<td>" +
                (s.notInterested ?? 0) +
                "</td>" +
                "<td>" +
                (s.dnc ?? 0) +
                "</td>" +
                "<td>" +
                (s.invalid ?? 0) +
                "</td>" +
                "</tr>"
              );
            })
            .join("");
          tb.querySelectorAll("[data-open-list]").forEach(btn => btn.addEventListener("click", async (e) => {
            const id = String(e.currentTarget.getAttribute("data-open-list") || "0");
            if($("listSelect")) $("listSelect").value = id;
            await loadContacts();
          }));
          tb.querySelectorAll("[data-export-list]").forEach(btn => btn.addEventListener("click", async (e) => {
            const id = String(e.currentTarget.getAttribute("data-export-list") || "0");
            if(!id || id === "0") return;
            // Direct download (CSV)
            window.location.href = "/api/contact-lists/export?format=csv&listId=" + encodeURIComponent(id);
          }));
          tb.querySelectorAll("[data-rename-list]").forEach(btn => btn.addEventListener("click", async (e) => {
            const id = Number(e.currentTarget.getAttribute("data-rename-list") || "0");
            const cur = rows.find(x => Number(x.id) === id);
            const next = prompt("שם חדש לרשימה:", cur?.name || "");
            if(!next) return;
            await api("/api/contact-lists/rename", { method:"POST", headers: {"Content-Type":"application/json"}, body: JSON.stringify({ id, name: next }) });
            toast("עודכן", "עודכן שם הרשימה", true);
            await loadLists();
          }));
          tb.querySelectorAll("[data-delete-list]").forEach(btn => btn.addEventListener("click", async (e) => {
            const id = Number(e.currentTarget.getAttribute("data-delete-list") || "0");
            const cur = rows.find(x => Number(x.id) === id);
            if(!confirm("למחוק את הרשימה '" + (cur?.name || id) + "'?\\nזה גם ימחק אנשי קשר ששייכים רק לרשימה הזאת (מספרים ששייכים גם לרשימות אחרות לא יימחקו).")) return;
            await api("/api/contact-lists/delete", { method:"POST", headers: {"Content-Type":"application/json"}, body: JSON.stringify({ id }) });
            toast("נמחק", "הרשימה נמחקה", true);
            if($("listSelect") && String($("listSelect").value || "0") === String(id)) $("listSelect").value = "0";
            await loadLists();
            await loadContacts();
          }));
        }
      }
      if($("refreshListsBtn")) $("refreshListsBtn").addEventListener("click", () => { if($("listSection").style.display !== "none") loadLists(); });

      // Bulk select + delete
      $("selectAllContacts").addEventListener("change", () => {
        const v = $("selectAllContacts").checked;
        document.querySelectorAll(".contactChk").forEach((c) => { c.checked = v; });
      });
      $("deleteSelectedBtn").addEventListener("click", async () => {
        const selected = Array.from(document.querySelectorAll(".contactChk"))
          .filter((c) => c.checked)
          .map((c) => c.getAttribute("data-phone"))
          .filter(Boolean);
        if(!selected.length) return setStatus($("bulkStatus"), "לא נבחרו מספרים", false);
        const ok = window.confirm("למחוק " + selected.length + " אנשי קשר שנבחרו?");
        if(!ok) return;
        try{
          const out = await api("/api/contacts/delete_many", { method:"POST", headers: {"Content-Type":"application/json"}, body: JSON.stringify({ phones: selected }) });
          setStatus($("bulkStatus"), "נמחקו " + (out.deleted ?? 0) + " אנשי קשר", true);
          toast("נמחק", "נמחקו " + (out.deleted ?? 0) + " אנשי קשר", true);
          await loadContacts();
          await loadAll();
        }catch(e){
          setStatus($("bulkStatus"), "שגיאה: " + e.message, false);
          toast("שגיאה", e.message, false);
        }
      });
      $("deleteAllBtn").addEventListener("click", async () => {
        const ok1 = window.confirm("למחוק את כל אנשי הקשר? זה בלתי הפיך.");
        if(!ok1) return;
        const ok2 = window.prompt("כדי לאשר, כתוב בדיוק: DELETE_ALL");
        if(ok2 !== "DELETE_ALL") return setStatus($("bulkStatus"), "בוטל", false);
        try{
          const out = await api("/api/contacts/delete_all", { method:"POST", headers: {"Content-Type":"application/json"}, body: JSON.stringify({ confirm: "DELETE_ALL" }) });
          setStatus($("bulkStatus"), "נמחקו " + (out.deleted ?? 0) + " אנשי קשר", true);
          toast("נמחק הכל", "נמחקו " + (out.deleted ?? 0) + " אנשי קשר", true);
          await loadContacts();
          await loadAll();
        }catch(e){
          setStatus($("bulkStatus"), "שגיאה: " + e.message, false);
          toast("שגיאה", e.message, false);
        }
      });

      $("showLeadsBtn").addEventListener("click", async () => {
        const sec = $("leadsSection");
        sec.style.display = sec.style.display === "none" ? "block" : "none";
        if(sec.style.display === "block") await loadLeads();
      });
      $("refreshLeadsBtn").addEventListener("click", loadLeads);
      $("leadsFilter").addEventListener("change", () => { if($("leadsSection").style.display !== "none") loadLeads(); });

      loadAll();
    </script>
  </body>
</html>`;
}


