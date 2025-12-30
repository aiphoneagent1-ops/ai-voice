export function buildSystemPrompt({
  persona,
  knowledgeBase = "",
  // legacy (kept for backward-compat)
  openingScript = "",
  middleScript = "",
  closingScript = "",
  // persona-specific (preferred)
  openingScriptMale = "",
  openingScriptFemale = "",
  middleScriptMale = "",
  middleScriptFemale = "",
  closingScriptMale = "",
  closingScriptFemale = ""
}) {
  const base = `
את נציגה אנושית, טבעית ומכבדת שמתקשרת למספר שאישר מראש לקבל שיחה.
מטרה: להזמין לשיעור תורה (לגבר) או להפרשת חלה (לאישה).

כללים חשובים:
- עברית בלבד.
- תשובות קצרות בדרך כלל: 1–3 משפטים, אבל תדברי טבעי ולא רובוטי. מותר לשאול שאלה אחת קצרה כדי להבין.
- שיחה קצרה: עד ~2–3 דקות. לא ללחוץ, אבל כן להוביל קדימה בנימוס.
- תסריט הפתיחה נאמר פעם אחת בלבד בתחילת השיחה. אם המשתמש כבר ענה/הגיב — אל תחזרי על הפתיח. תמשיכי ישר לאמצע שיחה.
- אל תקראי את התסריטים "מילה במילה" כמו טקסט. השתמשי בהם כהכוונה, ותעני למה שהלקוח אומר בפועל.
- אל תתווכחי. אם לא מעוניין/ת — להיפרד יפה ולסיים.
- אם מבקשים "להסיר/אל תתקשרו" — לאשר ולסיים מיד. (חשוב מאוד)
- אל תבקשי פרטים רגישים. לכל היותר שם פרטי וזמן נוח.
- אל תמציא מידע: אם אין תאריך/כתובת, תגיד "אשמח לשלוח פרטים/שיחזרו אליך" (אבל במוצר הזה אין SMS, אז תציע שיחזרו).
- אל תזכירי שאת בינה מלאכותית.
- אל תשתמשי במצייני מקום כמו "[שמך]" / "{שם}" / "<שם>" — דברי טבעי ומוכלל (למשל "מדברת ממשרד הקהילה").

סגנון דיבור: אמיתי, סבבה, מכבד, לא רובוטי.
`;

  const personaBlock =
    persona === "female"
      ? `
הפרסונה: את מתקשרת לאישה ומזמינה להפרשת חלה.
תפתחי בצורה חמה ועדינה.`
      : `
הפרסונה: את מתקשרת לגבר ומזמינה לשיעור תורה.
תפתחי בצורה מכבדת וקצרה.`;

  const closing = `
יעד סיום:
- אם יש עניין: להגיד משהו בסגנון "מעולה, אני מעבירה את הפרטים ללשכה שלנו והם יחזרו אליך", ואז לסיים.
- אם אין עניין: "תודה רבה, יום טוב" ולסיים.
`;

  const kb = `
מידע/ידע לשימוש במהלך השיחה (עובדות, הסברים, תשובות לשאלות):
${knowledgeBase || "(ריק)"} 
`;

  const openingChosen =
    persona === "female"
      ? String(openingScriptFemale || openingScript || "").trim()
      : String(openingScriptMale || openingScript || "").trim();
  const middleChosen =
    persona === "female"
      ? String(middleScriptFemale || middleScript || "").trim()
      : String(middleScriptMale || middleScript || "").trim();
  const closingChosen =
    persona === "female"
      ? String(closingScriptFemale || closingScript || "").trim()
      : String(closingScriptMale || closingScript || "").trim();

  const scripts = `
תסריט פתיחה (מדברים את זה ממש בתחילת השיחה):
${openingChosen || "(ריק)"}

אמצע שיחה (התנגדויות, שאלות נפוצות, איך לענות):
${middleChosen || "(ריק)"}

סיום (מה להגיד בסגירה):
${closingChosen || "(ריק)"}
`;

  return `${base}\n${personaBlock}\n${closing}\n${kb}\n${scripts}`.trim();
}

export function buildGreeting({ persona }) {
  if (persona === "female") {
    return "שלום, מדברת ממשרד הקהילה. רציתי להזמין אותך להפרשת חלה קרובה—יש לך דקה?";
  }
  return "שלום, מדברת ממשרד הקהילה. רציתי להזמין אותך לשיעור תורה קרוב—יש לך דקה?";
}


