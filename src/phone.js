export function normalizePhoneE164IL(raw) {
  if (!raw) return "";
  let s = String(raw).trim();
  if (!s) return "";

  // keep leading +, strip everything else to digits
  const hasPlus = s.startsWith("+");
  s = s.replace(/[^\d+]/g, "");
  if (hasPlus) {
    s = "+" + s.replace(/[^\d]/g, "");
  } else {
    s = s.replace(/[^\d]/g, "");
  }

  // Convert common Israeli formats to E.164
  // 05XXXXXXXX or 0XXXXXXXXX  -> +972XXXXXXXXX (drop leading 0)
  if (s.startsWith("0") && s.length >= 9) {
    s = `+972${s.slice(1)}`;
  }
  // 972XXXXXXXXX -> +972XXXXXXXXX
  if (!s.startsWith("+") && s.startsWith("972")) {
    s = `+${s}`;
  }

  // Accept only E.164 at this point
  if (!s.startsWith("+")) return "";
  if (!/^\+\d{8,15}$/.test(s)) return "";
  return s;
}


