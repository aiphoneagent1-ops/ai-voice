import twilio from "twilio";

const { twiml } = twilio;

function sayAttrs({ voice, language }) {
  // Twilio + Polly: אל תשלחו language, זה גורם לשגיאות (למשל 13520/13512).
  if (voice && String(voice).startsWith("Polly.")) {
    return { voice };
  }
  return { voice, language };
}

export function buildRecordTwiML({
  sayText,
  playUrl,
  actionUrl,
  voice = "Polly.Carmit",
  language = "he-IL",
  maxLengthSeconds = 8,
  timeoutSeconds = 2,
  playBeep = false
}) {
  const response = new twiml.VoiceResponse();

  if (playUrl) {
    response.play(playUrl);
  } else if (sayText) {
    response.say(sayAttrs({ voice, language }), sayText);
  }

  // Twilio לא תומך בעברית ב-<Gather speech>, לכן משתמשים ב-<Record> ואז מתמללים חיצונית.
  response.record({
    action: actionUrl,
    method: "POST",
    timeout: timeoutSeconds,
    maxLength: maxLengthSeconds,
    playBeep,
    trim: "trim-silence"
  });

  // אם לא הוקלט (timeout) — לא נאמר כלום כדי להימנע מבעיות TTS בשפות לא נתמכות.
  response.hangup();

  return response.toString();
}

export function buildSayAndHangup({
  sayText,
  voice = "Polly.Carmit",
  language = "he-IL"
}) {
  const response = new twiml.VoiceResponse();
  response.say(sayAttrs({ voice, language }), sayText);
  response.hangup();
  return response.toString();
}

export function buildPlayAndHangup({ playUrl }) {
  const response = new twiml.VoiceResponse();
  if (playUrl) response.play(playUrl);
  response.hangup();
  return response.toString();
}


