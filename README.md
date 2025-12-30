## AI Voice Outbound (Hebrew) — Twilio + OpenAI

מערכת שיחות יוצאות בעברית שמזמינה אנשים לשיעורי תורה (גברים) או להפרשת חלה (נשים), עם שיחה “חכמה” (~2 דקות) בעלות נמוכה.

### למה זה "הכי טוב+זול+מהיר" ל-V1
- **STT**: משתמשים ביכולות ה-Speech Recognition של Twilio (`<Gather input="speech">`) כדי לחסוך אינטגרציות/עלויות נוספות.
- **TTS**: משתמשים ב-Twilio `<Say>` עם קול Polly (עברית) במקום שירות TTS חיצוני (אפשר לשדרג).
- **LLM**: מודל קטן של OpenAI לתשובות קצרות + הובלת השיחה.

### דרישות
- חשבון Twilio + מספר יוצא (מומלץ מקומי למדינת היעד)
- OpenAI API key
- כתובת ציבורית ל-webhooks (למשל ngrok בזמן פיתוח)

### התקנה
```bash
npm install
```

צור קובץ `.env` לפי `config.example.env`.

#### (אופציונלי) קול טבעי יותר עם ElevenLabs
אם אתה רוצה קול ממש “אנושי”, הוסף ב-`.env`:
- `ELEVENLABS_API_KEY`
- `ELEVENLABS_VOICE_MALE`
- `ELEVENLABS_VOICE_FEMALE`

בלי ElevenLabs המערכת תשתמש ב-Twilio `<Say>` (זול יותר ומהיר להרים, אבל פחות “אנושי”).

### הפעלה מקומית
```bash
npm run dev
```

### פאנל ניהול (Mini Admin)
- פתח בדפדפן: `http://localhost:3000/admin`
- מאפשר:
  - העלאת אקסל (XLSX) של אנשי קשר
  - ייבוא מ-Google Sheets (CSV ציבורי)
  - עריכת "מה הסוכן יודע" ו"איך הוא מדבר" (משפיע על השיחות)
  - הפעלת חיוג אוטומטי למספרים חדשים

### חיבור Twilio Webhook
ב-Console של Twilio, תחת Phone Number → Voice configuration:
- **A call comes in**: `POST {BASE_URL}/twilio/voice`

### טעינת אנשי קשר מאקסל
קובץ אקסל צריך עמודות:
- `phone` (חובה)
- `gender` (אופציונלי: `male` / `female`)
- `first_name` (אופציונלי)

```bash
npm run import:xlsx -- /absolute/path/to/list.xlsx
```

### הפעלת קמפיין חיוג
```bash
npm run dial
```

### חיוג אוטומטי
ב-`/admin` אפשר להפעיל "חיוג אוטומטי למספרים חדשים". המערכת תחייג ברקע לכל אנשי הקשר עם `dial_status='new'`.

### פרודקשן (כדי שלא תצטרך npm run dev)
- השתמש ב-`npm start` (תסריט start כבר מוגדר).
- ב-Render/Railway/Fly.io מגדירים Environment Variables במקום `.env`.
- חובה שכתובת ציבורית קבועה תוגדר כ-`VOICE_WEBHOOK_URL` כדי שחיוג יוצא יעבוד.
- Twilio Webhook לשיחות נכנסות צריך להצביע ל: `{YOUR_PUBLIC_URL}/twilio/voice`.

### הערות חשובות
- יש **Do-Not-Call**: אם המשתמש אומר “אל תתקשרו יותר / תסירו אותי” המערכת מסמנת אותו ולא מחייגת שוב.
- מומלץ להגדיר מגבלת זמן לשיחה (כבר מובנה: ~2 דקות / מספר סבבים).


