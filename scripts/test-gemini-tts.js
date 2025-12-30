import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { geminiTtsToFile } from "../src/gemini_tts.js";

const apiKey = process.env.GEMINI_API_KEY || "";
const model = process.env.GEMINI_TTS_MODEL || "gemini-2.5-pro-preview-tts";
const voiceName = process.env.GEMINI_TTS_VOICE_NAME || "Zephyr";

const text =
  process.argv.slice(2).join(" ").trim() ||
  "שלום, מדבר ממשרד הקהילה. רציתי להזמין אותך לשיעור תורה קרוב. יש לך דקה?";

if (!apiKey) {
  console.error("Missing GEMINI_API_KEY in .env");
  process.exit(1);
}

const outDir = path.resolve("./data/tts-cache");
if (!fs.existsSync(outDir)) fs.mkdirSync(outDir, { recursive: true });

const out = await geminiTtsToFile({ apiKey, model, voiceName, text, outDir });
if (!out) {
  console.error("Gemini TTS failed (returned null). Try a different GEMINI_TTS_VOICE_NAME.");
  process.exit(2);
}

console.log(`OK: ${out} (model=${model}, voice=${voiceName})`);


