import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { GoogleGenAI } from "@google/genai";

function parseMimeType(mimeType) {
  const [fileType, ...params] = String(mimeType || "")
    .split(";")
    .map((s) => s.trim());
  const [, format] = fileType.split("/");

  const options = {
    numChannels: 1,
    sampleRate: 24000,
    bitsPerSample: 16
  };

  // examples: audio/L16;rate=24000
  if (format && format.startsWith("L")) {
    const bits = Number.parseInt(format.slice(1), 10);
    if (!Number.isNaN(bits)) options.bitsPerSample = bits;
  }

  for (const param of params) {
    const [key, value] = param.split("=").map((s) => s.trim());
    if (key === "rate") {
      const sr = Number.parseInt(value, 10);
      if (!Number.isNaN(sr)) options.sampleRate = sr;
    }
  }

  return options;
}

function createWavHeader(dataLength, { numChannels, sampleRate, bitsPerSample }) {
  const byteRate = (sampleRate * numChannels * bitsPerSample) / 8;
  const blockAlign = (numChannels * bitsPerSample) / 8;
  const buffer = Buffer.alloc(44);

  buffer.write("RIFF", 0);
  buffer.writeUInt32LE(36 + dataLength, 4);
  buffer.write("WAVE", 8);
  buffer.write("fmt ", 12);
  buffer.writeUInt32LE(16, 16);
  buffer.writeUInt16LE(1, 20); // PCM
  buffer.writeUInt16LE(numChannels, 22);
  buffer.writeUInt32LE(sampleRate, 24);
  buffer.writeUInt32LE(byteRate, 28);
  buffer.writeUInt16LE(blockAlign, 32);
  buffer.writeUInt16LE(bitsPerSample, 34);
  buffer.write("data", 36);
  buffer.writeUInt32LE(dataLength, 40);

  return buffer;
}

function convertRawPcmToWav(base64Data, mimeType) {
  const opts = parseMimeType(mimeType);
  const pcm = Buffer.from(base64Data || "", "base64");
  const header = createWavHeader(pcm.length, opts);
  return Buffer.concat([header, pcm]);
}

export async function geminiTtsToFile({
  apiKey,
  model,
  voiceName,
  text,
  outDir
}) {
  if (!apiKey) return null;
  if (!text) return null;

  const key = crypto
    .createHash("sha256")
    .update(`gemini::${model}::${voiceName}::${text}`)
    .digest("hex");

  const wavPath = path.join(outDir, `${key}.wav`);
  const mp3Path = path.join(outDir, `${key}.mp3`);
  if (fs.existsSync(mp3Path)) return `/tts/${key}.mp3`;
  if (fs.existsSync(wavPath)) return `/tts/${key}.wav`;

  try {
    const ai = new GoogleGenAI({ apiKey });

    const config = {
      responseModalities: ["audio"],
      speechConfig: {
        voiceConfig: {
          prebuiltVoiceConfig: { voiceName }
        }
      }
    };

    const contents = [{ role: "user", parts: [{ text }] }];

    const stream = await ai.models.generateContentStream({
      model,
      config,
      contents
    });

    let inlineData = null;
    for await (const chunk of stream) {
      const part = chunk?.candidates?.[0]?.content?.parts?.[0];
      if (part?.inlineData?.data) {
        inlineData = part.inlineData;
        break;
      }
    }

    if (!inlineData?.data) return null;

    const mimeType = inlineData.mimeType || "";
    let audioBuf;

    if (mimeType.includes("wav")) {
      audioBuf = Buffer.from(inlineData.data, "base64");
      fs.writeFileSync(wavPath, audioBuf);
      return `/tts/${key}.wav`;
    }
    if (mimeType.includes("mpeg") || mimeType.includes("mp3")) {
      audioBuf = Buffer.from(inlineData.data, "base64");
      fs.writeFileSync(mp3Path, audioBuf);
      return `/tts/${key}.mp3`;
    }
    if (mimeType.startsWith("audio/")) {
      // raw PCM (למשל audio/L16;rate=24000)
      audioBuf = convertRawPcmToWav(inlineData.data, mimeType);
      fs.writeFileSync(wavPath, audioBuf);
      return `/tts/${key}.wav`;
    }

    return null;
  } catch (e) {
    // חשוב: לא להפיל את ה-Webhook של Twilio
    return null;
  }
}


