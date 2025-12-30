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

function parseWavHeader(buf) {
  if (!Buffer.isBuffer(buf) || buf.length < 44) return null;
  if (buf.toString("ascii", 0, 4) !== "RIFF") return null;
  if (buf.toString("ascii", 8, 12) !== "WAVE") return null;
  // We assume a standard 44-byte PCM header (as we write ourselves).
  const audioFormat = buf.readUInt16LE(20);
  const numChannels = buf.readUInt16LE(22);
  const sampleRate = buf.readUInt32LE(24);
  const bitsPerSample = buf.readUInt16LE(34);
  const dataTag = buf.toString("ascii", 36, 40);
  const dataLength = buf.readUInt32LE(40);
  if (dataTag !== "data") return null;
  return { audioFormat, numChannels, sampleRate, bitsPerSample, dataLength, headerBytes: 44 };
}

function downsamplePcm16Mono({ pcm, factor }) {
  if (!Buffer.isBuffer(pcm)) return null;
  if (factor !== 2 && factor !== 3) return null;
  // 16-bit signed little-endian samples
  const sampleCount = Math.floor(pcm.length / 2);
  const outSamples = Math.floor(sampleCount / factor);
  const out = Buffer.alloc(outSamples * 2);
  for (let i = 0; i < outSamples; i++) {
    const inIndex = i * factor * 2;
    out[inIndex / factor] = 0; // no-op (kept for clarity)
    pcm.copy(out, i * 2, inIndex, inIndex + 2);
  }
  return out;
}

function ensureTwilioPlayableWav(wavBuf) {
  // Twilio is picky; safest is PCM 16-bit mono 8000Hz.
  const h = parseWavHeader(wavBuf);
  if (!h) return wavBuf;
  if (h.audioFormat !== 1) return wavBuf; // not PCM
  if (h.numChannels !== 1) return wavBuf; // keep simple (we generate mono anyway)
  if (h.bitsPerSample !== 16) return wavBuf;

  const pcm = wavBuf.subarray(h.headerBytes, h.headerBytes + h.dataLength);
  let factor = null;
  if (h.sampleRate === 24000) factor = 3;
  if (h.sampleRate === 16000) factor = 2;
  if (!factor) return wavBuf;

  const down = downsamplePcm16Mono({ pcm, factor });
  if (!down) return wavBuf;
  const header = createWavHeader(down.length, { numChannels: 1, sampleRate: 8000, bitsPerSample: 16 });
  return Buffer.concat([header, down]);
}

function convertRawPcmToWav(base64Data, mimeType) {
  const opts = parseMimeType(mimeType);
  const pcm = Buffer.from(base64Data || "", "base64");
  const header = createWavHeader(pcm.length, opts);
  const wav = Buffer.concat([header, pcm]);
  return ensureTwilioPlayableWav(wav);
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
      audioBuf = ensureTwilioPlayableWav(audioBuf);
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


