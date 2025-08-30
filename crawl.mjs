#!/usr/bin/env -S deno run --allow-read --allow-write --allow-net --allow-env
import {
  elementFlow,
  elementSink,
  parseFlow,
  pipe,
  PreamblePart,
  toIndeterminateLengthSequences,
  ValueChunk,
} from "https://esm.sh/gh/jpambrun/dicom-streams-js@f250f9f";
import stream from "node:stream";

import fs from "node:fs";
import fsp from "node:fs/promises";
import path from "node:path";
import PQueue from "npm:p-queue";
import process from "node:process";

import { open } from "npm:lmdb";

const queue = new PQueue({ concurrency: 20 });

let db = open({
  path: "_dicomweb",
  useVersions: true,
});

const SERIES_LIST_ITEMS = [
  "0020000D",
  "00200011",
  "0020000E",
  "00080060",
  "00080021",
  "00080031",
  "00080070",
  "00080080",
  "00080090",
  "00081010",
  "0008103E",
  "00081070",
  "00081090",
  "00181030",
];

const STUDY_LIST_ITEMS = [
  "0020000D",
  "00080020",
  "00080030",
  "00080050",
  // "00080060",
  "00080061",
  "00081030",
  "00090010",
  "00091011",
  "00091012",
  "00100010",
  "00100020",
  "00100030",
  "00100040",
  "00120062",
  "00120064",
  "00200010",
  "00201206",
  "00201208",
];

function tagToString(tag) {
  const hex = ("00000000" + tag.toString(16)).slice(-8);
  return hex.toLocaleUpperCase();
}

class FilterLargeValue extends stream.Transform {
  constructor(maxSize = 100) {
    super({ objectMode: true });
    this.maxSize = maxSize;
    this.currentVR = null;
    this.VRToFilter = ["OB", "OW", "OF", "UN"];
  }
  _transform(chunk, _encoding, callback) {
    if (chunk instanceof PreamblePart) return callback();
    this.currentVR = chunk?.vr?.name || this.currentVR;
    if (
      chunk instanceof ValueChunk &&
      chunk?.bytes?.length > this.maxSize &&
      this.VRToFilter.includes(this.currentVR)
    ) {
      chunk.bytes = Buffer.alloc(0);
    }
    callback(null, chunk);
  }
}

const convertToDicomweb = (src, dst = {}) => {
  const { characterSets, data } = src;
  for (const element of data) {
    const { vr, bigEndian, value, tag, items, fragments } = element;
    const hextag = tagToString(tag);
    if (value) {
      if (value.length === 0 && !element.range) {
        dst[hextag] = { vr: vr.name };
        continue;
      }

      switch (vr.name) {
        case "PN": {
          const valueStrings = value.toStrings(vr.name, bigEndian, characterSets).filter((pn) => pn !== "");
          if (valueStrings.length === 0) {
            dst[hextag] = { vr: vr.name };
          } else {
            dst[hextag] = { vr: vr.name, Value: valueStrings.map((pn) => ({ Alphabetic: pn })) };
          }
          break;
        }
        case "US":
        case "SS":
        case "UL":
        case "SL":
        case "IS":
        case "DS":
        case "FL":
        case "FD":
        case "SH":
          dst[hextag] = { vr: vr.name, Value: value.toNumbers(vr, bigEndian, characterSets) };
          break;
        case "OB":
        case "OW":
        case "OF":
          if (element.value.bytes.length > 0) {
            dst[hextag] = { vr: vr.name, InlineBinary: element.value.bytes.toString("base64") };
          } else if (element.range) {
            dst[hextag] = {
              vr: vr.name,
              BulkDataURI: `?range=${element.range[0]}-${element.range[0] + element.range[1]}`,
            };
          } else {
            console.warn(`No data: ${hextag} with VR ${vr.name}`);
          }
          break;
        default:
          dst[hextag] = { vr: vr.name, Value: value.toStrings(vr, bigEndian, characterSets) };
      }
    } else if (items && vr.name === "SQ") {
      dst[hextag] = {
        vr: vr.name,
        Value: [],
      };

      for (const item of items) {
        const converted = convertToDicomweb(item.elements);
        dst[hextag].Value.push(converted);
      }
    } else if (fragments) {
      dst[hextag] = {
        vr: vr.name,
        value: fragments.map((f) => {
          if (f.value.bytes.length > 0) {
            return { InlineBinary: f.value.bytes.toString("base64") };
          } else if (f.range) {
            return { BulkDataURI: `?range=${f.range[0]}-${f.range[0] + f.range[1]}` };
          } else {
            throw new Error(`No data: ${hextag} with VR ${vr.name}`);
          }
        }),
      };
    } else {
      throw new Error(`Unsupported element: ${hextag} with VR ${vr.name}`);
    }
  }
  return dst;
};

const insertStudy = async (studyInstanceUID, dicomDict) => {
  const modality = dicomDict["00080060"]?.Value?.[0];
  const studyModalityKey = `studymodalities:${studyInstanceUID}:modality:${modality}`;
  if (modality && !db.get(studyModalityKey)) {
    await db.put(studyModalityKey, modality);
  }

  const studyKey = `study:${studyInstanceUID}`;
  if (await db.get(studyKey)) return;

  const study = {};
  for (const item of STUDY_LIST_ITEMS) {
    if (dicomDict[item]?.Value?.[0]) {
      study[item] = dicomDict[item];
    }
  }

  if (!await db.put(studyKey, study, 1)) {
    throw new Error("Failed to insert study entry");
  }
};

const insertSeries = async (studyInstanceUID, seriesInstanceUID, dicomDict) => {
  const seriesKey = `series:${studyInstanceUID}:${seriesInstanceUID}`;
  if (await db.get(seriesKey)) return;

  const series = {};
  for (const item of SERIES_LIST_ITEMS) {
    if (dicomDict[item]?.Value?.[0]) {
      series[item] = dicomDict[item];
    }
  }

  const written = await db.put(seriesKey, series, 1);
  if (!written) {
    throw new Error("Failed to insert series entry");
  }
};

async function processDicomFile(filePath) {
  if (await db.get(`filepath:${filePath}`)) return;

  await new Promise((resolve) => {
    const stream = pipe(
      fs.createReadStream(filePath, { highWaterMark: 1024 * 1024 }),
      parseFlow(),
      toIndeterminateLengthSequences(),
      new FilterLargeValue(),
      elementFlow(),
      elementSink(async (elements) => {
        try {
          const dicomDict = convertToDicomweb(elements);
          const sopInstanceUID = dicomDict["00080018"].Value?.[0];
          const seriesInstanceUID = dicomDict["0020000E"].Value?.[0];
          const studyInstanceUID = dicomDict["0020000D"].Value?.[0];
          dicomDict["00083002"] = dicomDict["00020010"];
          dicomDict["00090001"] = { vr: "LT", Value: [filePath] };
          await db.put(`instance:${studyInstanceUID}:${seriesInstanceUID}:${sopInstanceUID}`, dicomDict);
          await insertStudy(studyInstanceUID, dicomDict);
          await insertSeries(studyInstanceUID, seriesInstanceUID, dicomDict);
          await db.put(`filepath:${filePath}`, `instance:${studyInstanceUID}:${seriesInstanceUID}:${sopInstanceUID}`);
        } catch (e) {
          console.error(`Error processing dict ${filePath}:`, e);
        }
        resolve();
      }),
    );

    stream.on("error", (e) => {
      if (e.message.includes("Not a DICOM stream")) {
        console.log(`Skipping non DICOM file: ${filePath}`);
      } else {
        console.error(`Error processing stream ${filePath}:`, e);
      }
      resolve();
    });
  });
}

console.log("Starting indexing...");
const t1 = Date.now();
let count = 0;
for await (const entry of fsp.glob("**/*", { withFileTypes: true })) {
  if (entry.isDirectory()) continue;
  if (![".dcm", ""].includes(path.extname(entry.name).toLowerCase())) continue;
  if (entry.name === "DICOMDIR") continue;
  const fullPath = path.join(entry.parentPath, entry.name);
  const relativePath = path.relative(process.cwd(), fullPath);

  count++;
  queue.add(() => processDicomFile(relativePath));
  await queue.onSizeLessThan(100);
}

await queue.onIdle();
console.log("Indexing time: ", (Date.now() - t1) / 1000, "s");
console.log("Total DICOM files processed: ", count);
