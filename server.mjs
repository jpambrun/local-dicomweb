#!/usr/bin/env -S deno run --allow-read --allow-write --allow-net --allow-env

import fs from "node:fs";
import { Application, Router } from "jsr:@oak/oak";
import { oakCors } from "https://deno.land/x/cors/mod.ts";
import { open } from "npm:lmdb";
import { processDicomFile } from './crawl.mjs'
import { createMultipartTransformStream } from './multipartParser.mjs'

const db = open({ path: "_dicomweb", useVersions: true });

const TRANSFER_SYNTAXES_TO_CONTENT_TYPE = {
  "1.2.840.10008.1.2": "image/uncompressed;transfer-syntax=1.2.840.10008.1.2",
  "1.2.840.10008.1.2.1": "image/uncompressed;transfer-syntax=1.2.840.10008.1.2.1",
  "1.2.840.10008.1.2.1.99": "image/uncompressed;transfer-syntax=1.2.840.10008.1.2.1.99",
  "1.2.840.10008.1.2.2": "image/uncompressed;transfer-syntax=1.2.840.10008.1.2.2",
  "1.2.840.10008.1.2.4.50": "image/jpeg;transfer-syntax=1.2.840.10008.1.2.4.50",
  "1.2.840.10008.1.2.4.51": "image/jpeg;transfer-syntax=1.2.840.10008.1.2.4.51",
  "1.2.840.10008.1.2.4.57": "image/jpeg;transfer-syntax=1.2.840.10008.1.2.4.57",
  "1.2.840.10008.1.2.4.70": "image/jll;transfer-syntax=1.2.840.10008.1.2.4.70",
  "1.2.840.10008.1.2.4.140": "image/x-jxl;transfer-syntax=1.2.840.10008.1.2.4.140",
  "1.2.840.10008.1.2.4.141": "image/x-jxl;transfer-syntax=1.2.840.10008.1.2.4.141",
  "1.2.840.10008.1.2.4.142": "image/x-jxl;transfer-syntax=1.2.840.10008.1.2.4.142",
  "1.2.840.10008.1.2.4.80": "image/jls;transfer-syntax=1.2.840.10008.1.2.4.80",
  "1.2.840.10008.1.2.4.81": "image/jls;transfer-syntax=1.2.840.10008.1.2.4.81",
  "1.2.840.10008.1.2.4.90": "image/jp2;transfer-syntax=1.2.840.10008.1.2.4.90",
  "1.2.840.10008.1.2.4.91": "image/jp2;transfer-syntax=1.2.840.10008.1.2.4.91",
  "3.2.840.10008.1.2.4.96": "image/jphc;transfer-syntax=3.2.840.10008.1.2.4.96",
  "1.2.840.10008.1.2.4.201": "image/jphc;transfer-syntax=1.2.840.10008.1.2.4.201",
  "1.2.840.10008.1.2.4.202": "image/jphc;transfer-syntax=1.2.840.10008.1.2.4.202",
  "1.2.840.10008.1.2.4.203": "image/jphc;transfer-syntax=1.2.840.10008.1.2.4.203",
  "1.2.840.10008.1.2.5": "image/dicom-rle;transfer-syntax=1.2.840.10008.1.2.5",
};

function recusivelyReplaceBulkDataURI(dict, pathPrefix = "") {
  for (const key in dict) {
    if (typeof dict[key] === "object" && dict[key] !== null) {
      recusivelyReplaceBulkDataURI(dict[key], `${pathPrefix}${key}/`);
    } else if (key === "BulkDataURI") {
      dict[key] = pathPrefix.replace(/\/$/, "") + dict[key]; // Remove trailing slash
    }
  }
  return dict;
}

const router = new Router();

router.get("/dicomweb/studies", async (ctx) => {
  const ModalitiesInStudy = ctx.request.url.searchParams.get("ModalitiesInStudy");
  const PatientName = ctx.request.url.searchParams.get("PatientName");
  const StudyDescription = ctx.request.url.searchParams.get("StudyDescription");
  const patientID = ctx.request.url.searchParams.get("00100020");

  const studies = [
    ...db.getRange({ start: "study:", end: "study:" + "\ufffd" })
      .map(({ value: study }) => {
        const studyInstanceUID = study["0020000D"]?.Value?.[0];
        const modalities = [
          ...db.getRange({
            start: `studymodalities:${studyInstanceUID}:modality:`,
            end: `studymodalities:${studyInstanceUID}:modality:\ufffd`,
          }).map(({ value }) => value),
        ];
        study["00080061"] = { vr: "CS", Value: modalities };
        return study;
      })
      .filter((study) => {
        if (ModalitiesInStudy) {
          const requestedModalities = ModalitiesInStudy.split(",").map((m) => m.trim());
          const modalities = study["00080061"]?.Value?.[0]?.split?.("\\") || [];
          return requestedModalities.some((m) => modalities.includes(m));
        }

        if (PatientName) {
          const patientName = study["00100010"]?.Value?.[0]?.Alphabetic || "";
          return patientName.toLowerCase().includes(PatientName.toLowerCase().replaceAll("*", ""));
        }

        if (StudyDescription) {
          const studyDesc = study["00081030"]?.Value?.[0] || "";
          return studyDesc.toLowerCase().includes(StudyDescription.toLowerCase().replaceAll("*", ""));
        }

        if (patientID) {
          const id = study["00100020"]?.Value?.[0] || "";
          return id.toLowerCase().includes(patientID.toLowerCase().replaceAll("*", ""));
        }
        return true;
      }).map((study) => {
        const studyInstanceUID = study["0020000D"]?.Value?.[0];
        study["00201208"] = {
          vr: "IS",
          Value: [
            db.getCount({ start: `instance:${studyInstanceUID}`, end: `instance:${studyInstanceUID}\ufffd` }),
          ],
        };
        return study;
      }),
  ];

  studies.sort((a, b) => {
    // by patient name, then by study date desc
    const patientNameA = a["00100010"]?.Value?.[0]?.Alphabetic || "";
    const patientNameB = b["00100010"]?.Value?.[0]?.Alphabetic || "";
    if (patientNameA < patientNameB) return -1;
    if (patientNameA > patientNameB) return 1;

    const studyDateA = a["00080020"]?.Value?.[0] || "";
    const studyDateB = b["00080020"]?.Value?.[0] || "";
    if (studyDateA < studyDateB) return 1;
    if (studyDateA > studyDateB) return -1;

    return 0;
  });
  ctx.response.body = studies;
});

router.get("/dicomweb/studies/:studyInstanceUID/series", async (ctx) => {
  const { studyInstanceUID } = ctx.params;
  const series = [
    ...db.getRange({ start: `series:${studyInstanceUID}:`, end: `series:${studyInstanceUID}:\ufffd` }).map((
      { value },
    ) => value),
  ];

  for (const s of series) {
    const seriesInstanceUID = s["0020000E"]?.Value?.[0];
    s["00201209"] = {
      vr: "IS",
      Value: [
        await db.getCount({
          start: `instance:${studyInstanceUID}:${seriesInstanceUID}`,
          end: `instance:${studyInstanceUID}:${seriesInstanceUID}\ufffd`,
        }),
      ],
    };
  }

  ctx.response.body = series;
});

router.post("/dicomweb/studies", async (ctx) => {
  const contentType = ctx.request.headers.get("content-type");
  const boundaryMatch = contentType.match(/boundary=([^;\s]+)/);
  if (!boundaryMatch) {
    ctx.response.status = 400;
    ctx.response.body = { error: "Missing boundary in content-type" };
    return;
  }
  const boundary = boundaryMatch[1].replaceAll(`"`, ``);
  const transform = createMultipartTransformStream(boundary);
  const partStream = ctx.request.body.stream.pipeThrough(transform);
  for await (const part of partStream) {
      const filename = `./_stow/${crypto.randomUUID()}.dcm`;
      await Deno.mkdir("./_stow", { recursive: true });
      await Deno.writeFile(filename, part)
      await processDicomFile(filename)
  }
  ctx.response.status = 202;
});


router.get("/dicomweb/studies/:studyInstanceUID/series/:seriesInstanceUID/metadata", async (ctx) => {
  const { studyInstanceUID, seriesInstanceUID } = ctx.params;

  const instances = [
    ...db.getRange({
      start: `instance:${studyInstanceUID}:${seriesInstanceUID}:`,
      end: `instance:${studyInstanceUID}:${seriesInstanceUID}:\ufffd`,
    }).map(({ value }) => value),
  ];
  if (!instances.length) {
    ctx.response.status = 404;
    ctx.response.body = { error: "Series not found" };
    return;
  }
  ctx.response.body = instances.map((dicomDict) => {
    const sopInstanceUID = dicomDict["00080018"]?.Value?.[0];
    const bulkDataPathPrefix = `/dicomweb/bulkdata/${studyInstanceUID}/${seriesInstanceUID}/${sopInstanceUID}/`;
    return recusivelyReplaceBulkDataURI(dicomDict, bulkDataPathPrefix);
  });
});

router.get(
  "/dicomweb/studies/:studyInstanceUID/series/:seriesInstanceUID/instances/:sopInstanceUID/frames/:frameNumber",
  async (ctx) => {
    const { sopInstanceUID, studyInstanceUID, seriesInstanceUID } = ctx.params;
    const dicomDict = db.get(`instance:${studyInstanceUID}:${seriesInstanceUID}:${sopInstanceUID}`);
    if (!dicomDict) {
      ctx.response.status = 404;
      ctx.response.body = { error: "Instance not found" };
      return;
    }

    const transferSyntax = dicomDict["00020010"]?.Value?.[0];
    const pixelData = dicomDict["7FE00010"];
    const filePath = dicomDict["00090001"]?.Value?.[0];
    const numberOfFrames = dicomDict["00280008"]?.Value?.[0];
    const frameIndex = parseInt(ctx.params.frameNumber, 10) - 1;
    const BulkDataURI = pixelData?.value?.[frameIndex]?.BulkDataURI || pixelData?.BulkDataURI;
    if (!BulkDataURI) throw new Error("No BulkDataURI found for multiframe data");

    if (numberOfFrames > 1 && pixelData?.BulkDataURI) {
      // one continuous array, we need to find the offset within to stream from.
      const rows = dicomDict["00280010"]?.Value?.[0];
      const cols = dicomDict["00280011"]?.Value?.[0];
      const bitsAllocated = dicomDict["00280100"]?.Value?.[0];
      const samplesPerPixel = dicomDict["00280002"]?.Value?.[0];

      if (!samplesPerPixel || !rows || !cols || !bitsAllocated) {
        throw new Error(
          `Missing required DICOM attributes for multiframe data ${samplesPerPixel}, ${rows}, ${cols}, ${bitsAllocated}`,
        );
      }

      const bytesPerFrame = rows * cols * samplesPerPixel * (bitsAllocated / 8);
      const expectedFrameSize = numberOfFrames * bytesPerFrame;

      const [start, end] = BulkDataURI.match(/range=(\d+)-(\d+)/).slice(1).map(Number);

      if (end - start !== expectedFrameSize) {
        throw new Error(`Unexpected frame size: ${end - start} !== ${expectedFrameSize}`);
      }
      if (frameIndex < 0 || frameIndex >= numberOfFrames) {
        throw new Error(`Invalid frame index: ${frameIndex}`);
      }

      const frameStream = fs.createReadStream(filePath, {
        start: start + frameIndex * bytesPerFrame,
        end: start + (frameIndex + 1) * bytesPerFrame,
        highWaterMark: 1024 * 1024,
      });
      ctx.response.body = frameStream;
    } else {
      const [start, end] = BulkDataURI.match(/range=(\d+)-(\d+)/).slice(1).map(Number);
      const frameStream = fs.createReadStream(filePath, { start, end, highWaterMark: 1024 * 1024 });
      ctx.response.body = frameStream;
      ctx.response.headers.set("Content-Length", end - start);
    }
    ctx.response.headers.set("Content-Type", TRANSFER_SYNTAXES_TO_CONTENT_TYPE[transferSyntax] || "UNKNOWN");
  },
);

router.get("/dicomweb/bulkdata/:studyInstanceUID/:seriesInstanceUID/:sopInstanceUID/:tagPath*", async (ctx) => {
  const { studyInstanceUID, seriesInstanceUID, sopInstanceUID, tagPath } = ctx.params;
  const dicomDict = db.get(`instance:${studyInstanceUID}:${seriesInstanceUID}:${sopInstanceUID}`);
  if (!dicomDict) {
    ctx.response.status = 404;
    ctx.response.body = { error: "Instance not found" };
    return;
  }

  const filePath = dicomDict["00090001"]?.Value?.[0];
  const [start, end] = ctx.request.url.searchParams.get("range").match(/(\d+)-(\d+)/).slice(1).map(Number);
  const frameStream = fs.createReadStream(filePath, { start, end, highWaterMark: 1024 * 1024 });
  ctx.response.body = frameStream;
  ctx.response.headers.set("Content-Type", "application/octet-stream");
});

const app = new Application();
app.use(oakCors());
app.use(router.routes());
app.use(router.allowedMethods());
app.listen({ port: 5010 });

console.log("DICOMweb server running on http://localhost:4001/dicomweb");
