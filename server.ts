import fs from "node:fs";
import { Hono } from "hono";
import {
	TRANSFER_SYNTAXES_TO_CONTENT_TYPE,
	db,
	isValidDicomUID,
	processDicomFile,
	recursivelyReplaceBulkDataURI,
} from "./lib";
import { createMultipartTransformStream } from "./multipartParser";

const app = new Hono();

app.use("/*", async (c, next) => {
	c.header("Access-Control-Allow-Origin", "*");
	c.header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS");
	c.header("Access-Control-Allow-Headers", "Content-Type, Authorization");
	if (c.req.method === "OPTIONS") {
		return new Response(null, { status: 200 });
	}
	await next();
});

app.get("/dicomweb/studies", async (c) => {
	const ModalitiesInStudy = c.req.query("ModalitiesInStudy");
	const PatientName = c.req.query("PatientName");
	const StudyDescription = c.req.query("StudyDescription");
	const patientID = c.req.query("00100020");
	const StudyInstanceUID = c.req.query("StudyInstanceUID");
	const limitParam = c.req.query("limit");
	const offsetParam = c.req.query("offset");
	const includeFieldParam = c.req.query("includefield");
	const limit = limitParam ? Number.parseInt(limitParam, 10) : undefined;
	const offset = offsetParam ? Number.parseInt(offsetParam, 10) : 0;
	const includeFields = includeFieldParam
		? decodeURIComponent(includeFieldParam).split(",")
		: null;

	const studies = [
		...db
			.getRange({ start: "study:", end: "study:" + "\ufffd" })
			.map(({ value: study }) => {
				const studyInstanceUID = study["0020000D"]?.Value?.[0];
				const modalities = [
					...db
						.getRange({
							start: `studymodalities:${studyInstanceUID}:modality:`,
							end: `studymodalities:${studyInstanceUID}:modality:\ufffd`,
						})
						.map(({ value }) => value),
				];
				study["00080061"] = { vr: "CS", Value: modalities };
				return study;
			})
			.filter((study) => {
				if (StudyInstanceUID) {
					const uid = study["0020000D"]?.Value?.[0] || "";
					return uid === StudyInstanceUID;
				}

				if (ModalitiesInStudy) {
					const requestedModalities = ModalitiesInStudy.split(",").map((m) =>
						m.trim(),
					);
					const modalities = study["00080061"]?.Value?.[0]?.split?.("\\") || [];
					return requestedModalities.some((m) => modalities.includes(m));
				}

				if (PatientName) {
					const patientName = study["00100010"]?.Value?.[0]?.Alphabetic || "";
					return patientName
						.toLowerCase()
						.includes(PatientName.toLowerCase().replaceAll("*", ""));
				}

				if (StudyDescription) {
					const studyDesc = study["00081030"]?.Value?.[0] || "";
					return studyDesc
						.toLowerCase()
						.includes(StudyDescription.toLowerCase().replaceAll("*", ""));
				}

				if (patientID) {
					const id = study["00100020"]?.Value?.[0] || "";
					return id
						.toLowerCase()
						.includes(patientID.toLowerCase().replaceAll("*", ""));
				}
				return true;
			})
			.map((study) => {
				const studyInstanceUID = study["0020000D"]?.Value?.[0];
				study["00201208"] = {
					vr: "IS",
					Value: [
						db.getCount({
							start: `instance:${studyInstanceUID}`,
							end: `instance:${studyInstanceUID}\ufffd`,
						}),
					],
				};
				return study;
			}),
	];

	studies.sort((a, b) => {
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

	const startIndex = Math.max(0, offset);
	const endIndex = limit !== undefined ? startIndex + limit : studies.length;
	const paginatedStudies = studies.slice(startIndex, endIndex);

	return c.json(paginatedStudies);
});

app.get("/dicomweb/studies/:studyInstanceUID/series", async (c) => {
	const studyInstanceUID = c.req.param("studyInstanceUID");
	if (!isValidDicomUID(studyInstanceUID)) {
		return c.json({ error: "Invalid studyInstanceUID" }, 400);
	}
	const series = [
		...db
			.getRange({
				start: `series:${studyInstanceUID}:`,
				end: `series:${studyInstanceUID}:\ufffd`,
			})
			.map(({ value }) => value),
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

	return c.json(series);
});

app.post("/dicomweb/studies", async (c) => {
	const contentType = c.req.header("content-type");
	if (!contentType) {
		return c.json({ error: "Missing content-type header" }, 400);
	}
	const boundaryMatch = contentType.match(/boundary=([^;\s]+)/);
	if (!boundaryMatch) {
		return c.json({ error: "Missing boundary in content-type" }, 400);
	}
	const boundary = boundaryMatch[1].replaceAll(`"`, "");
	const transform = createMultipartTransformStream(boundary);
	const body = await c.req.raw.arrayBuffer();
	const stream = new ReadableStream({
		start(controller) {
			controller.enqueue(new Uint8Array(body));
			controller.close();
		},
	});
	const partStream = stream.pipeThrough(transform);
	for await (const part of partStream) {
		const filename = `./_stow/${crypto.randomUUID()}.dcm`;
		await fs.promises.mkdir("./_stow", { recursive: true });
		await Bun.write(filename, part);
		await processDicomFile(filename);
	}
	return new Response(null, {
		status: 202,
		headers: {
			"Access-Control-Allow-Origin": "*",
		},
	});
});

app.get(
	"/dicomweb/studies/:studyInstanceUID/series/:seriesInstanceUID/metadata",
	async (c) => {
		const studyInstanceUID = c.req.param("studyInstanceUID");
		const seriesInstanceUID = c.req.param("seriesInstanceUID");
		if (
			!isValidDicomUID(studyInstanceUID) ||
			!isValidDicomUID(seriesInstanceUID)
		) {
			return c.json({ error: "Invalid UID" }, 400);
		}

		const instances = [
			...db
				.getRange({
					start: `instance:${studyInstanceUID}:${seriesInstanceUID}:`,
					end: `instance:${studyInstanceUID}:${seriesInstanceUID}:\ufffd`,
				})
				.map(({ value }) => value),
		];
		if (!instances.length) {
			return c.json({ error: "Series not found" }, 404);
		}
		return c.json(
			instances.map((dicomDict) => {
				const sopInstanceUID = dicomDict["00080018"]?.Value?.[0];
				const bulkDataPathPrefix = `/dicomweb/bulkdata/${studyInstanceUID}/${seriesInstanceUID}/${sopInstanceUID}/`;
				return recursivelyReplaceBulkDataURI(dicomDict, bulkDataPathPrefix);
			}),
		);
	},
);

app.get(
	"/dicomweb/studies/:studyInstanceUID/series/:seriesInstanceUID/instances/:sopInstanceUID/frames/:frameNumber",
	async (c) => {
		const studyInstanceUID = c.req.param("studyInstanceUID");
		const seriesInstanceUID = c.req.param("seriesInstanceUID");
		const sopInstanceUID = c.req.param("sopInstanceUID");
		const frameNumber = c.req.param("frameNumber");
		if (
			!isValidDicomUID(studyInstanceUID) ||
			!isValidDicomUID(seriesInstanceUID) ||
			!isValidDicomUID(sopInstanceUID)
		) {
			return c.json({ error: "Invalid UID" }, 400);
		}
		const frameNum = Number.parseInt(frameNumber, 10);
		if (Number.isNaN(frameNum) || frameNum < 1) {
			return c.json({ error: "Invalid frame number" }, 400);
		}
		const dicomDict = db.get(
			`instance:${studyInstanceUID}:${seriesInstanceUID}:${sopInstanceUID}`,
		);
		if (!dicomDict) {
			return c.json({ error: "Instance not found" }, 404);
		}

		const transferSyntax = dicomDict["00020010"]?.Value?.[0];
		const pixelData = dicomDict["7FE00010"];
		const filePath = dicomDict["00090001"]?.Value?.[0];
		const numberOfFrames = dicomDict["00280008"]?.Value?.[0];
		const frameIndex = frameNum - 1;
		const BulkDataURI =
			pixelData?.value?.[frameIndex]?.BulkDataURI || pixelData?.BulkDataURI;
		if (!BulkDataURI)
			throw new Error("No BulkDataURI found for multiframe data");

		if (numberOfFrames > 1 && pixelData?.BulkDataURI) {
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

			const [start, end] = BulkDataURI.match(/range=(\d+)-(\d+)/)
				.slice(1)
				.map(Number);

			if (end - start !== expectedFrameSize) {
				throw new Error(
					`Unexpected frame size: ${end - start} !== ${expectedFrameSize}`,
				);
			}
			if (frameIndex < 0 || frameIndex >= numberOfFrames) {
				throw new Error(`Invalid frame index: ${frameIndex}`);
			}

			const file = Bun.file(filePath);
			const slice = file.slice(
				start + frameIndex * bytesPerFrame,
				start + (frameIndex + 1) * bytesPerFrame,
			);
			return new Response(slice, {
				headers: {
					"Content-Type":
						TRANSFER_SYNTAXES_TO_CONTENT_TYPE[transferSyntax] || "UNKNOWN",
					"Access-Control-Allow-Origin": "*",
				},
			});
		}
		const [start, end] = BulkDataURI.match(/range=(\d+)-(\d+)/)
			.slice(1)
			.map(Number);
		const file = Bun.file(filePath);
		const slice = file.slice(start, end);
		return new Response(slice, {
			headers: {
				"Content-Type":
					TRANSFER_SYNTAXES_TO_CONTENT_TYPE[transferSyntax] || "UNKNOWN",
				"Content-Length": (end - start).toString(),
				"Access-Control-Allow-Origin": "*",
			},
		});
	},
);

app.get(
	"/dicomweb/bulkdata/:studyInstanceUID/:seriesInstanceUID/:sopInstanceUID/:tagPath*",
	async (c) => {
		const studyInstanceUID = c.req.param("studyInstanceUID");
		const seriesInstanceUID = c.req.param("seriesInstanceUID");
		const sopInstanceUID = c.req.param("sopInstanceUID");
		if (
			!isValidDicomUID(studyInstanceUID) ||
			!isValidDicomUID(seriesInstanceUID) ||
			!isValidDicomUID(sopInstanceUID)
		) {
			return c.json({ error: "Invalid UID" }, 400);
		}
		const rangeParam = c.req.query("range");
		if (!rangeParam || !/^\d+-\d+$/.test(rangeParam)) {
			return c.json({ error: "Invalid range parameter" }, 400);
		}
		const [start, end] = rangeParam.split("-").map(Number);
		if (Number.isNaN(start) || Number.isNaN(end) || start > end) {
			return c.json({ error: "Invalid range values" }, 400);
		}
		const dicomDict = db.get(
			`instance:${studyInstanceUID}:${seriesInstanceUID}:${sopInstanceUID}`,
		);
		if (!dicomDict) {
			return c.json({ error: "Instance not found" }, 404);
		}

		const filePath = dicomDict["00090001"]?.Value?.[0];
		const file = Bun.file(filePath);
		const slice = file.slice(start, end);
		return new Response(slice, {
			headers: {
				"Content-Type": "application/octet-stream",
				"Access-Control-Allow-Origin": "*",
			},
		});
	},
);

const port = 5010;
Bun.serve({
	port,
	fetch: app.fetch,
});

console.log(`DICOMweb server running on http://localhost:${port}/dicomweb`);
