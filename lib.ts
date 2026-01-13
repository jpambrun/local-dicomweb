import stream from "node:stream";
import {
	PreamblePart,
	ValueChunk,
	elementFlow,
	elementSink,
	parseFlow,
	pipe,
	toIndeterminateLengthSequences,
} from "@exini/dicom-streams-js";

import fs from "node:fs";
import { type RootDatabase, open } from "lmdb";

export const db: RootDatabase<any, any> = open({
	path: "_dicomweb",
	useVersions: true,
});

export const SERIES_LIST_ITEMS = [
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
] as const;

export const STUDY_LIST_ITEMS = [
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
] as const;

export const TRANSFER_SYNTAXES_TO_CONTENT_TYPE: Record<string, string> = {
	"1.2.840.10008.1.2": "image/uncompressed;transfer-syntax=1.2.840.10008.1.2",
	"1.2.840.10008.1.2.1":
		"image/uncompressed;transfer-syntax=1.2.840.10008.1.2.1",
	"1.2.840.10008.1.2.1.99":
		"image/uncompressed;transfer-syntax=1.2.840.10008.1.2.1.99",
	"1.2.840.10008.1.2.2":
		"image/uncompressed;transfer-syntax=1.2.840.10008.1.2.2",
	"1.2.840.10008.1.2.4.50": "image/jpeg;transfer-syntax=1.2.840.10008.1.2.4.50",
	"1.2.840.10008.1.2.4.51": "image/jpeg;transfer-syntax=1.2.840.10008.1.2.4.51",
	"1.2.840.10008.1.2.4.57": "image/jpeg;transfer-syntax=1.2.840.10008.1.2.4.57",
	"1.2.840.10008.1.2.4.70": "image/jll;transfer-syntax=1.2.840.10008.1.2.4.70",
	"1.2.840.10008.1.2.4.140":
		"image/x-jxl;transfer-syntax=1.2.840.10008.1.2.4.140",
	"1.2.840.10008.1.2.4.141":
		"image/x-jxl;transfer-syntax=1.2.840.10008.1.2.4.141",
	"1.2.840.10008.1.2.4.142":
		"image/x-jxl;transfer-syntax=1.2.840.10008.1.2.4.142",
	"1.2.840.10008.1.2.4.80": "image/jls;transfer-syntax=1.2.840.10008.1.2.4.80",
	"1.2.840.10008.1.2.4.81": "image/jls;transfer-syntax=1.2.840.10008.1.2.4.81",
	"1.2.840.10008.1.2.4.90": "image/jp2;transfer-syntax=1.2.840.10008.1.2.4.90",
	"1.2.840.10008.1.2.4.91": "image/jp2;transfer-syntax=1.2.840.10008.1.2.4.91",
	"3.2.840.10008.1.2.4.96": "image/jphc;transfer-syntax=3.2.840.10008.1.2.4.96",
	"1.2.840.10008.1.2.4.201":
		"image/jphc;transfer-syntax=1.2.840.10008.1.2.4.201",
	"1.2.840.10008.1.2.4.202":
		"image/jphc;transfer-syntax=1.2.840.10008.1.2.4.202",
	"1.2.840.10008.1.2.4.203":
		"image/jphc;transfer-syntax=1.2.840.10008.1.2.4.203",
	"1.2.840.10008.1.2.5": "image/dicom-rle;transfer-syntax=1.2.840.10008.1.2.5",
};

export function isValidDicomUID(uid: string): boolean {
	if (!uid || typeof uid !== "string" || uid.length > 64) return false;
	return /^[0-9.]+$/.test(uid);
}

export function recursivelyReplaceBulkDataURI(
	dict: Record<string, any>,
	pathPrefix = "",
): Record<string, any> {
	for (const key in dict) {
		if (typeof dict[key] === "object" && dict[key] !== null) {
			recursivelyReplaceBulkDataURI(dict[key], `${pathPrefix}${key}/`);
		} else if (key === "BulkDataURI") {
			dict[key] = pathPrefix.replace(/\/$/, "") + dict[key];
		}
	}
	return dict;
}

export function tagToString(tag: number): string {
	const hex = `00000000${tag.toString(16)}`.slice(-8);
	return hex.toLocaleUpperCase();
}

export class FilterLargeValue extends stream.Transform {
	maxSize: number;
	currentVR: string | null;
	VRToFilter: string[];

	constructor(maxSize = 100) {
		super({ objectMode: true });
		this.maxSize = maxSize;
		this.currentVR = null;
		this.VRToFilter = ["OB", "OW", "OF", "UN"];
	}

	_transform(
		chunk: any,
		_encoding: BufferEncoding,
		callback: (error?: Error | null, data?: any) => void,
	) {
		if (chunk instanceof PreamblePart) return callback();
		this.currentVR = chunk?.vr?.name || this.currentVR;
		if (
			chunk instanceof ValueChunk &&
			chunk.bytes !== undefined &&
			chunk.bytes.length > this.maxSize &&
			this.currentVR !== null &&
			this.VRToFilter.includes(this.currentVR)
		) {
			chunk.bytes = Buffer.alloc(0);
		}
		callback(null, chunk);
	}
}

export const convertToDicomweb = (
	src: any,
	dst: Record<string, any> = {},
): Record<string, any> => {
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
					const valueStrings = value
						.toStrings(vr.name, bigEndian, characterSets)
						.filter((pn: string) => pn !== "");
					if (valueStrings.length === 0) {
						dst[hextag] = { vr: vr.name };
					} else {
						dst[hextag] = {
							vr: vr.name,
							Value: valueStrings.map((pn: string) => ({ Alphabetic: pn })),
						};
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
					dst[hextag] = {
						vr: vr.name,
						Value: value.toNumbers(vr, bigEndian, characterSets),
					};
					break;
				case "OB":
				case "OW":
				case "OF":
					if (element.value.bytes.length > 0) {
						dst[hextag] = {
							vr: vr.name,
							InlineBinary: element.value.bytes.toString("base64"),
						};
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
					dst[hextag] = {
						vr: vr.name,
						Value: value.toStrings(vr, bigEndian, characterSets),
					};
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
				value: fragments.map((f: any) => {
					if (f.value.bytes.length > 0) {
						return { InlineBinary: f.value.bytes.toString("base64") };
					}
					if (f.range) {
						return {
							BulkDataURI: `?range=${f.range[0]}-${f.range[0] + f.range[1]}`,
						};
					}
					throw new Error(`No data: ${hextag} with VR ${vr.name}`);
				}),
			};
		} else {
			throw new Error(`Unsupported element: ${hextag} with VR ${vr.name}`);
		}
	}
	return dst;
};

export const insertStudy = async (
	studyInstanceUID: string,
	dicomDict: Record<string, any>,
): Promise<void> => {
	const modality = dicomDict["00080060"]?.Value?.[0];
	const studyModalityKey = `studymodalities:${studyInstanceUID}:modality:${modality}`;
	if (modality && !db.get(studyModalityKey)) {
		await db.put(studyModalityKey, modality);
	}

	const studyKey = `study:${studyInstanceUID}`;
	if (await db.get(studyKey)) return;

	const study: Record<string, any> = {};
	for (const item of STUDY_LIST_ITEMS) {
		if (dicomDict[item]?.Value?.[0]) {
			study[item] = dicomDict[item];
		}
	}

	if (!(await db.put(studyKey, study, 1))) {
		throw new Error("Failed to insert study entry");
	}
};

export const insertSeries = async (
	studyInstanceUID: string,
	seriesInstanceUID: string,
	dicomDict: Record<string, any>,
): Promise<void> => {
	const seriesKey = `series:${studyInstanceUID}:${seriesInstanceUID}`;
	if (await db.get(seriesKey)) return;

	const series: Record<string, any> = {};
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

export async function processDicomFile(filePath: string): Promise<void> {
	if (await db.get(`filepath:${filePath}`)) return;

	await new Promise<void>((resolve) => {
		const stream = pipe(
			fs.createReadStream(filePath, { highWaterMark: 1024 * 1024 }),
			parseFlow(),
			toIndeterminateLengthSequences(),
			new FilterLargeValue(),
			elementFlow(),
			elementSink(async (elements: any) => {
				try {
					const dicomDict = convertToDicomweb(elements);
					const sopInstanceUID = dicomDict["00080018"].Value?.[0];
					const seriesInstanceUID = dicomDict["0020000E"].Value?.[0];
					const studyInstanceUID = dicomDict["0020000D"].Value?.[0];
					dicomDict["00083002"] = dicomDict["00020010"];
					dicomDict["00090001"] = { vr: "LT", Value: [filePath] };
					await db.put(
						`instance:${studyInstanceUID}:${seriesInstanceUID}:${sopInstanceUID}`,
						dicomDict,
					);
					await insertStudy(studyInstanceUID, dicomDict);
					await insertSeries(studyInstanceUID, seriesInstanceUID, dicomDict);
					await db.put(
						`filepath:${filePath}`,
						`instance:${studyInstanceUID}:${seriesInstanceUID}:${sopInstanceUID}`,
					);
				} catch (e) {
					console.error(`Error processing dict ${filePath}:`, e);
				}
				resolve();
			}),
		);

		stream.on("error", (e: Error) => {
			if (e.message.includes("Not a DICOM stream")) {
				console.log(`Skipping non DICOM file: ${filePath}`);
			} else {
				console.error(`Error processing stream ${filePath}:`, e);
			}
			resolve();
		});
	});
}
