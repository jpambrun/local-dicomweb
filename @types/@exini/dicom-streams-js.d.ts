declare module "@exini/dicom-streams-js" {
	import type { Transform, Readable } from "node:stream";

	export interface VR {
		name: string;
	}

	export interface Element {
		vr: VR;
		bigEndian: boolean;
		value: {
			bytes: Buffer;
			toStrings: (
				vr: VR,
				bigEndian: boolean,
				characterSets: string[],
			) => string[];
			toNumbers: (
				vr: VR,
				bigEndian: boolean,
				characterSets: string[],
			) => number[];
		} | null;
		tag: number;
		items?: { elements: any }[];
		fragments?: { value: { bytes: Buffer }; range?: number[] }[];
		range?: number[];
	}

	export class PreamblePart {}

	export class ValueChunk {
		vr?: VR;
		bytes?: Buffer;
	}

	export function elementFlow(): Transform;
	export function elementSink(
		callback: (elements: any) => Promise<void>,
	): Transform;
	export function parseFlow(): Transform;
	export function pipe(...streams: (Transform | Readable)[]): Transform;
	export function toIndeterminateLengthSequences(): Transform;
}
