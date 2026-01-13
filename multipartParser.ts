// A lightweight multipart/related parser that emits the body bytes of each part as Uint8Array.
// Exposes:
// - createMultipartTransformStream(boundary) -> Web TransformStream<Uint8Array,Uint8Array>
// - MultipartParserTransform -> Node stream.Transform that outputs Buffers (Uint8Array-compatible)
// - iterateMultipartParts(asyncIterable, boundary) -> AsyncGenerator<Uint8Array>
//
// Notes:
// - This parser expects a boundary string (without the leading `--`).
// - It tolerates boundaries split across incoming chunks and handles the final boundary.
// - It strips part headers (everything up to the first CRLFCRLF) and yields only the body bytes.
// - Not a full MIME header parser; if you need headers, modify to parse the header block.
//
// Example usages (see bottom of file).
const textEncoder = new TextEncoder();

function indexOfSubarray(
	haystack: Uint8Array,
	needle: Uint8Array,
	from = 0,
): number {
	if (needle.length === 0) return from <= haystack.length ? from : -1;
	outer: for (
		let i = Math.max(0, from);
		i <= haystack.length - needle.length;
		i++
	) {
		for (let j = 0; j < needle.length; j++) {
			if (haystack[i + j] !== needle[j]) continue outer;
		}
		return i;
	}
	return -1;
}

function concatUint8(a: Uint8Array, b: Uint8Array): Uint8Array {
	const out = new Uint8Array(a.length + b.length);
	out.set(a, 0);
	out.set(b, a.length);
	return out;
}

// Core parser state machine used by all three interfaces
function createParser(boundary: string) {
	const boundaryLine = `--${boundary}`;
	const boundaryBytes = textEncoder.encode(boundaryLine);
	const crlf = textEncoder.encode("\r\n");
	const crlfBoundary = concatUint8(crlf, boundaryBytes);
	const crlfcrlf = textEncoder.encode("\r\n\r\n");

	let buffer: Uint8Array<ArrayBuffer> = new Uint8Array(0);
	let seenFirstBoundary = false;

	function pushChunk(chunk: Uint8Array | Buffer): Uint8Array[] {
		const u = chunk instanceof Uint8Array ? chunk : new Uint8Array(chunk);
		buffer = concatUint8(buffer, u) as Uint8Array<ArrayBuffer>;
		const outputs: Uint8Array[] = [];

		// Find and discard the initial preamble up to the first boundary
		if (!seenFirstBoundary) {
			const firstIdx = indexOfSubarray(buffer, boundaryBytes, 0);
			if (firstIdx === -1) {
				return outputs;
			}
			buffer = buffer.slice(firstIdx);
			seenFirstBoundary = true;
			const pos = boundaryBytes.length;

			if (
				buffer.length >= pos + 2 &&
				buffer[pos] === 0x2d /*-*/ &&
				buffer[pos + 1] === 0x2d
			) {
				buffer = buffer.slice(pos + 2);
				return outputs;
			}

			if (
				buffer.length >= pos + 2 &&
				buffer[pos] === 0x0d /*\r*/ &&
				buffer[pos + 1] === 0x0a /*\n*/
			) {
				buffer = buffer.slice(pos + 2);
			} else {
				if (buffer.length < pos + 2) return outputs;
				buffer = buffer.slice(pos);
			}
		}

		while (true) {
			const nextIdx = indexOfSubarray(buffer, crlfBoundary, 0);
			if (nextIdx === -1) {
				break;
			}

			const partContent = buffer.slice(0, nextIdx);

			const headerEnd = indexOfSubarray(partContent, crlfcrlf, 0);
			let body: Uint8Array;
			if (headerEnd !== -1) {
				body = partContent.slice(headerEnd + crlfcrlf.length);
			} else {
				body = partContent;
			}
			outputs.push(body);

			const afterBoundaryIdx = nextIdx + crlfBoundary.length;
			let isFinal = false;
			if (buffer.length >= afterBoundaryIdx + 2) {
				if (
					buffer[afterBoundaryIdx] === 0x2d &&
					buffer[afterBoundaryIdx + 1] === 0x2d
				) {
					isFinal = true;
				}
			} else {
			}

			buffer = buffer.slice(nextIdx + 2);

			if (buffer.length >= boundaryBytes.length) {
				buffer = buffer.slice(boundaryBytes.length);
				if (buffer.length >= 2 && buffer[0] === 0x2d && buffer[1] === 0x2d) {
					buffer = buffer.slice(2);
					if (buffer.length >= 2 && buffer[0] === 0x0d && buffer[1] === 0x0a)
						buffer = buffer.slice(2);
					buffer = new Uint8Array(0);
					break;
				}
				if (buffer.length >= 2 && buffer[0] === 0x0d && buffer[1] === 0x0a) {
					buffer = buffer.slice(2);
				}
			} else {
				break;
			}
		}

		return outputs;
	}

	function endAndFlush(): Uint8Array[] {
		const outputs: Uint8Array[] = [];
		if (!seenFirstBoundary) return outputs;
		if (buffer.length === 0) return outputs;

		const headerEnd = indexOfSubarray(
			buffer,
			textEncoder.encode("\r\n\r\n"),
			0,
		);
		let body: Uint8Array;
		if (headerEnd !== -1) {
			body = buffer.slice(headerEnd + textEncoder.encode("\r\n\r\n").length);
		} else {
			body = buffer;
		}
		if (body.length > 0) outputs.push(body);
		buffer = new Uint8Array(0);
		return outputs;
	}

	return { pushChunk, endAndFlush };
}

// Web TransformStream version
export function createMultipartTransformStream(
	boundary: string,
): TransformStream<Uint8Array, Uint8Array> {
	const parser = createParser(boundary);
	return new TransformStream({
		transform(chunk, controller) {
			const outputs = parser.pushChunk(chunk);
			for (const out of outputs) controller.enqueue(out);
		},
		flush(controller) {
			const outputs = parser.endAndFlush();
			for (const out of outputs) controller.enqueue(out);
		},
	});
}

// Node stream.Transform version
import stream from "node:stream";

export class MultipartParserTransform extends stream.Transform {
	_parser: ReturnType<typeof createParser>;

	constructor(boundary: string, options: stream.TransformOptions = {}) {
		super({ ...options, readableObjectMode: false, writableObjectMode: false });
		this._parser = createParser(boundary);
	}

	_transform(
		chunk: Buffer,
		_encoding: BufferEncoding,
		callback: (error?: Error | null) => void,
	) {
		try {
			const outputs = this._parser.pushChunk(chunk);
			for (const out of outputs) this.push(Buffer.from(out));
			callback();
		} catch (err) {
			callback(err as Error);
		}
	}

	_flush(callback: (error?: Error | null) => void) {
		try {
			const outputs = this._parser.endAndFlush();
			for (const out of outputs) this.push(Buffer.from(out));
			callback();
		} catch (err) {
			callback(err as Error);
		}
	}
}

// Async iterable / generator version
export async function* iterateMultipartParts(
	asyncIterable: AsyncIterable<Uint8Array>,
	boundary: string,
): AsyncGenerator<Uint8Array, void, unknown> {
	const parser = createParser(boundary);
	for await (const chunk of asyncIterable) {
		const outs = parser.pushChunk(chunk);
		for (const out of outs) yield out;
	}
	const finalOuts = parser.endAndFlush();
	for (const f of finalOuts) yield f;
}
