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

function indexOfSubarray(haystack, needle, from = 0) {
  if (needle.length === 0) return from <= haystack.length ? from : -1;
  outer: for (let i = Math.max(0, from); i <= haystack.length - needle.length; i++) {
    for (let j = 0; j < needle.length; j++) {
      if (haystack[i + j] !== needle[j]) continue outer;
    }
    return i;
  }
  return -1;
}

function concatUint8(a, b) {
  const out = new Uint8Array(a.length + b.length);
  out.set(a, 0);
  out.set(b, a.length);
  return out;
}

// Core parser state machine used by all three interfaces
function createParser(boundary) {
  const boundaryLine = `--${boundary}`;
  const boundaryBytes = textEncoder.encode(boundaryLine); // `--boundary`
  const crlf = textEncoder.encode('\r\n');
  const crlfBoundary = concatUint8(crlf, boundaryBytes); // '\r\n--boundary'
  const crlfcrlf = textEncoder.encode('\r\n\r\n');

  let buffer = new Uint8Array(0);
  let seenFirstBoundary = false;

  function pushChunk(chunk) {
    // Ensure chunk is Uint8Array
    const u = chunk instanceof Uint8Array ? chunk : new Uint8Array(chunk);
    buffer = concatUint8(buffer, u);
    const outputs = [];

    // Find and discard the initial preamble up to the first boundary
    if (!seenFirstBoundary) {
      const firstIdx = indexOfSubarray(buffer, boundaryBytes, 0);
      if (firstIdx === -1) {
        // Not found yet; keep buffering (but to avoid infinite memory, you may want to cap size)
        return outputs;
      }
      // Remove preamble before boundary
      buffer = buffer.slice(firstIdx);
      seenFirstBoundary = true;
      // Remove the first boundary line itself
      // boundaryBytes at position 0
      let pos = boundaryBytes.length;
      // boundary line may be followed by '--' (final) or CRLF
      if (buffer.length >= pos + 2 && buffer[pos] === 0x2D /*-*/ && buffer[pos + 1] === 0x2D) {
        // final boundary straight away: nothing to yield
        buffer = buffer.slice(pos + 2);
        return outputs;
      }
      // skip possible CRLF after boundary line
      if (buffer.length >= pos + 2 && buffer[pos] === 0x0D /*\r*/ && buffer[pos + 1] === 0x0A /*\n*/) {
        buffer = buffer.slice(pos + 2);
      } else {
        // if not enough bytes to decide, wait for more data
        if (buffer.length < pos + 2) return outputs;
        // else continue (no CRLF)
        buffer = buffer.slice(pos);
      }
    }

    // Now extract parts between subsequent boundaries.
    while (true) {
      // search for next delimiter which is '\r\n--boundary'
      const nextIdx = indexOfSubarray(buffer, crlfBoundary, 0);
      if (nextIdx === -1) {
        // No complete next boundary found yet. Keep buffer and wait for more data.
        break;
      }

      // Part content is buffer[0 .. nextIdx)
      const partContent = buffer.slice(0, nextIdx);

      // Within the part, headers end at '\r\n\r\n'. Remove headers and yield body.
      const headerEnd = indexOfSubarray(partContent, crlfcrlf, 0);
      let body;
      if (headerEnd !== -1) {
        body = partContent.slice(headerEnd + crlfcrlf.length);
      } else {
        // No headers (edge case), treat entire part as body
        body = partContent;
      }
      outputs.push(body);

      // Determine if this boundary is final by checking for `--` after the boundary marker.
      const afterBoundaryIdx = nextIdx + crlfBoundary.length;
      let isFinal = false;
      if (buffer.length >= afterBoundaryIdx + 2) {
        if (buffer[afterBoundaryIdx] === 0x2D /*-*/ && buffer[afterBoundaryIdx + 1] === 0x2D) {
          isFinal = true;
        }
      } else {
        // Not enough bytes yet to know if final; but we still can remove up to boundary and keep rest
      }

      // Remove up to the boundary marker (keep the boundary line itself for next iteration logic)
      // We remove the leading CRLF we matched so the next boundary starts at index 0
      buffer = buffer.slice(nextIdx + 2); // remove '\r\n' before '--boundary'
      // Now buffer starts with '--boundary...'
      // Remove the boundary line and its trailing CRLF if present, to position to start of next part (or end)
      if (buffer.length >= boundaryBytes.length) {
        buffer = buffer.slice(boundaryBytes.length);
        // if '--' -> final boundary; remove and stop
        if (buffer.length >= 2 && buffer[0] === 0x2D /*-*/ && buffer[1] === 0x2D) {
          // remove the '--' and any trailing CRLF, end parsing
          buffer = buffer.slice(2);
          // strip possible trailing CRLF
          if (buffer.length >= 2 && buffer[0] === 0x0D && buffer[1] === 0x0A) buffer = buffer.slice(2);
          // final boundary; no more parts
          // discard buffer
          buffer = new Uint8Array(0);
          break;
        }
        // skip single CRLF after boundary line if present
        if (buffer.length >= 2 && buffer[0] === 0x0D && buffer[1] === 0x0A) {
          buffer = buffer.slice(2);
        }
        // continue loop to find next boundary
      } else {
        // Not enough bytes to remove boundary; leave buffer as-is and break until more data arrives
        break;
      }
    }

    return outputs;
  }

  function endAndFlush() {
    // If there's leftover buffer after stream end, try to treat it as last part
    const outputs = [];
    if (!seenFirstBoundary) return outputs;
    if (buffer.length === 0) return outputs;
    // treat remaining buffer as a part (strip headers)
    const crlfcrlf = textEncoder.encode('\r\n\r\n');
    const headerEnd = indexOfSubarray(buffer, crlfcrlf, 0);
    let body;
    if (headerEnd !== -1) {
      body = buffer.slice(headerEnd + crlfcrlf.length);
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
export function createMultipartTransformStream(boundary) {
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
import stream from 'node:stream';
export class MultipartParserTransform extends stream.Transform {
  constructor(boundary, options = {}) {
    // objectMode false: we produce Buffers
    super({ ...options, readableObjectMode: false, writableObjectMode: false });
    this._parser = createParser(boundary);
  }
  _transform(chunk, encoding, callback) {
    try {
      const outputs = this._parser.pushChunk(chunk);
      for (const out of outputs) this.push(Buffer.from(out));
      callback();
    } catch (err) {
      callback(err);
    }
  }
  _flush(callback) {
    try {
      const outputs = this._parser.endAndFlush();
      for (const out of outputs) this.push(Buffer.from(out));
      callback();
    } catch (err) {
      callback(err);
    }
  }
}

// Async iterable / generator version
export async function* iterateMultipartParts(asyncIterable, boundary) {
  const parser = createParser(boundary);
  for await (const chunk of asyncIterable) {
    const outs = parser.pushChunk(chunk);
    for (const out of outs) yield out;
  }
  const finalOuts = parser.endAndFlush();
  for (const f of finalOuts) yield f;
}

/*
Example usage (Node-style with fs.createReadStream):

const fs = require('fs');
const { MultipartParserTransform } = require('./multipart_parser.mjs');

const boundary = 'my-boundary-from-content-type';
fs.createReadStream('multipart-body.bin')
  .pipe(new MultipartParserTransform(boundary))
  .on('data', (partBuffer) => {
    // partBuffer is a Buffer (Uint8Array) holding the body of one part (headers removed).
    console.log('got part length', partBuffer.length);
  });

Example usage (Deno / Web Streams):

// Suppose `req` is an Oak request and you got `boundary` from content-type
const transform = createMultipartTransformStream(boundary);
const partsStream = req.body?.value?.pipeThrough(transform); // req.body.value is ReadableStream<Uint8Array>
for await (const part of partsStream) {
  // part is a Uint8Array
  console.log('part size', part.length);
}

Example usage (async iterable):

const { iterateMultipartParts } = require('./multipart_parser.mjs');
async function handle(req) {
  const boundary = ...;
  const chunks = readableToAsyncIterable(req); // convert Node/Deno readable to async iterable of Uint8Array
  for await (const part of iterateMultipartParts(chunks, boundary)) {
    // part is Uint8Array
  }
}
*/
