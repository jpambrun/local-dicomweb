import fs from "node:fs";
import fsp from "node:fs/promises";
import path from "node:path";
import PQueue from "p-queue";

import { db, processDicomFile } from "./lib";

const queue = new PQueue({ concurrency: 20 });

console.log("Starting indexing...");
const t1 = Date.now();
let count = 0;
for await (const filePath of fsp.glob("**/*")) {
	const stat = await fs.promises.stat(filePath);
	if (stat.isDirectory()) continue;
	const extension = path.extname(filePath).toLowerCase().replace(".", "");
	const extensionIsNumber = !Number.isNaN(Number(extension));

	if (!["dcm", ""].includes(extension) && !extensionIsNumber) {
		console.log(`Skipping non DICOM file: ${path.basename(filePath)}`);
		continue;
	}

	if (path.basename(filePath) === "DICOMDIR") continue;
	const relativePath = filePath;

	count++;
	queue.add(() => processDicomFile(relativePath));
	await queue.onSizeLessThan(100);
}

await queue.onIdle();
console.log("Indexing time: ", (Date.now() - t1) / 1000, "s");
console.log("Total DICOM files processed: ", count);
