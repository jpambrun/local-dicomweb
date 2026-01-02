#!/usr/bin/env -S deno run --allow-read --allow-write --allow-net --allow-env --allow-ffi
import { mkdir } from "node:fs/promises";
import { open } from "npm:lmdb";

const db = open({ path: "_dicomweb", useVersions: true });

import { DuckDBInstance } from "npm:@duckdb/node-api";

const instance = await DuckDBInstance.create(":memory:");
const connection = await instance.connect();
await connection.run("INSTALL vortex;LOAD vortex;");

await mkdir("_vortex", { recursive: true });
await mkdir("_parquet", { recursive: true });

function isPrivateTag(tagString) {
	const groupHex = tagString.substring(0, 4);
	const group = parseInt(groupHex, 16);
	return group % 2 === 1;
}

const baseTags = new Set([
  "00200032",
  "00080032",
]);

for (const studyKey of db.getKeys({
	start: "study:",
	end: "study:" + "\ufffd",
})) {
	const studyInstanceUID = studyKey.split(":")[1];
	const instances = [
		...db
			.getRange({
				start: `instance:${studyInstanceUID}:`,
				end: `instance:${studyInstanceUID}:\ufffd`,
			})
			.map(({ value }) => value),
	];

	// maybe sort by series uid?

	const topLevelTagsSet = new Set();
	// const tagVrMap = new Map();
	for (const instance of instances) {
		for (const tag of Object.keys(instance)) {
			topLevelTagsSet.add(tag);
			// tagVrMap.set(tag, instance[tag].vr);
		}
	}

	const privateTags = Array.from(topLevelTagsSet).filter((tag) =>
		isPrivateTag(tag),
	);
	const publicTags = Array.from(baseTags.union(topLevelTagsSet)).filter(
		(tag) => !isPrivateTag(tag),
	);
	const publicTagList = publicTags.map((t) => `"${t}" varchar`).join(",");
	await connection.run(
		`create or replace table temp_instance_table(${publicTagList}, privateTags varchar)`,
	);
	const appender = await connection.createAppender('temp_instance_table');
	for (const instance of instances) {
	  for (const publicTag of publicTags){
			appender.appendVarchar(instance[publicTag] ? JSON.stringify(instance[publicTag]) : '{}');
		}

		const privateTagObject = {};
		for (const privateTag of privateTags) {
      if (instance[privateTag]) {
        privateTagObject[privateTag] = instance[privateTag];
      }
    }
		appender.appendVarchar(JSON.stringify(privateTagObject));
		appender.endRow();
	}
	appender.closeSync();
	// await connection.run(`COPY (FROM temp_instance_table) TO '_vortex/${studyInstanceUID}.vortex' (FORMAT vortex);`);
	await connection.run(`COPY (FROM temp_instance_table) TO '_parquet/${studyInstanceUID}.parquet' (FORMAT parquet);`);

}
