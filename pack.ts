import { mkdir } from "node:fs/promises";
import { DuckDBInstance } from "@duckdb/node-api";

import { db } from "./lib";

const instance = await DuckDBInstance.create(":memory:");
const connection = await instance.connect();
await connection.run("INSTALL vortex;LOAD vortex;");

await mkdir("_vortex", { recursive: true });
await mkdir("_parquet", { recursive: true });

function isPrivateTag(tagString: string): boolean {
	const groupHex = tagString.substring(0, 4);
	const group = Number.parseInt(groupHex, 16);
	return group % 2 === 1;
}

const baseTags = new Set(["00200032", "00080032"]);

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

	const topLevelTagsSet = new Set<string>();
	for (const instance of instances) {
		for (const tag of Object.keys(instance)) {
			topLevelTagsSet.add(tag);
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
	const appender = await connection.createAppender("temp_instance_table");
	for (const instance of instances) {
		for (const publicTag of publicTags) {
			appender.appendVarchar(
				instance[publicTag] ? JSON.stringify(instance[publicTag]) : "{}",
			);
		}

		const privateTagObject: Record<string, any> = {};
		for (const privateTag of privateTags) {
			if (instance[privateTag]) {
				privateTagObject[privateTag] = instance[privateTag];
			}
		}
		appender.appendVarchar(JSON.stringify(privateTagObject));
		appender.endRow();
	}
	appender.closeSync();
	await connection.run(
		`COPY (FROM temp_instance_table) TO '_parquet/${studyInstanceUID}.parquet' (FORMAT parquet);`,
	);
}
