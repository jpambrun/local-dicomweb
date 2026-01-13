import { connect } from "nats";
import { KVM } from "nats";
import { ObjectStoreManager, StorageType } from "nats";

const nc = await connect();

const sub = nc.subscribe("hello");
(async () => {
	for await (const m of sub) {
		console.log(`[${sub.getProcessed()}]: ${m.string()}`);
	}
	console.log("subscription closed");
})();

nc.publish("hello", "world");
nc.publish("hello", "again");

const kvm = new KVM(nc);
const kv = await kvm.create("testing", { history: 5 });
await kv.put("hello.world", "world");
console.log((await kv.get("hello.world"))?.string());

const osm = new ObjectStoreManager(nc);
const os = await osm.create("testing", { storage: StorageType.File });

const list = await os.list();
for (const i of list) {
	console.log(`list: ${i.name}`);
}

const sizeInBytes = 65536;
const uint8Array = new Uint8Array(sizeInBytes);
crypto.getRandomValues(uint8Array);

const stream = new ReadableStream({
	start(controller) {
		controller.enqueue(uint8Array);
		controller.close();
	},
});

await os.put(
	{
		name: `file${Math.round(Math.random() * 10000)}`,
	},
	stream,
);

for (const entry of await os.list()) {
	console.log(`object: ${entry.name} - ${entry.size} bytes`);
}

const jsm = await nc.jetstreamManager();
const js = nc.jetstream();
await jsm.streams.add({ name: "a", subjects: ["a.*"] });
const consumerInfo = await jsm.consumers.add("a", {
	name: "a",
	durable_name: "a",
	ack_policy: "explicit",
});

const consumer = await js.consumers.get("a", "a");

(async () => {
	while (true) {
		const m = await consumer.next();
		if (!m) continue;
		console.log("here", m.subject, m.string());
		m.ack();
	}
})();

await js.publish("a.b", new TextEncoder().encode("hello jetstream"));
await js.publish("a.b", new TextEncoder().encode("hello jetstream1"));
await js.publish("a.b", new TextEncoder().encode("hello jetstream2"));
await js.publish("a.b", new TextEncoder().encode("hello jetstream3"));
