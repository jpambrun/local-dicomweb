#!/usr/bin/env -S deno run --allow-read --allow-write --allow-net --allow-env --allow-ffi

import {connect, deferred, nuid} from "jsr:@nats-io/transport-deno";
// import * as nats_core from "jsr:@nats-io/nats-core";
import { jetstream, jetstreamManager, AckPolicy } from "jsr:@nats-io/jetstream";
import { Kvm } from "jsr:@nats-io/kv";
import { Objm, StorageType } from "jsr:@nats-io/obj";

const nc = await connect();

// pubsub
const sub = nc.subscribe("hello");
(async () => {
  for await (const m of sub) {
    console.log(`[${sub.getProcessed()}]: ${m.string()}`);
  }
  console.log("subscription closed");
})();

nc.publish("hello", "world");
nc.publish("hello", "again");


//kv
const kvm = new Kvm(nc);
const kv = await kvm.create("testing", { history: 5 });
await kv.put("hello.world", "world");
console.log((await kv.get("hello.world"))?.string())

// oject
const objm = new Objm(nc);
const os = await objm.create("testing", { storage: StorageType.File });

const list = await os.list();
list.forEach((i) => {
  console.log(`list: ${i.name}`);
});


const sizeInBytes = 65536;
const uint8Array = new Uint8Array(sizeInBytes);
crypto.getRandomValues(uint8Array);

const stream = new ReadableStream({
  start(controller) {
    controller.enqueue(uint8Array);
    controller.close();
  },
});


await os.put({
  name: `file${Math.round(Math.random()*10000)}`
}, stream)

for(const entry of await os.list()) {
  console.log(`object: ${entry.name} - ${entry.size} bytes`);
}


// jetsteam
//
const jsm = await jetstreamManager(nc);
const js = jetstream(nc);
await jsm.streams.add({ name: "a", subjects: ["a.*"] });
const consumerInfo = await jsm.consumers.add("a", {
    name: "a",
    durable_name: 'a',
    ack_policy: AckPolicy.Explicit,
  });


const consumer = await js.consumers.get("a", "a");
// console.log(consumer)

// await consumer.consume({
//   callback: (m) => {
//     console.log("here", m.subject, m.string());
//     m.ack();
//   },
// });
(async () => {
  while (true) {
    const m = await consumer.next();
    if (!m)  continue;
    console.log("here", m.subject, m.string());
    m.ack();
  }
})();

await js.publish("a.b", new TextEncoder().encode("hello jetstream"));
await js.publish("a.b", new TextEncoder().encode("hello jetstream1"));
await js.publish("a.b", new TextEncoder().encode("hello jetstream2"));
await js.publish("a.b", new TextEncoder().encode("hello jetstream3"));
