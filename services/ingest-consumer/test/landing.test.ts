import assert from "node:assert/strict";
import { test } from "node:test";
import { gunzipSync } from "node:zlib";
import { planLanding, type InboundMessage } from "../src/landing.js";

const at = new Date("2026-10-01T14:05:00Z");
const msg = (topic: string, partition: number, offset: number, value: unknown): InboundMessage => ({
  topic,
  partition,
  offset: String(offset),
  value: Buffer.from(typeof value === "string" ? value : JSON.stringify(value)),
});

test("routes events to entity folders with Hive-style partitions", () => {
  const plan = planLanding(
    [
      msg("raw_txns", 0, 10, { event_type: "transaction", transaction_id: "T1" }),
      msg("raw_txns", 0, 11, { event_type: "transaction", transaction_id: "T2" }),
      msg("raw_reference", 0, 3, { event_type: "customer", customer_id: "C1" }),
    ],
    at,
  );
  const keys = plan.objects.map((o) => o.key).sort();
  assert.deepEqual(keys, [
    "raw/customers/ingest_date=2026-10-01/hour=14/raw_reference-p0-3-3.jsonl.gz",
    "raw/transactions/ingest_date=2026-10-01/hour=14/raw_txns-p0-10-11.jsonl.gz",
  ]);
});

test("commits last offset + 1 per topic-partition", () => {
  const plan = planLanding(
    [msg("raw_txns", 1, 5, { event_type: "transaction" }), msg("raw_txns", 1, 9, { event_type: "transaction" }), msg("raw_txns", 2, 0, { event_type: "transaction" })],
    at,
  );
  const commits = plan.commits.sort((a, b) => a.partition - b.partition);
  assert.deepEqual(commits, [
    { topic: "raw_txns", partition: 1, offset: "10" },
    { topic: "raw_txns", partition: 2, offset: "1" },
  ]);
});

test("adds Kafka lineage metadata to every record", () => {
  const plan = planLanding([msg("raw_txns", 0, 42, { event_type: "transaction", transaction_id: "T1" })], at);
  const line = JSON.parse(gunzipSync(plan.objects[0]!.body).toString().trim());
  assert.equal(line.transaction_id, "T1");
  assert.deepEqual(line._kafka, { topic: "raw_txns", partition: 0, offset: "42", ingested_at: at.toISOString() });
});

test("sends malformed and unknown messages to the dead-letter prefix, still committing them", () => {
  const plan = planLanding([msg("raw_txns", 0, 1, "{not json"), msg("raw_txns", 0, 2, { event_type: "mystery" })], at);
  assert.equal(plan.stats.deadLetter, 2);
  assert.equal(plan.stats.valid, 0);
  assert.ok(plan.objects[0]!.key.startsWith("raw/_dead_letter/ingest_date=2026-10-01/"));
  assert.deepEqual(plan.commits, [{ topic: "raw_txns", partition: 0, offset: "3" }]);
});
