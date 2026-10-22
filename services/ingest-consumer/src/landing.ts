import { gzipSync } from "node:zlib";

/**
 * Pure functions that decide WHAT gets written WHERE in the raw landing zone.
 * Kept free of Kafka/S3 code so they're easy to unit test.
 *
 * Layout (Hive-style partitions, which DuckDB and Spark understand natively):
 *   raw/<entity>/ingest_date=YYYY-MM-DD/hour=HH/<topic>-p<partition>-<firstOffset>-<lastOffset>.jsonl.gz
 *   raw/_dead_letter/ingest_date=YYYY-MM-DD/<topic>-p<partition>-<firstOffset>-<lastOffset>.jsonl.gz
 *
 * Naming objects by Kafka offsets makes re-delivery after a crash overwrite the
 * same object instead of creating a duplicate file (when batch boundaries match).
 * Any remaining duplicates are removed in dbt staging by transaction_id.
 */

export const ENTITY_BY_EVENT_TYPE: Record<string, string> = {
  transaction: "transactions",
  customer: "customers",
  account: "accounts",
};

export interface InboundMessage {
  topic: string;
  partition: number;
  offset: string;
  value: Buffer | null;
}

export interface LandingObject {
  key: string;
  records: number;
  body: Buffer;
}

export interface LandingPlan {
  objects: LandingObject[];
  /** Offsets to commit once all objects are safely written (last offset + 1). */
  commits: { topic: string; partition: number; offset: string }[];
  stats: { valid: number; deadLetter: number };
}

export function planLanding(messages: InboundMessage[], ingestedAt: Date, prefix = "raw"): LandingPlan {
  const date = ingestedAt.toISOString().slice(0, 10);
  const hour = ingestedAt.toISOString().slice(11, 13);

  // group key -> lines, plus the offset range per topic-partition
  const groups = new Map<string, { entityPath: string; topic: string; partition: number; first: bigint; last: bigint; lines: string[] }>();
  const lastOffsets = new Map<string, { topic: string; partition: number; last: bigint }>();
  let valid = 0;
  let deadLetter = 0;

  for (const msg of messages) {
    const offset = BigInt(msg.offset);
    const tpKey = `${msg.topic}:${msg.partition}`;
    const prev = lastOffsets.get(tpKey);
    if (!prev || offset > prev.last) lastOffsets.set(tpKey, { topic: msg.topic, partition: msg.partition, last: offset });

    const kafkaMeta = { topic: msg.topic, partition: msg.partition, offset: msg.offset, ingested_at: ingestedAt.toISOString() };
    const raw = msg.value?.toString("utf8") ?? "";
    let entityPath: string;
    let line: string;

    try {
      const parsed = JSON.parse(raw) as Record<string, unknown>;
      const entity = ENTITY_BY_EVENT_TYPE[String(parsed.event_type)];
      if (!entity) throw new Error(`unknown event_type: ${String(parsed.event_type)}`);
      entityPath = `${prefix}/${entity}/ingest_date=${date}/hour=${hour}`;
      line = JSON.stringify({ ...parsed, _kafka: kafkaMeta });
      valid++;
    } catch (err) {
      entityPath = `${prefix}/_dead_letter/ingest_date=${date}`;
      line = JSON.stringify({ _kafka: kafkaMeta, error: (err as Error).message, raw });
      deadLetter++;
    }

    const groupKey = `${entityPath}|${tpKey}`;
    const group = groups.get(groupKey);
    if (group) {
      group.lines.push(line);
      if (offset < group.first) group.first = offset;
      if (offset > group.last) group.last = offset;
    } else {
      groups.set(groupKey, { entityPath, topic: msg.topic, partition: msg.partition, first: offset, last: offset, lines: [line] });
    }
  }

  const objects = [...groups.values()].map((g) => ({
    key: `${g.entityPath}/${g.topic}-p${g.partition}-${g.first}-${g.last}.jsonl.gz`,
    records: g.lines.length,
    body: gzipSync(g.lines.join("\n") + "\n"),
  }));

  const commits = [...lastOffsets.values()].map((o) => ({
    topic: o.topic,
    partition: o.partition,
    offset: (o.last + 1n).toString(),
  }));

  return { objects, commits, stats: { valid, deadLetter } };
}
