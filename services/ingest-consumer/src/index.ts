import { KafkaJS } from "@confluentinc/kafka-javascript";
import { createLogger, envNumber, kafkaConfig, s3Config } from "@aml/shared";
import { planLanding, type InboundMessage } from "./landing.js";
import { ObjectStore } from "./object-store.js";

/**
 * Kafka → MinIO raw landing zone ("bronze" layer).
 *
 * Delivery guarantee: AT-LEAST-ONCE.
 *   1. Buffer messages in memory.
 *   2. Every N messages or T ms, write them to object storage.
 *   3. Only after every object is written, commit the Kafka offsets.
 * A crash between 2 and 3 means some messages are re-read and re-written —
 * never lost. dbt staging deduplicates on transaction_id.
 */

const log = createLogger("ingest-consumer");

async function main() {
  const k = kafkaConfig();
  const store = new ObjectStore(s3Config(), log);
  const batchSize = envNumber("INGEST_BATCH_SIZE", 1000);
  const flushMs = envNumber("INGEST_FLUSH_MS", 10_000);

  const kafka = new KafkaJS.Kafka({ kafkaJS: { clientId: "ingest-consumer", brokers: k.brokers } });
  const consumer = kafka.consumer({
    kafkaJS: { groupId: "raw-landing-v1", fromBeginning: true, autoCommit: false },
  });

  let buffer: InboundMessage[] = [];
  let flushing: Promise<void> = Promise.resolve();
  const totals = { messages: 0, objects: 0, deadLetter: 0 };

  /** Serialized flush: only one runs at a time; callers await the same chain. */
  const flush = (): Promise<void> => {
    flushing = flushing.then(async () => {
      if (buffer.length === 0) return;
      const batch = buffer;
      buffer = [];

      const started = Date.now();
      const plan = planLanding(batch, new Date());
      // Upload all objects first; any failure throws and offsets stay uncommitted.
      await Promise.all(plan.objects.map((o) => store.put(o)));
      await consumer.commitOffsets(plan.commits);

      totals.messages += batch.length;
      totals.objects += plan.objects.length;
      totals.deadLetter += plan.stats.deadLetter;
      log.info("flushed batch", {
        messages: batch.length,
        objects: plan.objects.length,
        dead_letter: plan.stats.deadLetter,
        ms: Date.now() - started,
        totals,
      });
    });
    return flushing;
  };

  await consumer.connect();
  await consumer.subscribe({ topics: [k.topicTxns, k.topicReference] });
  log.info("subscribed", { topics: [k.topicTxns, k.topicReference], batch_size: batchSize, flush_ms: flushMs });

  const timer = setInterval(() => {
    flush().catch(fatal);
  }, flushMs);

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      buffer.push({ topic, partition, offset: message.offset, value: message.value });
      // Back-pressure: block consumption while a full batch is being written.
      if (buffer.length >= batchSize) await flush();
    },
  });

  let stopping = false;
  const shutdown = async (signal: string) => {
    if (stopping) return;
    stopping = true;
    log.info("shutting down", { signal });
    clearInterval(timer);
    try {
      await consumer.pause([{ topic: k.topicTxns }, { topic: k.topicReference }]);
      await flush();
    } finally {
      await consumer.disconnect();
      log.info("stopped", { totals });
      process.exit(0);
    }
  };
  process.once("SIGINT", () => void shutdown("SIGINT"));
  process.once("SIGTERM", () => void shutdown("SIGTERM"));
}

/** Crash on unrecoverable write errors: uncommitted offsets get re-delivered on restart. */
function fatal(err: unknown): never {
  log.error("fatal", { error: err instanceof Error ? err.stack : String(err) });
  process.exit(1);
}

main().catch(fatal);
