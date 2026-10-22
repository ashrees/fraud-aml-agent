import { mkdirSync, createWriteStream, type WriteStream } from "node:fs";
import { join } from "node:path";
import { KafkaJS } from "@confluentinc/kafka-javascript";
import type { Logger, ReferenceEvent, TransactionEvent } from "@aml/shared";

export interface Sink {
  connect(): Promise<void>;
  sendReference(events: ReferenceEvent[]): Promise<void>;
  sendTransactions(events: TransactionEvent[]): Promise<void>;
  close(): Promise<void>;
}

/**
 * Publishes to Kafka/Redpanda.
 * - Transactions are keyed by account_id, so all events for an account land on
 *   the same partition and stay in order (needed for velocity checks later).
 * - Idempotent producer + acks=all: no duplicates from producer retries.
 */
export class KafkaSink implements Sink {
  private readonly producer: KafkaJS.Producer;

  constructor(
    brokers: string[],
    private readonly topics: { txns: string; reference: string },
    private readonly log: Logger,
  ) {
    const kafka = new KafkaJS.Kafka({ kafkaJS: { clientId: "txn-simulator", brokers } });
    this.producer = kafka.producer({
      kafkaJS: { idempotent: true, acks: -1, compression: KafkaJS.CompressionTypes.LZ4 },
    });
  }

  async connect(): Promise<void> {
    await this.producer.connect();
    this.log.info("connected to kafka");
  }

  async sendReference(events: ReferenceEvent[]): Promise<void> {
    for (let i = 0; i < events.length; i += 500) {
      await this.producer.send({
        topic: this.topics.reference,
        messages: events.slice(i, i + 500).map((e) => ({
          key: e.event_type === "customer" ? e.customer_id : e.account_id,
          value: JSON.stringify(e),
          headers: { event_type: e.event_type, schema_version: String(e.schema_version) },
        })),
      });
    }
  }

  async sendTransactions(events: TransactionEvent[]): Promise<void> {
    if (events.length === 0) return;
    await this.producer.send({
      topic: this.topics.txns,
      messages: events.map((e) => ({
        key: e.account_id,
        value: JSON.stringify(e),
        headers: { event_type: e.event_type, schema_version: String(e.schema_version) },
      })),
    });
  }

  async close(): Promise<void> {
    await this.producer.disconnect();
  }
}

/**
 * Writes NDJSON files instead of publishing. Lets you develop and test the
 * generator (or load data straight into DuckDB) without Docker running.
 */
export class FileSink implements Sink {
  private streams: Record<string, WriteStream> = {};
  public counts = { customers: 0, accounts: 0, transactions: 0 };

  constructor(private readonly dir: string) {}

  async connect(): Promise<void> {
    mkdirSync(this.dir, { recursive: true });
    for (const name of ["customers", "accounts", "transactions"]) {
      this.streams[name] = createWriteStream(join(this.dir, `${name}.jsonl`));
    }
  }

  async sendReference(events: ReferenceEvent[]): Promise<void> {
    for (const e of events) {
      const name = e.event_type === "customer" ? "customers" : "accounts";
      this.counts[name]++;
      this.streams[name]!.write(JSON.stringify(e) + "\n");
    }
  }

  async sendTransactions(events: TransactionEvent[]): Promise<void> {
    for (const e of events) {
      this.counts.transactions++;
      if (!this.streams.transactions!.write(JSON.stringify(e) + "\n")) {
        await new Promise<void>((resolve) => this.streams.transactions!.once("drain", () => resolve()));
      }
    }
  }

  async close(): Promise<void> {
    await Promise.all(Object.values(this.streams).map((s) => new Promise<void>((resolve) => s.end(() => resolve()))));
  }
}
