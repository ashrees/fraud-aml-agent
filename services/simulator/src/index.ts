import { resolve } from "node:path";
import { parseArgs } from "node:util";
import { createLogger, envNumber, kafkaConfig, type ReferenceEvent } from "@aml/shared";
import { TransactionGenerator } from "./generator.js";
import { buildPopulation } from "./population.js";
import { Rng } from "./rng.js";
import { newStats, runBackfill, runLive } from "./simulation.js";
import { FileSink, KafkaSink, type Sink } from "./sinks.js";

const log = createLogger("simulator");

const { values: args } = parseArgs({
  options: {
    "dry-run": { type: "boolean", default: false },
    out: { type: "string", default: "data/dry-run" },
    "backfill-days": { type: "string" },
    "no-live": { type: "boolean", default: false },
    duration: { type: "string" },
  },
});

async function main() {
  const seed = envNumber("SIM_SEED", 42);
  const customers = envNumber("SIM_CUSTOMERS", 500);
  const suspiciousRate = envNumber("SIM_SUSPICIOUS_RATE", 0.02);
  const backfillDays = Number(args["backfill-days"] ?? envNumber("SIM_BACKFILL_DAYS", 30));
  const durationSec = Number(args.duration ?? envNumber("SIM_DURATION_SEC", 0));
  const live = !args["no-live"] && !(args["dry-run"] && args.duration === undefined);

  const now = new Date();
  const rng = new Rng(seed);
  const population = buildPopulation(rng, customers, suspiciousRate, now);
  const generator = new TransactionGenerator(rng, population);
  const stats = newStats();

  const k = kafkaConfig();
  const sink: Sink = args["dry-run"]
    ? new FileSink(resolve(process.env.INIT_CWD ?? process.cwd(), args.out!))
    : new KafkaSink(k.brokers, { txns: k.topicTxns, reference: k.topicReference }, log);

  log.info("starting", {
    mode: args["dry-run"] ? `dry-run → ${args.out}` : `kafka → ${k.brokers.join(",")}`,
    seed,
    customers,
    suspicious_customers: population.customers.filter((c) => c.suspicious).length,
    backfill_days: backfillDays,
    live,
  });

  await sink.connect();

  const controller = new AbortController();
  for (const sig of ["SIGINT", "SIGTERM"] as const) {
    process.once(sig, () => {
      log.info("shutting down", { signal: sig });
      controller.abort();
    });
  }

  // 1. Reference data first, so dims exist before facts arrive.
  const reference: ReferenceEvent[] = population.customers.flatMap((c) => [c.record, ...c.accounts]);
  await sink.sendReference(reference);
  log.info("reference data published", { records: reference.length });

  // 2. Historical backfill.
  if (backfillDays > 0) {
    await runBackfill({
      rng,
      population,
      generator,
      sink,
      log,
      now,
      days: backfillDays,
      txnsPerCustomerPerDay: envNumber("SIM_TXNS_PER_CUSTOMER_PER_DAY", 1.5),
      stats,
    });
    log.info("backfill complete", { ...stats });
  }

  // 3. Live stream.
  if (live && !controller.signal.aborted) {
    await runLive({
      generator,
      sink,
      log,
      rng,
      tps: envNumber("SIM_TPS", 50),
      scenarioEverySec: envNumber("SIM_LIVE_SCENARIO_EVERY_SEC", 60),
      scenarioSpanMin: envNumber("SIM_LIVE_SCENARIO_SPAN_MIN", 10),
      durationSec,
      stats,
      signal: controller.signal,
    });
  }

  await sink.close();
  log.info("done", { ...stats });
}

main().catch((err) => {
  log.error("fatal", { error: err instanceof Error ? err.stack : String(err) });
  process.exit(1);
});
