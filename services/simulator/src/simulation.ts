import type { Logger, TransactionEvent, Typology } from "@aml/shared";
import { EventQueue } from "./event-queue.js";
import { SCENARIO_TYPOLOGIES, TransactionGenerator } from "./generator.js";
import type { Population } from "./population.js";
import type { Rng } from "./rng.js";
import type { Sink } from "./sinks.js";

const HOUR = 3_600_000;
const DAY = 24 * HOUR;

/** Realistic time spans for each pattern during backfill. */
const BACKFILL_SPAN_MS: Record<Exclude<Typology, "NONE">, [number, number]> = {
  STRUCTURING: [2 * DAY, 7 * DAY],
  RAPID_MOVEMENT: [6 * HOUR, 48 * HOUR],
  FAN_IN: [1 * DAY, 4 * DAY],
  FAN_OUT: [1 * DAY, 3 * DAY],
};

export interface SimulationStats {
  transactions: number;
  suspicious: number;
  scenarios: Record<string, number>;
}

export function newStats(): SimulationStats {
  return { transactions: 0, suspicious: 0, scenarios: {} };
}

function record(stats: SimulationStats, events: TransactionEvent[]): void {
  stats.transactions += events.length;
  for (const e of events) if (e._label.is_suspicious) stats.suspicious++;
}

/**
 * Historical backfill: emit `days` of history ending at `now`, as fast as
 * possible. Gives dbt enough data for 30-day rolling metrics on day one.
 */
export async function runBackfill(opts: {
  rng: Rng;
  population: Population;
  generator: TransactionGenerator;
  sink: Sink;
  log: Logger;
  now: Date;
  days: number;
  txnsPerCustomerPerDay: number;
  stats: SimulationStats;
}): Promise<void> {
  const { rng, population, generator, sink, log, now, days, stats } = opts;
  const windowStart = now.getTime() - days * DAY;
  const queue = new EventQueue();

  // Plan 1–2 scenarios per suspicious customer, finishing before `now`.
  for (const customer of population.customers.filter((c) => c.suspicious)) {
    const count = rng.int(1, 2);
    for (let i = 0; i < count; i++) {
      const typology = rng.pick(SCENARIO_TYPOLOGIES);
      const [minSpan, maxSpan] = BACKFILL_SPAN_MS[typology];
      const span = rng.float(minSpan, maxSpan);
      const latestStart = Math.max(windowStart, now.getTime() - span - HOUR);
      const start = new Date(rng.float(windowStart, latestStart));
      queue.pushAll(generator.planScenario(customer, typology, start, span, () => now));
      stats.scenarios[typology] = (stats.scenarios[typology] ?? 0) + 1;
    }
  }

  const perDay = population.customers.length * opts.txnsPerCustomerPerDay;
  const meanGapMs = DAY / perDay;
  let clock = windowStart;
  let buffer: TransactionEvent[] = [];
  let lastLogged = 0;

  while (clock < now.getTime()) {
    // Exponential inter-arrival times => a Poisson stream, like real traffic.
    clock += -Math.log(1 - rng.next()) * meanGapMs;
    if (clock >= now.getTime()) break;
    buffer.push(...queue.popDue(clock));
    buffer.push(generator.normal(new Date(clock), now));

    if (buffer.length >= 1000) {
      await sink.sendTransactions(buffer);
      record(stats, buffer);
      buffer = [];
      if (stats.transactions - lastLogged >= 25_000) {
        lastLogged = stats.transactions;
        log.info("backfill progress", {
          sent: stats.transactions,
          through: new Date(clock).toISOString().slice(0, 10),
        });
      }
    }
  }
  buffer.push(...queue.popDue(now.getTime()));
  await sink.sendTransactions(buffer);
  record(stats, buffer);
}

/**
 * Live mode: emit `tps` transactions per second in real time, and start a new
 * (time-compressed) laundering scenario every `scenarioEverySec` seconds.
 */
export async function runLive(opts: {
  generator: TransactionGenerator;
  sink: Sink;
  log: Logger;
  rng: Rng;
  tps: number;
  scenarioEverySec: number;
  scenarioSpanMin: number;
  durationSec: number; // 0 = forever
  stats: SimulationStats;
  signal: AbortSignal;
}): Promise<void> {
  const { generator, sink, log, rng, tps, stats, signal } = opts;
  const queue = new EventQueue();
  const tickMs = 100;
  const startedAt = Date.now();
  let owed = 0;
  let lastScenario = 0;
  let lastReport = Date.now();
  let reportCount = 0;

  while (!signal.aborted) {
    const tickStart = Date.now();
    if (opts.durationSec > 0 && tickStart - startedAt >= opts.durationSec * 1000) break;

    if (
      generator.hasSuspiciousCustomers &&
      opts.scenarioEverySec > 0 &&
      tickStart - lastScenario >= opts.scenarioEverySec * 1000
    ) {
      lastScenario = tickStart;
      const typology = rng.pick(SCENARIO_TYPOLOGIES);
      const customer = generator.pickSuspiciousCustomer();
      const planned = generator.planScenario(customer, typology, new Date(tickStart), opts.scenarioSpanMin * 60_000);
      queue.pushAll(planned);
      stats.scenarios[typology] = (stats.scenarios[typology] ?? 0) + 1;
      log.info("scenario started", {
        typology,
        customer_id: customer.record.customer_id,
        scenario_id: planned[0]?._label.scenario_id,
        planned_txns: planned.length,
      });
    }

    owed += (tps * tickMs) / 1000;
    const batch: TransactionEvent[] = queue.popDue(tickStart);
    while (owed >= 1) {
      batch.push(generator.normal(new Date(), new Date()));
      owed -= 1;
    }
    await sink.sendTransactions(batch);
    record(stats, batch);
    reportCount += batch.length;

    if (Date.now() - lastReport >= 10_000) {
      log.info("live throughput", {
        tps: Math.round((reportCount * 1000) / (Date.now() - lastReport)),
        total: stats.transactions,
        pending_scenario_txns: queue.size,
      });
      lastReport = Date.now();
      reportCount = 0;
    }

    const elapsed = Date.now() - tickStart;
    if (elapsed < tickMs) await new Promise((r) => setTimeout(r, tickMs - elapsed));
  }
}
