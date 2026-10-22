import assert from "node:assert/strict";
import { test } from "node:test";
import { TransactionGenerator } from "../src/generator.js";
import { buildPopulation } from "../src/population.js";
import { Rng } from "../src/rng.js";

const NOW = new Date("2026-10-01T12:00:00Z");

function setup(seed = 7) {
  const rng = new Rng(seed);
  const population = buildPopulation(rng, 200, 0.1, NOW);
  return { rng, population, gen: new TransactionGenerator(rng, population) };
}

test("same seed produces identical data (reproducible evals)", () => {
  const a = setup(123);
  const b = setup(123);
  const txA = Array.from({ length: 50 }, () => a.gen.normal(NOW, NOW));
  const txB = Array.from({ length: 50 }, () => b.gen.normal(NOW, NOW));
  assert.deepEqual(txA, txB);
});

test("normal transactions are labelled clean and well-formed", () => {
  const { gen } = setup();
  for (let i = 0; i < 500; i++) {
    const t = gen.normal(NOW, NOW);
    assert.equal(t._label.is_suspicious, false);
    assert.ok(t.amount > 0);
    assert.match(t.transaction_id, /^TXN-[0-9A-F]{16}$/);
    assert.equal(Math.round(t.amount * 100) / 100, t.amount, "amounts must have 2-decimal precision");
  }
});

test("structuring deposits all stay under the $10,000 threshold", () => {
  const { gen } = setup();
  const customer = gen.pickSuspiciousCustomer();
  const events = gen.planScenario(customer, "STRUCTURING", NOW, 3 * 86_400_000);
  assert.ok(events.length >= 5);
  for (const e of events) {
    assert.equal(e.txn_type, "CASH_DEPOSIT");
    assert.ok(e.amount < 10_000 && e.amount >= 8_000, `amount ${e.amount} out of range`);
    assert.equal(e._label.typology, "STRUCTURING");
  }
});

test("rapid movement sends out 90–98% of what came in, after it arrived", () => {
  const { gen } = setup();
  const events = gen.planScenario(gen.pickSuspiciousCustomer(), "RAPID_MOVEMENT", NOW, 86_400_000);
  const [first, ...rest] = events;
  assert.equal(first!.txn_type, "WIRE_IN");
  const out = rest.reduce((s, e) => s + e.amount, 0);
  const ratio = out / first!.amount;
  assert.ok(ratio >= 0.89 && ratio <= 0.99, `ratio ${ratio}`);
  assert.ok(rest.every((e) => e.direction === "DEBIT" && e.event_time >= first!.event_time));
});

test("scenario events share one scenario_id and are time-ordered", () => {
  const { gen } = setup();
  const events = gen.planScenario(gen.pickSuspiciousCustomer(), "FAN_IN", NOW, 2 * 86_400_000);
  assert.equal(new Set(events.map((e) => e._label.scenario_id)).size, 1);
  const times = events.map((e) => e.event_time);
  assert.deepEqual(times, [...times].sort());
});
