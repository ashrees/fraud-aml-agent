# Fraud / AML Investigation Platform

An event-driven anti-money-laundering (AML) platform: a streaming core-banking
ingestion pipeline, dbt dimensional models, a metrics layer, and a multi-agent
investigator that drafts Suspicious Activity Report (SAR) narratives for a
**human reviewer to sign off**.

> **Design principle:** numbers come from code and the warehouse; language comes
> from the model. The LLM never invents an amount, a count or a risk score.

## Architecture

```
PHASE 1 — DATA PLATFORM
[TS Simulator] ──► Redpanda (Kafka API) ──► [TS Ingest Consumer] ──► MinIO (S3) raw zone
   raw_txns / raw_reference                    at-least-once           jsonl.gz, Hive partitions
                                                                              │
                                              dbt + DuckDB  ◄─────────────────┘
                                staging → intermediate → marts (fact_transactions,
                                dim_customers, dim_accounts, AML aggregates)
                                                     │
                                         Metrics API (semantic layer)

PHASE 2 — AGENTS (LangGraph.js)
alert ─► SQL/Data agent ─► Risk Scoring (deterministic) ─► Narrative & Audit agent
      ─► Citation Validator ─► Human-in-the-loop gateway ─► signed-off case + audit log

LATER — C++ real-time detection engine on the hot path (benchmarked vs. TS)
```

| Layer | Tech | Why |
|---|---|---|
| Services & agents | TypeScript (Node 22) | Typed, testable, one language across services |
| Broker | Redpanda | Kafka-compatible API, laptop-friendly |
| Raw zone | MinIO | S3-compatible; same code runs on AWS S3 |
| Warehouse | DuckDB (Snowflake as prod target) | Free, fast, reads straight from S3 |
| Modeling | dbt Core | Tests, docs, lineage |
| Ops DB | Postgres | Alerts, cases, HITL decisions, append-only audit log |
| Hot-path detection | C++ (planned) | Throughput/latency where it matters |

## Status

- [x] **Week 1** — Infra, transaction simulator, Kafka → MinIO ingestion
- [ ] Week 2 — DuckDB reads raw zone; dbt staging
- [ ] Week 3 — dbt core marts + tests
- [ ] Week 4 — AML marts + Metrics API
- [ ] Week 5 — SQL/Data agent with AST guardrails
- [ ] Week 6 — Deterministic risk scoring + alerts
- [ ] Week 7 — Narrative agent + citation validator
- [ ] Week 8 — HITL gateway + reviewer dashboard
- [ ] Week 9 — Evals, CI, demo video
- [ ] Stretch — C++ detection engine + benchmarks

## Quick start

Requirements: Node 22+, Docker Desktop (or Docker Engine with Compose).

```bash
cp .env.example .env
npm install
npm run infra:up          # Redpanda, MinIO, Postgres (+ topics, bucket, schema)

# terminal 1 — land raw events in MinIO
npm run ingest

# terminal 2 — publish reference data, 30 days of history, then a live stream
npm run sim
```

Then look around:

| UI | URL | Login |
|---|---|---|
| Redpanda Console (topics, messages) | http://localhost:8080 | — |
| MinIO Console (raw zone files) | http://localhost:9001 | minioadmin / minioadmin |

**No Docker?** Generate the same data to local files:

```bash
npm run sim:dry                  # 30 days → data/dry-run/*.jsonl
npm run sim:dry -- --backfill-days 7
```

## The simulator

Generates a reproducible (seeded) synthetic bank: customers across Canadian
cities, chequing/savings/business accounts, and realistic everyday activity per
segment (retail, small business, student, senior).

A small share of customers run **laundering typologies**, recorded as ground
truth in each event's `_label` field:

| Typology | Pattern |
|---|---|
| `STRUCTURING` | 5–12 cash deposits just under the $10,000 reporting threshold, across branches |
| `RAPID_MOVEMENT` | Large foreign wire in, 90–98% wired/transferred out shortly after |
| `FAN_IN` | 10–25 transfers from unrelated senders, then one consolidated outbound wire |
| `FAN_OUT` | One large credit sprayed to 10–20 recipients |

Legit cash-heavy small businesses occasionally deposit just under $10,000 too —
these are deliberate **false-positive decoys**, so detection can't just threshold
on amount.

`_label` is for **evaluation only**. dbt excludes it from the marts the agents
can query, so the system can't cheat.

Modes: **backfill** emits N days of history as fast as possible (so 30-day
rolling metrics work on day one); **live** emits `SIM_TPS` transactions per
second and starts a time-compressed scenario every `SIM_LIVE_SCENARIO_EVERY_SEC`.

## Ingestion guarantees

The ingest consumer gives **at-least-once** delivery:

1. Buffer messages (flush at `INGEST_BATCH_SIZE` messages or `INGEST_FLUSH_MS`).
2. Write gzip NDJSON objects to MinIO.
3. Commit Kafka offsets **only after** every object is written.

A crash between steps 2 and 3 re-delivers messages rather than losing them.
Objects are named by offset range so a replay usually overwrites the same file,
and dbt staging deduplicates on `transaction_id` for the rest. Malformed
messages go to a dead-letter prefix instead of crashing the pipeline.

Raw layout (Hive-style partitions DuckDB understands natively):

```
s3://aml-raw/raw/transactions/ingest_date=2026-10-01/hour=14/raw_txns-p0-1200-2199.jsonl.gz
s3://aml-raw/raw/customers/ingest_date=2026-10-01/hour=14/raw_reference-p0-0-1325.jsonl.gz
s3://aml-raw/raw/_dead_letter/ingest_date=2026-10-01/...
```

Every record carries `_kafka { topic, partition, offset, ingested_at }`, so any
SAR citation can be traced: report → `transaction_id` → fact row → raw object →
Kafka offset.

## Project layout

```
packages/shared/          event contracts (TransactionEvent, CustomerRecord…), config, logger
services/simulator/       seeded generator, typology planner, Kafka + file sinks
services/ingest-consumer/ Kafka → MinIO landing with offset-safe commits
db/postgres/init.sql      alerts, cases, review_decisions, append-only audit_log
docker-compose.yml        Redpanda, Console, MinIO, Postgres + init jobs
```

## Development

```bash
npm run build   # type-check all packages
npm test        # unit tests (generator invariants, landing/commit logic)
```

## Configuration

See `.env.example`. Key knobs: `SIM_CUSTOMERS`, `SIM_TPS`, `SIM_SUSPICIOUS_RATE`,
`SIM_SEED`, `SIM_BACKFILL_DAYS`, `INGEST_BATCH_SIZE`, `INGEST_FLUSH_MS`.

---

*All data is synthetic. No real customers or institutions.*
