import {
  SCHEMA_VERSION,
  type Channel,
  type Direction,
  type TransactionEvent,
  type TxnType,
  type Typology,
} from "@aml/shared";
import { ALL_BRANCHES, type Population, type SimCustomer } from "./population.js";
import { round2, type Rng } from "./rng.js";

/**
 * Canada's large cash transaction reporting threshold (FINTRAC LCTR) is $10,000.
 * Structuring = deliberately staying under it. Used only to SHAPE simulated
 * behaviour; detection thresholds will live in dbt / the risk engine.
 */
const REPORTING_THRESHOLD = 10_000;

/** Illustrative only — in a real system, load this from current FATF/regulator lists. */
const ELEVATED_RISK_COUNTRIES = ["PA", "KY", "AE", "CY", "MT", "BS"] as const;
const COMMON_COUNTRIES = ["CA", "CA", "CA", "US", "US", "GB", "IN", "FR", "CN", "PH"] as const;

const MERCHANTS = [
  "Loblaws", "Tim Hortons", "Shoppers Drug Mart", "Canadian Tire", "Metro", "Esso", "Amazon.ca",
  "Cineplex", "Best Buy", "IKEA", "Uber", "TTC Presto", "Sobeys", "Walmart", "Indigo",
];
const EMPLOYERS = ["Maple Tech Inc.", "Ontario Health", "Northwind Logistics", "City of Toronto", "Lakeshore Retail Ltd."];
const BILLERS = ["Toronto Hydro", "Rogers", "Bell Canada", "Enbridge Gas", "Intact Insurance", "Visa Payment"];
const SHELL_COMPANY_NAMES = [
  "Global Horizon Trading Ltd.", "Blue Harbour Holdings", "Apex Consulting FZE", "Silverline Imports SA",
  "Northstar Ventures Ltd.", "Crescent Logistics Inc.",
];

interface TypeProfile {
  type: TxnType;
  weight: number;
  median: number;
  spread: number;
}

/** What "normal" looks like for each customer segment. */
const SEGMENT_PROFILES: Record<SimCustomer["record"]["segment"], TypeProfile[]> = {
  RETAIL: [
    { type: "CARD_PURCHASE", weight: 50, median: 45, spread: 0.8 },
    { type: "BILL_PAYMENT", weight: 15, median: 150, spread: 0.6 },
    { type: "TRANSFER_OUT", weight: 10, median: 200, spread: 0.9 },
    { type: "TRANSFER_IN", weight: 8, median: 200, spread: 0.9 },
    { type: "PAYROLL", weight: 7, median: 2400, spread: 0.3 },
    { type: "CASH_WITHDRAWAL", weight: 8, median: 100, spread: 0.6 },
    { type: "CASH_DEPOSIT", weight: 2, median: 300, spread: 0.8 },
  ],
  // Legit cash-heavy businesses are the main source of false positives — keep them.
  SMALL_BUSINESS: [
    { type: "CASH_DEPOSIT", weight: 25, median: 1500, spread: 0.8 },
    { type: "CARD_PURCHASE", weight: 20, median: 120, spread: 0.9 },
    { type: "TRANSFER_OUT", weight: 15, median: 2000, spread: 0.8 },
    { type: "TRANSFER_IN", weight: 20, median: 2500, spread: 0.8 },
    { type: "WIRE_OUT", weight: 3, median: 8000, spread: 0.7 },
    { type: "WIRE_IN", weight: 3, median: 9000, spread: 0.7 },
    { type: "BILL_PAYMENT", weight: 14, median: 600, spread: 0.7 },
  ],
  STUDENT: [
    { type: "CARD_PURCHASE", weight: 60, median: 25, spread: 0.7 },
    { type: "TRANSFER_IN", weight: 15, median: 300, spread: 0.7 },
    { type: "CASH_WITHDRAWAL", weight: 15, median: 60, spread: 0.5 },
    { type: "BILL_PAYMENT", weight: 10, median: 80, spread: 0.5 },
  ],
  SENIOR: [
    { type: "CARD_PURCHASE", weight: 40, median: 60, spread: 0.7 },
    { type: "BILL_PAYMENT", weight: 25, median: 140, spread: 0.6 },
    { type: "CASH_WITHDRAWAL", weight: 20, median: 150, spread: 0.5 },
    { type: "PAYROLL", weight: 10, median: 1800, spread: 0.2 }, // pension deposit
    { type: "TRANSFER_OUT", weight: 5, median: 250, spread: 0.8 },
  ],
};

const DIRECTION: Record<TxnType, Direction> = {
  CASH_DEPOSIT: "CREDIT",
  CASH_WITHDRAWAL: "DEBIT",
  TRANSFER_IN: "CREDIT",
  TRANSFER_OUT: "DEBIT",
  WIRE_IN: "CREDIT",
  WIRE_OUT: "DEBIT",
  CARD_PURCHASE: "DEBIT",
  PAYROLL: "CREDIT",
  BILL_PAYMENT: "DEBIT",
};

export const SCENARIO_TYPOLOGIES: readonly Exclude<Typology, "NONE">[] = [
  "STRUCTURING",
  "RAPID_MOVEMENT",
  "FAN_IN",
  "FAN_OUT",
];

interface Counterparty {
  accountId: string | null;
  name: string | null;
  country: string | null;
}

export class TransactionGenerator {
  private readonly suspiciousCustomers: SimCustomer[];
  private scenarioCounter = 0;

  constructor(
    private readonly rng: Rng,
    private readonly population: Population,
  ) {
    this.suspiciousCustomers = population.customers.filter((c) => c.suspicious);
  }

  get hasSuspiciousCustomers(): boolean {
    return this.suspiciousCustomers.length > 0;
  }

  pickSuspiciousCustomer(): SimCustomer {
    return this.rng.pick(this.suspiciousCustomers);
  }

  // ── Normal behaviour ─────────────────────────────────────────────────────

  normal(eventTime: Date, producedAt: Date): TransactionEvent {
    const customer = this.rng.pick(this.population.customers);
    const account = this.rng.pick(customer.accounts);
    const profile = this.rng.weighted(SEGMENT_PROFILES[customer.record.segment].map((p) => [p, p.weight] as const));
    const amount = this.rng.amount(profile.median, profile.spread, 1, 250_000);
    const { channel, branchId } = this.channelFor(profile.type, customer);
    const cp = this.counterpartyFor(profile.type, account.account_id);

    return this.build({
      customer,
      accountId: account.account_id,
      type: profile.type,
      amount,
      channel,
      branchId,
      cp,
      description: this.describe(profile.type, cp),
      eventTime,
      producedAt,
      typology: "NONE",
      scenarioId: null,
    });
  }

  // ── Laundering scenarios (ground truth) ──────────────────────────────────

  /**
   * Plan every transaction of one laundering scenario up front, spread across
   * `spanMs` starting at `start`. Returns events sorted by event_time.
   * Backfill uses realistic spans (days); live mode compresses them (minutes)
   * so a demo shows full patterns quickly.
   */
  planScenario(
    customer: SimCustomer,
    typology: Exclude<Typology, "NONE">,
    start: Date,
    spanMs: number,
    producedAt: (eventTime: Date) => Date = (t) => t,
  ): TransactionEvent[] {
    const scenarioId = `SCN-${String(++this.scenarioCounter).padStart(5, "0")}-${typology}`;
    const account = customer.accounts[0]!;
    const at = (fraction: number) => new Date(start.getTime() + Math.min(1, Math.max(0, fraction)) * spanMs);
    const steps: { type: TxnType; amount: number; when: Date; cp: Counterparty; channel?: Channel; branchId?: string | null }[] = [];

    switch (typology) {
      case "STRUCTURING": {
        // Several cash deposits just under the threshold, often at different branches.
        const n = this.rng.int(5, 12);
        for (let i = 0; i < n; i++) {
          steps.push({
            type: "CASH_DEPOSIT",
            amount: round2(this.rng.float(REPORTING_THRESHOLD * 0.8, REPORTING_THRESHOLD - 50)),
            when: at(i / n + this.rng.float(0, 0.5 / n)),
            cp: { accountId: null, name: null, country: null },
            channel: this.rng.bool(0.8) ? "BRANCH" : "ATM",
            branchId: this.rng.pick(ALL_BRANCHES),
          });
        }
        break;
      }
      case "RAPID_MOVEMENT": {
        // Large foreign wire in, then 90–98% of it leaves within the span (pass-through).
        const inbound = round2(this.rng.float(20_000, 150_000));
        steps.push({
          type: "WIRE_IN",
          amount: inbound,
          when: at(0),
          cp: this.shellCounterparty(true),
          channel: "WIRE",
        });
        const outCount = this.rng.int(2, 4);
        const outTotal = inbound * this.rng.float(0.9, 0.98);
        const splits = this.splitAmount(outTotal, outCount);
        splits.forEach((amt, i) =>
          steps.push({
            type: this.rng.bool(0.5) ? "WIRE_OUT" : "TRANSFER_OUT",
            amount: amt,
            when: at(0.2 + (0.8 * (i + 1)) / outCount),
            cp: this.shellCounterparty(this.rng.bool(0.6)),
            channel: "WIRE",
          }),
        );
        break;
      }
      case "FAN_IN": {
        // Many unrelated senders, then one consolidated outbound wire.
        const n = this.rng.int(10, 25);
        let total = 0;
        for (let i = 0; i < n; i++) {
          const amt = round2(this.rng.float(500, 3_000));
          total += amt;
          steps.push({
            type: "TRANSFER_IN",
            amount: amt,
            when: at((i / n) * 0.85),
            cp: this.internalCounterparty(account.account_id),
            channel: this.rng.pick(["ONLINE", "MOBILE"] as const),
          });
        }
        steps.push({
          type: "WIRE_OUT",
          amount: round2(total * this.rng.float(0.85, 0.97)),
          when: at(0.95),
          cp: this.shellCounterparty(true),
          channel: "WIRE",
        });
        break;
      }
      case "FAN_OUT": {
        // One large credit, then sprayed to many fresh recipients.
        const inbound = round2(this.rng.float(30_000, 120_000));
        steps.push({ type: "WIRE_IN", amount: inbound, when: at(0), cp: this.shellCounterparty(false), channel: "WIRE" });
        const n = this.rng.int(10, 20);
        const splits = this.splitAmount(inbound * this.rng.float(0.85, 0.95), n);
        splits.forEach((amt, i) =>
          steps.push({
            type: "TRANSFER_OUT",
            amount: amt,
            when: at(0.1 + (0.9 * i) / n),
            cp: this.internalCounterparty(account.account_id),
            channel: this.rng.pick(["ONLINE", "MOBILE"] as const),
          }),
        );
        break;
      }
    }

    return steps
      .map((s) => {
        const fallback = this.channelFor(s.type, customer);
        return this.build({
          customer,
          accountId: account.account_id,
          type: s.type,
          amount: s.amount,
          channel: s.channel ?? fallback.channel,
          branchId: s.branchId !== undefined ? s.branchId : fallback.branchId,
          cp: s.cp,
          description: this.describe(s.type, s.cp),
          eventTime: s.when,
          producedAt: producedAt(s.when),
          typology,
          scenarioId,
        });
      })
      .sort((a, b) => a.event_time.localeCompare(b.event_time));
  }

  // ── Helpers ──────────────────────────────────────────────────────────────

  private build(args: {
    customer: SimCustomer;
    accountId: string;
    type: TxnType;
    amount: number;
    channel: Channel;
    branchId: string | null;
    cp: Counterparty;
    description: string;
    eventTime: Date;
    producedAt: Date;
    typology: Typology;
    scenarioId: string | null;
  }): TransactionEvent {
    return {
      schema_version: SCHEMA_VERSION,
      event_type: "transaction",
      transaction_id: `TXN-${this.rng.hex(16).toUpperCase()}`,
      account_id: args.accountId,
      customer_id: args.customer.record.customer_id,
      counterparty_account_id: args.cp.accountId,
      counterparty_name: args.cp.name,
      counterparty_country: args.cp.country,
      txn_type: args.type,
      direction: DIRECTION[args.type],
      amount: args.amount,
      currency: "CAD",
      channel: args.channel,
      branch_id: args.branchId,
      description: args.description,
      event_time: args.eventTime.toISOString(),
      produced_at: args.producedAt.toISOString(),
      _label: {
        is_suspicious: args.typology !== "NONE",
        typology: args.typology,
        scenario_id: args.scenarioId,
      },
    };
  }

  private channelFor(type: TxnType, customer: SimCustomer): { channel: Channel; branchId: string | null } {
    switch (type) {
      case "CASH_DEPOSIT":
      case "CASH_WITHDRAWAL": {
        const channel: Channel = this.rng.bool(0.6) ? "ATM" : "BRANCH";
        return { channel, branchId: this.rng.pick(customer.homeBranches) };
      }
      case "WIRE_IN":
      case "WIRE_OUT":
        return { channel: "WIRE", branchId: null };
      case "CARD_PURCHASE":
        return { channel: "POS", branchId: null };
      default:
        return { channel: this.rng.pick(["ONLINE", "MOBILE"] as const), branchId: null };
    }
  }

  private counterpartyFor(type: TxnType, ownAccountId: string): Counterparty {
    switch (type) {
      case "TRANSFER_IN":
      case "TRANSFER_OUT":
        return this.rng.bool(0.6)
          ? this.internalCounterparty(ownAccountId)
          : { accountId: `EXT-${this.rng.hex(10).toUpperCase()}`, name: `${this.rng.pick(MERCHANTS)} e-Transfer`, country: "CA" };
      case "WIRE_IN":
      case "WIRE_OUT":
        return {
          accountId: `EXT-${this.rng.hex(10).toUpperCase()}`,
          name: `${this.rng.pick(["Pacific", "Atlas", "Summit", "Harbor"])} Supply Co.`,
          country: this.rng.pick(COMMON_COUNTRIES),
        };
      case "CARD_PURCHASE":
        return { accountId: null, name: this.rng.pick(MERCHANTS), country: "CA" };
      case "PAYROLL":
        return { accountId: null, name: this.rng.pick(EMPLOYERS), country: "CA" };
      case "BILL_PAYMENT":
        return { accountId: null, name: this.rng.pick(BILLERS), country: "CA" };
      default:
        return { accountId: null, name: null, country: null };
    }
  }

  private internalCounterparty(ownAccountId: string): Counterparty {
    let pick = this.rng.pick(this.population.accountIndex);
    for (let tries = 0; pick.accountId === ownAccountId && tries < 5; tries++) {
      pick = this.rng.pick(this.population.accountIndex);
    }
    return { accountId: pick.accountId, name: pick.customerName, country: "CA" };
  }

  private shellCounterparty(elevatedRisk: boolean): Counterparty {
    return {
      accountId: `EXT-${this.rng.hex(10).toUpperCase()}`,
      name: this.rng.pick(SHELL_COMPANY_NAMES),
      country: elevatedRisk ? this.rng.pick(ELEVATED_RISK_COUNTRIES) : this.rng.pick(COMMON_COUNTRIES),
    };
  }

  /** Split a total into n uneven parts that still add up (to the cent). */
  private splitAmount(total: number, n: number): number[] {
    const weights = Array.from({ length: n }, () => this.rng.float(0.5, 1.5));
    const sum = weights.reduce((a, b) => a + b, 0);
    const parts = weights.map((w) => round2((w / sum) * total));
    const drift = round2(round2(total) - parts.reduce((a, b) => a + b, 0));
    parts[parts.length - 1] = round2(parts[parts.length - 1]! + drift);
    return parts;
  }

  private describe(type: TxnType, cp: Counterparty): string {
    switch (type) {
      case "CASH_DEPOSIT": return "Cash deposit";
      case "CASH_WITHDRAWAL": return "Cash withdrawal";
      case "CARD_PURCHASE": return `POS purchase - ${cp.name}`;
      case "PAYROLL": return `Direct deposit - ${cp.name}`;
      case "BILL_PAYMENT": return `Bill payment - ${cp.name}`;
      case "WIRE_IN": return `Incoming wire from ${cp.name} (${cp.country})`;
      case "WIRE_OUT": return `Outgoing wire to ${cp.name} (${cp.country})`;
      case "TRANSFER_IN": return `Transfer from ${cp.name}`;
      case "TRANSFER_OUT": return `Transfer to ${cp.name}`;
    }
  }
}
