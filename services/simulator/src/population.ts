import {
  SCHEMA_VERSION,
  type AccountRecord,
  type AccountType,
  type CustomerRecord,
  type CustomerSegment,
  type RiskRating,
} from "@aml/shared";
import type { Rng } from "./rng.js";

const FIRST_NAMES = [
  "Aarav", "Olivia", "Liam", "Priya", "Noah", "Emma", "Wei", "Sofia", "Lucas", "Amara",
  "Ethan", "Chloe", "Mateo", "Hana", "Omar", "Isla", "Daniel", "Mei", "Gabriel", "Zara",
  "Arjun", "Nora", "Samuel", "Leila", "Kenji", "Ava", "Diego", "Fatima", "Owen", "Ines",
];
const LAST_NAMES = [
  "Sharma", "Tremblay", "Nguyen", "Smith", "Patel", "Roy", "Chen", "Martin", "Singh", "Gagnon",
  "Kim", "Brown", "Wilson", "Garcia", "Lee", "Côté", "Ali", "Taylor", "Wong", "Campbell",
];

/** City, province, and the branches that serve it. */
const CITIES = [
  { city: "Toronto", province: "ON", branches: ["BR-TOR-001", "BR-TOR-002", "BR-TOR-003", "BR-TOR-004"] },
  { city: "Mississauga", province: "ON", branches: ["BR-MIS-001", "BR-MIS-002"] },
  { city: "Ottawa", province: "ON", branches: ["BR-OTT-001", "BR-OTT-002"] },
  { city: "Montréal", province: "QC", branches: ["BR-MTL-001", "BR-MTL-002", "BR-MTL-003"] },
  { city: "Vancouver", province: "BC", branches: ["BR-VAN-001", "BR-VAN-002"] },
  { city: "Calgary", province: "AB", branches: ["BR-CGY-001"] },
] as const;

export const ALL_BRANCHES: readonly string[] = CITIES.flatMap((c) => c.branches);

const OCCUPATIONS: Record<CustomerSegment, readonly string[]> = {
  RETAIL: ["Software Developer", "Nurse", "Teacher", "Accountant", "Electrician", "Sales Associate"],
  SMALL_BUSINESS: ["Restaurant Owner", "Convenience Store Owner", "Contractor", "Import/Export Trader", "Car Dealer"],
  STUDENT: ["Student"],
  SENIOR: ["Retired"],
};

export interface SimCustomer {
  record: CustomerRecord;
  accounts: AccountRecord[];
  homeBranches: readonly string[];
  /** Ground truth: this customer will run laundering scenarios. */
  suspicious: boolean;
}

export interface Population {
  customers: SimCustomer[];
  /** All internal account IDs — used as realistic transfer counterparties. */
  accountIndex: { accountId: string; customerName: string }[];
}

export function buildPopulation(rng: Rng, size: number, suspiciousRate: number, now: Date): Population {
  const customers: SimCustomer[] = [];
  // Exact number of suspicious customers (not a coin flip per customer), so small
  // populations still get a predictable amount of ground truth to evaluate on.
  const suspiciousTarget = Math.max(1, Math.round(size * suspiciousRate));
  const suspiciousIds = new Set<number>();
  while (suspiciousIds.size < Math.min(suspiciousTarget, size)) suspiciousIds.add(rng.int(1, size));

  for (let i = 1; i <= size; i++) {
    const segment = rng.weighted<CustomerSegment>([
      ["RETAIL", 60],
      ["SMALL_BUSINESS", 15],
      ["STUDENT", 15],
      ["SENIOR", 10],
    ]);
    const place = rng.pick(CITIES);
    const suspicious = suspiciousIds.has(i);

    // KYC rating is only loosely correlated with being suspicious — on purpose.
    // Real launderers often look low-risk at onboarding.
    const kycRisk = rng.weighted<RiskRating>(
      suspicious
        ? [["LOW", 45], ["MEDIUM", 35], ["HIGH", 20]]
        : [["LOW", 75], ["MEDIUM", 20], ["HIGH", 5]],
    );

    const age = segment === "STUDENT" ? rng.int(18, 25) : segment === "SENIOR" ? rng.int(66, 90) : rng.int(24, 65);
    const onboarded = daysAgo(now, rng.int(30, 365 * 8));
    const customerId = `CUST-${String(i).padStart(6, "0")}`;

    const record: CustomerRecord = {
      schema_version: SCHEMA_VERSION,
      event_type: "customer",
      customer_id: customerId,
      full_name: `${rng.pick(FIRST_NAMES)} ${rng.pick(LAST_NAMES)}`,
      date_of_birth: isoDate(new Date(now.getFullYear() - age, rng.int(0, 11), rng.int(1, 28))),
      segment,
      occupation: rng.pick(OCCUPATIONS[segment]),
      city: place.city,
      province: place.province,
      country: "CA",
      kyc_risk_rating: kycRisk,
      is_pep: rng.bool(0.005),
      onboarded_at: onboarded.toISOString(),
      updated_at: now.toISOString(),
    };

    const accountTypes: AccountType[] =
      segment === "SMALL_BUSINESS" ? ["CHEQUING", "BUSINESS"] : rng.bool(0.6) ? ["CHEQUING", "SAVINGS"] : ["CHEQUING"];

    const accounts: AccountRecord[] = accountTypes.map((type, idx) => ({
      schema_version: SCHEMA_VERSION,
      event_type: "account",
      account_id: `ACCT-${String(i).padStart(6, "0")}-${idx + 1}`,
      customer_id: customerId,
      account_type: type,
      currency: rng.bool(0.95) ? "CAD" : "USD",
      home_branch_id: rng.pick(place.branches),
      opened_at: new Date(onboarded.getTime() + rng.int(0, 30) * 86_400_000).toISOString(),
      status: "ACTIVE",
      updated_at: now.toISOString(),
    }));

    customers.push({ record, accounts, homeBranches: place.branches, suspicious });
  }

  const accountIndex = customers.flatMap((c) =>
    c.accounts.map((a) => ({ accountId: a.account_id, customerName: c.record.full_name })),
  );

  return { customers, accountIndex };
}

export function daysAgo(now: Date, days: number): Date {
  return new Date(now.getTime() - days * 86_400_000);
}

export function isoDate(d: Date): string {
  return d.toISOString().slice(0, 10);
}
