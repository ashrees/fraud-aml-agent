/**
 * Event contracts shared by every service. These are the "raw" core-banking
 * shapes as they land in the broker and the object store. dbt turns them into
 * fact_transactions / dim_customers / dim_accounts later.
 */

export const SCHEMA_VERSION = 1 as const;

export type Channel = "BRANCH" | "ATM" | "ONLINE" | "MOBILE" | "WIRE" | "POS";

export type TxnType =
  | "CASH_DEPOSIT"
  | "CASH_WITHDRAWAL"
  | "TRANSFER_IN"
  | "TRANSFER_OUT"
  | "WIRE_IN"
  | "WIRE_OUT"
  | "CARD_PURCHASE"
  | "PAYROLL"
  | "BILL_PAYMENT";

export type Direction = "CREDIT" | "DEBIT";

/**
 * Laundering typology the simulator injected. This is GROUND TRUTH used only
 * for evaluating detection and agents — downstream detection must never read it.
 */
export type Typology =
  | "NONE"
  | "STRUCTURING"   // cash deposits kept just under the reporting threshold
  | "RAPID_MOVEMENT" // money in, money out within hours (pass-through)
  | "FAN_IN"        // many unrelated senders into one account
  | "FAN_OUT";      // one account sprays funds to many recipients

export interface TransactionEvent {
  schema_version: typeof SCHEMA_VERSION;
  event_type: "transaction";
  transaction_id: string;
  account_id: string;
  customer_id: string;
  counterparty_account_id: string | null;
  counterparty_name: string | null;
  counterparty_country: string | null; // ISO-3166 alpha-2
  txn_type: TxnType;
  direction: Direction;
  amount: number;          // positive, 2-decimal precision
  currency: "CAD" | "USD";
  channel: Channel;
  branch_id: string | null;
  description: string;
  event_time: string;      // ISO-8601, when the transaction happened
  produced_at: string;     // ISO-8601, when the simulator emitted it
  /** Ground-truth label for evaluation only. */
  _label: { is_suspicious: boolean; typology: Typology; scenario_id: string | null };
}

export type RiskRating = "LOW" | "MEDIUM" | "HIGH";
export type CustomerSegment = "RETAIL" | "SMALL_BUSINESS" | "STUDENT" | "SENIOR";

export interface CustomerRecord {
  schema_version: typeof SCHEMA_VERSION;
  event_type: "customer";
  customer_id: string;
  full_name: string;
  date_of_birth: string;
  segment: CustomerSegment;
  occupation: string;
  city: string;
  province: string;
  country: string;
  kyc_risk_rating: RiskRating;
  is_pep: boolean;             // politically exposed person
  onboarded_at: string;
  updated_at: string;
}

export type AccountType = "CHEQUING" | "SAVINGS" | "BUSINESS";

export interface AccountRecord {
  schema_version: typeof SCHEMA_VERSION;
  event_type: "account";
  account_id: string;
  customer_id: string;
  account_type: AccountType;
  currency: "CAD" | "USD";
  home_branch_id: string;
  opened_at: string;
  status: "ACTIVE" | "FROZEN" | "CLOSED";
  updated_at: string;
}

export type ReferenceEvent = CustomerRecord | AccountRecord;
export type RawEvent = TransactionEvent | ReferenceEvent;
