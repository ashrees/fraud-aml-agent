import { existsSync } from "node:fs";
import { resolve } from "node:path";

/** Load the repo-root .env once (Node >= 20.12 has a built-in loader). */
let loaded = false;
export function loadEnv(): void {
  if (loaded) return;
  loaded = true;
  for (const candidate of [".env", "../.env", "../../.env"]) {
    const path = resolve(process.cwd(), candidate);
    if (existsSync(path)) {
      process.loadEnvFile(path);
      return;
    }
  }
}

export function env(name: string, fallback?: string): string {
  loadEnv();
  const value = process.env[name] ?? fallback;
  if (value === undefined || value === "") {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return value;
}

export function envNumber(name: string, fallback: number): number {
  const raw = env(name, String(fallback));
  const n = Number(raw);
  if (!Number.isFinite(n)) throw new Error(`${name} must be a number, got "${raw}"`);
  return n;
}

export const kafkaConfig = () => ({
  brokers: env("KAFKA_BROKERS", "localhost:19092").split(","),
  topicTxns: env("TOPIC_TXNS", "raw_txns"),
  topicReference: env("TOPIC_REFERENCE", "raw_reference"),
});

export const s3Config = () => ({
  endpoint: env("S3_ENDPOINT", "http://localhost:9000"),
  region: env("S3_REGION", "us-east-1"),
  bucket: env("S3_BUCKET", "aml-raw"),
  accessKeyId: env("S3_ACCESS_KEY_ID", "amladmin"),
  secretAccessKey: env("S3_SECRET_ACCESS_KEY", "amladmin-secret"),
});
