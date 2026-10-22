import { PutObjectCommand, S3Client } from "@aws-sdk/client-s3";
import type { Logger } from "@aml/shared";
import type { LandingObject } from "./landing.js";

/**
 * Thin S3 wrapper. Works against MinIO locally and real AWS S3 unchanged —
 * only the endpoint and credentials differ.
 */
export class ObjectStore {
  private readonly client: S3Client;

  constructor(
    private readonly cfg: { endpoint: string; region: string; bucket: string; accessKeyId: string; secretAccessKey: string },
    private readonly log: Logger,
  ) {
    this.client = new S3Client({
      endpoint: cfg.endpoint,
      region: cfg.region,
      forcePathStyle: true, // required for MinIO
      credentials: { accessKeyId: cfg.accessKeyId, secretAccessKey: cfg.secretAccessKey },
    });
  }

  async put(obj: LandingObject, attempts = 4): Promise<void> {
    for (let attempt = 1; ; attempt++) {
      try {
        await this.client.send(
          new PutObjectCommand({
            Bucket: this.cfg.bucket,
            Key: obj.key,
            Body: obj.body,
            ContentType: "application/x-ndjson",
            ContentEncoding: "gzip",
            Metadata: { records: String(obj.records) },
          }),
        );
        return;
      } catch (err) {
        if (attempt >= attempts) throw err;
        const backoff = 250 * 2 ** attempt;
        this.log.warn("object put failed, retrying", { key: obj.key, attempt, backoff_ms: backoff, error: (err as Error).message });
        await new Promise((r) => setTimeout(r, backoff));
      }
    }
  }
}
