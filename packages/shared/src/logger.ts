/** Minimal structured JSON logger — one line per event, easy to grep and ship. */
type Level = "debug" | "info" | "warn" | "error";

export function createLogger(service: string) {
  const log = (level: Level, msg: string, fields: Record<string, unknown> = {}) => {
    const line = JSON.stringify({ ts: new Date().toISOString(), level, service, msg, ...fields });
    (level === "error" || level === "warn" ? process.stderr : process.stdout).write(line + "\n");
  };
  return {
    debug: (msg: string, f?: Record<string, unknown>) => {
      if (process.env.LOG_LEVEL === "debug") log("debug", msg, f);
    },
    info: (msg: string, f?: Record<string, unknown>) => log("info", msg, f),
    warn: (msg: string, f?: Record<string, unknown>) => log("warn", msg, f),
    error: (msg: string, f?: Record<string, unknown>) => log("error", msg, f),
  };
}

export type Logger = ReturnType<typeof createLogger>;
