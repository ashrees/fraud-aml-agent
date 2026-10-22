/**
 * Seeded pseudo-random generator (mulberry32). Same seed => same customers,
 * same transactions, same injected laundering scenarios. That makes the
 * dataset reproducible, which is what lets you write repeatable evals later.
 */
export class Rng {
  private state: number;

  constructor(seed: number) {
    this.state = seed >>> 0;
  }

  /** Uniform float in [0, 1). */
  next(): number {
    this.state = (this.state + 0x6d2b79f5) >>> 0;
    let t = this.state;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  }

  float(min: number, max: number): number {
    return min + this.next() * (max - min);
  }

  int(min: number, maxInclusive: number): number {
    return Math.floor(this.float(min, maxInclusive + 1));
  }

  bool(probability: number): boolean {
    return this.next() < probability;
  }

  pick<T>(items: readonly T[]): T {
    if (items.length === 0) throw new Error("pick() on empty array");
    return items[Math.floor(this.next() * items.length)]!;
  }

  /** Pick using relative weights, e.g. [["A", 3], ["B", 1]] => A 75% of the time. */
  weighted<T>(entries: readonly (readonly [T, number])[]): T {
    const total = entries.reduce((sum, [, w]) => sum + w, 0);
    let roll = this.next() * total;
    for (const [value, weight] of entries) {
      roll -= weight;
      if (roll < 0) return value;
    }
    return entries[entries.length - 1]![0];
  }

  /** Standard normal via Box–Muller. */
  normal(): number {
    const u = Math.max(this.next(), Number.EPSILON);
    const v = this.next();
    return Math.sqrt(-2 * Math.log(u)) * Math.cos(2 * Math.PI * v);
  }

  /** Log-normal amount around a median — realistic for spending (many small, few large). */
  amount(median: number, spread = 0.6, min = 1, max = 1_000_000): number {
    const value = median * Math.exp(spread * this.normal());
    return round2(Math.min(max, Math.max(min, value)));
  }

  hex(length: number): string {
    let out = "";
    while (out.length < length) out += Math.floor(this.next() * 0x10000).toString(16).padStart(4, "0");
    return out.slice(0, length);
  }
}

export const round2 = (n: number): number => Math.round(n * 100) / 100;
