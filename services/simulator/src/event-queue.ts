import type { TransactionEvent } from "@aml/shared";

/**
 * Min-heap of planned scenario transactions ordered by event_time, so they can
 * be merged into the normal stream at the right moment.
 */
export class EventQueue {
  private heap: { t: number; event: TransactionEvent }[] = [];

  get size(): number {
    return this.heap.length;
  }

  pushAll(events: TransactionEvent[]): void {
    for (const event of events) this.push(event);
  }

  push(event: TransactionEvent): void {
    this.heap.push({ t: Date.parse(event.event_time), event });
    let i = this.heap.length - 1;
    while (i > 0) {
      const parent = (i - 1) >> 1;
      if (this.heap[parent]!.t <= this.heap[i]!.t) break;
      [this.heap[parent], this.heap[i]] = [this.heap[i]!, this.heap[parent]!];
      i = parent;
    }
  }

  /** Remove and return every event due at or before `time`, in order. */
  popDue(time: number): TransactionEvent[] {
    const due: TransactionEvent[] = [];
    while (this.heap.length > 0 && this.heap[0]!.t <= time) due.push(this.pop()!);
    return due;
  }

  private pop(): TransactionEvent | undefined {
    if (this.heap.length === 0) return undefined;
    const top = this.heap[0]!;
    const last = this.heap.pop()!;
    if (this.heap.length > 0) {
      this.heap[0] = last;
      let i = 0;
      for (;;) {
        const l = 2 * i + 1;
        const r = l + 1;
        let smallest = i;
        if (l < this.heap.length && this.heap[l]!.t < this.heap[smallest]!.t) smallest = l;
        if (r < this.heap.length && this.heap[r]!.t < this.heap[smallest]!.t) smallest = r;
        if (smallest === i) break;
        [this.heap[smallest], this.heap[i]] = [this.heap[i]!, this.heap[smallest]!];
        i = smallest;
      }
    }
    return top.event;
  }
}
