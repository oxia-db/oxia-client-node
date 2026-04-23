// Copyright 2025 The Oxia Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

/**
 * Unbounded single-consumer async queue. Producers call `push()`; the
 * consumer awaits `take()` which resolves to the next item or signals
 * end-of-stream once `close()` has been called and the buffer is drained.
 */
export class AsyncQueue<T> {
  private readonly items: T[] = [];
  private readonly waiters: ((r: IteratorResult<T, void>) => void)[] = [];
  private closed = false;

  push(value: T): void {
    if (this.closed) return;
    const w = this.waiters.shift();
    if (w) w({ value, done: false });
    else this.items.push(value);
  }

  take(): Promise<IteratorResult<T, void>> {
    const head = this.items.shift();
    if (head !== undefined) return Promise.resolve({ value: head, done: false });
    if (this.closed) return Promise.resolve({ value: undefined, done: true });
    return new Promise((resolve) => {
      this.waiters.push(resolve);
    });
  }

  close(): void {
    if (this.closed) return;
    this.closed = true;
    const waiters = this.waiters.splice(0);
    for (const w of waiters) w({ value: undefined, done: true });
  }
}
