// Copyright 2025 The Oxia Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

export class Backoff {
  private readonly startDelayMs: number;
  private readonly maxDelayMs: number;
  private delayMs: number;

  constructor(startDelayMs = 100, maxDelayMs = 60_000) {
    this.startDelayMs = startDelayMs;
    this.maxDelayMs = maxDelayMs;
    this.delayMs = startDelayMs;
  }

  reset(): void {
    this.delayMs = this.startDelayMs;
  }

  async waitNext(signal?: AbortSignal): Promise<void> {
    const d = this.delayMs;
    this.delayMs = Math.min(this.delayMs * 2, this.maxDelayMs);
    await sleep(d, signal);
  }
}

export function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) {
      reject(signal.reason);
      return;
    }
    const timer = setTimeout(() => {
      signal?.removeEventListener('abort', onAbort);
      resolve();
    }, ms);
    const onAbort = () => {
      clearTimeout(timer);
      reject(signal?.reason);
    };
    signal?.addEventListener('abort', onAbort, { once: true });
  });
}
