// Copyright 2025 The Oxia Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

/**
 * K-way merge over N async iterables, ordered by the supplied comparator
 * (stable for equal keys). Used to merge per-shard ranges back into the
 * server's hierarchical order.
 *
 * Each input stream is assumed to be already sorted by `compare`. On
 * teardown (consumer break or throw) all remaining source iterators
 * receive `return()`, so long-lived gRPC streams are cancelled promptly.
 */
export async function* mergeSorted<T>(
  sources: AsyncIterable<T>[],
  compare: (a: T, b: T) => number,
): AsyncIterable<T> {
  const iters = sources.map((s) => s[Symbol.asyncIterator]());

  interface Head {
    value: T;
    index: number;
  }

  const heads: (Head | null)[] = new Array(iters.length).fill(null);

  const advance = async (i: number): Promise<void> => {
    const { value, done } = await iters[i].next();
    heads[i] = done ? null : { value, index: i };
  };

  try {
    await Promise.all(iters.map((_, i) => advance(i)));

    while (true) {
      let pick: Head | null = null;
      for (const h of heads) {
        if (h === null) continue;
        if (pick === null || compare(h.value, pick.value) < 0) {
          pick = h;
        }
      }
      if (pick === null) return;

      yield pick.value;
      await advance(pick.index);
    }
  } finally {
    // Release any still-open source iterators, even if the consumer
    // stopped mid-stream.
    await Promise.allSettled(iters.map((it) => it.return?.()));
  }
}
