// Copyright 2025 The Oxia Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

import { status as GrpcStatus, type ServiceError } from '@grpc/grpc-js';
import type { GetSequenceUpdatesResponse } from '../proto/generated/client.js';
import type { CloseableAsyncIterable } from '../types.js';
import { AsyncQueue } from './asyncQueue.js';
import { Backoff } from './backoff.js';
import { fromNumber } from './longs.js';
import type { ServiceDiscovery } from './serviceDiscovery.js';

/**
 * Single-shard subscription to a sequential-key prefix. Yields the
 * latest assigned key each time the sequence advances (intermediate
 * updates may be collapsed into the most recent).
 */
export class SequenceUpdatesStream implements CloseableAsyncIterable<string> {
  private readonly discovery: ServiceDiscovery;
  private readonly shardId: number;
  private readonly prefixKey: string;
  private readonly queue = new AsyncQueue<string>();
  private readonly abort = new AbortController();
  private readonly worker: Promise<void>;
  private closedFlag = false;

  constructor(discovery: ServiceDiscovery, shardId: number, prefixKey: string) {
    this.discovery = discovery;
    this.shardId = shardId;
    this.prefixKey = prefixKey;
    this.worker = this.runWorker();
  }

  private async runWorker(): Promise<void> {
    const backoff = new Backoff();
    while (!this.closedFlag) {
      try {
        await this.consumeStream();
        backoff.reset();
      } catch (err) {
        if (this.closedFlag) return;
        const e = err as ServiceError | Error;
        if ((e as ServiceError).code !== GrpcStatus.CANCELLED) {
          // eslint-disable-next-line no-console
          console.warn(
            `oxia: sequence-updates stream for shard ${this.shardId} failed: ${e.message}`,
          );
        }
        try {
          await backoff.waitNext(this.abort.signal);
        } catch {
          return;
        }
      }
    }
  }

  private consumeStream(): Promise<void> {
    return new Promise((resolve, reject) => {
      const client = this.discovery.getLeaderForShard(this.shardId);
      const stream = client.getSequenceUpdates({
        shard: fromNumber(this.shardId),
        key: this.prefixKey,
      });

      const onAbort = () => stream.cancel();
      this.abort.signal.addEventListener('abort', onAbort, { once: true });

      stream.on('data', (msg: GetSequenceUpdatesResponse) => {
        if (msg.highestSequenceKey) this.queue.push(msg.highestSequenceKey);
      });
      stream.on('error', (err) => {
        this.abort.signal.removeEventListener('abort', onAbort);
        reject(err);
      });
      stream.on('end', () => {
        this.abort.signal.removeEventListener('abort', onAbort);
        resolve();
      });
    });
  }

  [Symbol.asyncIterator](): AsyncIterator<string> {
    return {
      next: () => this.queue.take(),
      return: async (): Promise<IteratorResult<string, void>> => {
        this.close();
        return { value: undefined, done: true };
      },
    };
  }

  close(): void {
    if (this.closedFlag) return;
    this.closedFlag = true;
    this.abort.abort();
    this.queue.close();
    void this.worker.catch(() => {
      /* swallowed — we're tearing down */
    });
  }
}
