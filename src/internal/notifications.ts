// Copyright 2025 The Oxia Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

import { status as GrpcStatus, type ServiceError } from '@grpc/grpc-js';
import type {
  Notification as PbNotification,
  NotificationBatch,
} from '../proto/generated/client.js';
import {
  type CloseableAsyncIterable,
  type Notification,
  NotificationType,
} from '../types.js';
import { AsyncQueue } from './asyncQueue.js';
import { Backoff } from './backoff.js';
import { fromNumber, toNumber } from './longs.js';
import type { ServiceDiscovery } from './serviceDiscovery.js';

/**
 * Multi-shard change notification stream. Opens one
 * `GetNotifications` RPC per shard, forwards events into a shared
 * queue, and reconnects a failing shard stream with exponential
 * backoff. The consumer reads via `for await (...)`.
 */
export class NotificationStream implements CloseableAsyncIterable<Notification> {
  private readonly discovery: ServiceDiscovery;
  private readonly queue = new AsyncQueue<Notification>();
  private readonly abort = new AbortController();
  private readonly workers: Promise<void>[] = [];
  private readonly lastOffset = new Map<number, number>();
  private closedFlag = false;

  constructor(discovery: ServiceDiscovery) {
    this.discovery = discovery;
    for (const leader of discovery.getAllShardLeaders()) {
      this.workers.push(this.runShardWorker(leader.shard));
    }
  }

  private async runShardWorker(shardId: number): Promise<void> {
    const backoff = new Backoff();
    while (!this.closedFlag) {
      try {
        await this.consumeStream(shardId);
        backoff.reset();
      } catch (err) {
        if (this.closedFlag) return;
        const e = err as ServiceError | Error;
        if ((e as ServiceError).code !== GrpcStatus.CANCELLED) {
          // eslint-disable-next-line no-console
          console.warn(`oxia: notifications stream for shard ${shardId} failed: ${e.message}`);
        }
        try {
          await backoff.waitNext(this.abort.signal);
        } catch {
          return;
        }
      }
    }
  }

  private consumeStream(shardId: number): Promise<void> {
    return new Promise((resolve, reject) => {
      const client = this.discovery.getLeaderForShard(shardId);
      const lastOffset = this.lastOffset.get(shardId);
      const stream = client.getNotifications({
        shard: fromNumber(shardId),
        startOffsetExclusive: lastOffset !== undefined ? fromNumber(lastOffset) : undefined,
      });

      const onAbort = () => stream.cancel();
      this.abort.signal.addEventListener('abort', onAbort, { once: true });

      stream.on('data', (batch: NotificationBatch) => {
        for (const [key, n] of Object.entries(batch.notifications ?? {})) {
          this.queue.push(toNotification(key, n));
        }
        this.lastOffset.set(shardId, toNumber(batch.offset));
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

  [Symbol.asyncIterator](): AsyncIterator<Notification> {
    return {
      next: () => this.queue.take(),
      return: async (): Promise<IteratorResult<Notification, void>> => {
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
    // Best-effort wait for workers; swallow errors since we're tearing down.
    void Promise.allSettled(this.workers);
  }
}

function toNotification(key: string, pb: PbNotification): Notification {
  const type = pb.type as NotificationType;
  const versionId = pb.versionId !== undefined ? toNumber(pb.versionId) : 0;
  return {
    key,
    type,
    versionId,
    keyRangeEnd: pb.keyRangeLast ?? undefined,
  };
}
