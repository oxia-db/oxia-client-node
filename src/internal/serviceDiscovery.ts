// Copyright 2025 The Oxia Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

import { status as GrpcStatus, type ServiceError } from '@grpc/grpc-js';
import {
  type NamespaceShardsAssignment,
  type OxiaClientClient,
  ShardKeyRouter,
} from '../proto/generated/client.js';
import { toNumber } from './longs.js';
import { Backoff } from './backoff.js';
import type { ConnectionPool } from './connectionPool.js';
import { xxh332 } from './hash.js';

export interface HashRange {
  minInclusive: number;
  maxInclusive: number;
}

export interface Shard {
  id: number;
  leader: string;
  hashRange: HashRange;
}

export interface Leader {
  shard: number;
  client: OxiaClientClient;
}

/**
 * Subscribes to the streaming `GetShardAssignments` RPC, keeps the
 * shard-to-leader map up to date, and exposes lookups for the client.
 *
 * The stream pushes full snapshots on every change, so a new response
 * replaces the previous assignment map atomically.
 */
export class ServiceDiscovery {
  private readonly pool: ConnectionPool;
  private readonly namespace: string;
  private readonly serviceAddress: string;
  private shards = new Map<number, Shard>();
  private closed = false;
  private readonly abort = new AbortController();
  private ready: Promise<void>;
  private resolveReady!: () => void;
  private rejectReady!: (err: Error) => void;
  private loopPromise: Promise<void> | null = null;

  constructor(pool: ConnectionPool, serviceAddress: string, namespace: string) {
    this.pool = pool;
    this.serviceAddress = serviceAddress;
    this.namespace = namespace;
    this.ready = new Promise((resolve, reject) => {
      this.resolveReady = resolve;
      this.rejectReady = reject;
    });
  }

  start(initTimeoutMs = 30_000): Promise<void> {
    this.loopPromise = this.run();
    const timeout = new Promise<never>((_, reject) => {
      const t = setTimeout(() => {
        this.rejectReady(
          new Error(
            `Timed out after ${initTimeoutMs}ms waiting for initial shard assignments from ${this.serviceAddress}`,
          ),
        );
        reject(new Error('init timeout'));
      }, initTimeoutMs);
      t.unref?.();
    });
    return Promise.race([this.ready, timeout]);
  }

  private async run(): Promise<void> {
    const backoff = new Backoff();
    while (!this.closed) {
      try {
        await this.consumeStream();
        backoff.reset();
      } catch (err) {
        if (this.closed) return;
        const e = err as ServiceError | Error;
        if ((e as ServiceError).code !== GrpcStatus.CANCELLED) {
          // eslint-disable-next-line no-console
          console.warn(`oxia: failed to get shard assignments: ${e.message}`);
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
      const client = this.pool.get(this.serviceAddress);
      const stream = client.getShardAssignments({ namespace: this.namespace });

      const onAbort = () => stream.cancel();
      this.abort.signal.addEventListener('abort', onAbort, { once: true });

      stream.on('data', (msg) => {
        const nsAssignments = msg.namespaces[this.namespace] as NamespaceShardsAssignment | undefined;
        if (nsAssignments) {
          try {
            this.applyAssignments(nsAssignments);
            this.resolveReady();
          } catch (e) {
            stream.cancel();
            reject(e as Error);
          }
        }
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

  private applyAssignments(a: NamespaceShardsAssignment): void {
    if (a.shardKeyRouter !== ShardKeyRouter.XXHASH3) {
      throw new Error(`Unsupported shard key router: ${a.shardKeyRouter}`);
    }
    const next = new Map<number, Shard>();
    for (const s of a.assignments) {
      if (s.shardBoundaries?.$case !== 'int32HashRange') {
        continue;
      }
      const range = s.shardBoundaries.int32HashRange;
      const shardId = toNumber(s.shard);
      next.set(shardId, {
        id: shardId,
        leader: s.leader,
        hashRange: {
          minInclusive: range.minHashInclusive,
          maxInclusive: range.maxHashInclusive,
        },
      });
    }
    this.shards = next;
  }

  getShardForKey(key: string): Shard {
    const h = xxh332(key);
    for (const s of this.shards.values()) {
      if (h >= s.hashRange.minInclusive && h <= s.hashRange.maxInclusive) {
        return s;
      }
    }
    throw new Error(`No shard found for key ${JSON.stringify(key)}`);
  }

  getLeaderForKey(key: string, partitionKey?: string): Leader {
    const s = this.getShardForKey(partitionKey ?? key);
    return { shard: s.id, client: this.pool.get(s.leader) };
  }

  getLeaderForShard(shard: number): OxiaClientClient {
    const s = this.shards.get(shard);
    if (!s) throw new Error(`No leader known for shard ${shard}`);
    return this.pool.get(s.leader);
  }

  getAllShardLeaders(): Leader[] {
    return [...this.shards.values()].map((s) => ({
      shard: s.id,
      client: this.pool.get(s.leader),
    }));
  }

  async close(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    this.abort.abort();
    if (this.loopPromise) {
      try {
        await this.loopPromise;
      } catch {
        // swallow
      }
    }
  }
}
