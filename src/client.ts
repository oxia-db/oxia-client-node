// Copyright 2025 The Oxia Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

import Long from 'long';
import {
  KeyComparisonType,
  type PutResponse,
  type ReadResponse,
  Status,
  type WriteResponse,
  type OxiaClientClient,
} from './proto/generated/client.js';
import {
  InvalidOptionsError,
  KeyNotFoundError,
  OxiaError,
  SessionNotFoundError,
  UnexpectedVersionIdError,
} from './exceptions.js';
import { ConnectionPool } from './internal/connectionPool.js';
import { ServiceDiscovery } from './internal/serviceDiscovery.js';
import { callUnary, firstStreamMessage } from './internal/rpc.js';
import { ComparisonType, type GetResult, type PutResult, versionFromProto } from './types.js';
import { fromNumber } from './internal/longs.js';

export interface OxiaClientOptions {
  /** Oxia namespace. Defaults to `default`. */
  namespace?: string;
  /** Timeout for the initial shard-assignment fetch, in ms. Defaults to 30_000. */
  initTimeoutMs?: number;
}

export interface PutOptions {
  partitionKey?: string;
  expectedVersionId?: number;
}

export interface DeleteOptions {
  partitionKey?: string;
  expectedVersionId?: number;
}

export interface GetOptions {
  partitionKey?: string;
  comparisonType?: ComparisonType;
  includeValue?: boolean;
}

/**
 * Asynchronous client for the Oxia service.
 *
 * Build one with `OxiaClient.connect(serviceAddress, options?)`. Call
 * `close()` when done to release the gRPC channels and background
 * shard-assignment watcher.
 */
export class OxiaClient {
  private readonly pool: ConnectionPool;
  private readonly discovery: ServiceDiscovery;
  private closed = false;

  private constructor(pool: ConnectionPool, discovery: ServiceDiscovery) {
    this.pool = pool;
    this.discovery = discovery;
  }

  static async connect(
    serviceAddress: string,
    options: OxiaClientOptions = {},
  ): Promise<OxiaClient> {
    const namespace = options.namespace ?? 'default';
    const pool = new ConnectionPool();
    const discovery = new ServiceDiscovery(pool, serviceAddress, namespace);
    try {
      await discovery.start(options.initTimeoutMs ?? 30_000);
    } catch (err) {
      await discovery.close();
      pool.close();
      throw err;
    }
    return new OxiaClient(pool, discovery);
  }

  async put(key: string, value: string | Uint8Array, opts: PutOptions = {}): Promise<PutResult> {
    this.ensureOpen();
    const leader = this.discovery.getLeaderForKey(key, opts.partitionKey);
    const pr = {
      key,
      value: coerceValue(value),
      expectedVersionId:
        opts.expectedVersionId !== undefined ? fromNumber(opts.expectedVersionId) : undefined,
      partitionKey: opts.partitionKey,
      sequenceKeyDelta: [] as Long[],
      secondaryIndexes: [],
    };
    const resp = await callUnary<WriteResponse>((cb) =>
      leader.client.write(
        {
          shard: fromNumber(leader.shard),
          puts: [pr],
          deletes: [],
          deleteRanges: [],
        },
        cb,
      ),
    );
    const putRes = resp.puts[0];
    checkStatus(putRes.status);
    return toPutResult(key, putRes);
  }

  async delete(key: string, opts: DeleteOptions = {}): Promise<boolean> {
    this.ensureOpen();
    const leader = this.discovery.getLeaderForKey(key, opts.partitionKey);
    const resp = await callUnary<WriteResponse>((cb) =>
      leader.client.write(
        {
          shard: fromNumber(leader.shard),
          puts: [],
          deletes: [
            {
              key,
              expectedVersionId:
                opts.expectedVersionId !== undefined
                  ? fromNumber(opts.expectedVersionId)
                  : undefined,
            },
          ],
          deleteRanges: [],
        },
        cb,
      ),
    );
    const st = resp.deletes[0].status;
    if (st === Status.KEY_NOT_FOUND) return false;
    checkStatus(st);
    return true;
  }

  async get(key: string, opts: GetOptions = {}): Promise<GetResult> {
    this.ensureOpen();
    const cmp = opts.comparisonType ?? ComparisonType.EQUAL;
    if (cmp !== ComparisonType.EQUAL && opts.partitionKey === undefined) {
      throw new InvalidOptionsError(
        'Non-EQUAL get() across all shards is not supported yet (phase 2)',
      );
    }
    const leader = this.discovery.getLeaderForKey(key, opts.partitionKey);
    return await getSingleShard(leader.client, leader.shard, key, cmp, opts.includeValue ?? true);
  }

  async close(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    await this.discovery.close();
    this.pool.close();
  }

  private ensureOpen(): void {
    if (this.closed) {
      throw new OxiaError('client is closed');
    }
  }
}

function coerceValue(v: string | Uint8Array): Uint8Array {
  if (typeof v === 'string') return new TextEncoder().encode(v);
  return v;
}

function toPutResult(requestedKey: string, resp: PutResponse): PutResult {
  if (!resp.version) {
    throw new OxiaError('put response missing version');
  }
  return {
    key: resp.key ?? requestedKey,
    version: versionFromProto(resp.version),
  };
}

async function getSingleShard(
  client: OxiaClientClient,
  shard: number,
  key: string,
  cmp: KeyComparisonType,
  includeValue: boolean,
): Promise<GetResult> {
  const stream = client.read({
    shard: fromNumber(shard),
    gets: [
      {
        key,
        includeValue,
        comparisonType: cmp,
      },
    ],
  });
  const resp = await firstStreamMessage<ReadResponse>(stream);
  const getRes = resp.gets[0];
  checkStatus(getRes.status);
  if (!getRes.version) {
    throw new OxiaError('get response missing version');
  }
  return {
    key: getRes.key ?? key,
    value: getRes.value,
    version: versionFromProto(getRes.version),
  };
}

function checkStatus(status: Status): void {
  switch (status) {
    case Status.OK:
      return;
    case Status.KEY_NOT_FOUND:
      throw new KeyNotFoundError();
    case Status.UNEXPECTED_VERSION_ID:
      throw new UnexpectedVersionIdError();
    case Status.SESSION_DOES_NOT_EXIST:
      throw new SessionNotFoundError();
    default:
      throw new OxiaError(`unknown status: ${status}`);
  }
}
