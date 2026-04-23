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
  type ListResponse,
  type OxiaClientClient,
  type PutResponse,
  type RangeScanResponse,
  type ReadResponse,
  type SecondaryIndex,
  Status,
  type WriteResponse,
} from './proto/generated/client.js';
import {
  InvalidOptionsError,
  KeyNotFoundError,
  OxiaError,
  SessionNotFoundError,
  UnexpectedVersionIdError,
} from './exceptions.js';
import { compareWithSlash } from './internal/compare.js';
import { ConnectionPool } from './internal/connectionPool.js';
import { fromNumber } from './internal/longs.js';
import { mergeSorted } from './internal/mergeStreams.js';
import { callUnary, firstStreamMessage } from './internal/rpc.js';
import { type Leader, ServiceDiscovery } from './internal/serviceDiscovery.js';
import { SessionManager } from './internal/sessions.js';
import { ComparisonType, type GetResult, type PutResult, versionFromProto } from './types.js';

export interface OxiaClientOptions {
  /** Oxia namespace. Defaults to `default`. */
  namespace?: string;
  /** Timeout for the initial shard-assignment fetch, in ms. Defaults to 30_000. */
  initTimeoutMs?: number;
  /**
   * Session timeout in milliseconds for ephemeral records. Must be at
   * least 2000ms when `heartbeatIntervalMs` is left unset. Default: 30_000.
   */
  sessionTimeoutMs?: number;
  /**
   * Heartbeat cadence for keeping sessions alive. Must be < `sessionTimeoutMs`.
   * If unset, defaults to `max(sessionTimeoutMs / 10, 2000ms)`, capped at
   * `sessionTimeoutMs - 1`.
   */
  heartbeatIntervalMs?: number;
  /**
   * Client identity tag stored on every ephemeral record this client
   * creates. If unset, a random UUID hex is generated.
   */
  clientIdentifier?: string;
}

export interface PutOptions {
  partitionKey?: string;
  expectedVersionId?: number;
  /**
   * If true, the record is tied to this client's session on its shard
   * and is automatically removed when the session closes or expires.
   */
  ephemeral?: boolean;
  /** Secondary index entries: `{ indexName: secondaryKey }`. */
  secondaryIndexes?: Record<string, string>;
  /**
   * Server-assigned sequential suffixes for the key. Requires `partitionKey`
   * and is incompatible with `expectedVersionId`.
   */
  sequenceKeysDeltas?: number[];
}

export interface DeleteOptions {
  partitionKey?: string;
  expectedVersionId?: number;
}

export interface DeleteRangeOptions {
  /** If set, only the shard owning this partition key is affected. */
  partitionKey?: string;
}

export interface GetOptions {
  partitionKey?: string;
  comparisonType?: ComparisonType;
  includeValue?: boolean;
  /** Name of a secondary index to query. */
  useIndex?: string;
}

export interface ListOptions {
  /** If set, only the shard owning this partition key is queried. */
  partitionKey?: string;
  /** Name of a secondary index to query. */
  useIndex?: string;
}

export interface RangeScanOptions {
  /** If set, only the shard owning this partition key is scanned. */
  partitionKey?: string;
  /** Name of a secondary index to query. */
  useIndex?: string;
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
  private readonly sessionManager: SessionManager;
  private closed = false;

  private constructor(
    pool: ConnectionPool,
    discovery: ServiceDiscovery,
    sessionManager: SessionManager,
  ) {
    this.pool = pool;
    this.discovery = discovery;
    this.sessionManager = sessionManager;
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
    const sessionManager = new SessionManager({
      discovery,
      sessionTimeoutMs: options.sessionTimeoutMs ?? 30_000,
      heartbeatIntervalMs: options.heartbeatIntervalMs,
      clientIdentifier: options.clientIdentifier,
    });
    return new OxiaClient(pool, discovery, sessionManager);
  }

  async put(key: string, value: string | Uint8Array, opts: PutOptions = {}): Promise<PutResult> {
    this.ensureOpen();

    if (opts.sequenceKeysDeltas !== undefined) {
      if (opts.sequenceKeysDeltas.length === 0) {
        throw new InvalidOptionsError('sequenceKeysDeltas must not be empty');
      }
      if (opts.partitionKey === undefined) {
        throw new InvalidOptionsError('sequenceKeysDeltas requires partitionKey');
      }
      if (opts.expectedVersionId !== undefined) {
        throw new InvalidOptionsError(
          'sequenceKeysDeltas is incompatible with expectedVersionId',
        );
      }
    }

    const leader = this.discovery.getLeaderForKey(key, opts.partitionKey);
    const pr: {
      key: string;
      value: Uint8Array;
      expectedVersionId?: Long;
      partitionKey?: string;
      sequenceKeyDelta: Long[];
      secondaryIndexes: SecondaryIndex[];
      sessionId?: Long;
      clientIdentity?: string;
    } = {
      key,
      value: coerceValue(value),
      expectedVersionId:
        opts.expectedVersionId !== undefined ? fromNumber(opts.expectedVersionId) : undefined,
      partitionKey: opts.partitionKey,
      sequenceKeyDelta: (opts.sequenceKeysDeltas ?? []).map(fromNumber),
      secondaryIndexes: toSecondaryIndexes(opts.secondaryIndexes),
    };
    if (opts.ephemeral) {
      const session = await this.sessionManager.getSession(leader.shard);
      pr.sessionId = fromNumber(session.sessionId);
      pr.clientIdentity = session.clientIdentifier;
    }
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

  async deleteRange(
    minKeyInclusive: string,
    maxKeyExclusive: string,
    opts: DeleteRangeOptions = {},
  ): Promise<void> {
    this.ensureOpen();
    const leaders =
      opts.partitionKey !== undefined
        ? [this.discovery.getLeaderForKey(opts.partitionKey, opts.partitionKey)]
        : this.discovery.getAllShardLeaders();

    await Promise.all(
      leaders.map(async (leader) => {
        const resp = await callUnary<WriteResponse>((cb) =>
          leader.client.write(
            {
              shard: fromNumber(leader.shard),
              puts: [],
              deletes: [],
              deleteRanges: [
                {
                  startInclusive: minKeyInclusive,
                  endExclusive: maxKeyExclusive,
                },
              ],
            },
            cb,
          ),
        );
        checkStatus(resp.deleteRanges[0].status);
      }),
    );
  }

  async get(key: string, opts: GetOptions = {}): Promise<GetResult> {
    this.ensureOpen();
    const cmp = opts.comparisonType ?? ComparisonType.EQUAL;
    const includeValue = opts.includeValue ?? true;
    const singleShard =
      opts.partitionKey !== undefined ||
      (cmp === ComparisonType.EQUAL && opts.useIndex === undefined);

    if (singleShard) {
      const leader = this.discovery.getLeaderForKey(key, opts.partitionKey);
      return await getSingleShard(leader.client, leader.shard, key, cmp, includeValue, opts.useIndex);
    }

    // Non-EQUAL or secondary-index lookup without a partition key: fan out
    // to every shard and pick the best match by hierarchical key order.
    const leaders = this.discovery.getAllShardLeaders();
    const settled = await Promise.allSettled(
      leaders.map((leader) =>
        getSingleShard(leader.client, leader.shard, key, cmp, includeValue, opts.useIndex),
      ),
    );
    const results: GetResult[] = [];
    for (const s of settled) {
      if (s.status === 'fulfilled') {
        results.push(s.value);
      } else if (!(s.reason instanceof KeyNotFoundError)) {
        throw s.reason;
      }
    }
    if (results.length === 0) throw new KeyNotFoundError();
    results.sort((a, b) => compareWithSlash(a.key, b.key));
    switch (cmp) {
      case ComparisonType.EQUAL:
      case ComparisonType.CEILING:
      case ComparisonType.HIGHER:
        return results[0];
      case ComparisonType.FLOOR:
      case ComparisonType.LOWER:
        return results[results.length - 1];
      default:
        throw new OxiaError(`unknown comparison type: ${cmp}`);
    }
  }

  async list(
    minKeyInclusive: string,
    maxKeyExclusive: string,
    opts: ListOptions = {},
  ): Promise<string[]> {
    this.ensureOpen();
    const leaders =
      opts.partitionKey !== undefined
        ? [this.discovery.getLeaderForKey(opts.partitionKey, opts.partitionKey)]
        : this.discovery.getAllShardLeaders();

    const perShard = await Promise.all(
      leaders.map((leader) =>
        listFromShard(leader, minKeyInclusive, maxKeyExclusive, opts.useIndex),
      ),
    );
    if (perShard.length === 1) return perShard[0];
    const all = perShard.flat();
    all.sort(compareWithSlash);
    return all;
  }

  rangeScan(
    minKeyInclusive: string,
    maxKeyExclusive: string,
    opts: RangeScanOptions = {},
  ): AsyncIterable<GetResult> {
    this.ensureOpen();
    const leaders =
      opts.partitionKey !== undefined
        ? [this.discovery.getLeaderForKey(opts.partitionKey, opts.partitionKey)]
        : this.discovery.getAllShardLeaders();

    const streams = leaders.map((leader) =>
      rangeScanFromShard(leader, minKeyInclusive, maxKeyExclusive, opts.useIndex),
    );
    if (streams.length === 1) return streams[0];
    return mergeSorted(streams, (a, b) => compareWithSlash(a.key, b.key));
  }

  async close(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    // Close sessions first so the server cleans up ephemerals before the
    // channels go away.
    await this.sessionManager.close();
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

function toSecondaryIndexes(
  indexes: Record<string, string> | undefined,
): SecondaryIndex[] {
  if (!indexes) return [];
  return Object.entries(indexes).map(([indexName, secondaryKey]) => ({
    indexName,
    secondaryKey,
  }));
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
  useIndex: string | undefined,
): Promise<GetResult> {
  const stream = client.read({
    shard: fromNumber(shard),
    gets: [
      {
        key,
        includeValue,
        comparisonType: cmp,
        secondaryIndexName: useIndex,
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

async function listFromShard(
  leader: Leader,
  minKeyInclusive: string,
  maxKeyExclusive: string,
  useIndex: string | undefined,
): Promise<string[]> {
  const stream = leader.client.list({
    shard: fromNumber(leader.shard),
    startInclusive: minKeyInclusive,
    endExclusive: maxKeyExclusive,
    secondaryIndexName: useIndex,
    includeInternalKeys: false,
  });
  const keys: string[] = [];
  for await (const resp of stream as AsyncIterable<ListResponse>) {
    if (resp.keys?.length) keys.push(...resp.keys);
  }
  return keys;
}

async function* rangeScanFromShard(
  leader: Leader,
  minKeyInclusive: string,
  maxKeyExclusive: string,
  useIndex: string | undefined,
): AsyncIterable<GetResult> {
  const stream = leader.client.rangeScan({
    shard: fromNumber(leader.shard),
    startInclusive: minKeyInclusive,
    endExclusive: maxKeyExclusive,
    secondaryIndexName: useIndex,
    includeInternalKeys: false,
  });
  try {
    for await (const resp of stream as AsyncIterable<RangeScanResponse>) {
      for (const rec of resp.records ?? []) {
        if (!rec.version) continue;
        yield {
          key: rec.key ?? '',
          value: rec.value,
          version: versionFromProto(rec.version),
        };
      }
    }
  } finally {
    stream.cancel();
  }
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
