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
import type { Authentication } from './auth.js';
import { compareWithSlash } from './internal/compare.js';
import { ConnectionPool } from './internal/connectionPool.js';
import { fromNumber } from './internal/longs.js';
import { mergeSorted } from './internal/mergeStreams.js';
import { NotificationStream } from './internal/notifications.js';
import { callUnary, firstStreamMessage } from './internal/rpc.js';
import { SequenceUpdatesStream } from './internal/sequenceUpdates.js';
import { type Leader, ServiceDiscovery } from './internal/serviceDiscovery.js';
import { SessionManager } from './internal/sessions.js';
import {
  type CloseableAsyncIterable,
  ComparisonType,
  type GetResult,
  type Notification,
  type PutResult,
  versionFromProto,
} from './types.js';

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
  /**
   * Deadline applied to every unary RPC (put, delete, session management).
   * Streaming subscriptions (notifications, shard assignments, sequence
   * updates, list, rangeScan) are left unbounded. Default: 30_000.
   */
  requestTimeoutMs?: number;
  /**
   * Optional {@link Authentication} implementation whose credentials are
   * attached as gRPC metadata on every outgoing RPC.
   */
  authentication?: Authentication;
}

/** Options for {@link OxiaClient.put}. */
export interface PutOptions {
  /**
   * Override shard routing: records sharing the same `partitionKey` are
   * co-located on the same shard regardless of their record keys.
   */
  partitionKey?: string;
  /**
   * Make the write conditional on the current server-side version.
   * Pass {@link EXPECTED_RECORD_DOES_NOT_EXIST} to assert the key is
   * new. If the server's version does not match, the put fails with
   * {@link UnexpectedVersionIdError}.
   */
  expectedVersionId?: number;
  /**
   * If true, the record is tied to this client's session on its shard
   * and is automatically removed when the session closes or expires.
   */
  ephemeral?: boolean;
  /**
   * Secondary index entries to attach to the record, as
   * `{ indexName: secondaryKey }`. The entries stay in sync with the
   * record: overwrites replace the old secondary keys, and deletes
   * remove them.
   */
  secondaryIndexes?: Record<string, string>;
  /**
   * Request server-assigned sequential suffixes for the key. Each
   * delta advances a server-maintained counter (one counter per level
   * of nesting) and produces zero-padded suffix(es) joined with `-`.
   * Requires `partitionKey` and is incompatible with `expectedVersionId`.
   *
   * @example
   * ```ts
   * // first put  -> key "a-00000000000000000001"
   * // second put -> key "a-00000000000000000002"
   * await client.put('a', 'v', { sequenceKeysDeltas: [1], partitionKey: 'p' });
   * ```
   */
  sequenceKeysDeltas?: number[];
}

/** Options for {@link OxiaClient.delete}. */
export interface DeleteOptions {
  /** Override shard routing (see {@link PutOptions.partitionKey}). */
  partitionKey?: string;
  /**
   * Make the delete conditional on the current server-side version.
   * Fails with {@link UnexpectedVersionIdError} on mismatch.
   */
  expectedVersionId?: number;
}

/** Options for {@link OxiaClient.deleteRange}. */
export interface DeleteRangeOptions {
  /**
   * If set, only the shard owning this partition key is affected.
   * Otherwise the delete is fanned out to every shard.
   */
  partitionKey?: string;
}

/** Options for {@link OxiaClient.get}. */
export interface GetOptions {
  /** Override shard routing (see {@link PutOptions.partitionKey}). */
  partitionKey?: string;
  /**
   * Comparison mode. Defaults to {@link ComparisonType.EQUAL}. Non-EQUAL
   * modes query every shard when no `partitionKey` is set.
   */
  comparisonType?: ComparisonType;
  /**
   * If `false`, the response's `value` is `undefined` (metadata-only
   * read). Defaults to `true`.
   */
  includeValue?: boolean;
  /**
   * Look up the record via this secondary index (the `key` argument
   * is interpreted as a secondary key). Without a `partitionKey`, the
   * lookup is fanned out to every shard.
   */
  useIndex?: string;
}

/** Options for {@link OxiaClient.list}. */
export interface ListOptions {
  /** If set, only the shard owning this partition key is queried. */
  partitionKey?: string;
  /**
   * Treat the range bounds as secondary keys on this index. The
   * returned primary keys are those whose indexed value falls in range.
   */
  useIndex?: string;
}

/** Options for {@link OxiaClient.rangeScan}. */
export interface RangeScanOptions {
  /** If set, only the shard owning this partition key is scanned. */
  partitionKey?: string;
  /**
   * Treat the range bounds as secondary keys on this index. The
   * yielded records are those whose indexed value falls in range.
   */
  useIndex?: string;
}

/** Options for {@link OxiaClient.getSequenceUpdates}. */
export interface SequenceUpdatesOptions {
  /**
   * Partition key used to route the subscription to the shard that
   * owns the sequence. Must match the `partitionKey` used on the puts
   * that created the sequence. Required.
   */
  partitionKey: string;
}

/**
 * Asynchronous client for the Oxia service.
 *
 * Build one with {@link OxiaClient.connect}. Call {@link OxiaClient.close}
 * when done to release the gRPC channels, stop the background
 * shard-assignment watcher, and cleanly tear down any open sessions.
 *
 * The client is safe to use from multiple async contexts — every
 * method is non-blocking and internally dispatches to the appropriate
 * shard leader.
 *
 * @example
 * ```ts
 * const client = await OxiaClient.connect('localhost:6648');
 * try {
 *   await client.put('/users/alice', 'hello');
 *   const r = await client.get('/users/alice');
 *   console.log(new TextDecoder().decode(r.value));
 * } finally {
 *   await client.close();
 * }
 * ```
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

  /**
   * Connect to an Oxia service and wait for the initial shard
   * assignments to arrive before returning.
   *
   * @param serviceAddress `host:port` of any Oxia server. The client
   *   uses this address only to discover shard assignments; subsequent
   *   requests go directly to each shard's leader.
   * @param options See {@link OxiaClientOptions}.
   * @throws If the initial shard-assignment fetch does not complete
   *   within `initTimeoutMs`.
   */
  static async connect(
    serviceAddress: string,
    options: OxiaClientOptions = {},
  ): Promise<OxiaClient> {
    const namespace = options.namespace ?? 'default';
    const pool = new ConnectionPool({
      requestTimeoutMs: options.requestTimeoutMs ?? 30_000,
      authentication: options.authentication,
    });
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

  /**
   * Associate a value with a key.
   *
   * @param key   The key to write. When {@link PutOptions.sequenceKeysDeltas}
   *   is used this is the prefix; the returned
   *   {@link PutResult.key} is the full server-assigned key.
   * @param value Bytes to store. Strings are encoded as UTF-8.
   * @param opts  See {@link PutOptions}.
   * @returns The final key and version metadata for the written record.
   * @throws {@link UnexpectedVersionIdError} if `expectedVersionId`
   *   mismatches the server's current version.
   * @throws {@link SessionNotFoundError} for an ephemeral put when the
   *   client's session on the target shard no longer exists server-side.
   * @throws {@link InvalidOptionsError} for incompatible option
   *   combinations (e.g. `sequenceKeysDeltas` without `partitionKey`).
   */
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

  /**
   * Delete the record stored at `key`.
   *
   * @param key  The key to delete.
   * @param opts See {@link DeleteOptions}.
   * @returns `true` if a record existed and was removed, `false` if no
   *   record was found at that key (or at that key within the target
   *   partition).
   * @throws {@link UnexpectedVersionIdError} if `expectedVersionId`
   *   mismatches the server's current version.
   */
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

  /**
   * Delete every record whose key falls in `[minKeyInclusive, maxKeyExclusive)`.
   *
   * Without a `partitionKey` the operation is fanned out to every shard;
   * with one it targets a single shard.
   *
   * Bounds are interpreted in Oxia's hierarchical key order —
   * see https://oxia-db.github.io/docs/features/oxia-key-sorting
   *
   * @param minKeyInclusive Start of the range (inclusive).
   * @param maxKeyExclusive End of the range (exclusive).
   * @param opts            See {@link DeleteRangeOptions}.
   */
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

  /**
   * Retrieve the record for a key.
   *
   * With the default `EQUAL` comparison this is an exact-match lookup
   * on a single shard. With any other {@link ComparisonType} — or with
   * `useIndex` — the client fans out to every shard and picks the best
   * match in hierarchical key order.
   *
   * @param key  The key to look up. When `useIndex` is set, this is
   *   treated as a secondary key for that index.
   * @param opts See {@link GetOptions}.
   * @returns The matched record (primary key, value, and version).
   * @throws {@link KeyNotFoundError} if no record matches.
   */
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

  /**
   * List every key in `[minKeyInclusive, maxKeyExclusive)`.
   *
   * Without a `partitionKey`, each shard is queried in parallel and the
   * results are merged. Keys are returned in Oxia's hierarchical sort
   * order — see https://oxia-db.github.io/docs/features/oxia-key-sorting
   *
   * Use {@link OxiaClient.rangeScan} if you also need the values.
   *
   * @param minKeyInclusive Start of the range (inclusive).
   * @param maxKeyExclusive End of the range (exclusive).
   * @param opts            See {@link ListOptions}.
   * @returns Sorted keys in the range, or `[]` if none match.
   */
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

  /**
   * Stream records in `[minKeyInclusive, maxKeyExclusive)` as an
   * `AsyncIterable<GetResult>`.
   *
   * Without a `partitionKey`, one streaming RPC is opened per shard and
   * the results are merged in hierarchical key order. Break out of the
   * `for await (...)` loop to cancel the remaining server streams.
   *
   * Use {@link OxiaClient.list} if you only need the keys (cheaper).
   *
   * @param minKeyInclusive Start of the range (inclusive).
   * @param maxKeyExclusive End of the range (exclusive).
   * @param opts            See {@link RangeScanOptions}.
   */
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

  /**
   * Subscribe to change notifications across every shard. Returns an
   * AsyncIterable that yields one {@link Notification} per change; the
   * returned object's `close()` method stops the subscription and ends
   * the iterator. Multiple subscriptions can run concurrently.
   */
  getNotifications(): CloseableAsyncIterable<Notification> {
    this.ensureOpen();
    return new NotificationStream(this.discovery);
  }

  /**
   * Subscribe to updates on a sequential key. Yields the latest assigned
   * key each time the sequence advances; the returned object's `close()`
   * method ends the subscription.
   *
   * @param prefixKey The key prefix used with sequence-key puts.
   * @param options   Requires `partitionKey` to route to the owning shard.
   */
  getSequenceUpdates(
    prefixKey: string,
    options: SequenceUpdatesOptions,
  ): CloseableAsyncIterable<string> {
    this.ensureOpen();
    if (!options.partitionKey) {
      throw new InvalidOptionsError('getSequenceUpdates requires a partitionKey');
    }
    const leader = this.discovery.getLeaderForKey(options.partitionKey, options.partitionKey);
    return new SequenceUpdatesStream(this.discovery, leader.shard, prefixKey);
  }

  /**
   * Release all resources held by the client: close any open sessions
   * (triggering server-side removal of this client's ephemerals), stop
   * the shard-assignment watcher, and close every gRPC channel.
   *
   * Idempotent. Any subsequent client call throws {@link OxiaError}.
   */
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
