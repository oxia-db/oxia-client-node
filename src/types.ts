// Copyright 2025 The Oxia Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

import { toNumber, toOptionalNumber, toOptionalString } from './internal/longs.js';
import { KeyComparisonType, type Version as PbVersion } from './proto/generated/client.js';

/**
 * Key comparison mode for {@link OxiaClient.get}.
 *
 * `EQUAL` is the default and does an exact-match lookup. The other four
 * modes treat the provided key as a probe into the sorted key space and
 * return the nearest record in the direction specified:
 *
 * - `FLOOR`   — the highest key `<=` the probe
 * - `CEILING` — the lowest key `>=` the probe
 * - `LOWER`   — the highest key strictly `<` the probe
 * - `HIGHER`  — the lowest key strictly `>` the probe
 *
 * Mirrors the server-side proto enum `KeyComparisonType`.
 */
export const ComparisonType = {
  /** Exact match. */
  EQUAL: KeyComparisonType.EQUAL,
  /** Highest key `<=` the supplied key. */
  FLOOR: KeyComparisonType.FLOOR,
  /** Lowest key `>=` the supplied key. */
  CEILING: KeyComparisonType.CEILING,
  /** Highest key strictly `<` the supplied key. */
  LOWER: KeyComparisonType.LOWER,
  /** Lowest key strictly `>` the supplied key. */
  HIGHER: KeyComparisonType.HIGHER,
} as const;
export type ComparisonType = (typeof ComparisonType)[keyof typeof ComparisonType];

/**
 * Sentinel value for the `expectedVersionId` option on
 * {@link OxiaClient.put}. Asserts that no record currently exists for
 * the key; the put fails with {@link UnexpectedVersionIdError} if one
 * does.
 */
export const EXPECTED_RECORD_DOES_NOT_EXIST = -1;

/**
 * Metadata about the state of an Oxia record.
 *
 * Returned as part of every {@link PutResult}, {@link GetResult}, and
 * record emitted by {@link OxiaClient.rangeScan}. All timestamps come
 * from the server's clock.
 */
export class Version {
  /**
   * Monotonically increasing version identifier assigned by the server.
   * Every write bumps this value; pass it back as `expectedVersionId`
   * for optimistic concurrency.
   */
  readonly versionId: number;
  /**
   * Number of modifications made since the record was (re)created.
   * Zero on the initial put; resets to `0` if the record is deleted and
   * recreated.
   */
  readonly modificationsCount: number;
  /**
   * When the current incarnation of the record was first created. If a
   * record is deleted and recreated, this timestamp resets.
   */
  readonly createdTimestamp: Date;
  /** When the record was last modified. */
  readonly modifiedTimestamp: Date;
  /**
   * Session identifier for ephemeral records. `undefined` for
   * non-ephemeral records.
   */
  readonly sessionId?: number;
  /**
   * Identity string of the client that last wrote this ephemeral
   * record. `undefined` for non-ephemeral records.
   */
  readonly clientIdentity?: string;

  constructor(params: {
    versionId: number;
    modificationsCount: number;
    createdTimestamp: Date;
    modifiedTimestamp: Date;
    sessionId?: number;
    clientIdentity?: string;
  }) {
    this.versionId = params.versionId;
    this.modificationsCount = params.modificationsCount;
    this.createdTimestamp = params.createdTimestamp;
    this.modifiedTimestamp = params.modifiedTimestamp;
    this.sessionId = params.sessionId;
    this.clientIdentity = params.clientIdentity;
  }

  /**
   * True if the record is tied to a client session (i.e. created with
   * `{ ephemeral: true }`) and will be deleted when that session ends.
   */
  isEphemeral(): boolean {
    return this.sessionId !== undefined;
  }
}

/** @internal */
export function versionFromProto(pb: PbVersion): Version {
  return new Version({
    versionId: toNumber(pb.versionId),
    modificationsCount: toNumber(pb.modificationsCount),
    createdTimestamp: new Date(toNumber(pb.createdTimestamp)),
    modifiedTimestamp: new Date(toNumber(pb.modifiedTimestamp)),
    sessionId: toOptionalNumber(pb.sessionId),
    clientIdentity: toOptionalString(pb.clientIdentity),
  });
}

/** Result of {@link OxiaClient.put}. */
export interface PutResult {
  /**
   * The stored key. Normally this equals the key passed to
   * {@link OxiaClient.put}, except when `sequenceKeysDeltas` is used —
   * the server then appends one or more zero-padded suffixes and
   * returns the final assigned key here.
   */
  key: string;
  /** Version metadata for the write. */
  version: Version;
}

/** Result of {@link OxiaClient.get}. */
export interface GetResult {
  /**
   * The matched record's primary key. For non-EQUAL comparisons and
   * secondary-index lookups, this is the key that actually matched —
   * not the probe/query value that was passed in.
   */
  key: string;
  /**
   * The stored value, or `undefined` if the caller passed
   * `includeValue: false`.
   */
  value: Uint8Array | undefined;
  /** Version metadata for the record. */
  version: Version;
}

/** Kind of change a {@link Notification} represents. */
export const NotificationType = {
  /** A record that did not exist before was created. */
  KEY_CREATED: 0,
  /** An existing record's value was updated. */
  KEY_MODIFIED: 1,
  /** A record was deleted. */
  KEY_DELETED: 2,
  /** A range of keys was deleted via {@link OxiaClient.deleteRange}. */
  KEY_RANGE_DELETED: 3,
} as const;
export type NotificationType = (typeof NotificationType)[keyof typeof NotificationType];

/** A single change event emitted by {@link OxiaClient.getNotifications}. */
export interface Notification {
  /** The key whose state changed. */
  key: string;
  /** Kind of change. */
  type: NotificationType;
  /**
   * Current version id of the record, or `0` for delete events.
   */
  versionId: number;
  /**
   * For {@link NotificationType.KEY_RANGE_DELETED}, the exclusive end of
   * the deleted range. `undefined` for other notification types.
   */
  keyRangeEnd?: string;
}

/**
 * An `AsyncIterable` with an explicit teardown hook. Returned by
 * {@link OxiaClient.getNotifications} and
 * {@link OxiaClient.getSequenceUpdates}.
 *
 * Either iterate with `for await (...)` and call `close()` when done,
 * or `break` out of the loop — the async iterator's `return()` method
 * delegates to `close()` as well.
 */
export interface CloseableAsyncIterable<T> extends AsyncIterable<T> {
  /** Stop the subscription and end the iterator. Idempotent. */
  close(): void;
}
