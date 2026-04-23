// Copyright 2025 The Oxia Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

import { KeyComparisonType, type Version as PbVersion } from './proto/generated/client.js';
import { toNumber, toOptionalNumber, toOptionalString } from './internal/longs.js';

/**
 * Key comparison mode for {@link OxiaClient.get}.
 *
 * Mirrors the proto `KeyComparisonType` enum.
 */
export const ComparisonType = {
  EQUAL: KeyComparisonType.EQUAL,
  FLOOR: KeyComparisonType.FLOOR,
  CEILING: KeyComparisonType.CEILING,
  LOWER: KeyComparisonType.LOWER,
  HIGHER: KeyComparisonType.HIGHER,
} as const;
export type ComparisonType = (typeof ComparisonType)[keyof typeof ComparisonType];

/**
 * Pass as `expectedVersionId` to assert the record does not already exist.
 */
export const EXPECTED_RECORD_DOES_NOT_EXIST = -1;

/** Metadata about the state of an Oxia record. */
export class Version {
  readonly versionId: number;
  readonly modificationsCount: number;
  readonly createdTimestamp: Date;
  readonly modifiedTimestamp: Date;
  readonly sessionId?: number;
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

  isEphemeral(): boolean {
    return this.sessionId !== undefined;
  }
}

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
  key: string;
  version: Version;
}

/** Result of {@link OxiaClient.get}. */
export interface GetResult {
  key: string;
  value: Uint8Array | undefined;
  version: Version;
}

/** Kind of change a {@link Notification} represents. */
export const NotificationType = {
  KEY_CREATED: 0,
  KEY_MODIFIED: 1,
  KEY_DELETED: 2,
  KEY_RANGE_DELETED: 3,
} as const;
export type NotificationType = (typeof NotificationType)[keyof typeof NotificationType];

/** A single change event in the Oxia database. */
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
 * An AsyncIterable with an explicit teardown hook. Returned by
 * {@link OxiaClient.getNotifications} and {@link OxiaClient.getSequenceUpdates}.
 */
export interface CloseableAsyncIterable<T> extends AsyncIterable<T> {
  close(): void;
}
