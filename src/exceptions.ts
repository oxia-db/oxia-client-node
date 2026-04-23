// Copyright 2025 The Oxia Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

/**
 * Base class for all errors thrown by the Oxia client. Catch this if
 * you want to distinguish client-originated failures from arbitrary
 * JavaScript errors; the more specific subclasses cover the common
 * cases.
 */
export class OxiaError extends Error {
  constructor(message?: string) {
    super(message);
    this.name = 'OxiaError';
  }
}

/**
 * Thrown when the options passed to a client method are internally
 * inconsistent — for example, `sequenceKeysDeltas` without a
 * `partitionKey`, or a non-EQUAL get with `useIndex` but no valid route.
 */
export class InvalidOptionsError extends OxiaError {
  constructor(message?: string) {
    super(message);
    this.name = 'InvalidOptionsError';
  }
}

/**
 * Thrown by {@link OxiaClient.get} when no record matches the query
 * (either on exact lookup or on a non-EQUAL comparison when no key
 * in the target range exists).
 */
export class KeyNotFoundError extends OxiaError {
  constructor(message?: string) {
    super(message ?? 'key not found');
    this.name = 'KeyNotFoundError';
  }
}

/**
 * Thrown by conditional writes (`put` / `delete` with
 * `expectedVersionId`) when the server's current version for the key
 * does not match the one the caller asserted.
 */
export class UnexpectedVersionIdError extends OxiaError {
  constructor(message?: string) {
    super(message ?? 'unexpected version id');
    this.name = 'UnexpectedVersionIdError';
  }
}

/**
 * Thrown by an ephemeral `put` when the session it was attached to no
 * longer exists on the server (typically because the session timed out
 * or was closed out-of-band).
 */
export class SessionNotFoundError extends OxiaError {
  constructor(message?: string) {
    super(message ?? 'session does not exist');
    this.name = 'SessionNotFoundError';
  }
}
