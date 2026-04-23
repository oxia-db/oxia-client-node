// Copyright 2025 The Oxia Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

// Helpers to convert between `Long` (used by ts-proto for int64 fields with
// `forceLong=long`) and regular numbers. Oxia timestamps fit in 53 bits for
// centuries, and version_ids / session_ids likewise never approach 2^53 in
// practice, so converting to `number` is safe for the client surface.

import Long from 'long';

export type LongLike = Long | number | bigint | string;

export function toNumber(v: LongLike | null | undefined): number {
  if (v == null) return 0;
  if (typeof v === 'number') return v;
  if (typeof v === 'bigint') return Number(v);
  if (typeof v === 'string') return Number(v);
  return v.toNumber();
}

export function toOptionalNumber(v: LongLike | null | undefined): number | undefined {
  if (v == null) return undefined;
  return toNumber(v);
}

export function toOptionalString(v: string | null | undefined): string | undefined {
  if (v == null || v === '') return undefined;
  return v;
}

export function fromNumber(v: number): Long {
  return Long.fromNumber(v);
}
