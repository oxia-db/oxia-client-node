// Copyright 2025 The Oxia Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

import { xxh3 } from '@node-rs/xxhash';

const MASK_32 = 0xffffffffn;

/**
 * xxh3 64-bit hash of `key` truncated to the low 32 bits.
 *
 * Matches the server-side routing hash (`common/hash.Xxh332`) used to
 * assign keys to shards. Verified against the server's test vectors:
 *   "foo" -> 125730186
 *   "bar" -> 2687685474
 *   "baz" -> 862947621
 */
export function xxh332(key: string): number {
  return Number(xxh3.xxh64(key) & MASK_32);
}
