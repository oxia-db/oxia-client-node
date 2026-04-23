// Copyright 2025 The Oxia Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

/**
 * Hierarchical key comparison: split by `/`, compare segment-by-segment,
 * and treat a shorter prefix as less than a longer one when the prefixes
 * match. Mirrors the Python client's `compare_with_slash` and the Java
 * client's `CompareWithSlash`.
 *
 * Used to merge per-shard results back into the server's hierarchical
 * sort order. See https://oxia-db.github.io/docs/features/oxia-key-sorting
 */
export function compareWithSlash(a: string, b: string): number {
  let ai = 0;
  let bi = 0;
  while (ai < a.length && bi < b.length) {
    const slashA = a.indexOf('/', ai);
    const slashB = b.indexOf('/', bi);
    if (slashA < 0 && slashB < 0) {
      return cmpSlice(a, ai, a.length, b, bi, b.length);
    }
    if (slashA < 0) return -1;
    if (slashB < 0) return 1;

    const segCmp = cmpSlice(a, ai, slashA, b, bi, slashB);
    if (segCmp !== 0) return segCmp;

    ai = slashA + 1;
    bi = slashB + 1;
  }

  const aRest = a.length - ai;
  const bRest = b.length - bi;
  if (aRest < bRest) return -1;
  if (aRest > 0) return 1;
  return 0;
}

function cmpSlice(a: string, aStart: number, aEnd: number, b: string, bStart: number, bEnd: number): number {
  const aLen = aEnd - aStart;
  const bLen = bEnd - bStart;
  const n = Math.min(aLen, bLen);
  for (let i = 0; i < n; i++) {
    const ca = a.charCodeAt(aStart + i);
    const cb = b.charCodeAt(bStart + i);
    if (ca !== cb) return ca < cb ? -1 : 1;
  }
  if (aLen !== bLen) return aLen < bLen ? -1 : 1;
  return 0;
}
