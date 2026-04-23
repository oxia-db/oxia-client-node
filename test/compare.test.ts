// Copyright 2025 The Oxia Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

import { describe, expect, it } from 'vitest';
import { compareWithSlash } from '../src/internal/compare.js';

// Normalize to 0 (not -0) so expect(...).toBe uses value equality cleanly —
// `toBe` relies on `Object.is`, which distinguishes +0 from -0.
const sign = (n: number): 1 | 0 | -1 => (n < 0 ? -1 : n > 0 ? 1 : 0);

describe('compareWithSlash', () => {
  it('returns 0 for identical strings', () => {
    expect(compareWithSlash('a/b/c', 'a/b/c')).toBe(0);
    expect(compareWithSlash('', '')).toBe(0);
  });

  it('shorter prefix ranks before longer one at same boundary', () => {
    expect(sign(compareWithSlash('a', 'a/b'))).toBe(-1);
    expect(sign(compareWithSlash('a/b', 'a'))).toBe(1);
  });

  it('compares by segments, not raw codepoints', () => {
    // Natural sort would place 'a/b' < 'a1' (since '/' (0x2f) < '1' (0x31)),
    // but hierarchically 'a' == 'a' + depth bonus vs 'a1' means 'a/b' > 'a1'.
    expect(sign(compareWithSlash('a1', 'a/b'))).toBe(-1);
  });

  it('handles common Oxia prefix layouts correctly', () => {
    const keys = ['/test/a', '/test/aa', '/test/a/b', '/test/a/b/c', '/test/aa/a'];
    const sorted = [...keys].sort(compareWithSlash);
    // Hierarchical order: at each segment boundary, a missing next segment
    // ranks before a present one. So /test/aa (ends after `aa`) comes
    // before /test/a/b (has another segment) even though `aa` > `a`.
    expect(sorted).toEqual(['/test/a', '/test/aa', '/test/a/b', '/test/a/b/c', '/test/aa/a']);
  });

  it('is antisymmetric: sign(cmp(a,b)) + sign(cmp(b,a)) === 0', () => {
    // Avoid `-sign(x)` in the assertion so `-0` vs `0` doesn't trip
    // Object.is-based `toBe`.
    const samples = ['', 'a', 'a/b', 'a/bb', 'a/b/c', 'b', '/x/y'];
    for (const a of samples) {
      for (const b of samples) {
        expect(sign(compareWithSlash(a, b)) + sign(compareWithSlash(b, a))).toBe(0);
      }
    }
  });
});
