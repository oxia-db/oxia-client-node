// Copyright 2025 The Oxia Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

import { randomUUID } from 'node:crypto';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { ComparisonType, KeyNotFoundError, OxiaClient } from '../src/index.js';
import { OxiaContainer } from './oxiaContainer.js';

const newKey = () => `/test-${randomUUID().replace(/-/g, '')}`;
const decode = (v: Uint8Array | undefined) => new TextDecoder().decode(v);

describe('OxiaClient — non-EQUAL get across shards', () => {
  const container = new OxiaContainer(10);
  let client: OxiaClient;
  let prefix: string;

  beforeAll(async () => {
    await container.start();
    client = await OxiaClient.connect(container.serviceUrl());
    prefix = newKey();
    for (const [s, v] of [
      ['/a', '0'],
      ['/c', '2'],
      ['/d', '3'],
      ['/e', '4'],
      ['/g', '6'],
    ] as const) {
      await client.put(prefix + s, v);
    }
  });

  afterAll(async () => {
    await client?.close();
    await container.stop();
  });

  const at = async (key: string, cmp?: ComparisonType) => {
    const r = await client.get(prefix + key, cmp !== undefined ? { comparisonType: cmp } : {});
    return { key: r.key, value: decode(r.value) };
  };

  it('EQUAL returns the exact key', async () => {
    expect(await at('/a')).toEqual({ key: `${prefix}/a`, value: '0' });
  });

  it('FLOOR picks the greatest key <= target', async () => {
    expect(await at('/a', ComparisonType.FLOOR)).toEqual({ key: `${prefix}/a`, value: '0' });
    expect(await at('/b', ComparisonType.FLOOR)).toEqual({ key: `${prefix}/a`, value: '0' });
    expect(await at('/f', ComparisonType.FLOOR)).toEqual({ key: `${prefix}/e`, value: '4' });
  });

  it('CEILING picks the smallest key >= target', async () => {
    expect(await at('/a', ComparisonType.CEILING)).toEqual({ key: `${prefix}/a`, value: '0' });
    expect(await at('/b', ComparisonType.CEILING)).toEqual({ key: `${prefix}/c`, value: '2' });
    expect(await at('/f', ComparisonType.CEILING)).toEqual({ key: `${prefix}/g`, value: '6' });
  });

  it('LOWER picks the greatest key strictly < target', async () => {
    await expect(at('/a', ComparisonType.LOWER)).rejects.toBeInstanceOf(KeyNotFoundError);
    expect(await at('/b', ComparisonType.LOWER)).toEqual({ key: `${prefix}/a`, value: '0' });
    expect(await at('/c', ComparisonType.LOWER)).toEqual({ key: `${prefix}/a`, value: '0' });
    expect(await at('/f', ComparisonType.LOWER)).toEqual({ key: `${prefix}/e`, value: '4' });
  });

  it('HIGHER picks the smallest key strictly > target', async () => {
    expect(await at('/a', ComparisonType.HIGHER)).toEqual({ key: `${prefix}/c`, value: '2' });
    expect(await at('/b', ComparisonType.HIGHER)).toEqual({ key: `${prefix}/c`, value: '2' });
    expect(await at('/e', ComparisonType.HIGHER)).toEqual({ key: `${prefix}/g`, value: '6' });
    expect(await at('/f', ComparisonType.HIGHER)).toEqual({ key: `${prefix}/g`, value: '6' });
  });
});
