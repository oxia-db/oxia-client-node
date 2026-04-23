// Copyright 2025 The Oxia Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { ComparisonType, KeyNotFoundError, OxiaClient } from '../src/index.js';
import { OxiaContainer } from './oxiaContainer.js';

const decode = (v: Uint8Array | undefined) => new TextDecoder().decode(v);

describe('OxiaClient — secondary indexes (single shard)', () => {
  const container = new OxiaContainer(1);
  let client: OxiaClient;

  beforeAll(async () => {
    await container.start();
    client = await OxiaClient.connect(container.serviceUrl());
    for (let i = 0; i < 10; i++) {
      const key = String.fromCharCode('a'.charCodeAt(0) + i);
      await client.put(key, String(i), { secondaryIndexes: { 'value-idx': String(i) } });
    }
  });

  afterAll(async () => {
    await client?.close();
    await container.stop();
  });

  it('list by secondary index returns primary keys for matches', async () => {
    expect(await client.list('1', '4', { useIndex: 'value-idx' })).toEqual(['b', 'c', 'd']);
  });

  it('rangeScan by secondary index yields primary key+value in secondary order', async () => {
    const got: [string, string][] = [];
    for await (const r of client.rangeScan('1', '4', { useIndex: 'value-idx' })) {
      got.push([r.key, decode(r.value)]);
    }
    expect(got).toEqual([
      ['b', '1'],
      ['c', '2'],
      ['d', '3'],
    ]);
  });

  it('EQUAL get by secondary-index value returns the primary key', async () => {
    // Indexed value -> primary key lookup: the returned key is the primary
    // key of the matching record ('alpha'), not the secondary key.
    await client.put('alpha', 'VALUE_007', { secondaryIndexes: { idx: '007' } });

    const defaultCmp = await client.get('007', { useIndex: 'idx' });
    expect(defaultCmp.key).toBe('alpha');
    expect(decode(defaultCmp.value)).toBe('VALUE_007');

    const explicit = await client.get('007', {
      useIndex: 'idx',
      comparisonType: ComparisonType.EQUAL,
    });
    expect(explicit.key).toBe('alpha');
  });
});

describe('OxiaClient — secondary-index FLOOR/CEILING/HIGHER across shards', () => {
  const container = new OxiaContainer(10);
  let client: OxiaClient;

  beforeAll(async () => {
    await container.start();
    client = await OxiaClient.connect(container.serviceUrl());
    // Primary keys b..j map to secondary keys 001..009, zero-padded so the
    // server's byte-sort order is numeric.
    for (let i = 1; i <= 9; i++) {
      const key = String.fromCharCode('a'.charCodeAt(0) + i);
      const val = i.toString().padStart(3, '0');
      await client.put(key, val, { secondaryIndexes: { 'value-idx': val } });
    }
  });

  afterAll(async () => {
    await client?.close();
    await container.stop();
  });

  const lookup = async (q: string, cmp?: ComparisonType) => {
    const r = await client.get(q, { useIndex: 'value-idx', ...(cmp !== undefined ? { comparisonType: cmp } : {}) });
    return { key: r.key, value: decode(r.value) };
  };

  it('EQUAL: hit and miss', async () => {
    expect(await lookup('001')).toEqual({ key: 'b', value: '001' });
    await expect(lookup('000')).rejects.toBeInstanceOf(KeyNotFoundError);
    await expect(lookup('999')).rejects.toBeInstanceOf(KeyNotFoundError);
  });

  it('FLOOR: largest indexed value <= query', async () => {
    await expect(lookup('000', ComparisonType.FLOOR)).rejects.toBeInstanceOf(KeyNotFoundError);
    expect(await lookup('005', ComparisonType.FLOOR)).toEqual({ key: 'f', value: '005' });
    expect(await lookup('999', ComparisonType.FLOOR)).toEqual({ key: 'j', value: '009' });
  });

  it('CEILING: smallest indexed value >= query', async () => {
    expect(await lookup('000', ComparisonType.CEILING)).toEqual({ key: 'b', value: '001' });
    expect(await lookup('005', ComparisonType.CEILING)).toEqual({ key: 'f', value: '005' });
    await expect(lookup('999', ComparisonType.CEILING)).rejects.toBeInstanceOf(KeyNotFoundError);
  });

  it('HIGHER: smallest indexed value strictly > query', async () => {
    expect(await lookup('000', ComparisonType.HIGHER)).toEqual({ key: 'b', value: '001' });
    expect(await lookup('001', ComparisonType.HIGHER)).toEqual({ key: 'c', value: '002' });
    await expect(lookup('009', ComparisonType.HIGHER)).rejects.toBeInstanceOf(KeyNotFoundError);
  });
});
