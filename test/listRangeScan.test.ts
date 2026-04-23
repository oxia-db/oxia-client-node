// Copyright 2025 The Oxia Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

import { randomUUID } from 'node:crypto';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { OxiaClient } from '../src/index.js';
import { compareWithSlash } from '../src/internal/compare.js';
import { OxiaContainer } from './oxiaContainer.js';

const newKey = () => `/test-${randomUUID().replace(/-/g, '')}`;
const decode = (v: Uint8Array | undefined) => new TextDecoder().decode(v);

describe('OxiaClient — list / rangeScan / deleteRange (multi-shard)', () => {
  const container = new OxiaContainer(5);
  let client: OxiaClient;

  beforeAll(async () => {
    await container.start();
    client = await OxiaClient.connect(container.serviceUrl());
  });

  afterAll(async () => {
    await client?.close();
    await container.stop();
  });

  it('list returns keys in hierarchical order across shards', async () => {
    const prefix = newKey();
    const suffixes = ['/a', '/b', '/c', '/d'];
    for (const s of suffixes) await client.put(prefix + s, 'v');

    const keys = await client.list(`${prefix}/a`, `${prefix}/e`);
    expect(keys).toEqual(suffixes.map((s) => prefix + s));
  });

  it('list on an empty range returns []', async () => {
    const prefix = newKey();
    await client.put(`${prefix}/a`, '0');
    expect(await client.list(`${prefix}/y`, `${prefix}/z`)).toEqual([]);
    expect(await client.list('ZZZ_nonexistent_a', 'ZZZ_nonexistent_z')).toEqual([]);
  });

  it('rangeScan yields keys and values in hierarchical order', async () => {
    const prefix = newKey();
    const pairs: [string, string][] = [
      ['/b', '1'],
      ['/a', '0'],
      ['/d', '3'],
      ['/c', '2'],
      ['/e', '4'],
    ];
    for (const [s, v] of pairs) await client.put(prefix + s, v);

    const seen: [string, string][] = [];
    for await (const r of client.rangeScan(`${prefix}/b`, `${prefix}/e`)) {
      seen.push([r.key, decode(r.value)]);
    }
    expect(seen).toEqual([
      [`${prefix}/b`, '1'],
      [`${prefix}/c`, '2'],
      [`${prefix}/d`, '3'],
    ]);
  });

  it('deleteRange removes only keys in [min, max)', async () => {
    const prefix = newKey();
    for (const s of ['/a', '/b', '/c', '/d']) await client.put(prefix + s, 'v');

    await client.deleteRange(`${prefix}/b`, `${prefix}/d`);

    expect(await client.list(`${prefix}/a`, `${prefix}/e`)).toEqual([`${prefix}/a`, `${prefix}/d`]);
  });
});

describe('OxiaClient — hierarchical sort consistency across shards', () => {
  // Isolated container: the server's hierarchical key encoding (count-of-/
  // prefix) means ranges that span different slash-counts can bleed across
  // unrelated prefixes. Any keys from other tests in the same server would
  // skew this test's cross-shard merge assertion.
  const container = new OxiaContainer(3);
  let client: OxiaClient;

  beforeAll(async () => {
    await container.start();
    client = await OxiaClient.connect(container.serviceUrl());
  });

  afterAll(async () => {
    await client?.close();
    await container.stop();
  });

  it('list preserves hierarchical ordering when it diverges from natural ordering', async () => {
    const prefix = newKey();
    const suffixes = ['/a', '/aa', '/a/b', '/a/b/c', '/aa/a', '/b', '/b/a', '/b/ab'];
    const keys = suffixes.map((s) => prefix + s);
    for (const k of keys) await client.put(k, 'v');

    const lo = `${prefix}/`;
    const hi = `${prefix}/~/~/~/~`;
    const expected = [...keys].sort(compareWithSlash);
    expect(await client.list(lo, hi)).toEqual(expected);

    // Guard: this test would silently pass if hierarchical == natural.
    expect([...keys].sort()).not.toEqual(expected);
  });
});

describe('OxiaClient — partition routing (multi-shard)', () => {
  const container = new OxiaContainer(5);
  let client: OxiaClient;

  beforeAll(async () => {
    await container.start();
    client = await OxiaClient.connect(container.serviceUrl());
  });

  afterAll(async () => {
    await client?.close();
    await container.stop();
  });

  it('partitionKey co-locates records; list/delete/deleteRange are partition-scoped', async () => {
    const pk = 'pk-1';
    await client.put('item-a', '0', { partitionKey: pk });
    await client.put('item-b', '1', { partitionKey: pk });
    await client.put('item-c', '2', { partitionKey: pk });

    // Same-partition reads/lists work.
    const got = await client.get('item-a', { partitionKey: pk });
    expect(decode(got.value)).toBe('0');
    expect(await client.list('item-', 'item-~', { partitionKey: pk })).toEqual([
      'item-a',
      'item-b',
      'item-c',
    ]);

    // A wrong partition key routes to (with overwhelming probability) a
    // different shard — list returns nothing.
    expect(await client.list('item-', 'item-~', { partitionKey: 'pk-wrong' })).toEqual([]);

    // delete on the wrong partition: no-op.
    expect(await client.delete('item-c', { partitionKey: 'pk-wrong' })).toBe(false);
    expect(await client.delete('item-c', { partitionKey: pk })).toBe(true);

    // deleteRange with wrong partition: no-op; with right partition: clears.
    await client.deleteRange('item-', 'item-~', { partitionKey: 'pk-wrong' });
    expect(await client.list('item-', 'item-~', { partitionKey: pk })).toEqual([
      'item-a',
      'item-b',
    ]);

    await client.deleteRange('item-', 'item-~', { partitionKey: pk });
    expect(await client.list('item-', 'item-~', { partitionKey: pk })).toEqual([]);
  });
});
