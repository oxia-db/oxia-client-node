// Copyright 2025 The Oxia Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { InvalidOptionsError, KeyNotFoundError, OxiaClient } from '../src/index.js';
import { OxiaContainer } from './oxiaContainer.js';

const decode = (v: Uint8Array | undefined) => new TextDecoder().decode(v);
const pad = (n: number) => n.toString().padStart(20, '0');

describe('OxiaClient — sequence keys', () => {
  const container = new OxiaContainer(2);
  let client: OxiaClient;

  beforeAll(async () => {
    await container.start();
    client = await OxiaClient.connect(container.serviceUrl());
  });

  afterAll(async () => {
    await client?.close();
    await container.stop();
  });

  it('rejects sequenceKeysDeltas without partitionKey', async () => {
    await expect(client.put('a', '0', { sequenceKeysDeltas: [1] })).rejects.toBeInstanceOf(
      InvalidOptionsError,
    );
  });

  it('rejects sequenceKeysDeltas combined with expectedVersionId', async () => {
    await expect(
      client.put('a', '0', {
        sequenceKeysDeltas: [1],
        partitionKey: 'x',
        expectedVersionId: 1,
      }),
    ).rejects.toBeInstanceOf(InvalidOptionsError);
  });

  it('server assigns monotonically increasing suffixes', async () => {
    const partitionKey = 'seq-x';

    const first = await client.put('seq', '0', { sequenceKeysDeltas: [1], partitionKey });
    expect(first.key).toBe(`seq-${pad(1)}`);

    const second = await client.put('seq', '1', { sequenceKeysDeltas: [3], partitionKey });
    expect(second.key).toBe(`seq-${pad(4)}`);

    const third = await client.put('seq', '2', { sequenceKeysDeltas: [1, 6], partitionKey });
    expect(third.key).toBe(`seq-${pad(5)}-${pad(6)}`);

    // Base key itself wasn't written.
    await expect(client.get('seq', { partitionKey })).rejects.toBeInstanceOf(KeyNotFoundError);

    // Assigned keys are readable by exact key + same partition.
    const r1 = await client.get(`seq-${pad(1)}`, { partitionKey });
    expect(decode(r1.value)).toBe('0');
    const r3 = await client.get(`seq-${pad(5)}-${pad(6)}`, { partitionKey });
    expect(decode(r3.value)).toBe('2');
  });

  it('preserves insertion order under rapid sequential puts', async () => {
    const partitionKey = 'seq-rapid';
    for (let i = 1; i <= 25; i++) {
      const r = await client.put('b', `v${i}`, {
        sequenceKeysDeltas: [1],
        partitionKey,
      });
      expect(r.key).toBe(`b-${pad(i)}`);
    }
  });
});
