// Copyright 2025 The Oxia Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

import { randomUUID } from 'node:crypto';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import {
  EXPECTED_RECORD_DOES_NOT_EXIST,
  KeyNotFoundError,
  OxiaClient,
  UnexpectedVersionIdError,
} from '../src/index.js';
import { OxiaContainer } from './oxiaContainer.js';

const newKey = () => `/test-${randomUUID().replace(/-/g, '')}`;
const decode = (v: Uint8Array | undefined) => new TextDecoder().decode(v);

describe('OxiaClient — phase 1 (put/get/delete, single shard)', () => {
  const container = new OxiaContainer();
  let client: OxiaClient;

  beforeAll(async () => {
    await container.start();
    client = await OxiaClient.connect(container.serviceUrl());
  });

  afterAll(async () => {
    await client?.close();
    await container.stop();
  });

  it('put + get returns the written value and version', async () => {
    const k = newKey();
    const put = await client.put(k, 'hello');

    expect(put.key).toBe(k);
    expect(put.version.modificationsCount).toBe(0);

    const got = await client.get(k);
    expect(got.key).toBe(k);
    expect(decode(got.value)).toBe('hello');
    expect(got.version.versionId).toBe(put.version.versionId);
    expect(got.version.modificationsCount).toBe(0);
  });

  it('put with EXPECTED_RECORD_DOES_NOT_EXIST succeeds then rejects a duplicate', async () => {
    const k = newKey();
    const first = await client.put(k, 'v1', {
      expectedVersionId: EXPECTED_RECORD_DOES_NOT_EXIST,
    });
    expect(first.version.modificationsCount).toBe(0);

    await expect(
      client.put(k, 'v2', { expectedVersionId: EXPECTED_RECORD_DOES_NOT_EXIST }),
    ).rejects.toBeInstanceOf(UnexpectedVersionIdError);
  });

  it('put with matching expectedVersionId increments modificationsCount', async () => {
    const k = newKey();
    const v0 = await client.put(k, 'v0');
    const v1 = await client.put(k, 'v1', { expectedVersionId: v0.version.versionId });
    expect(v1.version.modificationsCount).toBe(1);
  });

  it('put with wrong expectedVersionId rejects', async () => {
    const k = newKey();
    await client.put(k, 'v0');
    await expect(client.put(k, 'v1', { expectedVersionId: 999_999 })).rejects.toBeInstanceOf(
      UnexpectedVersionIdError,
    );
  });

  it('get on missing key throws KeyNotFoundError', async () => {
    await expect(client.get(newKey())).rejects.toBeInstanceOf(KeyNotFoundError);
  });

  it('delete returns true on existing key, false on missing key', async () => {
    const k = newKey();
    await client.put(k, 'x');

    expect(await client.delete(k)).toBe(true);
    expect(await client.delete(k)).toBe(false);
    await expect(client.get(k)).rejects.toBeInstanceOf(KeyNotFoundError);
  });

  it('delete with matching expectedVersionId succeeds', async () => {
    const k = newKey();
    const put = await client.put(k, 'x');
    expect(await client.delete(k, { expectedVersionId: put.version.versionId })).toBe(true);
  });

  it('delete with wrong expectedVersionId rejects', async () => {
    const k = newKey();
    await client.put(k, 'x');
    await expect(client.delete(k, { expectedVersionId: 999_999 })).rejects.toBeInstanceOf(
      UnexpectedVersionIdError,
    );
  });

  it('get with includeValue=false returns metadata only', async () => {
    const k = newKey();
    await client.put(k, 'hidden');
    const got = await client.get(k, { includeValue: false });
    expect(got.key).toBe(k);
    expect(got.value).toBeUndefined();
  });

  it('put accepts Uint8Array values', async () => {
    const k = newKey();
    const bytes = new Uint8Array([0, 1, 2, 250, 255]);
    await client.put(k, bytes);
    const got = await client.get(k);
    expect(got.value).toBeDefined();
    expect(Array.from(got.value!)).toEqual(Array.from(bytes));
  });
});
