// Copyright 2025 The Oxia Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

import { randomUUID } from 'node:crypto';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { KeyNotFoundError, OxiaClient } from '../src/index.js';
import { resolveHeartbeatIntervalMs } from '../src/internal/sessions.js';
import { OxiaContainer } from './oxiaContainer.js';

const newKey = () => `/test-${randomUUID().replace(/-/g, '')}`;
const decode = (v: Uint8Array | undefined) => new TextDecoder().decode(v);

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

describe('resolveHeartbeatIntervalMs (unit)', () => {
  it('rejects explicit interval >= sessionTimeout', () => {
    expect(() => resolveHeartbeatIntervalMs(5_000, 5_000)).toThrow(RangeError);
    expect(() => resolveHeartbeatIntervalMs(5_000, 6_000)).toThrow(RangeError);
  });

  it('rejects non-positive explicit interval', () => {
    expect(() => resolveHeartbeatIntervalMs(5_000, 0)).toThrow(RangeError);
    expect(() => resolveHeartbeatIntervalMs(5_000, -1)).toThrow(RangeError);
  });

  it('rejects default path when timeout is below 2s floor', () => {
    expect(() => resolveHeartbeatIntervalMs(1_999)).toThrow(RangeError);
  });

  it('defaults to ~timeout/10 but never below 2s and never >= timeout', () => {
    expect(resolveHeartbeatIntervalMs(30_000)).toBe(3_000);
    expect(resolveHeartbeatIntervalMs(5_000)).toBe(2_000);
    expect(resolveHeartbeatIntervalMs(2_500)).toBe(2_000);
    // When floor == timeout edge, cap below timeout.
    expect(resolveHeartbeatIntervalMs(2_000)).toBe(1_999);
  });
});

describe('OxiaClient — ephemeral records', () => {
  const container = new OxiaContainer(1);
  let serviceUrl: string;

  beforeAll(async () => {
    await container.start();
    serviceUrl = container.serviceUrl();
  });

  afterAll(async () => {
    await container.stop();
  });

  it('ephemeral put marks the version as ephemeral and carries clientIdentity', async () => {
    const client = await OxiaClient.connect(serviceUrl, { clientIdentifier: 'client-1' });
    try {
      const k = newKey();
      const put = await client.put(k, 'v', { ephemeral: true });
      expect(put.version.isEphemeral()).toBe(true);
      expect(put.version.modificationsCount).toBe(0);
      expect(put.version.clientIdentity).toBe('client-1');

      const got = await client.get(k);
      expect(got.version.isEphemeral()).toBe(true);
      expect(got.version.clientIdentity).toBe('client-1');
      expect(got.version.sessionId).toBeDefined();
      expect(decode(got.value)).toBe('v');
    } finally {
      await client.close();
    }
  });

  it('closing the creating client removes the ephemeral record server-side', async () => {
    const creator = await OxiaClient.connect(serviceUrl, { sessionTimeoutMs: 5_000 });
    const observer = await OxiaClient.connect(serviceUrl);
    try {
      const k = newKey();
      await creator.put(k, 'v', { ephemeral: true });

      // Observer can see the record while creator is alive.
      const got = await observer.get(k);
      expect(decode(got.value)).toBe('v');

      // Closing the creator triggers server-side removal of its ephemerals.
      await creator.close();

      // Wait for the server to propagate the session-scoped delete.
      const deadline = Date.now() + 5_000;
      let gone = false;
      while (Date.now() < deadline) {
        try {
          await observer.get(k);
        } catch (e) {
          if (e instanceof KeyNotFoundError) {
            gone = true;
            break;
          }
          throw e;
        }
        await sleep(50);
      }
      expect(gone).toBe(true);

      // Delete on missing key returns false, not throws.
      expect(await observer.delete(k)).toBe(false);
    } finally {
      await observer.close();
    }
  });

  it('overriding an ephemeral with a non-ephemeral put persists beyond session close', async () => {
    const client1 = await OxiaClient.connect(serviceUrl, { sessionTimeoutMs: 5_000 });
    const k = newKey();
    const eph = await client1.put(k, 'x', { ephemeral: true });
    expect(eph.version.isEphemeral()).toBe(true);

    const persisted = await client1.put(k, 'y', { ephemeral: false });
    expect(persisted.version.isEphemeral()).toBe(false);
    expect(persisted.version.modificationsCount).toBe(1);

    await client1.close();

    const client2 = await OxiaClient.connect(serviceUrl);
    try {
      const got = await client2.get(k);
      expect(decode(got.value)).toBe('y');
      expect(got.version.isEphemeral()).toBe(false);
      expect(got.version.clientIdentity).toBeUndefined();
    } finally {
      await client2.close();
    }
  });

  it('subsequent ephemeral puts reuse the same session on the same shard', async () => {
    const client = await OxiaClient.connect(serviceUrl);
    try {
      const k1 = newKey();
      const k2 = newKey();
      const r1 = await client.put(k1, 'a', { ephemeral: true });
      const r2 = await client.put(k2, 'b', { ephemeral: true });
      expect(r1.version.sessionId).toBe(r2.version.sessionId);
    } finally {
      await client.close();
    }
  });

  it('rejects sessionTimeoutMs below 2s on the first ephemeral put (lazy session creation)', async () => {
    const client = await OxiaClient.connect(serviceUrl, { sessionTimeoutMs: 1_999 });
    try {
      await expect(client.put(newKey(), 'v', { ephemeral: true })).rejects.toBeInstanceOf(
        RangeError,
      );
    } finally {
      await client.close();
    }
  });

  it('heartbeat keeps the session alive past the session timeout window', async () => {
    const client = await OxiaClient.connect(serviceUrl, { sessionTimeoutMs: 3_000 });
    try {
      const k = newKey();
      await client.put(k, 'v', { ephemeral: true });

      // Wait longer than the session timeout; the keepalive loop should
      // be renewing the session the whole time.
      await sleep(4_500);

      const got = await client.get(k);
      expect(decode(got.value)).toBe('v');
      expect(got.version.isEphemeral()).toBe(true);
    } finally {
      await client.close();
    }
  }, 30_000);
});
