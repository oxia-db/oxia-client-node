// Copyright 2025 The Oxia Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

import { randomUUID } from 'node:crypto';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { type Authentication, OxiaClient, TokenAuthentication } from '../src/index.js';
import { OxiaContainer } from './oxiaContainer.js';

describe('TokenAuthentication (unit)', () => {
  it('wraps a static token as Authorization: Bearer ...', async () => {
    const auth = new TokenAuthentication('abc');
    expect(await auth.generateCredentials()).toEqual({ authorization: 'Bearer abc' });
  });

  it('calls the supplier on every credential generation (refresh support)', async () => {
    let n = 0;
    const auth = new TokenAuthentication(() => `token-${++n}`);
    expect(await auth.generateCredentials()).toEqual({ authorization: 'Bearer token-1' });
    expect(await auth.generateCredentials()).toEqual({ authorization: 'Bearer token-2' });
  });

  it('supports async token suppliers', async () => {
    const auth = new TokenAuthentication(async () => 'async-tok');
    expect(await auth.generateCredentials()).toEqual({ authorization: 'Bearer async-tok' });
  });
});

class CountingAuth implements Authentication {
  calls = 0;
  generateCredentials(): Record<string, string> {
    this.calls++;
    return { authorization: `Bearer count-${this.calls}` };
  }
}

describe('Authentication interceptor', () => {
  const container = new OxiaContainer(1);
  let serviceUrl: string;

  beforeAll(async () => {
    await container.start();
    serviceUrl = container.serviceUrl();
  });

  afterAll(async () => {
    await container.stop();
  });

  it('invokes generateCredentials at least once per unary RPC issued by the user', async () => {
    // The service-discovery stream and the session keepalive loop also
    // trigger credential generation, so we can't pin an exact count.
    // Instead, sample the counter before and after a few user-driven
    // RPCs and assert it strictly grew.
    const auth = new CountingAuth();
    const client = await OxiaClient.connect(serviceUrl, { authentication: auth });
    try {
      const before = auth.calls;
      const key = `/auth-${randomUUID().replace(/-/g, '')}`;
      await client.put(key, 'v1');
      await client.get(key);
      await client.put(key, 'v2');
      await client.delete(key);
      expect(auth.calls - before).toBeGreaterThanOrEqual(4);
    } finally {
      await client.close();
    }
  });
});

describe('requestTimeoutMs deadline', () => {
  it('unary RPCs to an unreachable server fail fast under a small deadline', async () => {
    // 127.0.0.1:1 is never listening; connect() blocks on shard
    // discovery, so the short init timeout surfaces the failure.
    await expect(
      OxiaClient.connect('127.0.0.1:1', {
        requestTimeoutMs: 500,
        initTimeoutMs: 1_500,
      }),
    ).rejects.toThrow();
  }, 10_000);
});
