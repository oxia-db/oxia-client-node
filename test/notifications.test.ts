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
  type CloseableAsyncIterable,
  InvalidOptionsError,
  type Notification,
  NotificationType,
  OxiaClient,
} from '../src/index.js';
import { OxiaContainer } from './oxiaContainer.js';

const newKey = () => `/test-${randomUUID().replace(/-/g, '')}`;

async function nextNotification(
  stream: CloseableAsyncIterable<Notification>,
  timeoutMs = 5_000,
): Promise<Notification> {
  const iter = stream[Symbol.asyncIterator]();
  const next = iter.next();
  let timer: NodeJS.Timeout | undefined;
  const timeout = new Promise<IteratorResult<Notification, void>>((_, reject) => {
    timer = setTimeout(
      () => reject(new Error(`timed out waiting for notification after ${timeoutMs}ms`)),
      timeoutMs,
    );
  });
  try {
    const res = await Promise.race([next, timeout]);
    if (res.done) throw new Error('stream ended unexpectedly');
    return res.value;
  } finally {
    if (timer) clearTimeout(timer);
  }
}

describe('OxiaClient — getNotifications', () => {
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

  it('yields KEY_CREATED / KEY_MODIFIED / KEY_DELETED for basic writes', async () => {
    const notifications = client.getNotifications();
    try {
      const ka = newKey();
      const kb = newKey();

      const p1 = await client.put(ka, '0');
      const n1 = await nextNotification(notifications);
      expect(n1.type).toBe(NotificationType.KEY_CREATED);
      expect(n1.key).toBe(ka);
      expect(n1.versionId).toBe(p1.version.versionId);

      const p2 = await client.put(ka, '1');
      const n2 = await nextNotification(notifications);
      expect(n2.type).toBe(NotificationType.KEY_MODIFIED);
      expect(n2.key).toBe(ka);
      expect(n2.versionId).toBe(p2.version.versionId);

      await client.put(kb, '0');
      await client.delete(ka);

      // Drain the next two events (order across shards is not guaranteed).
      const tail = [await nextNotification(notifications), await nextNotification(notifications)];
      const byKey = new Map(tail.map((n) => [n.key, n]));
      const created = byKey.get(kb);
      const deleted = byKey.get(ka);
      expect(created?.type).toBe(NotificationType.KEY_CREATED);
      expect(deleted?.type).toBe(NotificationType.KEY_DELETED);
      expect(deleted?.versionId).toBe(0);
    } finally {
      notifications.close();
    }
  });

  it('multiple concurrent subscribers each see the same events', async () => {
    const sub1 = client.getNotifications();
    const sub2 = client.getNotifications();
    try {
      const k = newKey();
      const p = await client.put(k, 'v');

      const n1 = await nextNotification(sub1);
      const n2 = await nextNotification(sub2);
      expect(n1.key).toBe(k);
      expect(n2.key).toBe(k);
      expect(n1.type).toBe(NotificationType.KEY_CREATED);
      expect(n2.type).toBe(NotificationType.KEY_CREATED);
      expect(n1.versionId).toBe(p.version.versionId);
      expect(n2.versionId).toBe(p.version.versionId);
    } finally {
      sub1.close();
      sub2.close();
    }
  });

  it('close() ends the for-await-of loop', async () => {
    const notifications = client.getNotifications();
    // Kick off the iterator, then close it; the loop must terminate.
    const loop = (async () => {
      for await (const _ of notifications) {
        // discard
      }
    })();
    notifications.close();
    await expect(loop).resolves.toBeUndefined();
  });
});

describe('OxiaClient — getSequenceUpdates', () => {
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

  it('rejects when partitionKey is missing', () => {
    expect(() =>
      client.getSequenceUpdates('seq', { partitionKey: '' as unknown as string }),
    ).toThrow(InvalidOptionsError);
  });

  it('yields the current highest sequence on a fresh subscription, then live advances', async () => {
    const partitionKey = 'seq-live';

    // Write two entries before subscribing.
    const k1 = await client.put('a', '0', { sequenceKeysDeltas: [1], partitionKey });
    const k2 = await client.put('a', '1', { sequenceKeysDeltas: [1], partitionKey });

    const updates = client.getSequenceUpdates('a', { partitionKey });
    try {
      const iter = updates[Symbol.asyncIterator]();
      const initial = await iter.next();
      expect(initial.done).toBe(false);
      // The server emits the current highest sequence first.
      expect(initial.value).toBe(k2.key);
      expect(initial.value).not.toBe(k1.key);

      // A subsequent write advances the sequence.
      const k3 = await client.put('a', '2', { sequenceKeysDeltas: [1], partitionKey });
      const next = await iter.next();
      expect(next.done).toBe(false);
      expect(next.value).toBe(k3.key);
    } finally {
      updates.close();
    }
  });

  it('close() terminates the for-await-of loop', async () => {
    const partitionKey = 'seq-close';
    // Create one entry so the subscription has something to send.
    await client.put('b', 'v', { sequenceKeysDeltas: [1], partitionKey });
    const updates = client.getSequenceUpdates('b', { partitionKey });
    const loop = (async () => {
      for await (const _ of updates) {
        // discard
      }
    })();
    updates.close();
    await expect(loop).resolves.toBeUndefined();
  });
});
