// Copyright 2025 The Oxia Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

import { randomUUID } from 'node:crypto';
import { OxiaError } from '../exceptions.js';
import type {
  CloseSessionResponse,
  CreateSessionResponse,
  KeepAliveResponse,
} from '../proto/generated/client.js';
import { sleep } from './backoff.js';
import { fromNumber, toNumber } from './longs.js';
import { callUnary } from './rpc.js';
import type { ServiceDiscovery } from './serviceDiscovery.js';

const DEFAULT_HEARTBEAT_FLOOR_MS = 2_000;

export function resolveHeartbeatIntervalMs(
  sessionTimeoutMs: number,
  heartbeatIntervalMs?: number,
): number {
  if (heartbeatIntervalMs !== undefined) {
    if (heartbeatIntervalMs <= 0) {
      throw new RangeError('heartbeatIntervalMs must be greater than zero');
    }
    if (heartbeatIntervalMs >= sessionTimeoutMs) {
      throw new RangeError('heartbeatIntervalMs must be smaller than sessionTimeoutMs');
    }
    return heartbeatIntervalMs;
  }
  if (sessionTimeoutMs < DEFAULT_HEARTBEAT_FLOOR_MS) {
    throw new RangeError(
      `sessionTimeoutMs must be at least ${DEFAULT_HEARTBEAT_FLOOR_MS}ms when heartbeatIntervalMs is not provided (got ${sessionTimeoutMs})`,
    );
  }
  // Aim for ~10 heartbeats per timeout window, floor at 2s, and cap one
  // millisecond below the timeout so we always have room to schedule a
  // heartbeat before the local expiry check fires.
  const target = Math.max(Math.floor(sessionTimeoutMs / 10), DEFAULT_HEARTBEAT_FLOOR_MS);
  return Math.min(target, sessionTimeoutMs - 1);
}

export class Session {
  readonly shardId: number;
  readonly clientIdentifier: string;
  readonly sessionId: number;

  private readonly discovery: ServiceDiscovery;
  private readonly sessionTimeoutMs: number;
  private readonly heartbeatIntervalMs: number;
  private readonly onClose: ((s: Session) => void) | undefined;
  private readonly loopPromise: Promise<void>;
  private readonly abort = new AbortController();
  private lastKeepaliveAt: number;
  private closed = false;

  private constructor(params: {
    shardId: number;
    clientIdentifier: string;
    sessionId: number;
    discovery: ServiceDiscovery;
    sessionTimeoutMs: number;
    heartbeatIntervalMs: number;
    onClose?: (s: Session) => void;
  }) {
    this.shardId = params.shardId;
    this.clientIdentifier = params.clientIdentifier;
    this.sessionId = params.sessionId;
    this.discovery = params.discovery;
    this.sessionTimeoutMs = params.sessionTimeoutMs;
    this.heartbeatIntervalMs = params.heartbeatIntervalMs;
    this.onClose = params.onClose;
    this.lastKeepaliveAt = Date.now();
    this.loopPromise = this.runKeepaliveLoop();
  }

  static async open(params: {
    shardId: number;
    clientIdentifier: string;
    discovery: ServiceDiscovery;
    sessionTimeoutMs: number;
    heartbeatIntervalMs?: number;
    onClose?: (s: Session) => void;
  }): Promise<Session> {
    const hb = resolveHeartbeatIntervalMs(params.sessionTimeoutMs, params.heartbeatIntervalMs);
    const client = params.discovery.getLeaderForShard(params.shardId);
    const resp = await callUnary<CreateSessionResponse>((cb) =>
      client.createSession(
        {
          shard: fromNumber(params.shardId),
          sessionTimeoutMs: params.sessionTimeoutMs,
          clientIdentity: params.clientIdentifier,
        },
        cb,
      ),
    );
    return new Session({
      shardId: params.shardId,
      clientIdentifier: params.clientIdentifier,
      sessionId: toNumber(resp.sessionId),
      discovery: params.discovery,
      sessionTimeoutMs: params.sessionTimeoutMs,
      heartbeatIntervalMs: hb,
      onClose: params.onClose,
    });
  }

  isClosed(): boolean {
    return this.closed;
  }

  private async runKeepaliveLoop(): Promise<void> {
    while (!this.closed) {
      try {
        await sleep(this.heartbeatIntervalMs, this.abort.signal);
      } catch {
        return;
      }
      if (this.closed) return;

      if (Date.now() - this.lastKeepaliveAt > this.sessionTimeoutMs) {
        // Too many consecutive failures: give up and self-close.
        await this.close().catch(() => {
          /* ignore */
        });
        return;
      }
      try {
        const client = this.discovery.getLeaderForShard(this.shardId);
        await callUnary<KeepAliveResponse>((cb) =>
          client.keepAlive(
            {
              shard: fromNumber(this.shardId),
              sessionId: fromNumber(this.sessionId),
            },
            cb,
          ),
        );
        this.lastKeepaliveAt = Date.now();
      } catch {
        // Swallow: the next iteration retries, and the self-expiry check
        // above will close the session if responses never come back.
      }
    }
  }

  async close(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    this.abort.abort();
    this.onClose?.(this);
    try {
      const client = this.discovery.getLeaderForShard(this.shardId);
      await callUnary<CloseSessionResponse>((cb) =>
        client.closeSession(
          {
            shard: fromNumber(this.shardId),
            sessionId: fromNumber(this.sessionId),
          },
          cb,
        ),
      );
    } catch {
      // Server-side close failed (e.g. shard already gone) — the session
      // will time out naturally.
    }
    try {
      await this.loopPromise;
    } catch {
      // already handled
    }
  }
}

export class SessionManager {
  private readonly discovery: ServiceDiscovery;
  private readonly sessionTimeoutMs: number;
  private readonly heartbeatIntervalMs?: number;
  readonly clientIdentifier: string;
  private readonly sessions = new Map<number, Session>();
  private readonly pending = new Map<number, Promise<Session>>();
  private closed = false;

  constructor(params: {
    discovery: ServiceDiscovery;
    sessionTimeoutMs: number;
    heartbeatIntervalMs?: number;
    clientIdentifier?: string;
  }) {
    this.discovery = params.discovery;
    this.sessionTimeoutMs = params.sessionTimeoutMs;
    this.heartbeatIntervalMs = params.heartbeatIntervalMs;
    this.clientIdentifier = params.clientIdentifier ?? randomUUID().replace(/-/g, '');
  }

  async getSession(shardId: number): Promise<Session> {
    if (this.closed) throw new OxiaError('session manager is closed');

    const existing = this.sessions.get(shardId);
    if (existing && !existing.isClosed()) return existing;

    const inflight = this.pending.get(shardId);
    if (inflight) return inflight;

    const p = Session.open({
      shardId,
      clientIdentifier: this.clientIdentifier,
      discovery: this.discovery,
      sessionTimeoutMs: this.sessionTimeoutMs,
      heartbeatIntervalMs: this.heartbeatIntervalMs,
      onClose: (s) => this.onSessionClosed(s),
    })
      .then((s) => {
        this.pending.delete(shardId);
        if (this.closed) {
          // Raced with close(): tear down immediately.
          void s.close();
          throw new OxiaError('session manager is closed');
        }
        this.sessions.set(shardId, s);
        return s;
      })
      .catch((err) => {
        this.pending.delete(shardId);
        throw err;
      });
    this.pending.set(shardId, p);
    return p;
  }

  private onSessionClosed(session: Session): void {
    // Only evict when the tracked session is still the one that closed,
    // otherwise a late callback from an already-replaced session would
    // drop a live replacement.
    if (this.sessions.get(session.shardId) === session) {
      this.sessions.delete(session.shardId);
    }
  }

  async close(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    const sessions = [...this.sessions.values()];
    this.sessions.clear();
    await Promise.allSettled(sessions.map((s) => s.close()));
  }
}
