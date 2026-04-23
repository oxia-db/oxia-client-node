// Copyright 2025 The Oxia Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

import { type Interceptor, credentials } from '@grpc/grpc-js';
import type { Authentication } from '../auth.js';
import { OxiaClientClient } from '../proto/generated/client.js';
import { authInterceptor, unaryDeadlineInterceptor } from './interceptors.js';

export interface ConnectionPoolOptions {
  requestTimeoutMs?: number;
  authentication?: Authentication;
}

/**
 * Maintains one `@grpc/grpc-js` channel/stub per server address. All shard
 * leaders for a given namespace are reached through this pool, so a stable
 * cluster requires at most N channels where N is the number of servers.
 */
export class ConnectionPool {
  private readonly clients = new Map<string, OxiaClientClient>();
  private readonly interceptors: Interceptor[];
  private closed = false;

  constructor(options: ConnectionPoolOptions = {}) {
    this.interceptors = [];
    if (options.requestTimeoutMs !== undefined) {
      this.interceptors.push(unaryDeadlineInterceptor(options.requestTimeoutMs));
    }
    if (options.authentication !== undefined) {
      this.interceptors.push(authInterceptor(options.authentication));
    }
  }

  get(address: string): OxiaClientClient {
    if (this.closed) {
      throw new Error('ConnectionPool is closed');
    }
    let c = this.clients.get(address);
    if (c === undefined) {
      const clientOptions = this.interceptors.length > 0 ? { interceptors: this.interceptors } : {};
      c = new OxiaClientClient(address, credentials.createInsecure(), clientOptions);
      this.clients.set(address, c);
    }
    return c;
  }

  close(): void {
    if (this.closed) return;
    this.closed = true;
    for (const c of this.clients.values()) {
      c.close();
    }
    this.clients.clear();
  }
}
