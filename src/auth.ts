// Copyright 2025 The Oxia Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

/**
 * Pluggable authentication for Oxia client RPCs. Implementations return
 * a map of gRPC metadata entries to attach to every outgoing call (for
 * example, `{ authorization: 'Bearer <token>' }`). `generateCredentials`
 * is invoked once per RPC, so dynamic token-refresh schemes work by
 * returning a fresh value each call.
 */
export interface Authentication {
  generateCredentials(): Promise<Record<string, string>> | Record<string, string>;
}

/**
 * Bearer-token authentication. Emits an `authorization: Bearer <token>`
 * metadata header. The token can be a static string or a (sync or async)
 * callable invoked on every RPC.
 */
export class TokenAuthentication implements Authentication {
  private readonly supplier: () => string | Promise<string>;

  constructor(token: string | (() => string | Promise<string>)) {
    this.supplier = typeof token === 'function' ? token : () => token;
  }

  async generateCredentials(): Promise<Record<string, string>> {
    const token = await this.supplier();
    return { authorization: `Bearer ${token}` };
  }
}
