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
 * example, `{ authorization: 'Bearer <token>' }`).
 * {@link Authentication.generateCredentials} is invoked once per RPC,
 * so dynamic token-refresh schemes work by returning a fresh value on
 * each call.
 */
export interface Authentication {
  /**
   * Return the gRPC metadata to attach to the next outgoing RPC. Keys
   * are interpreted as header names (gRPC will lowercase them); values
   * are arbitrary strings.
   *
   * May return a plain object or a Promise for async refresh. If this
   * method throws or the returned Promise rejects, the affected call
   * is cancelled with gRPC status `UNAUTHENTICATED`.
   */
  generateCredentials(): Promise<Record<string, string>> | Record<string, string>;
}

/**
 * Bearer-token authentication. Emits an `authorization: Bearer <token>`
 * metadata header. The token can be a static string or a (sync or async)
 * callable invoked on every RPC — useful for rotating credentials
 * without reconnecting.
 *
 * @example
 * ```ts
 * new TokenAuthentication('my-static-token');
 * new TokenAuthentication(async () => await fetchTokenFromIdp());
 * ```
 */
export class TokenAuthentication implements Authentication {
  private readonly supplier: () => string | Promise<string>;

  /**
   * @param token A static bearer token, or a zero-argument callable
   *   (sync or async) that returns the current bearer token. The
   *   callable is invoked on every RPC.
   */
  constructor(token: string | (() => string | Promise<string>)) {
    this.supplier = typeof token === 'function' ? token : () => token;
  }

  /**
   * Produce the `authorization` metadata header for the next RPC by
   * resolving the token supplier.
   */
  async generateCredentials(): Promise<Record<string, string>> {
    const token = await this.supplier();
    return { authorization: `Bearer ${token}` };
  }
}
