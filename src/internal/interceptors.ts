// Copyright 2025 The Oxia Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

import {
  status as GrpcStatus,
  InterceptingCall,
  type Interceptor,
  type InterceptorOptions,
  type Listener,
  type Metadata,
} from '@grpc/grpc-js';
import type { Authentication } from '../auth.js';

/**
 * Prepends `authorization`-style metadata to every RPC. The underlying
 * {@link Authentication.generateCredentials} is invoked per-call so
 * refresh-on-demand tokens work.
 *
 * On generator failure, the call is cancelled with UNAUTHENTICATED so
 * the client surface sees a real gRPC error rather than a hang.
 */
export function authInterceptor(auth: Authentication): Interceptor {
  return (options: InterceptorOptions, nextCall) => {
    const call = new InterceptingCall(nextCall(options), {
      start(metadata: Metadata, listener: Listener, next) {
        Promise.resolve(auth.generateCredentials())
          .then((creds) => {
            for (const [k, v] of Object.entries(creds)) {
              metadata.add(k, v);
            }
            next(metadata, listener);
          })
          .catch((err) => {
            const msg = err instanceof Error ? err.message : String(err);
            call.cancelWithStatus(
              GrpcStatus.UNAUTHENTICATED,
              `oxia: authentication generateCredentials failed: ${msg}`,
            );
          });
      },
    });
    return call;
  };
}

/**
 * Applies a default deadline to every **unary** RPC. Server-streaming
 * and bi-di calls are left unbounded since long-lived subscriptions
 * (notifications, shard assignments, sequence updates) legitimately
 * outlast any finite timeout.
 *
 * If the caller already supplied a deadline via per-call options, it
 * wins (we only fill in the default).
 */
export function unaryDeadlineInterceptor(timeoutMs: number): Interceptor {
  return (options: InterceptorOptions, nextCall) => {
    const def = options.method_definition;
    const isUnary = !def.requestStream && !def.responseStream;
    if (!isUnary || options.deadline !== undefined) {
      return new InterceptingCall(nextCall(options));
    }
    const withDeadline: InterceptorOptions = {
      ...options,
      deadline: new Date(Date.now() + timeoutMs),
    };
    return new InterceptingCall(nextCall(withDeadline));
  };
}
