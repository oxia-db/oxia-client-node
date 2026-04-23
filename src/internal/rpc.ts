// Copyright 2025 The Oxia Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

import type { ClientReadableStream, ClientUnaryCall, ServiceError } from '@grpc/grpc-js';

/**
 * Promisify a callback-style unary RPC. The caller supplies a thunk that
 * invokes the `@grpc/grpc-js` client's trailing `(err, resp)` callback,
 * keeping the per-RPC request/metadata arguments opaque here.
 */
export function callUnary<Resp>(
  invoke: (cb: (err: ServiceError | null, resp: Resp) => void) => ClientUnaryCall,
): Promise<Resp> {
  return new Promise<Resp>((resolve, reject) => {
    invoke((err, resp) => {
      if (err) reject(err);
      else resolve(resp);
    });
  });
}

/**
 * Collect all messages from a server-streaming RPC into an array. Used
 * for RPCs whose response is a stream but whose total payload fits
 * comfortably in memory (e.g. a single-request Read that returns one
 * ReadResponse, or a List/RangeScan over a bounded range).
 */
export function collectStream<T>(stream: ClientReadableStream<T>): Promise<T[]> {
  return new Promise((resolve, reject) => {
    const out: T[] = [];
    stream.on('data', (x) => out.push(x as T));
    stream.on('error', reject);
    stream.on('end', () => resolve(out));
  });
}

/**
 * Read the first message from a server-streaming RPC, then cancel. Used
 * for Read() which returns a stream of ReadResponse — with a single
 * GetRequest in the batch, there is exactly one response.
 */
export function firstStreamMessage<T>(stream: ClientReadableStream<T>): Promise<T> {
  return new Promise((resolve, reject) => {
    let settled = false;
    stream.on('data', (x) => {
      if (settled) return;
      settled = true;
      stream.cancel();
      resolve(x as T);
    });
    stream.on('error', (err) => {
      if (settled) return;
      // Cancellation after we've already resolved surfaces as CANCELLED;
      // ignore it.
      settled = true;
      reject(err);
    });
    stream.on('end', () => {
      if (settled) return;
      settled = true;
      reject(new Error('stream ended without any message'));
    });
  });
}
