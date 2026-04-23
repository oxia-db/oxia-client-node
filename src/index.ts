// Copyright 2025 The Oxia Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

export { type Authentication, TokenAuthentication } from './auth.js';
export {
  OxiaClient,
  type OxiaClientOptions,
  type PutOptions,
  type DeleteOptions,
  type DeleteRangeOptions,
  type GetOptions,
  type ListOptions,
  type RangeScanOptions,
  type SequenceUpdatesOptions,
} from './client.js';
export {
  type CloseableAsyncIterable,
  ComparisonType,
  EXPECTED_RECORD_DOES_NOT_EXIST,
  type GetResult,
  type Notification,
  NotificationType,
  type PutResult,
  Version,
} from './types.js';
export {
  OxiaError,
  InvalidOptionsError,
  KeyNotFoundError,
  UnexpectedVersionIdError,
  SessionNotFoundError,
} from './exceptions.js';
