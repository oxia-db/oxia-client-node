// Copyright 2025 The Oxia Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

export {
  OxiaClient,
  type OxiaClientOptions,
  type PutOptions,
  type DeleteOptions,
  type DeleteRangeOptions,
  type GetOptions,
  type ListOptions,
  type RangeScanOptions,
} from './client.js';
export {
  ComparisonType,
  EXPECTED_RECORD_DOES_NOT_EXIST,
  Version,
  type PutResult,
  type GetResult,
} from './types.js';
export {
  OxiaError,
  InvalidOptionsError,
  KeyNotFoundError,
  UnexpectedVersionIdError,
  SessionNotFoundError,
} from './exceptions.js';
