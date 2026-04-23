// Copyright 2025 The Oxia Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

export class OxiaError extends Error {
  constructor(message?: string) {
    super(message);
    this.name = 'OxiaError';
  }
}

export class InvalidOptionsError extends OxiaError {
  constructor(message?: string) {
    super(message);
    this.name = 'InvalidOptionsError';
  }
}

export class KeyNotFoundError extends OxiaError {
  constructor(message?: string) {
    super(message ?? 'key not found');
    this.name = 'KeyNotFoundError';
  }
}

export class UnexpectedVersionIdError extends OxiaError {
  constructor(message?: string) {
    super(message ?? 'unexpected version id');
    this.name = 'UnexpectedVersionIdError';
  }
}

export class SessionNotFoundError extends OxiaError {
  constructor(message?: string) {
    super(message ?? 'session does not exist');
    this.name = 'SessionNotFoundError';
  }
}
