// Copyright 2025 The Oxia Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

import { GenericContainer, type StartedTestContainer, Wait } from 'testcontainers';

export class OxiaContainer {
  private started?: StartedTestContainer;
  constructor(private readonly shards: number = 1) {}

  async start(): Promise<StartedTestContainer> {
    this.started = await new GenericContainer('oxia/oxia:latest')
      .withExposedPorts(6648)
      .withCommand(['oxia', 'standalone', `--shards=${this.shards}`])
      .withWaitStrategy(Wait.forLogMessage(/Serving Prometheus metrics/))
      .start();
    return this.started;
  }

  async stop(): Promise<void> {
    if (this.started) {
      await this.started.stop();
      this.started = undefined;
    }
  }

  serviceUrl(): string {
    if (!this.started) throw new Error('container not started');
    return `${this.started.getHost()}:${this.started.getMappedPort(6648)}`;
  }
}
