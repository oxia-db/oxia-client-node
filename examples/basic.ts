// Copyright 2025 The Oxia Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Run a local standalone Oxia server on port 6648, then:
//   npx tsx examples/basic.ts

import { OxiaClient } from '../src/index.js';

async function main() {
  const client = await OxiaClient.connect('localhost:6648');
  try {
    const prefix = `/example-${process.pid}`;

    // Write a few records.
    for (const [k, v] of [
      ['/a', 'apple'],
      ['/b', 'banana'],
      ['/c', 'cherry'],
    ]) {
      const r = await client.put(prefix + k, v);
      console.log('put', r.key, 'version', r.version.versionId);
    }

    // Read one back.
    const got = await client.get(`${prefix}/b`);
    console.log('get', got.key, '=', new TextDecoder().decode(got.value));

    // List and scan.
    console.log('list:', await client.list(`${prefix}/`, `${prefix}/~`));
    for await (const rec of client.rangeScan(`${prefix}/a`, `${prefix}/d`)) {
      console.log('  scan', rec.key, '=', new TextDecoder().decode(rec.value));
    }

    // Clean up.
    await client.deleteRange(`${prefix}/`, `${prefix}/~`);
    console.log('cleaned up');
  } finally {
    await client.close();
  }
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
