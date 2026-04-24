# @oxia-db/client

Node.js / TypeScript client for [Oxia](https://oxia-db.github.io), modeled
after the official Python client.

## Install

```bash
npm install @oxia-db/client
```

Requires Node.js 18+.

## Quick start

```ts
import { OxiaClient, EXPECTED_RECORD_DOES_NOT_EXIST } from '@oxia-db/client';

const client = await OxiaClient.connect('localhost:6648');
try {
  const { key, version } = await client.put('/users/alice', 'hello', {
    expectedVersionId: EXPECTED_RECORD_DOES_NOT_EXIST, // fail if already exists
  });

  const got = await client.get('/users/alice');
  console.log(got.key, new TextDecoder().decode(got.value), got.version.versionId);

  await client.delete('/users/alice');
} finally {
  await client.close();
}
```

## Client options

```ts
await OxiaClient.connect('localhost:6648', {
  namespace: 'default',          // default: 'default'
  sessionTimeoutMs: 30_000,      // default: 30s; min 2000ms
  heartbeatIntervalMs: 3_000,    // default: ~sessionTimeoutMs/10, floored at 2s
  clientIdentifier: 'my-worker', // default: random UUID hex
  requestTimeoutMs: 30_000,      // default: 30s; applied to unary RPCs only
  initTimeoutMs: 30_000,         // time to wait for the first shard map
  authentication: undefined,     // see "Authentication" below
});
```

## Operations

### put / delete / get

```ts
await client.put('/k', 'v');
await client.put('/k', 'v2', { expectedVersionId: v1.version.versionId });
await client.put('/k', 'v',  { ephemeral: true });                  // removed when client closes
await client.put('/k', 'v',  { secondaryIndexes: { 'by-user': 'alice' } });

await client.delete('/k');
await client.delete('/k', { expectedVersionId: v.version.versionId });

const r = await client.get('/k');                                   // EQUAL
const r2 = await client.get('/k', { comparisonType: ComparisonType.FLOOR });
const r3 = await client.get('alice', { useIndex: 'by-user' });      // by secondary index
```

### Range operations

```ts
// List all keys in a range (hierarchical ordering; see note below).
const keys = await client.list('/users/', '/users/~');

// Stream records in a range.
for await (const rec of client.rangeScan('/users/', '/users/~')) {
  console.log(rec.key, new TextDecoder().decode(rec.value));
}

// Delete a range.
await client.deleteRange('/tmp/', '/tmp/~');
```

With a `partitionKey`, operations are limited to a single shard:

```ts
await client.put('entry-a', 'v', { partitionKey: 'session-42' });
await client.list('entry-', 'entry-~', { partitionKey: 'session-42' });
```

### Sequential keys

```ts
const r = await client.put('ticket', 'payload', {
  partitionKey: 'queue',
  sequenceKeysDeltas: [1],  // server assigns a zero-padded suffix
});
console.log(r.key); // -> "ticket-00000000000000000001"
```

### Notifications

```ts
const notifications = client.getNotifications();
for await (const n of notifications) {
  console.log(n.type, n.key, n.versionId);
  if (shouldStop()) notifications.close(); // ends the loop
}
```

### Sequence updates

```ts
const updates = client.getSequenceUpdates('ticket', { partitionKey: 'queue' });
for await (const latestKey of updates) {
  console.log('sequence advanced to', latestKey);
}
```

## Authentication

```ts
import { OxiaClient, TokenAuthentication } from '@oxia-db/client';

// Static bearer token:
const client = await OxiaClient.connect('oxia.example.com:6648', {
  authentication: new TokenAuthentication('my-token'),
});

// Or a refresh-on-demand supplier:
const client2 = await OxiaClient.connect('oxia.example.com:6648', {
  authentication: new TokenAuthentication(async () => fetchTokenFromIdp()),
});
```

You can also implement the `Authentication` interface directly to add
arbitrary gRPC metadata headers to every outgoing RPC:

```ts
import type { Authentication } from '@oxia-db/client';

class MyAuth implements Authentication {
  generateCredentials() {
    return { 'x-my-header': 'value' };
  }
}
```

## Errors

All client errors extend `OxiaError`:

| Error                       | Meaning                                                    |
| --------------------------- | ---------------------------------------------------------- |
| `KeyNotFoundError`          | `get` / `delete` against a missing key                     |
| `UnexpectedVersionIdError`  | conditional `put` / `delete` saw a different version       |
| `SessionNotFoundError`      | ephemeral `put` referenced a session the server has closed |
| `InvalidOptionsError`       | incompatible option combination (e.g. `sequenceKeysDeltas` without `partitionKey`) |

## License

Apache License 2.0. See `LICENSE` and `NOTICE`.
