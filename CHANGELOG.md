# Changelog

## 1.0.0 (2026-10-13)

First release. petit-journal was developed inside a private codebase, where it runs in production.
The earlier history is tied to that code, so this repository starts from a single commit.
Everything after it happens here in the open.

- `openEventStore(pool, { codec, tables })`: creates the two tables and the dedupe index if they do
  not exist, then resolves to the store, so no write runs before its tables exist. Safe on every
  boot and from concurrent boots; a failed setup rejects at boot with the reason.
  `createEventStore` is the same store, synchronous, and never creates tables;
  `ensureSchema(pool, { tables })` is the setup on its own.
- An append-only event log and one folded projection row per stream on PostgreSQL, with the pool
  injected and no dependencies. Table names are configurable, optionally schema-qualified.
- `append` with `dedupeKey` for retry-safe writes and `expectedSeq` for optimistic concurrency.
- `transaction(stream, work)`: several operations and your own SQL (`tx.query`) on one stream,
  under a per-stream advisory lock, committed or rolled back together.
- `defineProjection(store, { initial, reduce, keep, redacted })`: `append`, `update`, `state` and
  `rebuild` for a projection folded by your reducer, on either store. `update`'s `decide` can
  return a plan that appends and redacts earlier events in one transaction, re-folding the
  projection so it never holds a redacted fact.
- `compact` for bounded history that keeps the newest events, and `redact` for forgetting one
  event's payload behind a tombstone.
- An optional payload codec for encryption or compression, with an AES-256-GCM example.
- `createMockEventStore`: the same contract in memory, for tests.
- `WrongExpectedSeqError`, `UnknownSeqError` and `CompactedLogError`.
