# petit-journal

[![CI](https://github.com/magna-illusions/petit-journal/actions/workflows/ci.yml/badge.svg)](https://github.com/magna-illusions/petit-journal/actions/workflows/ci.yml)
[![npm](https://img.shields.io/npm/v/petit-journal)](https://www.npmjs.com/package/petit-journal)

Per-stream event logs with an atomic snapshot, on the Postgres you already run. Each stream gets
an append-only log and one folded projection row, changed together in one transaction under a
per-stream lock; you inject the `pg` pool and write the reducer.

*Le Petit Journal* was a Paris daily that never rewrote yesterday's edition.

## Install

```sh
npm install petit-journal pg
```

Node 22.13 or later, ES modules or `require()`, types included. PostgreSQL 11 or later. `pg` is
yours to install; the package never imports it.

## Quick start

```js
import pg from 'pg'
import { openEventStore, defineProjection } from 'petit-journal'

const pool = new pg.Pool({ connectionString: process.env.DATABASE_URL })
pool.on('error', (err) => console.error('idle connection lost', err.message)) // pg crashes the process otherwise
const store = await openEventStore(pool) // creates the tables if they are missing, then the store

const ledger = defineProjection(store, {
  initial: { balance: 0 },
  reduce: (state, event) =>
    event.type === 'Deposited' ? { balance: state.balance + event.payload.amount } : state,
})

const account = { ownerId: 'acme', stream: 'ledger', subjectId: 'main' }
await ledger.append(account, { type: 'Deposited', payload: { amount: 100 } })
console.log(await ledger.state(account)) // { balance: 100 }
await pool.end()
```

`ledger.append` loads the projection, appends, folds the event on and saves the state, in one
transaction under the stream's lock. Readers call `ledger.state`. In tests, use
`createMockEventStore()` in place of `await openEventStore(pool)`: the same contract, in memory.

## How it works

- A **stream** is `{ ownerId, stream, subjectId }`. It has an event log, numbered
  `seq = 1, 2, 3…`, and at most one projection row: the folded state and the highest `seq` in it.
- **A projection is a reducer.** `defineProjection` gives a stream `append`, `update`, `state` and
  `rebuild`; each write loads, appends, folds, saves and compacts as one transaction.
- **Every write takes the stream's lock**, a Postgres advisory lock held to the end of its
  transaction. Writers to one stream take turns; reads never wait.
- **Appends are safe to retry and to race.** A repeated `dedupeKey` returns the first event;
  `expectedSeq` appends only if the stream has not moved.
- **History stays bounded.** Compaction deletes what the projection has folded, keeping the newest
  `keep` events.
- **Payloads can be sealed** by an optional codec, for encryption or compression.
- **One fact can be forgotten.** `redact` replaces chosen payloads with a tombstone and keeps
  their `seq`, `type` and time.

The [design notes](https://github.com/magna-illusions/petit-journal/blob/main/docs/design.md) cover
each of these in depth.

## API

### `openEventStore(pool, options?)` and `createEventStore(pool, options?)`

Both make the same store from anything shaped like `pg.Pool`. `openEventStore` sets up the tables
first; `createEventStore` is synchronous and never runs DDL. `options`:

- `codec`: `{ encode(value, context), decode(stored, context) }`, applied to every payload and
  projection state; see [encrypting payloads](#encrypting-payloads).
- `tables`: `{ events, projections }`, to rename the tables.

### `defineProjection(store, { initial, reduce, keep?, redacted? })`

Folds a stream's events into its projection row, on either store. `initial` is the state of a
stream never written; each fold from it starts from a fresh copy. `reduce(state, event)` returns
the next state, synchronously. `keep`, when set, compacts after each save. Redacted events are
skipped, or folded with `redacted: 'fold'`.

| Method | Returns |
|---|---|
| `append(stream, eventOrEvents, options?)` | `{ state, events, duplicate, redacted }` after appending, folding, saving and compacting in one transaction. |
| `update(stream, decide, options?)` | The same, for what `decide(state, ctx)` returns: an array of events (`[]` for nothing), or a plan; see below. |
| `state(stream)` | The folded state, or `initial` for a stream never written. |
| `rebuild(stream)` | Re-folds the whole log from `initial` and saves it, as after a reducer change. |

- **A batch is one unit.** `expectedSeq` is the stream's `seq` before it, and `dedupeKey` goes on
  its first event. A hit returns `duplicate: true` and folds nothing; an `update` checks the key
  before it calls `decide`. An empty batch writes nothing and applies no option.
- **`decide` runs under the stream's lock**, so keep it quick. It gets a copy of the state.
- **A projection behind its log catches up** when something appended with the store's own methods.
- **A compacted log is refused** where a fold needs events compaction removed, with a
  `CompactedLogError`.
- **`reduce` must return the next state.** `undefined` or a promise throws, rather than fold onto it.

#### Appending and redacting in one transaction

A forget path appends an event and redacts the earlier events that hold the words. `decide` reads
the log through `ctx.loadEvents(afterSeq?)` and returns a plan `{ append, redact }`, where each
`redact` group has its own `seqs`, `reason` and `tombstone`:

```js
const forget = (word) => async (state, ctx) => {
  const holding = (await ctx.loadEvents())
    .filter((e) => e.type === 'Noted' && !e.redacted && e.payload.text.includes(word))
    .map((e) => e.seq)
  return {
    append: [{ type: 'Forgot', payload: { seqs: holding } }],
    redact: holding.map((seq) => ({ seqs: [seq], reason: 'forget request', tombstone: { note: seq } })),
  }
}
await notes.update(stream, forget('secret')) // { ..., events: [the Forgot event], redacted: [1, 2] }
```

The events are appended, the groups redacted, and the whole log re-folded from `initial` and saved,
all in one transaction, so the saved state never holds a redacted fact. If any part fails, none of
it is written.

- **Only earlier events.** A redacted seq the stream did not hold before the update is a
  `TypeError`.
- **Only an uncompacted log, and no `keep`.** The re-fold needs every event since seq 1, so such a
  plan is refused on a projection with `keep`, from its first call, and on a log compacted by
  hand. Use the store's own methods then; see [redaction](#redaction).
- **A plan with only `redact` is the atomic forget.** It appends nothing, so its `dedupeKey` is
  never stored, and a retry redacts again: harmless, a tombstone replaced with a new time.
- **A `dedupeKey` some other write already stored makes the plan a `duplicate`**, and its
  redactions do not run. Give each forget a key of its own.

#### Folding redacted events

By default `reduce` never sees a redacted event. With `redacted: 'fold'` it gets each one, marked
`redacted: true`, with the tombstone as its payload, in every fold. Use it when the state must show
that something was forgotten.

### Store methods

`defineProjection` covers the usual cycle. The store's own methods are the escape hatch: a
projection row with no events behind it, a custom re-fold, your own SQL in the same transaction.
Every method takes a `StreamId`, `{ ownerId, stream, subjectId }`: the partition, the kind of log
and the aggregate. An `ownerId` in the standard uuid form (8-4-4-4-12, braces optional) is written
lowercase without braces; any other id, 32 bare hex digits included, is kept exactly as given.

| Method | Returns |
|---|---|
| `append(stream, event, options?)` | `{ event, appended }`. On a dedupe hit `appended` is `false` and `event` is the first write. |
| `loadEvents(stream, afterSeq = 0)` | Stored events with `seq > afterSeq`, in order. A redacted one has `redacted: true`. |
| `loadProjection(stream)` | `{ ownerId, stream, subjectId, state, throughSeq, updatedAt }`, or `null`. |
| `saveProjection(stream, state, throughSeq)` | Upserts the row. `throughSeq` is the highest `seq` the state embodies. |
| `maxSeq(stream)` | The newest `seq`, or `0`. |
| `compact(stream, throughSeq, keep)` | How many folded events it deleted, keeping the newest `keep`, the newest event, and anything past the saved projection. |
| `redact(stream, seqs, { reason, tombstone? })` | The redacted seqs. Each payload becomes `{ ...tombstone, $redacted: true, reason, at }`. |
| `transaction(stream, work)` | What `work` returned, after `COMMIT`. Rolls back and rethrows if `work` rejects. |

Inside `transaction`, `work` gets a `tx` with the same methods minus the stream, plus
`tx.query(text, params)` for your own SQL on its connection, `tx.stream`, and
`tx.eventByDedupeKey(key)`. To keep a column of your own in step with every save, wrap the store's
`transaction`; the design notes show how.

`event` is `{ type, payload, occurredAt?, source? }`:

| Field | Type | Default | |
|---|---|---|---|
| `type` | non-empty string | required | Your event type. |
| `payload` | JSON value | required | Opaque to the store. A top-level `$redacted` key is refused. |
| `occurredAt` | `Date`, or ISO 8601 with `Z` or an offset | `now()` at insert | When it happened. |
| `source` | string | `null` | Free text, such as the writing service. |

`options` is `{ dedupeKey?, expectedSeq? }`:

| Option | Type | |
|---|---|---|
| `dedupeKey` | non-empty string | A key seen before in this stream returns that event, `appended: false`, and writes nothing. |
| `expectedSeq` | integer `>= 0` | Append only if the newest `seq` is exactly this (`0`: empty); otherwise throw `WrongExpectedSeqError`. A dedupe hit wins first. |

Both stores refuse malformed input with a `TypeError` before writing; see the
[design notes](https://github.com/magna-illusions/petit-journal/blob/main/docs/design.md).

Also exported: `createMockEventStore(options?)`, `ensureSchema(pool, { tables }?)`, and the errors
`WrongExpectedSeqError`, `UnknownSeqError` (from `redact`; nothing is redacted) and
`CompactedLogError`.

### Transactions

`transaction` takes a connection, begins, takes the stream's lock and runs `work(tx)`. Inside it:

- **Await every `tx` call.** One that lands after `work` returned rejects.
- **Use `tx`, not `store`, for that stream.** A `store` write would wait for the lock `work` holds,
  so it throws at once; a `store` read misses what `work` has not committed.
- **Keep it short.** Do slow work first, and re-check with `expectedSeq`.

## Schema

Two ways to get the tables. `await openEventStore(pool)` creates them at startup if they are
missing. If you run your own migrations, put the SQL below in one and use `createEventStore(pool)`,
which never touches the schema. `ensureSchema(pool)` runs the same setup on its own, for a
migration script or a test.

Setup is safe on every boot, from several processes at once, and fails at boot with the reason. It
creates exactly this:

```sql
CREATE TABLE IF NOT EXISTS "domain_events" (
  id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  owner_id    TEXT NOT NULL,
  stream      TEXT NOT NULL,
  subject_id  TEXT NOT NULL,
  seq         BIGINT NOT NULL,
  type        TEXT NOT NULL,
  payload     JSONB NOT NULL,
  occurred_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  source      TEXT,
  dedupe_key  TEXT,
  UNIQUE (owner_id, stream, subject_id, seq)
);

CREATE UNIQUE INDEX IF NOT EXISTS "domain_events_dedupe_key"
  ON "domain_events" (owner_id, stream, subject_id, dedupe_key)
  WHERE dedupe_key IS NOT NULL;

CREATE TABLE IF NOT EXISTS "domain_projections" (
  owner_id    TEXT NOT NULL,
  stream      TEXT NOT NULL,
  subject_id  TEXT NOT NULL,
  state       JSONB NOT NULL,
  through_seq BIGINT NOT NULL,
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (owner_id, stream, subject_id)
);
```

`gen_random_uuid()` is built in from PostgreSQL 13; on 11 and 12 it needs `pgcrypto`. `owner_id` is
`TEXT`; in your own migrations it may be `UUID` if every owner is one. You may add columns of your
own to either table.

Other names are an option away: `createEventStore(pool, { tables: { events: 'ledger_events',
projections: 'app.ledger_state' } })`. The rules for names, and for the index `openEventStore`
creates beside an existing table, are in the design notes.

## Retry-safe writes

A retried request, such as a redelivered queue message, must not append twice. Pass a `dedupeKey`
that is the same on every attempt, and an `occurredAt` from the request, so the stored event is the
same whichever attempt lands:

```js
// `request` is what your caller retries: a queue message, a workflow activity's input.
async function deposit(request) {
  return ledger.append(account, {
    type: 'Deposited',
    payload: { amount: request.amount },
    occurredAt: request.requestedAt, // the request's time, not the attempt's
  }, { dedupeKey: request.id })      // the same key on every attempt
}
const request = { id: 'req-7f3a', amount: 50, requestedAt: '2026-10-08T12:00:00Z' }
const first = await deposit(request)  // { events: [the event], duplicate: false, state }
const again = await deposit(request)  // { events: [], duplicate: true, state }: nothing folded twice
```

A key protects only while its event is in the log: compaction deletes the key with the event. Keep
`keep` above the number of events a stream takes within your longest redelivery window.

## Redaction

`redact` is the one exception to append-only: it forgets an event's payload and keeps its `id`,
`seq`, `type`, `occurredAt` and `source`. With `defineProjection`, forget through `update`: the plan
re-folds the projection in the same transaction, so no reader sees the redacted log with the old
state.

```js
await ledger.update(account, () => ({ redact: [{ seqs: [first.events[0].seq], reason: 'erasure request 42' }] }))
```

Without `defineProjection`, redact and re-fold in one `store.transaction`:

```js
// The ledger's reducer. A tombstone keeps its event's type: skip it before reading the payload.
const fold = (state, event) => (event.redacted ? state : { balance: state.balance + event.payload.amount })

await store.transaction(account, async (tx) => {
  await tx.redact([first.events[0].seq], { reason: 'erasure request 42' })
  const events = await tx.loadEvents()
  await tx.saveProjection(events.reduce(fold, { balance: 0 }), events.at(-1).seq)
})
```

Both re-fold from seq 1. Once compaction has pruned the stream, correct the state by hand, in the
same transaction as the redaction.

## Encrypting payloads

The codec's main use.
[`examples/encrypting.js`](https://github.com/magna-illusions/petit-journal/blob/main/examples/encrypting.js)
seals every payload and state with AES-256-GCM from `node:crypto`, under a key per owner from your
`keyFor(ownerId)`, backed by your KMS or secret store. Pass it as
`openEventStore(pool, { codec: encryptingCodec(keyFor) })`. A wrong key, a tampered or
truncated value, or one copied from another stream or event type fails `decode`, and the store
call rejects with the stream, seq and type. Also:

- **Same-type swaps go unnoticed.** Two events of the same type in one stream can be swapped for
  each other, since `seq` is not known at encode time.
- **Length shows.** A ciphertext is as long as its plaintext, plus a fixed overhead.
- **A key seals at most about 2³² values.** Past that, the chance of a repeated random 96-bit IV
  exceeds NIST's 2⁻³² bound, and a repeated IV breaks GCM. One key per owner keeps each count small.
- **Rows written before the codec are plaintext,** and fail `decode`. Re-encode them first.

**Crypto-shredding.** Delete an owner's key and every payload and state of theirs is unreadable
everywhere, including backups, WAL and dead row versions, which `redact` cannot reach. It forgets a
whole owner, not one fact.

**What stays in clear:** each event's `type`, `source`, `dedupe_key` and `occurred_at`, and
`owner_id`, `stream` and `subject_id`. Keep personal data out of them.

## Timeouts

Bound every wait in the pool's config, which `pg` passes to each connection:

```js
new pg.Pool({ connectionString, connectionTimeoutMillis: 5000, lock_timeout: 10000, idle_in_transaction_session_timeout: 30000 })
```

- `lock_timeout` caps how long a write waits for its stream's lock.
- `idle_in_transaction_session_timeout` ends a transaction whose `work` stalls: the call rejects
  at once with the database's reason, even if `work` never returns, and the store discards the
  connection.
- `connectionTimeoutMillis` caps the wait for a connection from the pool.

## Examples

- [`examples/quickstart.js`](https://github.com/magna-illusions/petit-journal/blob/main/examples/quickstart.js):
  the quick start above.
- [`examples/encrypting.js`](https://github.com/magna-illusions/petit-journal/blob/main/examples/encrypting.js):
  an AES-256-GCM codec with a key per owner.

## Limits

- **One stream at a time.** It owns two tables and folds each stream into its own row, on the
  write path. There are no queries, read models or ordering across streams, no subscriptions or
  outbox, and it never alters its tables once created. Emmett is a framework and Message DB a
  message store with pub/sub and consumers; petit-journal is the event log and snapshot without
  either.
- **Timestamps come back as ISO strings, to the millisecond.** `timestamptz` keeps microseconds;
  the string drops them.
- **Redaction reaches the live tables only.** Dead row versions, WAL, CDC consumers, the
  projection's previous row and backups keep the old payload;
  [crypto-shredding](#encrypting-payloads) reaches them all.
- **A codec makes payloads opaque to SQL.** Filter on a clear column of your own, written in the
  same transaction.
- **The mock differs in four ways:** `tx.query` rejects, default times are the epoch, event ids are
  `mock-<seq>`, and an object's keys come back in the order written, where `jsonb` sorts them.

## Design

The full design, from the lock key to what redaction does not reach, is in
[docs/design.md](https://github.com/magna-illusions/petit-journal/blob/main/docs/design.md).

## License

MIT. See [LICENSE](https://github.com/magna-illusions/petit-journal/blob/main/LICENSE).
