# petit-journal: design notes

How the store works and why, in full. The [README](../README.md) is the guide: install, the
quick start, the schema, the API tables and the limits.

## Overview

A small append-only event store over PostgreSQL, with a folded read-model and bounded history.
No ORM, no framework, no dependencies: you hand it a `pg` pool, it hands you an event log per
stream, a projection row per stream, and a transaction scope to change both at once.

- **Events** go into `domain_events`, numbered `seq = 1, 2, 3…` per stream.
- **Projections** live in `domain_projections`: one row per stream holding your folded state
  and the highest `seq` it embodies. `defineProjection` keeps it folded with your reducer.
- **The schema** is two tables and an index, created by `openEventStore` or by your own migrations,
  under default or configured names.
- **Compaction** deletes events the projection has already absorbed, keeping a trailing audit
  window, so the log stays bounded.
- **Transactions** let you load, decide, append, save and run your own SQL on one stream as one
  unit under a per-stream lock.
- **Optimistic concurrency** (`expectedSeq`), **idempotent retries** (`dedupeKey`) and an
  optional **payload codec** (encryption, compression) are built in.
- **Redaction** is the one exception to append-only: it replaces a single event's
  payload with a tombstone, for erasing one fact without erasing the whole subject.

## Folding

The projection row stays current as long as every append is folded into it in the same
transaction. `defineProjection(store, { initial, reduce, keep })` does that for you (see
[defineProjection](#defineprojectionstore--initial-reduce-keep-redacted-)), and readers call its `state`.
The store's own methods do not fold: a plain `append` leaves the projection behind until something
catches it up. `defineProjection` catches it up on its next read or write, and `rebuild` re-folds
it from the start (see [rebuild](#projections-compaction-and-rebuild)).

## Schema notes

The tables are in the [README](../README.md#schema), exactly as `ensureSchema` creates them.

- There are two routes. `await openEventStore(pool, options)` runs `ensureSchema` and resolves to
  the store only once it has finished, so no code ever holds a store whose tables may not exist,
  and a failed setup rejects at boot. `createEventStore(pool, options)` never runs DDL, for an
  application that runs its own migrations. `ensureSchema(pool, { tables })` is exported on its
  own for migration scripts and tests. The gate is the async factory itself: the store carries no
  ready promise and no state that holds writes back.
- Setup is never lazy, on a first write: creating tables needs privileges an application's
  database role often lacks, and in a busy application the first writers would reach tables that
  do not exist yet. Every statement is `IF NOT EXISTS`, so it is safe on every boot.
- The mock has no `openEventStore` counterpart: it has no tables, so one would only wrap
  `createMockEventStore` in a promise. Tests swap `await openEventStore(pool)` for
  `createMockEventStore()`.
- `IF NOT EXISTS` alone does not make concurrent boots safe: two sessions creating the same table
  at once both pass the check, and one fails with a unique violation on
  `pg_type_typname_nsp_index`. So `ensureSchema` sends the statements as one query behind
  `pg_advisory_xact_lock(hashtextextended('petit-journal:schema', 0))`. `pg` sends a query without
  parameters as a simple query, and Postgres runs a simple query's statements as one transaction,
  which holds the lock until they are all done.
- The same query ends by selecting every column the store uses from both tables, `LIMIT 0`.
  `CREATE TABLE IF NOT EXISTS` skips a name anything already holds: an index Postgres named
  itself (`ev_pkey`, `ev_owner_id_stream_subject_id_seq_key`) or an older table of another shape.
  Without the check that setup passes and the first write fails; with it, setup fails at boot,
  and the statements before it roll back with it.
- `tables: { events, projections }` renames the tables. A name cannot be bound as a parameter, so
  it is checked at construction (lowercase letters, digits and underscores, starting with a letter
  or underscore, at most 52 characters, optionally `schema.table`) and quoted as an identifier in
  every statement. The dedupe index is `<table>_dedupe_key`, in the table's schema; 52 characters
  leave room for that suffix within Postgres's 63. Names that may resolve to one relation are
  refused: an unqualified name goes through the `search_path`, so `evy` and `public.evy` may be
  the same table, and a projections table named like the dedupe index would collide with it.
- Names in `pg_*` schemas and `information_schema` are refused: Postgres keeps those for itself.
- `ensureSchema` names the dedupe index after the events table. On existing tables whose dedupe
  index has another name, it adds a second unique index: name yours to match, or use
  `createEventStore`.
- A failed setup says which way it failed. An error from a session that started, which carries a
  severity and a SQLSTATE, is "could not set up the tables …", with a pointer to `createEventStore`
  on 42501, a role without CREATE. A connection that never opened, a failed login (28), a missing
  database (3D), too many connections (53) or a server going down (57) is "could not reach the
  database: …", with the inner messages when the error is an `AggregateError` of every address
  tried, so the reason is never empty. The original error is the `cause`.
- Poolers: the store holds no session state, and its only locks are transaction-scoped, so a
  transaction-mode pooler such as PgBouncer fits in principle (not tested here). `ensureSchema`
  sends one multi-statement simple query; a pool that speaks only the extended protocol rejects
  that ("cannot insert multiple commands into a prepared statement"). Run the README's SQL as a
  migration there instead.
- PostgreSQL 11 or later: the lock uses `hashtextextended`, which 10 does not have.
  `gen_random_uuid()` is built in from 13; on 11 and 12 it needs `pgcrypto`.
- The store reads `payload` and `state` as `::text` and parses them itself, once. `pg`'s default
  type parser would hand `jsonb` back already parsed, and a JSON string such as `"123"` could then
  not be told apart from JSON text; read as text, every JSON value reads back as written, whatever
  parsers the pool has.
- The `UNIQUE` constraint doubles as the index for loading a stream in order; no second index is
  needed.
- `owner_id` is `TEXT` here, so any id fits. In your own migrations it may be `UUID` if every
  owner is one; the store binds it as a parameter either way. A uuid owner in the standard form
  is written lowercase without braces (see [streams](#streams)), so a `TEXT` column holds one
  spelling of it too.
- There are no foreign keys, so the tables can be created in any order relative to your schema.
- You may add columns of your own to either table, for example a clear column your queries filter
  on, and write them from inside a [transaction](#transactions).

## Streams

Every call addresses a stream by a `StreamId`, a three-part tuple:

| Field | Column | Meaning |
|---|---|---|
| `ownerId` | `owner_id` | The **partition**: a tenant, an account, a user, whoever the data belongs to. |
| `stream` | `stream` | The **kind** of log, such as `'ledger'` or `'settings'`. A namespace for event types and a reducer. |
| `subjectId` | `subject_id` | The **aggregate** within that owner and kind: the specific thing whose history this is. |

`seq` is per tuple, starting at 1, and different tuples almost never contend (see the
[lock key](#transactions)). All three parts are required; a `StreamId` missing one is rejected
before any SQL runs.

**One spelling per uuid owner.** In a `UUID` column Postgres reads one uuid in upper or lower
case, with or without braces, as one value, so the rows of one owner are one set however it is
written. The lock key is text, though: two spellings would take two locks for one stream, and
writers through each would fold onto the same loaded state and lose events. So an `ownerId` in the
standard 8-4-4-4-12 form is written lowercase and without braces everywhere: in the SQL, the lock
key, the codec context, the mock's keys and `tx.stream`. A lowercase id is left exactly as it was,
so its lock key is byte-for-byte the one earlier versions took. Anything else is an ordinary `TEXT`
id and is used as given: 32 bare hex digits (an md5) are not taken for a uuid, and two cases of
them are two owners. That cannot be undone once ids are in rows and lock keys, so it errs on the
side of leaving an id alone.

Passing the same value as `ownerId` and `subjectId` is the normal case when an owner has one
aggregate per stream: one ledger per account, one settings record per user. The split leaves room
for an owner with many subjects in one stream, each with its own sequence, without a schema change.

## Transactions

```js
const result = await store.transaction(stream, async (tx) => {
  // tx.loadProjection(), tx.loadEvents(afterSeq), tx.maxSeq(), tx.eventByDedupeKey(key)
  // tx.append(event, options), tx.saveProjection(state, throughSeq), tx.compact(throughSeq, keep)
  // tx.redact(seqs, { reason, tombstone })
  // tx.query(sql, params): your own SQL, on the same connection
  // tx.stream: the StreamId this transaction is on
  return anything
})
```

`transaction` takes a connection from the pool, runs `BEGIN`, and takes the stream's advisory lock
(`pg_advisory_xact_lock`). Then it runs `work(tx)`. If `work` resolves, it commits and returns what
`work` returned. If `work` rejects, it rolls back and rethrows. Either way the lock is released
with the transaction and the connection goes back to the pool.

Use it whenever a write depends on what you read: load the projection, decide, append, fold, save.
Every write to the stream takes the same lock: other transactions, and a standalone `append`,
`saveProjection`, `compact` or `redact`. So no two writers can fold onto the same loaded state
and lose an event, and no write lands in the middle of a transaction. Reads outside a transaction
do not wait. They see the last committed state.

While it holds the connection, the store listens for the client's `'error'` event. A backend that
dies mid-transaction (terminated, or ended by `idle_in_transaction_session_timeout`) makes `pg`
emit it, and with nothing listening that would crash the process. Heard, it ends the call at once,
even when `work` is waiting on something that never settles, with an error that names the stream
and carries the database's error as its `cause`; the connection is released as broken, so the pool
discards it, and any later `tx` call is refused.

If a statement inside `work` fails, Postgres aborts the transaction. Catching that error and
returning normally does not save anything: the `COMMIT` becomes a rollback, and `transaction`
rejects instead of reporting success.

`tx.query` is the reason the scope exists. A value your SQL must filter on (a due date, a status)
can sit in a column of your own beside the projection, written in the same transaction, so the
column and the state can never disagree:

```js
await store.transaction(stream, async (tx) => {
  const { event } = await tx.append({ type: 'Scheduled', payload: { dueOn } })
  await tx.saveProjection(next, event.seq)
  await tx.query(
    'UPDATE domain_projections SET due_on = $4 WHERE owner_id = $1 AND stream = $2 AND subject_id = $3',
    [tx.stream.ownerId, tx.stream.stream, tx.stream.subjectId, dueOn],
  )
})
```

To have every write of a stream also update a column of your own, wrap the store's `transaction`
and hand `work` `{ ...tx, saveProjection }`, where your `saveProjection` calls `tx.saveProjection`
and then `tx.query`. Spread `tx`, so every other method stays: `defineProjection` needs them,
`eventByDedupeKey` included.

```js
const withDueOn = (store) => (stream, work) => store.transaction(stream, (tx) => work({
  ...tx,
  async saveProjection(state, throughSeq) {
    await tx.saveProjection(state, throughSeq)
    await tx.query('UPDATE domain_projections SET due_on = $4 WHERE owner_id = $1 AND stream = $2 AND subject_id = $3',
      [tx.stream.ownerId, tx.stream.stream, tx.stream.subjectId, state.dueOn])
  },
}))
```

Rules that follow from holding a lock:

- **Await every `tx` call inside `work`.** A call still pending when `work` returns would run after
  `COMMIT`, outside the lock, so it rejects instead.
- **Inside `work`, use `tx`, not `store`, for that stream.** A `store` call for the same stream
  runs on another connection: a write would wait for the lock that `work` holds, forever, so the
  store refuses it with a `TypeError` at once. It tracks the locks the current async context holds
  with one `AsyncLocalStorage` for the whole process, so a write through a second store, on the
  same pool or on other tables, is refused just the same; a lock stops counting when its
  transaction ends, so work `work` merely scheduled writes freely afterwards. A read does not see
  what `work` has written but not yet committed.
- **Keep `work` short.** Slow work, such as a network call or a model call, belongs before the
  transaction. Pass what you decided into it and re-check there, for example with
  [`expectedSeq`](#expected-seq).
- The transaction runs at your database's default isolation level (`READ COMMITTED` unless you
  changed it). Each statement sees everything committed before it, which is what the lock relies
  on.

The lock key is the text `ownerId:stream:subjectId`, hashed with `hashtextextended(key, 0)`. It
stays the same across versions, so other code can take the same lock to serialize with the store.
Two different streams can share a lock: their parts can join to the same text around `:`
(`ledger:eur` + `x` and `ledger` + `eur:x`), or two keys can collide in the 64-bit hash. The key
does not name the tables either, so one stream tuple in two stores on different tables takes one
lock.
Such streams wait for each other's writes; their rows never mix, because every statement is
scoped to the full tuple.

## API

All methods return promises. The store's methods are plain closures, so `store.transaction` can be
passed around without binding.

### `createEventStore(pool, { codec, tables }?)` and `openEventStore(pool, { codec, tables }?)`

`pool` is anything with `query(text, params)` and `connect()` returning a client with `query` and
`release()`. `pg.Pool` fits as-is. The package does not depend on `pg`. A `pool` without
`query` and `connect` is refused with a `TypeError` here, not at the first write.

| The store calls | For |
|---|---|
| `pool.query(text, params)` | Reads outside a transaction. Must resolve to `{ rows, rowCount }`. |
| `pool.query(text)` | `openEventStore` and `ensureSchema` only: one multi-statement query, which `pg` runs as a single transaction. |
| `pool.connect()` | A client for each write and each `transaction`. |
| `client.query(text, params)` | `BEGIN`, the advisory lock, the statements, `COMMIT`. The `COMMIT` result's `command` is read: `'ROLLBACK'` means a statement failed, and the call rejects. |
| `client.on('error')`, `client.removeListener('error')` | If the client has them: listened to while the store holds it. |
| `client.release(err?)` | Always called. Passed the error when the connection died or even `ROLLBACK` failed, so the pool discards it. |
 `codec` is
optional; see [payload codec](#payload-codec). `tables` is optional; see
[schema notes](#schema-notes).

`openEventStore` makes the same store and resolves to it once `ensureSchema` has finished; a bad
option is refused before any SQL. See [schema notes](#schema-notes).

### `ensureSchema(pool, { tables }?)`

Creates both tables and the dedupe index if they do not exist; see [schema notes](#schema-notes).
A failure names the tables and keeps the database's error as its `cause`.

### `append(stream, event, { dedupeKey, expectedSeq }?) → { event, appended }`

Also `tx.append(event, options)` inside a transaction.

Appends at `seq = MAX(seq) + 1` under the stream's lock. On its own, `append` is a one-operation
transaction.

`event` is `{ type, payload, occurredAt?, source? }`. The store assigns `id` and `seq`, and sets
`occurredAt` to `now()` when it is omitted. `payload` is any JSON value, opaque to the store.

It always returns `{ event, appended }`:

- `event` is the stored event: `{ id, ownerId, stream, subjectId, seq, type, payload, occurredAt, source }`.
- `appended` is `true` when this call wrote it. It is `false` only for a [dedupe](#dedupe) hit,
  and `event` is then what the first call wrote.

Malformed input is rejected with a `TypeError` before anything is written or encoded:

- an event without a non-empty string `type`, or with a `payload` of `undefined` (`null` is a JSON
  value and is fine);
- a payload with its own `$redacted` key (see [redaction](#redaction));
- an event that carries `dedupeKey` or `expectedSeq` itself: on the event they would be ignored,
  and a retry would append twice, so the message names the options argument;
- an `occurredAt` that is not an instant. A `Date`, or ISO 8601 with `Z` or an offset, is one.
  Postgres reads a time without a zone in its own time zone, and words such as `'now'` at the
  moment each retry lands, so a retried event would not be the same event. A calendar date that
  `Date` would roll over (`2026-02-30`) is refused as Postgres refuses it;
- a stream id part, `type`, `source` or `dedupeKey` with `\u0000` or half a surrogate pair: the
  `TEXT` columns refuse `\u0000` as a raw database error, and `pg` writes half a pair as U+FFFD,
  so a `type` would come back changed and fail its codec context;
- a string anywhere in what `jsonb` would receive, value or key, with `\u0000` or half a surrogate
  pair, which it cannot store. That is the payload or state itself, or with a codec the codec's
  output, so a sealed value may hold any string. It is checked after encoding and before anything
  is written, tombstones included, and the message names where it is (`event payload.list[1]`);
- malformed options (a non-object, an empty `dedupeKey`, a negative or fractional `expectedSeq`).

A stream at seq 2^53 − 1 refuses another append: past that a JavaScript number would round, and
two events would share a seq.

#### Expected seq

```js
const seen = await store.maxSeq(stream)
// … decide, slowly, without holding anything …
await store.append(stream, event, { expectedSeq: seen })
```

If you keep every append folded into the projection, `projection.throughSeq` (or `0` when there is
no projection yet) is the same number, and one read gives you both the state and the seq.

`expectedSeq` is optimistic concurrency. The append happens only if the stream's newest `seq` is
exactly `expectedSeq` (`0` for an empty stream). Otherwise it throws `WrongExpectedSeqError` and
writes nothing:

```js
import { WrongExpectedSeqError } from 'petit-journal'

try {
  await store.append(stream, event, { expectedSeq: 7 })
} catch (err) {
  if (!(err instanceof WrongExpectedSeqError)) throw err
  err.expectedSeq // 7
  err.actualSeq   // 8: someone else appended first
  err.stream      // the StreamId
}
```

The check runs under the lock, so it is exact. Inside a transaction the error leaves the
transaction usable: nothing was written, so you can catch it, reload and decide again, or let it
propagate and roll back.

#### Dedupe

The lock stops two *concurrent* writers from taking one `seq`. It cannot stop the same request
from being appended twice *in sequence*, for example by a message handler that appended, then
threw, then was redelivered. Pass a `dedupeKey` that is stable across retries of one request and
distinct per new request:

```js
const { event, appended } = await store.append(stream, newEvent, { dedupeKey: message.id })
if (!appended) {
  // A retry: `event` is what the first attempt wrote. The retried payload was not written.
}
```

Keys are scoped to the stream. A dedupe hit is returned before `expectedSeq` is checked, because
a retry's own first attempt is what moved the stream. With both options, a retry is answered as a
retry, not as a conflict. A key lives in its event's row, so compaction deletes it with the event:
keep more events than a stream takes in your longest redelivery window. `tx.eventByDedupeKey(key)`
reads the event a key wrote, or `null`, for a caller that wants to recognise a retry before it
plans anything.

### `redact(stream, seqs, { reason, tombstone? }) → number[]`

Also `tx.redact(seqs, options)` inside a transaction. Replaces the payload of each event at `seqs`
with a tombstone, under the stream's lock, all or nothing. A seq the stream does not hold rejects
with `UnknownSeqError` and nothing is redacted. Returns the redacted seqs, ascending and
de-duplicated. See [redaction](#redaction).

### `loadEvents(stream, afterSeq = 0) → StoredEvent[]`

Also `tx.loadEvents(afterSeq)`. Events with `seq > afterSeq`, in order. Pass
`projection.throughSeq` to get only what the projection has not folded yet. A redacted event
carries `redacted: true`; no other event has the field.

### `loadProjection(stream) → { ownerId, stream, subjectId, state, throughSeq, updatedAt } | null`

Also `tx.loadProjection()`. The projection row, or `null` if the stream has never been projected.

### `saveProjection(stream, state, throughSeq)`

Also `tx.saveProjection(state, throughSeq)`. Upserts the row, under the stream's lock. `throughSeq`
is the highest `seq` the state embodies, and compaction trusts it: a `throughSeq` above what you
actually folded authorizes deleting events that were never applied. A `state` of `undefined`, a
string in it that `jsonb` cannot store, or a `throughSeq` that is not an integer `>= 0`, is
rejected with a `TypeError` before anything is written.

### `maxSeq(stream) → number`

Also `tx.maxSeq()`. The newest `seq` in the stream, or `0` when it is empty.

### `compact(stream, throughSeq, keep) → number`

Also `tx.compact(throughSeq, keep)`. Under the stream's lock, deletes folded events outside the
trailing audit window and returns how many it deleted. It is safe to call after every save: it
does nothing until the stream outgrows `keep`. Both arguments are required integers `>= 0`;
anything else, a missing `keep` included, is rejected with a `TypeError` rather than read as
"delete nothing". When a projection row exists, the cutoff never passes its `through_seq`, read in
the same transaction: an event the projection has not folded stays, so it can always catch up. See
[compaction](#projections-compaction-and-rebuild).

### `transaction(stream, work) → whatever work returns`

See [transactions](#transactions).

### `createMockEventStore({ codec, tables }?)`

An in-memory store with the same interface and the same contract, for tests and no-database modes:
the same `seq` assignment, `dedupeKey` and `expectedSeq` behaviour, and the same
`{ event, appended }` shape. Transactions on one stream run one at a time, and one that throws
restores the stream (events, projection and dedupe keys) to how it found it. The mock stores what
`jsonb` would hand back and hands out copies, so a codec that returns bytes or a `BigInt` fails in
a test too, and mutating a value you passed in or got back never changes what it holds.

It checks `tables` as the pg store does, and ignores them. `compact` deletes as the pg store does,
dedupe keys of deleted events included, so code that needs the oldest events fails in a no-DB test
too. Four differences: `tx.query` rejects (there is no SQL); event ids are `mock-<seq>` rather
than UUIDs; `occurredAt`, a projection's `updatedAt` and a redaction tombstone's `at` default to the
epoch, so assertions are stable; and an object's keys come back in the order they were written,
where `jsonb` sorts them (shorter keys first). Compare objects by value, not by key order.

### `defineProjection(store, { initial, reduce, keep?, redacted? })`

Returns `{ append, update, state, rebuild }` for one kind of projection, built only on the store's
public methods, so it runs on either store. Each write is one `store.transaction`:

1. Load the projection, and fold on any event past its `throughSeq` (one appended with the store's
   own methods). A stream never written starts from a fresh copy of `initial`.
2. Append the events, then run any redactions an `update` plan asked for.
3. Fold the appended events onto the loaded state with `reduce`, or, when the plan redacted,
   re-fold the whole log from `initial`. Save the state at the last `seq`.
4. With `keep` set, `compact(lastSeq, keep)`.

`reduce(state, event)` is synchronous. Redacted events are skipped before it sees them, so a
reducer never handles a tombstone, unless `redacted: 'fold'`: then it gets each one, with
`redacted: true` and the tombstone payload, in every fold path (catching up and `rebuild`), for a
state that keeps forgotten entries as forgotten. Because every fold from `initial` starts from a
fresh copy (`structuredClone`), a reducer that mutates its state cannot change `initial` or
another stream.

Every write returns `{ state, events, duplicate, redacted }`: the state after it, what it
appended, whether it was a dedupe hit, and the seqs it redacted. There is no `appended` flag:
`events` says what was appended, and the store's own `appended` means something else (a dedupe
hit returns the first write there).

- **`append(stream, eventOrEvents, options?)`.** Options apply to the batch as a whole.
  `expectedSeq` is the stream's `seq` before the batch. A `dedupeKey` goes on the batch's first
  event: the batch commits as one, so a hit there means the whole batch is in, and nothing is
  appended or folded (`duplicate: true`). That is the one way to make a batch retry-safe without a
  key per event, and it keeps the stored key exactly the one you passed. An empty batch writes
  nothing and applies no option, whatever is passed, so its key stays unused (no `events`,
  `duplicate: false`): a caller whose planning legitimately finds nothing to do needs no special case.
- **`update(stream, decide, options?)`** does the same with what `decide(state, ctx)` returns: an
  array of events (`[]` for nothing), or a plan `{ append, redact }` whose `append` is an array
  too. An array is events and an object is a plan, so nothing is guessed from keys; anything else,
  a bare event included, is a `TypeError`. `decide` may be async. It runs under the stream's lock,
  so every other write to the stream waits for it: keep it quick. It gets a copy of the state, and
  `ctx.loadEvents(afterSeq?)` to read the log inside the transaction. With a `dedupeKey` that is
  already stored, `update` returns `duplicate: true` before it calls `decide`: a retry is not
  planned again, against a state its first attempt moved.
- **A plan appends and redacts in one transaction.** `redact` is a list of groups
  `{ seqs, reason, tombstone? }`, each its own `tx.redact`, so each event can get its own
  tombstone. The events are appended first, then the groups redacted, so a dedupe hit on the
  first append returns before any redaction: the first attempt made them. Then the whole log is
  re-folded from `initial`, under the same `redacted` policy as every fold, and saved: a fold of
  the appended events onto the loaded state would keep the redacted facts the state already held.
  Any failure rolls back the append and every redaction. A plan with only `redact` is the atomic
  forget. It is the one shape that covers a forget path atomically without handing out the
  transaction itself: `decide` reads, and returns what to write.
- **Keys and plans.** The `dedupeKey` lives on the first appended event, so a redact-only plan
  stores no key: a retry redacts again, which only replaces each tombstone with one stamped later.
  And a plan whose key some other write already stored is answered as a `duplicate` before
  `decide` runs, so its redactions never happen: a forget needs a key of its own.
- **What a plan may redact.** Only events the stream held before the update: a later seq, which
  would be one of its own appends or none at all, is a `TypeError`. And only on a log that still
  starts at seq 1, since the re-fold needs every event; on a compacted log the plan is refused
  with an error pointing to `store.transaction`, where you redact and correct the state yourself.
  On a projection with `keep` it is refused from the first call: compaction would otherwise make
  it fail only later, once the log had been cut, which a short test would never show.
- **`state(stream)`** reads first without a lock, so readers do not queue behind writers: the
  projection, then the events past it. A compaction that lands between those two reads can open a
  gap; then it reads again inside a transaction, under the lock, where nothing lands in between, so
  a gap there is real. Reading under the lock every time would be simpler, but every read would
  then wait for every write to the stream.
- **`rebuild(stream)`** re-folds the whole log from `initial` and saves it, in one transaction, as
  after a reducer change. On an empty stream it writes nothing and returns `initial`. It refuses a
  log that does not start at `seq` 1: compaction removed the oldest events, and a state folded
  without them would be wrong.

Any fold that finds the log does not continue from the projection's `throughSeq` (compacted past
it, as only the store's own methods can do) refuses rather than fold from the wrong place. Every
such refusal is a `CompactedLogError`, with the `stream` and the oldest `seq` left (`firstSeq`).

Two more checks guard the state that gets saved. `reduce` must return the next state: `undefined`
(a reducer that forgot to return) or a promise (an async reducer, whose next event would fold onto
the promise) throws a `TypeError` naming the event. And a write checks that its first event landed
right after the state it folded; under the lock it always does, unless a writer took another lock
for the same rows, and then the write refuses rather than save a state that skips events.

## Projections, compaction and rebuild

The projection row is the snapshot. Every write folds into it in the same transaction as the
append, so reads are a single row lookup and never replay events.

**Compaction** keeps the log bounded. `compact(throughSeq, keep)` deletes events that satisfy
both rules:

1. The projection has folded them: `seq <= throughSeq`.
2. They are older than the newest `keep` events, which stay as an audit and replay trail.

The newest event is always kept, even with `keep: 0`. `seq` is assigned from the newest
surviving event, so an emptied log would restart at 1, behind the projection. With 200 events:
`compact(200, 100)` deletes seq 1 to 100; `compact(50, 100)` deletes 1 to 50, never past what
was folded; with 100 events, `compact(100, 100)` deletes nothing; with 30, `compact(30, 0)`
deletes 1 to 29.

A small `keep` gives up the ability to replay from scratch. That is a valid choice, but make it
knowingly.

**Rebuild** replays events onto a state. `defineProjection` does both of these for you (its
`state` catches up, its `rebuild` re-folds from `initial`). By hand it is a plain fold over
`loadEvents`, which returns events in `seq` order:

```js
// Catch up a projection that is behind:
const projection = await store.loadProjection(stream)
const events = await store.loadEvents(stream, projection?.throughSeq ?? 0)
const state = events.reduce(reduce, projection?.state ?? initial)

// Rebuild from scratch after changing the reducer, when compaction has kept every event:
await store.transaction(stream, async (tx) => {
  const events = await tx.loadEvents()
  if (events.length > 0) await tx.saveProjection(events.reduce(reduce, initial), events.at(-1).seq)
})
```

Payload evolution is up to you. `payload` is opaque `jsonb`, so a reducer has to handle every
shape it ever wrote. Compaction helps here: old events eventually stop existing, down to `keep`.

## Payload codec

```js
const store = createEventStore(pool, {
  codec: {
    encode: async (value, context) => seal(await keyFor(context.ownerId), value),
    decode: async (stored, context) => open(await keyFor(context.ownerId), stored),
  },
})
```

An optional pair of functions applied to every event `payload` and every projection `state`.
`encode` runs on the way into storage and `decode` on the way out, including `append`'s return
value. Use it to encrypt, compress or version what is stored. The store does not know which.

- **`encode` must return a JSON value.** It is `JSON.stringify`'d into the same `jsonb` column, and
  `decode` receives it parsed. Wrap binary output, for example as base64 in an object such as
  `{ v: 1, iv, ciphertext }`. Both functions may be async.
- **The context** says what is being encoded:
  `{ kind: 'event', ownerId, stream, subjectId, type }` or
  `{ kind: 'projection', ownerId, stream, subjectId }`. It is built from the `StreamId` you pass,
  with a uuid owner in its one spelling, never from the stored row. So decode sees exactly what
  encode saw.
- **Failures are loud.** A throw from either function rejects the call. Nothing falls back to clear
  text, and nothing stored is returned undecoded. An `encode` that returns `undefined` is refused.
  A decode failure names what was being read (`could not decode the Deposited event at seq 7 on
  ledger/main: …`), with the codec's error as the `cause`.
- **Order.** A standalone `append` encodes before it takes a connection, so a slow or failing codec
  never holds the lock. It decodes its return value before `COMMIT`, so a decode failure rolls the
  append back instead of reporting failure for a stored event. Inside a transaction, encoding
  happens under the lock.
- **A deduped retry still runs `encode`**, because the payload is encoded before the store knows
  the key exists. The first call's payload is returned, decoded.
- **Encoded columns are opaque to SQL.** `payload->>'x'` returns what the codec wrote, not the
  field. Anything your SQL must filter, sort or index on belongs in a clear column of its own,
  written with `tx.query` in the same [transaction](#transactions).

Adding a codec to a table that already has rows is a migration you own: older rows are still in
the old form when `decode` sees them. A version field in the encoded shape is one way to tell them
apart.

## Redaction

The log is append-only, with one exception: `redact` overwrites the payload of events already in
it. It exists for "forget this one thing", such as a GDPR erasure request for a single fact, when
the rest of the subject's history must stay. To erase everything about a subject, crypto-shred
its key instead (see [what redaction does not reach](#what-redaction-does-not-reach)).

```js
await store.redact(stream, [7], { reason: 'erasure request 42' }) // → [7]
```

Each event keeps its `id`, `seq`, `type`, `occurredAt` and `source`. Only `payload` changes, to a
tombstone:

```js
{ $redacted: true, reason: 'erasure request 42', at: '2026-10-07T09:30:00.000Z' }
```

- **All or nothing, under the lock.** `redact` takes the stream's lock like every other write.
  It checks every seq before it writes any. A seq the stream does not hold (never appended, or
  already compacted away) rejects with `UnknownSeqError`, whose `seqs` lists the missing ones,
  and nothing is redacted. Inside a transaction, a later error rolls the redaction back with
  everything else.
- **`reason` is required.** It is stored in the tombstone, so the log says why a payload is gone.
  Use an identifier, such as a request id, not a description of what was removed.
- **Your own tombstone keeps what a fold still needs.** Pass `tombstone` to keep fields that are not
  the fact being forgotten, such as an amount the balance depends on:
  `redact(stream, [7], { reason, tombstone: { amount: 50 } })`. `$redacted`, `reason` and `at`
  are always the store's and override the same keys in yours.
- **Telling a tombstone apart.** `loadEvents` sets `redacted: true` on an event whose decoded
  payload has `$redacted: true`, so the `$redacted` key is reserved: `append` rejects a payload
  that has its own `$redacted` key, with a `TypeError` and before the codec runs. Otherwise an
  application that spreads request JSON into a payload could forge a tombstone, and its reducer
  would skip a real event. A deduped retry of a redacted event returns the tombstone, not the original.
  Redacting a redacted event again replaces its tombstone.
- **Only the payload is redacted.** `dedupe_key`, `type`, `source` and `occurred_at` (and `id`,
  `seq`, and any column of your own) are left exactly as they were. Keep personal data out of
  them: a dedupe key built from an email address, or a `type` that names a person, outlives the
  redaction. A clear column of your own that holds part of the fact must be cleared with
  `tx.query` in the same transaction as `tx.redact`.
- **It goes through the codec.** The tombstone is encoded like any payload, under the event's own
  context (its `type` included), so an encrypting codec stores it sealed too. The encoding happens
  under the lock, because only the stored row knows each event's type.

### Projections are yours to re-fold

`redact` does not touch the projection, and the projection may still embody the fact. With
`defineProjection`, redact through an `update` plan: it re-folds the whole log from `initial` in
the same transaction, after the redactions, so no reader ever sees the redacted log with the old
state (see [defineProjection](#defineprojectionstore--initial-reduce-keep-redacted-)). Without
it, redact and re-fold in one transaction by hand. Reducers then meet tombstones under the
original `type`, so handle `event.redacted` before reading the payload:

```js
const reduce = (state, event) =>
  event.redacted ? state : { balance: state.balance + event.payload.amount }

await store.transaction(stream, async (tx) => {
  await tx.redact([7], { reason: 'erasure request 42' })
  const events = await tx.loadEvents()
  await tx.saveProjection(events.reduce(reduce, { balance: 0 }), events.at(-1).seq)
})
```

That full rebuild needs every event since the start. Once [compaction](#projections-compaction-and-rebuild)
has pruned the stream, the projection is the only record of the pruned events: correct the state
yourself (remove the fact from it) and save it in the same transaction as the redaction. A fact
that only lives in the projection, because its event was compacted away, is removed the same way.

### What redaction does not reach

`redact` changes what the live table returns, and nothing else. Postgres never overwrites a row
in place: an `UPDATE` writes a new row version and leaves the old one behind as a dead tuple. The
old payload, in clear or as the old ciphertext, can survive in:

- **The table's data files.** Plain `VACUUM` (and autovacuum) only marks a dead tuple's space as
  reusable. It does not wipe it: the bytes can stay on disk until new rows happen to overwrite
  them. Only `VACUUM FULL` or `CLUSTER` rewrites the table into new files, and even then the old
  files' blocks are handed back to the filesystem, not erased.
- **TOAST.** A large payload is stored out of line in the table's TOAST table. Its old chunks are
  dead tuples there, under the same rules.
- **WAL.** The original `INSERT` and the redacting `UPDATE` are both in `pg_wal` until it is
  recycled, and in any WAL archive kept for point-in-time recovery for as long as that is kept.
  No form of `VACUUM` touches WAL.
- **Logical decoding and CDC.** A consumer that already received the row (a replication slot, a
  change-data-capture pipeline, a search index) keeps its copy. Redaction reaches it only as a
  later update, if it applies one.
- **The projection.** Re-folding after a redaction upserts `domain_projections`, which leaves the
  previous state, with the fact in it, as a dead row version of its own.
- **Backups**, until they age out.

Crypto-shredding is the only thing that reaches all of these: destroy the key, and every copy of
what it sealed, live, dead, logged, replicated or backed up, becomes unreadable. That works for a
whole subject, not for one fact sealed under the same key as the rest. An application that needs
a single fact gone from all of them must account for that itself, for example by sealing such
facts under a key of their own, or by stating its WAL and backup retention as the erasure
deadline.

## What this is not

There are no subscriptions, no outbox, no cross-stream queries, transactions or read models, and
no separate snapshot rows (the projection row is the snapshot). Ordering is guaranteed within one
stream and nowhere else. `ensureSchema` creates the tables once and never alters them: evolving
the schema is a migration you own.

## Packaging

Requires Node 22.13 or later. The source is plain ES modules with no build step, and `exports`
points straight at it. `require()` loads it too, as a `require()` of an ES module, which Node
supports unflagged from 22.12 and without an experimental warning from 22.13. Types ship with the
package.

```js
import {
  openEventStore, createEventStore, ensureSchema, createMockEventStore, defineProjection,
  WrongExpectedSeqError, UnknownSeqError, CompactedLogError,
} from 'petit-journal'
// or
const { createEventStore } = require('petit-journal')
```

## Source layout

`src/index.js` is the only entry. Each folder under `src/` is one concern, with its tests beside it:

- `pg/`: the store on a pool, and `openEventStore`, which sets up the schema before it hands one out.
- `mock/`: the store in memory, with the same contract.
- `transaction/`: the `tx` both stores hand to `transaction(stream, work)`, and the record of the
  locks the current async context holds.
- `schema/`: the table names, the SQL and `ensureSchema`.
- `projection/`: `defineProjection`, what its `decide` may return, its checks and its errors.
- `compaction/`, `redaction/`, `codec/`: each operation's input checks and pure logic, shared by
  both stores.

The small primitives both stores share sit beside `index.js`:

- `stream-id.js`: the `StreamId`, its one spelling, `afterSeq` and the lock key.
- `append.js` and `save-check.js`: the input checks for `append` and `saveProjection`.
- `storable-text.js`: the strings Postgres can store. `plain-object.js` and `instant.js`, the
  timestamp normaliser.

Outside `src/`, `examples/` holds the README's examples as the type check sees them, and
`test/types.ts` holds the declarations to the code; neither ships.

## Tests

```sh
npm test
npm run typecheck
```

`node:test` over `src/**/*.test.js`, with no database. The pure parts (the compaction boundary, the redaction input and
tombstone) are tested as functions at their boundaries. The PostgreSQL store is driven against a
fake pool that records every statement, and the tests assert on the SQL itself: the transaction
sequence, the lock key, the parameters, and that nothing reaches the connection after `COMMIT`.
The mock is held to the same contract, and the codec runs through both. `defineProjection` runs
every test on both stores, the pg one over a fake pool that keeps and deletes rows. `typecheck`
compiles
`test/types.ts`, which holds the hand-written declarations to the code's exports and shapes, and
the README's examples in `examples/`, against the declarations and `pg`'s own types.
