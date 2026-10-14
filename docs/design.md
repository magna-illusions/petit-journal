# petit-journal: design notes

How the store works and why. The [README](../README.md) covers install, the quick start, the
schema, the API tables and the limits; these notes do not repeat them.

## The pool

`pool` is anything with `query(text, params)` and `connect()` returning a client with `query` and
`release()`. `pg.Pool` fits as-is, and the package does not depend on `pg`. A `pool` without
`query` and `connect` is refused with a `TypeError` at construction, not at the first write.

| The store calls | For |
|---|---|
| `pool.query(text, params)` | Reads outside a transaction. Must resolve to `{ rows, rowCount }`. |
| `pool.query(text)` | `openEventStore` and `ensureSchema` only: one multi-statement query, which `pg` runs as a single transaction. |
| `pool.connect()` | A client for each write and each `transaction`. |
| `client.query(text, params)` | `BEGIN`, the advisory lock, the statements, `COMMIT`. The `COMMIT` result's `command` is read: `'ROLLBACK'` means a statement failed, and the call rejects. |
| `client.on('error')`, `client.removeListener('error')` | If the client has them: listened to while the store holds it. |
| `client.release(err?)` | Always called. Passed the error when the connection died or even `ROLLBACK` failed, so the pool discards it. |

All methods return promises. They are plain closures, so `store.transaction` can be passed around
without binding.

## Schema notes

The tables are in the [README](../README.md#schema), exactly as `ensureSchema` creates them.

- `await openEventStore(pool, options)` runs `ensureSchema` and resolves to the store only once it
  has finished. So no code ever holds a store whose tables may not exist, and a failed setup
  rejects at boot. `createEventStore(pool, options)` never runs DDL. The async factory is the only
  gate: the store carries no ready promise and no state that holds writes back. A bad option is
  refused before any SQL.
- Setup never runs lazily on a first write. Creating tables needs privileges an application's
  database role often lacks, and in a busy application the first writers would reach tables that
  do not exist yet. Every statement is `IF NOT EXISTS`, so it is safe on every boot.
- The mock has no `openEventStore` counterpart. It has no tables, so one would only wrap
  `createMockEventStore` in a promise.
- `IF NOT EXISTS` alone does not make concurrent boots safe: two sessions creating the same table
  at once both pass the check, and one fails with a unique violation on
  `pg_type_typname_nsp_index`. So `ensureSchema` sends the statements as one query behind
  `pg_advisory_xact_lock(hashtextextended('petit-journal:schema', 0))`. `pg` sends a query without
  parameters as a simple query, and Postgres runs a simple query's statements as one transaction,
  which holds the lock until they are all done.
- The same query ends by selecting every column the store uses from both tables, `LIMIT 0`.
  `CREATE TABLE IF NOT EXISTS` skips a name that something already holds, such as an index
  Postgres named itself (`ev_pkey`, `ev_owner_id_stream_subject_id_seq_key`) or an older table of
  another shape. Without the check, setup would pass and the first write fail. With it, setup fails
  at boot, and the statements before it roll back.
- `tables: { events, projections }` renames the tables. A name cannot be bound as a parameter, so
  it is checked at construction and quoted as an identifier in every statement. It must be
  lowercase letters, digits and underscores, start with a letter or underscore, and be at most 52
  characters, optionally as `schema.table`. The dedupe index is `<table>_dedupe_key`, in the
  table's schema, and 52 characters leave room for that suffix within Postgres's 63. Names that
  may resolve to one relation are refused: an unqualified name goes through the `search_path`, so
  `evy` and `public.evy` may be the same table, and a projections table named like the dedupe
  index would collide with it. Names in `pg_*` schemas and `information_schema` are refused too,
  since Postgres keeps those for itself.
- `ensureSchema` names the dedupe index after the events table. If an existing table's dedupe
  index has another name, it adds a second unique index. Name yours to match, or use
  `createEventStore`.
- A failed setup says which way it failed. If the server could not be reached or refused the
  session (a failed login, a missing database, too many connections, a shutdown), the message is
  "could not reach the database: …". When `pg` tried several addresses it lists each one's error,
  so the reason is never empty. Any other failure is "could not set up the tables …", and a role
  without CREATE (42501) is pointed to `createEventStore`. The original error is the `cause`.
- Poolers: the store holds no session state, and its only locks are transaction-scoped, so a
  transaction-mode pooler such as PgBouncer should fit (not tested here). `ensureSchema` sends one
  multi-statement simple query, and a pool that speaks only the extended protocol rejects it
  ("cannot insert multiple commands into a prepared statement"). Run the README's SQL as a
  migration there instead.
- PostgreSQL 11 or later: the lock uses `hashtextextended`, which 10 does not have.
- The store reads `payload` and `state` as `::text` and parses them itself, once. `pg`'s default
  type parser would hand `jsonb` back already parsed, and a JSON string such as `"123"` could then
  not be told apart from JSON text. Read as text, every JSON value reads back as written, whatever
  parsers the pool has.
- The `UNIQUE` constraint doubles as the index for loading a stream in order.
- `owner_id` is `TEXT` so that any id fits. The store binds it as a parameter, so a `UUID` column
  in your own migrations works too. A uuid owner in the standard form is written in one spelling
  (see [streams](#streams)), so a `TEXT` column holds one spelling of it as well.
- There are no foreign keys, so the tables can be created in any order relative to your schema.
- You may add columns of your own to either table, for example a clear column your queries filter
  on, and write them from inside a [transaction](#transactions).

## Streams

Every call addresses a stream by a `StreamId`:

| Field | Column | Meaning |
|---|---|---|
| `ownerId` | `owner_id` | The partition: a tenant, an account, a user, whoever the data belongs to. |
| `stream` | `stream` | The kind of log, such as `'ledger'` or `'settings'`. A namespace for event types and a reducer. |
| `subjectId` | `subject_id` | The aggregate within that owner and kind: the specific thing whose history this is. |

`seq` is per tuple, starting at 1, and different tuples almost never contend (see the
[lock key](#transactions)). All three parts are required; a `StreamId` missing one is rejected
before any SQL runs.

A uuid owner has one spelling. In a `UUID` column Postgres reads a uuid in upper or lower case,
with or without braces, as one value, so one owner's rows are one set however it is written. The
lock key is text, though. Two spellings would take two locks for one stream, and writers through
each would fold onto the same loaded state and lose events. So an `ownerId` in the standard
8-4-4-4-12 form is written lowercase and without braces everywhere: in the SQL, the lock key, the
codec context, the mock's keys and `tx.stream`. A lowercase id is left as it is, so a lowercase
id's lock key never changes. Anything else is an ordinary `TEXT` id and is used as given: 32 bare
hex digits (an md5) are not taken for a uuid, and two cases of them are two owners. Once ids are
in rows and lock keys this cannot be undone, so it errs on the side of leaving an id alone.

Passing the same value as `ownerId` and `subjectId` is normal when an owner has one aggregate per
stream: one ledger per account, one settings record per user. The split leaves room for an owner
with many subjects in one stream, each with its own sequence, without a schema change.

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
Every write to the stream takes the same lock, whether from another transaction or a standalone
`append`, `saveProjection`, `compact` or `redact`. So no two writers can fold onto the same loaded
state and lose an event, and no write lands in the middle of a transaction. Reads outside a
transaction do not wait; they see the last committed state.

While it holds the connection, the store listens for the client's `'error'` event. A backend that
dies mid-transaction (terminated, or ended by `idle_in_transaction_session_timeout`) makes `pg`
emit it, and with nothing listening that would crash the process. The store ends the call at once,
even when `work` is waiting on something that never settles. The error names the stream and
carries the database's error as its `cause`. The connection is released as broken, so the pool
discards it, and any later `tx` call is refused.

If a statement inside `work` fails, Postgres aborts the transaction. Catching that error and
returning normally does not save anything: the `COMMIT` becomes a rollback, and `transaction`
rejects instead of reporting success.

`tx.query` is the reason the scope exists. A value your SQL must filter on, such as a due date or a
status, can sit in a column of your own beside the projection, written in the same transaction,
so the column and the state can never disagree:

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
and then `tx.query`. Spread `tx` so every other method stays, since `defineProjection` needs them,
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

Holding a lock has consequences:

- A `tx` call still pending when `work` returns would run after `COMMIT`, outside the lock. So it
  rejects instead.
- A `store` call for the same stream inside `work` runs on another connection. A write would wait
  forever for the lock that `work` holds, so the store refuses it at once with a `TypeError`. It
  tracks the locks the current async context holds with one `AsyncLocalStorage` for the whole
  process, so a write through a second store, on the same pool or on other tables, is refused just
  the same. A lock stops counting when its transaction ends, so code that `work` merely scheduled
  can write freely afterwards. A `store` read does not see what `work` has written but not yet
  committed.
- Slow work, such as a network call or a model call, belongs before the transaction. Pass what
  you decided into it and re-check there, for example with [`expectedSeq`](#expected-seq).
- The transaction runs at your database's default isolation level (`READ COMMITTED` unless you
  changed it). Each statement sees everything committed before it, which is what the lock relies
  on.

The lock key is the text `ownerId:stream:subjectId`, hashed with `hashtextextended(key, 0)`. It
stays the same across versions, so other code can take the same lock to serialize with the store.
Two different streams can share a lock. Their parts can join to the same text around `:`
(`ledger:eur` + `x` and `ledger` + `eur:x`), or two keys can collide in the 64-bit hash. The key
does not name the tables either, so one stream tuple in two stores on different tables takes one
lock. Such streams wait for each other's writes. Their rows never mix, because every statement is
scoped to the full tuple.

## Appending

`append` writes at `seq = MAX(seq) + 1` under the stream's lock. On its own it is a one-operation
transaction. The stored event it returns is
`{ id, ownerId, stream, subjectId, seq, type, payload, occurredAt, source }`.

Malformed input is rejected with a `TypeError` before anything is written or encoded:

- an event without a non-empty string `type`, or with a `payload` of `undefined` (`null` is a JSON
  value and is fine);
- a payload with its own `$redacted` key (see [redaction](#redaction));
- an event that carries `dedupeKey` or `expectedSeq` itself. They would be ignored there and a
  retry would append twice, so the message names the options argument;
- an `occurredAt` that is not an instant. A `Date`, or ISO 8601 with `Z` or an offset, is one.
  Postgres reads a time without a zone in its own time zone, and words such as `'now'` at the
  moment each retry lands, so a retried event would not be the same event. A calendar date that
  `Date` would roll over (`2026-02-30`) is refused, as Postgres refuses it;
- a stream id part, `type`, `source` or `dedupeKey` with `\u0000` or half a surrogate pair. The
  `TEXT` columns refuse `\u0000` with a raw database error, and `pg` writes half a pair as U+FFFD,
  so a `type` would come back changed and fail its codec context;
- a string anywhere in what `jsonb` would receive, value or key, with `\u0000` or half a surrogate
  pair, which `jsonb` cannot store. That is the payload or state itself, or with a codec the
  codec's output, so a sealed value may hold any string. The check runs after encoding and before
  anything is written, tombstones included, and the message says where the string is
  (`event payload.list[1]`);
- malformed options: a non-object, an empty `dedupeKey`, a negative or fractional `expectedSeq`.

A stream at seq 2^53 − 1 refuses another append. Past that a JavaScript number would round, and
two events would share a seq.

### Expected seq

```js
const seen = await store.maxSeq(stream)
// … decide, slowly, without holding anything …
await store.append(stream, event, { expectedSeq: seen })
```

If you keep every append folded into the projection, `projection.throughSeq` (or `0` when there is
no projection yet) is the same number, and one read gives you both the state and the seq.

The append happens only if the stream's newest `seq` is exactly `expectedSeq` (`0` for an empty
stream). Otherwise it throws `WrongExpectedSeqError` and writes nothing:

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
transaction usable. Nothing was written, so you can catch it, reload and decide again, or let it
propagate and roll back.

### Dedupe

The lock stops two concurrent writers from taking one `seq`. It cannot stop the same request from
being appended twice in sequence, for example by a message handler that appended, then threw,
then was redelivered. A `dedupeKey` that is stable across retries of one request, and distinct per
new request, handles that:

```js
const { event, appended } = await store.append(stream, newEvent, { dedupeKey: message.id })
if (!appended) {
  // A retry: `event` is what the first attempt wrote. The retried payload was not written.
}
```

Keys are scoped to the stream. A dedupe hit is returned before `expectedSeq` is checked, because
a retry's own first attempt is what moved the stream. With both options, a retry is answered as a
retry, not as a conflict. `tx.eventByDedupeKey(key)` reads the event a key wrote, or `null`, for a
caller that wants to recognise a retry before it plans anything.

## defineProjection

The projection row stays current as long as every append is folded into it in the same
transaction. The store's own methods do not fold: a plain `append` leaves the projection behind
until something catches it up. `defineProjection(store, { initial, reduce, keep, redacted })`
folds for you. It is built only on the store's public methods, so it runs on either store. Each
write is one `store.transaction`:

1. Load the projection, and fold on any event past its `throughSeq` (one appended with the store's
   own methods). A stream never written starts from a fresh copy of `initial`.
2. Append the events, then run any redactions an `update` plan asked for.
3. Fold the appended events onto the loaded state with `reduce`, or, when the plan redacted,
   re-fold the whole log from `initial`. Save the state at the last `seq`.
4. With `keep` set, `compact(lastSeq, keep)`.

`reduce(state, event)` is synchronous. Redacted events are skipped before it sees them, so a
reducer never handles a tombstone. With `redacted: 'fold'` it gets each one instead, with
`redacted: true` and the tombstone as payload, in every fold path (catching up and `rebuild`
included), for a state that keeps forgotten entries as forgotten. Every fold from `initial` starts
from a fresh copy (`structuredClone`), so a reducer that mutates its state cannot change `initial`
or another stream.

Writes return `{ state, events, duplicate, redacted }` and have no `appended` flag. `events` says
what was appended, and the store's own `appended` means something else: a dedupe hit returns the
first write there.

A `dedupeKey` on a batch goes on its first event. The batch commits as one, so a hit there means
the whole batch is in. That makes a batch retry-safe without a key per event, and keeps the stored
key exactly the one you passed. An empty batch writes nothing and ignores its options, so a caller
whose planning finds nothing to do needs no special case.

`update` takes what `decide(state, ctx)` returns: an array of events, or a plan
`{ append, redact }` whose `append` is an array too. An array is events and an object is a plan,
so nothing is guessed from keys; anything else, a bare event included, is a `TypeError`. `decide`
may be async, and `ctx.loadEvents(afterSeq?)` reads the log inside the transaction. With a
`dedupeKey` that is already stored, `update` returns `duplicate: true` before it calls `decide`, so
a retry is not planned again against a state its first attempt moved.

In a plan, `redact` is a list of groups `{ seqs, reason, tombstone? }`, each its own `tx.redact`,
so each event can get its own tombstone. The events are appended first, then the groups redacted,
so a dedupe hit on the first append returns before any redaction: the first attempt made them.
Then the whole log is re-folded from `initial`, under the same `redacted` policy as every fold, and
saved. Folding only the appended events onto the loaded state would keep the redacted facts the
state already held. Any failure rolls back the append and every redaction. A plan lets `decide`
erase facts atomically without being handed the transaction itself: it reads, and returns what to
write.

A plan can only redact events the stream held before the update; a later seq would be one of its
own appends or none at all. And the log must still start at seq 1, since the re-fold needs every
event. On a compacted log the plan is refused with an error pointing to `store.transaction`, where
you redact and correct the state yourself. On a projection with `keep` it is refused from the
first call. Otherwise it would fail only later, once compaction had cut the log, which a short
test would never show.

`state(stream)` reads first without a lock, so readers do not queue behind writers: the projection,
then the events past it. A compaction that lands between those two reads can open a gap. Then it
reads again inside a transaction, under the lock, where nothing lands in between, so a gap there
is real. Reading under the lock every time would be simpler, but every read would then wait for
every write to the stream.

`rebuild(stream)` re-folds the whole log from `initial` and saves it, in one transaction. On an
empty stream it writes nothing and returns `initial`. It refuses a log that does not start at
`seq` 1, because compaction removed the oldest events and a state folded without them would be
wrong.

Any fold that finds the log does not continue from the projection's `throughSeq` (compacted past
it, which only the store's own methods can do) refuses rather than fold from the wrong place.
Every such refusal is a `CompactedLogError`, with the `stream` and the oldest `seq` left
(`firstSeq`).

Two more checks guard the state that gets saved. `reduce` must return the next state: `undefined`
(a reducer that forgot to return) or a promise (an async reducer, whose next event would fold onto
the promise) throws a `TypeError` naming the event. And a write checks that its first event landed
right after the state it folded. Under the lock it always does, unless a writer took another lock
for the same rows; then the write refuses rather than save a state that skips events.

## Compaction and rebuild

The projection row is the snapshot. Every write folds into it in the same transaction as the
append, so reads are a single row lookup and never replay events.

`compact(throughSeq, keep)` deletes events that meet both rules:

1. The projection has folded them: `seq <= throughSeq`.
2. They are older than the newest `keep` events, which stay as an audit and replay trail.

The newest event is always kept, even with `keep: 0`, because `seq` is assigned from the newest
surviving event and an emptied log would restart at 1, behind the projection. With 200 events,
`compact(200, 100)` deletes seq 1 to 100 and `compact(50, 100)` deletes 1 to 50, never past what
was folded. With 100 events, `compact(100, 100)` deletes nothing; with 30, `compact(30, 0)` deletes
1 to 29.

When a projection row exists, the cutoff never passes its `through_seq`, read in the same
transaction, so an event the projection has not folded stays and it can always catch up. So the
`throughSeq` you pass to `saveProjection` must be true: a value above what you actually folded
authorizes deleting events that were never applied. Both arguments are required integers `>= 0`,
and a missing `keep` is a `TypeError` rather than "delete nothing". `compact` does nothing until
the stream outgrows `keep`, so it is safe to call after every save.

A small `keep` gives up the ability to replay from scratch. That is a valid choice, but make it
knowingly.

`defineProjection` catches up and rebuilds for you. By hand, both are a plain fold over
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

The codec is an optional pair of functions applied to every event `payload` and every projection
`state`. `encode` runs on the way into storage and `decode` on the way out, including `append`'s
return value. Use it to encrypt, compress or version what is stored; the store does not know
which.

`encode` must return a JSON value. It is `JSON.stringify`'d into the same `jsonb` column, and
`decode` receives it parsed. Wrap binary output, for example as base64 in an object such as
`{ v: 1, iv, ciphertext }`. Both functions may be async.

The context says what is being encoded: `{ kind: 'event', ownerId, stream, subjectId, type }` or
`{ kind: 'projection', ownerId, stream, subjectId }`. It is built from the `StreamId` you pass,
with a uuid owner in its one spelling, never from the stored row. So decode sees exactly what
encode saw.

Failures are loud. A throw from either function rejects the call. Nothing falls back to clear
text, and nothing stored is returned undecoded. An `encode` that returns `undefined` is refused. A
decode failure names what was being read (`could not decode the Deposited event at seq 7 on
ledger/main: …`), with the codec's error as the `cause`.

A standalone `append` encodes before it takes a connection, so a slow or failing codec never holds
the lock. It decodes its return value before `COMMIT`, so a decode failure rolls the append back
instead of reporting failure for a stored event. Inside a transaction, encoding happens under the
lock. A deduped retry still runs `encode`, because the payload is encoded before the store knows
the key exists; the first call's payload is returned, decoded.

Encoded columns are opaque to SQL: `payload->>'x'` returns what the codec wrote, not the field.
Anything your SQL must filter, sort or index on belongs in a clear column of its own, written with
`tx.query` in the same [transaction](#transactions).

Adding a codec to a table that already has rows is a migration you own: older rows are still in
the old form when `decode` sees them. A version field in the encoded shape is one way to tell them
apart.

## Redaction

The log is append-only, with one exception: `redact` overwrites the payload of events already in
it. It exists for "forget this one thing", such as a GDPR erasure request for a single fact, when
the rest of the subject's history must stay. To erase a whole owner, use
[`eraseOwner`](#erasing-an-owner), and crypto-shred its key to reach the copies no delete can (see
[what redaction does not reach](#what-redaction-does-not-reach)).

```js
await store.redact(stream, [7], { reason: 'erasure request 42' }) // → [7]
```

Each event keeps its `id`, `seq`, `type`, `occurredAt` and `source`. Only `payload` changes, to a
tombstone:

```js
{ $redacted: true, reason: 'erasure request 42', at: '2026-10-07T09:30:00.000Z' }
```

`redact` takes the stream's lock like every other write, and it is all or nothing. It checks every
seq before it writes any. A seq the stream does not hold (never appended, or already compacted
away) rejects with `UnknownSeqError`, whose `seqs` lists the missing ones, and nothing is redacted.
Otherwise it returns the redacted seqs, sorted and without repeats. Inside a transaction, a later
error rolls the redaction back with everything else.

`reason` is required. It is stored in the tombstone, so the log says why a payload is gone. Use an
identifier, such as a request id, not a description of what was removed.

Pass `tombstone` to keep fields a fold still needs that are not the fact being forgotten, such as
an amount the balance depends on: `redact(stream, [7], { reason, tombstone: { amount: 50 } })`.
`$redacted`, `reason` and `at` are always the store's and override the same keys in yours.

`loadEvents` sets `redacted: true` on an event whose decoded payload has `$redacted: true`, and no
other event has the field. So the `$redacted` key is reserved: `append` rejects a payload that has
its own, with a `TypeError` and before the codec runs. Otherwise an application that spreads
request JSON into a payload could forge a tombstone, and its reducer would skip a real event. A
deduped retry of a redacted event returns the tombstone, not the original. Redacting a redacted
event again replaces its tombstone.

Only the payload is redacted. Every other column stays as it was, which is why the
[README](../README.md#encrypting-payloads) says to keep personal data out of them. A clear column of your own that holds part of the fact must be cleared with
`tx.query` in the same transaction as `tx.redact`.

The tombstone goes through the codec like any payload, under the event's own context (its `type`
included), so an encrypting codec stores it sealed too. The encoding happens under the lock,
because only the stored row knows each event's type.

### Projections are yours to re-fold

`redact` does not touch the projection, which may still hold the fact. With `defineProjection`, an
`update` plan re-folds the whole log from `initial` in the same transaction, after the redactions,
so no reader ever sees the redacted log with the old state. Without it, redact and re-fold in one
transaction yourself, as the [README](../README.md#redaction) shows. Reducers then meet tombstones under the original
`type`, so they must check `event.redacted` before reading the payload.

A full re-fold needs every event since the start. Once
[compaction](#compaction-and-rebuild) has pruned the stream, the projection is the only record of
the pruned events. Remove the fact from the state yourself and save it in the same transaction as
the redaction. A fact whose event was compacted away lives only in the projection, and is removed
the same way.

### Erasing an owner

`eraseOwner(ownerId)` deletes every event and projection row of one owner, in all of its streams.
It returns `{ events, projections }`, how many rows went from each table. Both deletes are one SQL
statement, so they commit together or not at all. It is for an owner who leaves as a whole, such as
an erasure request for an account.

The owner is spelled as writes spell it, so a uuid in another case or with braces reaches the same
rows. The codec plays no part: rows are deleted, never decoded, so it works after the owner's key
is gone.

It takes no stream lock. Stop that owner's writes before you erase, because a write that lands
after the erase puts data back whatever the store does. Locking would not change that, and it
would mean finding every stream first and holding one lock per stream. A write that races the
erase can leave a stream that starts past seq 1, which a projection then refuses with
`CompactedLogError`. Inside a transaction on one of the owner's streams, `eraseOwner` throws a
`TypeError` at once. It could wait on rows that transaction has written, and the transaction's own
writes would land after the erase. The guard is process-wide, so it also refuses an erase of that
owner through another store instance, which is rare and fails safe. Called inside a transaction on
another owner's stream, `eraseOwner` runs on the pool, outside that transaction, so a rollback
there does not undo it.

Like `redact`, it reaches the live tables only. Everything in the next section applies to deleted
rows too.

### What redaction does not reach

`redact` changes what the live table returns, and nothing else. Postgres never overwrites a row in
place: an `UPDATE` writes a new row version and leaves the old one behind as a dead tuple. The old
payload, in clear or as the old ciphertext, can survive in:

- The table's data files. Plain `VACUUM` (and autovacuum) only marks a dead tuple's space as
  reusable. It does not wipe it, and the bytes can stay on disk until new rows happen to overwrite
  them. Only `VACUUM FULL` or `CLUSTER` rewrites the table into new files, and even then the old
  files' blocks are handed back to the filesystem, not erased.
- TOAST. A large payload is stored out of line in the table's TOAST table. Its old chunks are dead
  tuples there, under the same rules.
- WAL. The original `INSERT` and the redacting `UPDATE` are both in `pg_wal` until it is recycled,
  and in any WAL archive kept for point-in-time recovery for as long as that is kept. No form of
  `VACUUM` touches WAL.
- Logical decoding and CDC. A consumer that already received the row (a replication slot, a
  change-data-capture pipeline, a search index) keeps its copy. Redaction reaches it only as a
  later update, if it applies one.
- The projection. Re-folding after a redaction upserts `domain_projections`, which leaves the
  previous state, with the fact in it, as a dead row version of its own.
- Backups, until they age out.

Crypto-shredding is the only thing that reaches all of these. Destroy the key, and every copy of
what it sealed becomes unreadable: live, dead, logged, replicated or backed up. That works for a
whole subject, not for one fact sealed under the same key as the rest. An application that needs
a single fact gone from all of them must handle that itself, for example by sealing such facts
under a key of their own, or by stating its WAL and backup retention as the erasure deadline.

## The mock

`createMockEventStore({ codec, tables }?)` is an in-memory store with the same interface and
contract, for tests and no-database modes. Transactions on one stream run one at a time, and one
that throws restores the stream (events, projection and dedupe keys) to how it found it. The mock
stores what `jsonb` would hand back and hands out copies. So a codec that returns bytes or a
`BigInt` fails in a test too, and mutating a value you passed in or got back never changes what it
holds.

It checks `tables` as the pg store does, and ignores them. `compact` deletes as the pg store does,
dedupe keys of deleted events included, so code that needs the oldest events fails in a no-DB test
too. The four ways it differs are listed under [Limits](../README.md#limits) in the README. Since
its key order differs from `jsonb`'s, compare objects by value.

## Packaging

The source is plain ES modules with no build step, and `exports` points straight at it.
`require()` loads it as a `require()` of an ES module, which Node supports unflagged from 22.12 and
without an experimental warning from 22.13; hence the 22.13 floor.

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

The small modules both stores share sit beside `index.js`:

- `stream-id.js`: the `StreamId`, its one spelling, `afterSeq` and the lock key.
- `append.js` and `save-check.js`: the input checks for `append` and `saveProjection`.
- `storable-text.js`: the strings Postgres can store.
- `plain-object.js`: tells an object from an array or `null`.
- `instant.js`: the timestamp normaliser.

Outside `src/`, `examples/` holds the README's examples as the type check sees them, and
`test/types.ts` holds the declarations to the code; neither ships.

## Tests

```sh
npm test
npm run typecheck
```

`npm test` runs `node:test` over `src/**/*.test.js`, with no database. The pure parts (the
compaction boundary, the redaction input and tombstone) are tested as functions. The PostgreSQL
store runs against a fake pool that records every statement, and the tests assert on the SQL
itself: the transaction sequence, the lock key, the parameters, and that nothing reaches the
connection after `COMMIT`. The mock is held to the same contract, and the codec runs through both.
Every `defineProjection` test runs on both stores, the pg one over a fake pool that keeps and
deletes rows.

`npm run typecheck` compiles `test/types.ts`, which holds the hand-written declarations to the
code's exports and shapes, and the README's examples in `examples/`, against the declarations and
`pg`'s own types.
