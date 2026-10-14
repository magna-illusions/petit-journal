# Glossary

The terms in context first:

> Acme's main bank account is a **stream**: owner `acme`, stream `ledger`, subject `main`. Each
> deposit is an **event** at the next **seq**, and your reducer **folds** it into the
> **projection**, `{ balance: 100 }`, in the same transaction. A retried deposit carries the same
> **dedupe key**, so it is stored once, and a note a customer asks you to forget is **redacted**,
> leaving a **tombstone**. Once the log is long, **compaction** deletes the oldest events the
> projection already holds.

| Term | Meaning |
|---|---|
| **stream** | One event log and its projection, named by `{ ownerId, stream, subjectId }`. The `stream` part on its own is the kind of log, such as `'ledger'`. |
| **owner** (`ownerId`) | Whoever the data belongs to: a tenant, an account, a user. The first part of every stream. A uuid owner is stored lowercase, without braces. `eraseOwner` deletes everything one owner has. |
| **`subjectId`** | The thing within an owner and a kind of log whose history this is, such as one account of several. Often the same as `ownerId`. |
| **event** | One stored fact: `{ type, payload, occurredAt, source }`, plus the `id` and `seq` the store gives it. Once written it never changes, unless it is redacted, or deleted by compaction or `eraseOwner`. |
| **seq** | An event's number in its stream, given in order: 1, 2, 3. `expectedSeq` appends only if the stream is still at the seq you last read. |
| **lock** | The stream's Postgres advisory lock. Every write but `eraseOwner` takes it, so writers to one stream take turns. Reads never wait. |
| **projection** | The one row per stream that holds the folded state, and `throughSeq`, the highest seq folded into it. With `update`, saved in the same transaction as the events. |
| **reduce, fold** | `reduce(state, event)` returns the next state. Folding runs it over events in order, from `initial` or from the saved state, as `Array.reduce` does. |
| **decide, update** | `update(stream, decide)` calls `decide(state, ctx)` under the stream's lock and writes what it returns: events to append, or a plan that also redacts. |
| **dedupe key** | A key you give an append so a retry is stored once. A key the stream has seen returns the first event and writes nothing. |
| **redact** | Replace chosen events' payloads with a tombstone, keeping their `seq`, `type` and time. The one exception to append-only. |
| **tombstone** | What a redacted payload becomes: `{ $redacted: true, reason, at }`, plus the fields you pass as `tombstone`. The event reads back with `redacted: true`. |
| **compaction** | Deleting events the projection has folded, keeping the newest `keep`, so a stream's log stays bounded. |
| **codec** | Your `encode` and `decode`, applied to every payload and state on the way into and out of the tables, to encrypt or compress them. |

The full model is in the [design notes](design.md).
