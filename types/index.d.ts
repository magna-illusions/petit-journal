/**
 * An append-only event store over PostgreSQL. Payloads are opaque to the
 * store — each caller supplies its own event types and reducer.
 *
 * A stream is addressed by the tuple (ownerId, stream, subjectId): ownerId is
 * the partition (a tenant, an account, a user), stream the kind of log, and
 * subjectId the aggregate within that owner and kind. Passing the same value
 * for ownerId and subjectId is the normal case when an owner has one aggregate
 * per stream. Stored in the owner_id, stream and subject_id columns.
 */
export interface StreamId {
  /**
   * A uuid in the standard 8-4-4-4-12 form, in any case and with or without
   * braces, is written lowercase without braces everywhere: in the rows, the
   * lock key and the codec context. Any other id, 32 bare hex digits included,
   * is kept as given.
   */
  ownerId: string
  stream: string
  subjectId: string
}

/**
 * Input to append(): the store assigns id, seq, and (if absent) occurredAt.
 * dedupeKey and expectedSeq belong in append's options; on the event they are
 * refused, since there they would be ignored.
 */
export interface NewEvent {
  type: string
  /**
   * Any JSON value. An object with its own `$redacted` key is rejected: that
   * key marks redact's tombstones. Without a codec, no string in it, value or
   * key, may hold \u0000 or half a surrogate pair, which Postgres cannot store;
   * with one, the same holds for the codec's output.
   */
  payload: unknown
  /** An instant: a Date, or ISO 8601 with Z or an offset. Default: now, at insert. */
  occurredAt?: string | Date
  source?: string
}

/** P narrows the payload where the caller knows it; the store never does. */
export interface StoredEvent<P = unknown> extends StreamId {
  id: string
  seq: number
  type: string
  payload: P
  /** ISO 8601 in UTC, to the millisecond, whatever pg's type parser returns. */
  occurredAt: string
  source: string | null
  /**
   * Present, and true, only on an event whose payload was redacted: its
   * decoded payload is a RedactionTombstone. Absent on every other event.
   */
  redacted?: true
}

/** The live read-model: the folded state plus the highest seq it embodies. */
export interface Projection<S> extends StreamId {
  state: S
  throughSeq: number
  /** ISO 8601 in UTC, to the millisecond, as StoredEvent.occurredAt. */
  updatedAt: string
}

/** append's third argument: retry safety and optimistic concurrency, both optional. */
export interface AppendOptions {
  /**
   * Makes append safe to retry: an event already appended to this stream under
   * the same key is returned instead, with appended: false, and nothing is
   * written. Stable across retries of one request, distinct per new request.
   */
  dedupeKey?: string
  /**
   * Append only if the stream's newest seq is exactly this (0: an empty
   * stream); otherwise reject with WrongExpectedSeqError and write nothing.
   * A dedupe hit is returned before this is checked.
   */
  expectedSeq?: number
}

/**
 * One shape whether or not a dedupeKey was given. `appended` is false only for
 * a dedupe hit, and `event` is then what the first call wrote.
 */
export interface AppendResult {
  event: StoredEvent
  appended: boolean
}

/** What eraseOwner deleted: how many events and how many projection rows. */
export interface EraseOwnerResult {
  events: number
  projections: number
}

/** Thrown by append when `expectedSeq` is not the stream's newest seq. */
export class WrongExpectedSeqError extends Error {
  readonly name: 'WrongExpectedSeqError'
  readonly stream: StreamId
  readonly expectedSeq: number
  readonly actualSeq: number
}

/** redact's options: why the payloads go, and what the tombstone keeps. */
export interface RedactOptions {
  /**
   * Why the payload was removed, recorded in the tombstone: an erasure request
   * id, say. Required and non-empty.
   */
  reason: string
  /**
   * Fields to keep in the tombstone, such as a non-personal amount a fold still
   * needs. $redacted, reason and at are always the store's and override these.
   */
  tombstone?: Record<string, unknown>
}

/**
 * What a redacted event's payload becomes, encoded through the codec like any
 * payload. `at` is when it was redacted (ISO 8601; the epoch in the mock).
 */
export type RedactionTombstone = Record<string, unknown> & {
  $redacted: true
  reason: string
  at: string
}

/** Thrown by redact when a seq is not in the stream; nothing was redacted. */
export class UnknownSeqError extends Error {
  readonly name: 'UnknownSeqError'
  readonly stream: StreamId
  /** The requested seqs the stream does not hold, ascending. */
  readonly seqs: number[]
}

/**
 * Thrown by defineProjection when it needs events compaction has removed: a
 * projection behind a log that now starts past it, a rebuild from initial, a
 * redacting plan's re-fold. Nothing was written.
 */
export class CompactedLogError extends Error {
  readonly name: 'CompactedLogError'
  readonly stream: StreamId
  /** The oldest seq the log still holds. */
  readonly firstSeq: number
}

/** What a query resolves to, as pg returns it. */
export interface QueryResult<R = any> {
  rows: R[]
  rowCount: number | null
}

/**
 * The minimal pg.Pool surface the store needs — duck-typed so the package
 * carries no pg dependency; each caller injects its own pool. pg.Pool fits.
 */
export interface PoolClient {
  query(text: string, params?: unknown[]): Promise<QueryResult>
  /** Passed the error when the connection may be broken, so the pool discards it. */
  release(err?: Error): void
  /**
   * Listened to while the store holds the client: a backend that dies emits
   * 'error' here, and pg's clients crash the process when nothing listens.
   */
  on?(event: 'error', listener: (err: Error) => void): unknown
  removeListener?(event: 'error', listener: (err: Error) => void): unknown
}

/** The pool the store needs: pg.Pool fits as-is. */
export interface PoolLike {
  query(text: string, params?: unknown[]): Promise<QueryResult>
  connect(): Promise<PoolClient>
}

/**
 * What transaction(stream, work) hands `work`: the stream's operations, bound
 * to the one connection that holds the stream's lock, plus that connection's
 * query for the caller's own SQL. Everything commits together when `work`
 * resolves and rolls back together when it rejects. Every call must be awaited
 * inside `work`; one that lands after it returned rejects.
 */
export interface StreamTransaction {
  readonly stream: StreamId
  append(event: NewEvent, options?: AppendOptions): Promise<AppendResult>
  redact(seqs: number[], options: RedactOptions): Promise<number[]>
  /**
   * The event a dedupeKey wrote in this stream, decoded, or null: a retry can be
   * recognised before it is planned again.
   */
  eventByDedupeKey(dedupeKey: string): Promise<StoredEvent | null>
  loadEvents(afterSeq?: number): Promise<StoredEvent[]>
  loadProjection<S>(): Promise<Projection<S> | null>
  saveProjection<S>(state: S, throughSeq: number): Promise<void>
  maxSeq(): Promise<number>
  compact(throughSeq: number, keep: number): Promise<number>
  /**
   * The caller's own SQL on the transaction's connection. Not available on the
   * mock store, where it rejects.
   */
  query<R = any>(text: string, params?: unknown[]): Promise<QueryResult<R>>
}

/** A store, from openEventStore, createEventStore or createMockEventStore. */
export interface EventStore {
  append(stream: StreamId, event: NewEvent, options?: AppendOptions): Promise<AppendResult>
  /**
   * Overwrites the payload of each event at `seqs` with a tombstone and keeps
   * id, seq, type, occurredAt and source. Under the stream's lock, all or
   * nothing: a seq the stream does not hold rejects with UnknownSeqError.
   * Returns the redacted seqs, ascending and de-duplicated. Does not touch the
   * projection: re-fold it in the same transaction (see the README).
   */
  redact(stream: StreamId, seqs: number[], options: RedactOptions): Promise<number[]>
  /**
   * Deletes every event and projection row of one owner, in all its streams,
   * in one transaction. The owner is spelled as writes spell it. It takes no
   * stream lock, so stop that owner's writes first; inside a transaction on one
   * of its streams it throws a TypeError. Rows are deleted, never decoded.
   */
  eraseOwner(ownerId: string): Promise<EraseOwnerResult>
  loadEvents(stream: StreamId, afterSeq?: number): Promise<StoredEvent[]>
  loadProjection<S>(stream: StreamId): Promise<Projection<S> | null>
  saveProjection<S>(stream: StreamId, state: S, throughSeq: number): Promise<void>
  maxSeq(stream: StreamId): Promise<number>
  compact(stream: StreamId, throughSeq: number, keep: number): Promise<number>
  /**
   * One transaction under the stream's lock: writers to the stream wait until
   * it ends. Inside `work`, use `tx`: a write through this store to the same
   * stream would wait for the lock `work` holds, so it throws a TypeError.
   */
  transaction<T>(stream: StreamId, work: (tx: StreamTransaction) => Promise<T>): Promise<T>
}

/**
 * What a codec is told about the value it is encoding or decoding: the stream
 * it belongs to and whether it is an event payload or a projection state.
 * Enough to choose per owner — a per-tenant key, say — without the store
 * knowing owners have keys.
 */
export type CodecContext =
  | (StreamId & { kind: 'event'; type: string })
  | (StreamId & { kind: 'projection' })

/**
 * Applied to every event payload and projection state on the way into and out
 * of storage. `encode` must return a JSON value: it is JSON.stringify'd into the
 * same jsonb column, and `decode` gets it back parsed. Either may be async.
 * A throw — or `encode` returning undefined — rejects the store call; nothing
 * is written in clear and nothing stored is returned undecoded.
 */
export interface PayloadCodec {
  encode(value: unknown, context: CodecContext): unknown | Promise<unknown>
  decode(stored: unknown, context: CodecContext): unknown | Promise<unknown>
}

/**
 * Table names: lowercase letters, digits and underscores, starting with a
 * letter or underscore, at most 52 characters, optionally schema-qualified
 * ('app.events'). Anything else throws a TypeError at construction.
 */
export interface EventStoreTables {
  /** Default 'domain_events'. Its dedupe index is named '<table>_dedupe_key'. */
  events?: string
  /** Default 'domain_projections'. */
  projections?: string
}

/** Options for openEventStore, createEventStore and createMockEventStore. */
export interface EventStoreOptions {
  codec?: PayloadCodec
  tables?: EventStoreTables
}

/** Never creates a table: for an application that runs its own migrations. */
export function createEventStore(pool: PoolLike, options?: EventStoreOptions): EventStore

/**
 * Creates the tables if they do not exist, then resolves to the store, so no
 * write can run before its tables are there. Rejects at boot if the setup fails.
 */
export function openEventStore(pool: PoolLike, options?: EventStoreOptions): Promise<EventStore>

/**
 * The setup openEventStore runs, on its own: both tables and the dedupe index,
 * IF NOT EXISTS, behind an advisory lock so concurrent boots do not race. One
 * multi-statement query, which pg runs as a single transaction.
 */
export function ensureSchema(
  pool: { query(text: string): Promise<unknown> },
  options?: { tables?: EventStoreTables },
): Promise<void>

/** In-memory EventStore for tests and no-database modes. */
export function createMockEventStore(options?: EventStoreOptions): EventStore
/**
 * defineProjection(store, definition): the load → append → fold → save →
 * compact cycle, on either store. Every fold from `initial` starts from a fresh
 * copy of it, and redacted events are skipped before `reduce` sees them unless
 * `redacted: 'fold'`.
 * P is the payload type `reduce` reads. It defaults to any, so a plain
 * JavaScript reducer type-checks; name it to have the payload checked.
 */
export interface ProjectionDefinition<S, P = any> {
  /** Any JSON value: the state of a stream never written. */
  initial: S
  /** Folds one event onto the state. Synchronous. */
  reduce: (state: S, event: StoredEvent<P>) => S
  /**
   * When set, compact after each save, keeping this many of the newest events
   * (the newest is always kept). Unset: never compact.
   */
  keep?: number
  /**
   * 'skip' (the default): redacted events never reach reduce. 'fold': reduce
   * gets them, with redacted: true and the tombstone as payload, in every fold
   * (catching up and rebuild), so a state can show what was forgotten.
   */
  redacted?: 'skip' | 'fold'
}

/** What defineProjection's append and update resolve to. */
export interface ProjectionResult<S> {
  /** The folded state after this call. */
  state: S
  /** What this call appended, in order: empty for an empty batch or a dedupe hit. */
  events: StoredEvent[]
  /**
   * True only for a dedupe hit: the batch was already in. An empty batch, which
   * writes nothing and applies no option, has no events and duplicate: false.
   */
  duplicate: boolean
  /** The seqs this call redacted, from an update plan: empty otherwise. */
  redacted: number[]
}

/**
 * One group of earlier events to redact, with its own reason and tombstone.
 * Every seq must be an event the stream held before the update.
 */
export interface PlannedRedaction extends RedactOptions {
  seqs: number[]
}

/**
 * What `decide` may return in place of events: events to append, and earlier
 * events to redact, in the same transaction. The events are appended first,
 * then the redactions run. A plan that redacts then re-folds the whole log
 * from `initial` and saves that, so the saved state never holds a redacted
 * fact; it is refused on a log compaction has cut (one that no longer starts
 * at seq 1), and on any projection with `keep`. A plan with only `redact` is
 * the atomic forget.
 */
export interface UpdatePlan {
  append?: NewEvent[]
  redact?: PlannedRedaction[]
}

/** What `decide` gets besides the state: reads inside its transaction. */
export interface DecideContext {
  /** As tx.loadEvents: decoded, with redacted: true on a tombstoned event. */
  loadEvents(afterSeq?: number): Promise<StoredEvent[]>
}

/** What update's decide returns: the events to append, or a plan that also redacts. An array is events. */
export type Decision = NewEvent[] | UpdatePlan

/** A projection folded by your reducer, from defineProjection. */
export interface DefinedProjection<S> {
  /**
   * One event or a batch, in one transaction. `expectedSeq` is the stream's seq
   * before the batch; `dedupeKey` goes on its first event, and a hit appends
   * and folds nothing. An empty batch writes nothing, whatever the options.
   */
  append(stream: StreamId, events: NewEvent | NewEvent[], options?: AppendOptions): Promise<ProjectionResult<S>>
  /**
   * `decide` gets a copy of the state and a context to read the log, and returns
   * the events to append (an array, [] for nothing) or an UpdatePlan. It
   * runs under the stream's lock, so keep it quick. Options apply to what is
   * appended: a dedupe hit skips the plan's redactions too, and with nothing to
   * append no option applies.
   */
  update(
    stream: StreamId,
    decide: (state: S, ctx: DecideContext) => Decision | Promise<Decision>,
    options?: AppendOptions,
  ): Promise<ProjectionResult<S>>
  /**
   * The folded state, caught up with any event past the saved projection, or
   * a copy of `initial` for a stream never written.
   */
  state(stream: StreamId): Promise<S>
  /**
   * Re-folds the whole log from `initial` and saves it, as after a reducer
   * change. Rejects when compaction has removed the oldest events.
   */
  rebuild(stream: StreamId): Promise<S>
}

/** Folds a stream's events into its projection row with your reducer, on either store. */
export function defineProjection<S, P = any>(store: EventStore, definition: ProjectionDefinition<S, P>): DefinedProjection<S>
