import { pruneUpToSeq, checkCompaction } from '../compaction/compaction.js'
import { readStream, lockKey, describeStream, readAfterSeq } from '../stream-id.js'
import { encodeAppend, checkExpectedSeq, checkNextSeq } from '../append.js'
import { checkProjection } from '../save-check.js'
import { bindTransaction, endedError, checkWork } from '../transaction/transaction.js'
import { heldLocks as locks } from '../transaction/held-locks.js'
import { resolveCodec, eventContext, projectionContext, encodeWith, decodeWith } from '../codec/codec.js'
import { readRedaction, tombstoneOf, markRedacted, checkSeqsFound } from '../redaction/redaction.js'
import { toIso } from '../instant.js'
import { readTables } from '../schema/schema.js'

// Thin Postgres binding. Decision logic (the compaction boundary, the input
// checks) lives in the pure modules (compaction.js, redaction.js, …); this is
// the SQL. The pg pool is injected, so the application owns it and the package
// carries no pg dependency.

// jsonb is read as text and parsed here, exactly once. pg's own type parser
// would hand it back already parsed, and a JSON string then cannot be told
// apart from JSON text; as text, every value reads back the same whatever the
// pool's parsers are.
const EVENT_COLUMNS = 'id, owner_id, stream, subject_id, seq, type, payload::text AS payload, occurred_at, source'
const IN_STREAM = 'owner_id = $1 AND stream = $2 AND subject_id = $3'

const fromJson = (text) => JSON.parse(text)

// pg would serialise a Date in the process's local time, with a whole-minute
// offset that shifts an instant before standard time; bound as UTC ISO text,
// it is exact. A string has been checked to carry its own zone.
const occurredAtParam = (occurredAt) => (occurredAt instanceof Date ? occurredAt.toISOString() : occurredAt ?? null)

function toEvent(r) {
  return {
    id: r.id, ownerId: r.owner_id, stream: r.stream, subjectId: r.subject_id,
    seq: Number(r.seq), type: r.type, payload: fromJson(r.payload),
    occurredAt: toIso(r.occurred_at), source: r.source,
  }
}

// Every operation on one stream, issued through `query`: the pool's for a
// standalone call, one client's inside a transaction. appendEncoded assumes the
// stream's advisory lock is held on that client. `tables` holds the quoted
// table names.
function streamOps(query, codec, tables, stream) {
  const { events, projections } = tables
  const tuple = [stream.ownerId, stream.stream, stream.subjectId]

  // Decoded under the stream the call was made with, the context it was encoded
  // under, not the row's: an owner-bound codec reads back what it sealed
  // whatever the column hands back.
  async function readEvent(row) {
    const event = toEvent(row)
    event.payload = await decodeWith(codec, event.payload, eventContext(stream, event.type), event.seq)
    return markRedacted(event)
  }

  async function maxSeq() {
    const res = await query(`SELECT COALESCE(MAX(seq), 0) AS max FROM ${events} WHERE ${IN_STREAM}`, tuple)
    return Number(res.rows[0].max)
  }

  async function eventByDedupeKey(dedupeKey) {
    const res = await query(`SELECT ${EVENT_COLUMNS} FROM ${events} WHERE ${IN_STREAM} AND dedupe_key = $4`, [...tuple, dedupeKey])
    return res.rows[0] ? readEvent(res.rows[0]) : null
  }

  // seq = MAX(seq)+1 under the lock. A dedupeKey seen before returns what that
  // first append wrote, before the expected seq is checked: the retry's own
  // first attempt is what moved the stream.
  async function appendEncoded(event, payload, { dedupeKey, expectedSeq }) {
    const existing = dedupeKey && await eventByDedupeKey(dedupeKey)
    if (existing) return { event: existing, appended: false }
    const current = await maxSeq()
    checkExpectedSeq(stream, expectedSeq, current)
    checkNextSeq(stream, current)
    const res = await query(
      `INSERT INTO ${events} (owner_id, stream, subject_id, seq, type, payload, occurred_at, source, dedupe_key)
       VALUES ($1, $2, $3, $4, $5, $6, COALESCE($7::timestamptz, now()), $8, $9)
       RETURNING ${EVENT_COLUMNS}`,
      [...tuple, current + 1, event.type, JSON.stringify(payload), occurredAtParam(event.occurredAt), event.source ?? null, dedupeKey ?? null],
    )
    return { event: await readEvent(res.rows[0]), appended: true }
  }

  // Every seq is checked before any is written, and each tombstone is encoded
  // under its own event's context, as that event's payload was. Only the
  // payload column changes.
  async function redact(redaction) {
    const res = await query(`SELECT seq, type FROM ${events} WHERE ${IN_STREAM} AND seq = ANY($4::bigint[]) ORDER BY seq`, [...tuple, redaction.seqs])
    checkSeqsFound(stream, redaction.seqs, res.rows.map((r) => Number(r.seq)))
    const at = new Date().toISOString()
    for (const { seq, type } of res.rows) {
      const payload = await encodeWith(codec, tombstoneOf(redaction, at), eventContext(stream, type))
      await query(`UPDATE ${events} SET payload = $5 WHERE ${IN_STREAM} AND seq = $4`, [...tuple, Number(seq), JSON.stringify(payload)])
    }
    return redaction.seqs
  }

  async function loadEvents(afterSeq) {
    afterSeq = readAfterSeq(afterSeq)
    const res = await query(`SELECT ${EVENT_COLUMNS} FROM ${events} WHERE ${IN_STREAM} AND seq > $4 ORDER BY seq`, [...tuple, afterSeq])
    return Promise.all(res.rows.map(readEvent))
  }

  async function loadProjection() {
    const res = await query(`SELECT state::text AS state, through_seq, updated_at FROM ${projections} WHERE ${IN_STREAM}`, tuple)
    if (res.rows.length === 0) return null
    const r = res.rows[0]
    return {
      ownerId: stream.ownerId, stream: stream.stream, subjectId: stream.subjectId,
      state: await decodeWith(codec, fromJson(r.state), projectionContext(stream)),
      throughSeq: Number(r.through_seq),
      updatedAt: toIso(r.updated_at),
    }
  }

  async function saveProjection(state, throughSeq) {
    return saveEncoded(await encodeWith(codec, state, projectionContext(stream)), throughSeq)
  }

  async function saveEncoded(stored, throughSeq) {
    await query(
      `INSERT INTO ${projections} (owner_id, stream, subject_id, state, through_seq, updated_at)
       VALUES ($1, $2, $3, $4, $5, now())
       ON CONFLICT (owner_id, stream, subject_id)
       DO UPDATE SET state = EXCLUDED.state, through_seq = EXCLUDED.through_seq, updated_at = now()`,
      [...tuple, JSON.stringify(stored), throughSeq],
    )
  }

  // Prune folded events older than the newest `keep`. Returns the number
  // deleted. Never past the saved projection's through_seq: an event it has
  // not folded must stay, or it could never catch up.
  async function compact(throughSeq, keep) {
    const saved = await query(`SELECT through_seq FROM ${projections} WHERE ${IN_STREAM}`, tuple)
    const folded = saved.rows.length > 0 ? Math.min(throughSeq, Number(saved.rows[0].through_seq)) : throughSeq
    const cutoff = pruneUpToSeq({ throughSeq: folded, maxSeq: await maxSeq(), keep })
    if (cutoff === null) return 0
    const res = await query(`DELETE FROM ${events} WHERE ${IN_STREAM} AND seq <= $4`, [...tuple, cutoff])
    return res.rowCount ?? 0
  }

  return { appendEncoded, eventByDedupeKey, redact, loadEvents, loadProjection, saveProjection, saveEncoded, maxSeq, compact }
}

// Checked at construction, so a wrong argument fails here and not at the first
// write, possibly far from where the store was made.
function checkPool(pool) {
  if (typeof pool?.query !== 'function' || typeof pool?.connect !== 'function') {
    throw new TypeError('createEventStore needs a pool shaped like pg.Pool, with query(text, params) and connect()')
  }
}

// pg emits 'error' on a client whose backend dies (terminated, or an idle-in-
// transaction timeout); unheard, that crashes the process. Heard, it records
// the cause and rejects `died`, so a transaction whose work never settles
// still ends at once, and its connection is discarded.
function watchClient(client) {
  let lost
  let onError
  const died = new Promise((_, reject) => {
    onError = (err) => {
      lost ??= err
      reject(err)
    }
  })
  died.catch(() => {})
  client.on?.('error', onError)
  return { died, get lost() { return lost }, stop: () => client.removeListener?.('error', onError) }
}

const lostError = (stream, cause) =>
  new Error(`lost the connection during the transaction on ${describeStream(stream)}: ${cause.message}`, { cause })

function createEventStore(pool, options) {
  checkPool(pool)
  const codec = resolveCodec(options)
  const tables = readTables(options)
  const encode = (value, context) => encodeWith(codec, value, context)
  const onPool = (stream) => streamOps((text, params) => pool.query(text, params), codec, tables, readStream(stream))

  // One transaction on one client, under the stream's advisory lock: writers to
  // one stream serialize, different streams wait for each other only if their
  // lock keys collide, and the lock goes with the commit or the rollback.
  // UNIQUE(seq) is the backstop. Once `run` has settled, its query refuses:
  // nothing may slip in after the COMMIT.
  async function withLock(stream, run) {
    const key = lockKey(stream)
    locks.refuseIfHeld(key, stream)
    const client = await pool.connect()
    const watch = watchClient(client)
    let open = true
    let broken
    const query = async (text, params) => {
      if (!open) throw endedError(stream)
      return client.query(text, params)
    }
    try {
      await client.query('BEGIN')
      await client.query('SELECT pg_advisory_xact_lock(hashtextextended($1, 0))', [key])
      const result = await Promise.race([locks.holding(key, () => run(query)), watch.died])
      open = false
      await commit(client, stream)
      return result
    } catch (err) {
      open = false
      if (watch.lost) throw lostError(stream, watch.lost)
      await client.query('ROLLBACK').catch((rollbackErr) => { broken = rollbackErr })
      throw err
    } finally {
      // A connection that died, or could not even roll back, is discarded.
      watch.stop()
      client.release(watch.lost ?? broken)
    }
  }

  // Postgres answers COMMIT on a transaction a failed statement aborted with a
  // ROLLBACK, not an error — as when `work` caught a SQL error and carried on.
  async function commit(client, stream) {
    const res = await client.query('COMMIT')
    if (res?.command === 'ROLLBACK') {
      throw new Error(`the transaction on ${describeStream(stream)} rolled back instead of committing: a statement in it failed`)
    }
  }

  // Several operations on one stream, and the caller's own SQL, as one
  // transaction under the stream's lock: all of it commits or none of it does.
  async function transaction(stream, work) {
    stream = readStream(stream)
    checkWork(work)
    return withLock(stream, (query) => work(bindTransaction(stream, streamOps(query, codec, tables, stream), encode, query)))
  }

  // Encoded before a connection is taken, so a slow codec never holds the
  // stream's lock and a failing one never opens a transaction. The return is
  // decoded before COMMIT, so a decode that throws rolls the append back rather
  // than reporting failure for an event that is stored.
  async function append(stream, event, options) {
    stream = readStream(stream)
    const { payload, appendOptions } = await encodeAppend(stream, event, options, encode)
    return locked(stream, (ops) => ops.appendEncoded(event, payload, appendOptions))
  }

  // Encoded before a connection is taken, as in append.
  async function saveProjection(stream, state, throughSeq) {
    stream = readStream(stream)
    checkProjection(state, throughSeq)
    const stored = await encodeWith(codec, state, projectionContext(stream))
    return locked(stream, (ops) => ops.saveEncoded(stored, throughSeq))
  }

  // Checked before a connection is taken. The tombstone is encoded under the
  // lock: its context is each event's type, which only the row knows.
  async function redact(stream, seqs, options) {
    stream = readStream(stream)
    const redaction = readRedaction(seqs, options)
    return locked(stream, (ops) => ops.redact(redaction))
  }

  // Checked before a connection is taken.
  async function compact(stream, throughSeq, keep) {
    stream = readStream(stream)
    checkCompaction(throughSeq, keep)
    return locked(stream, (ops) => ops.compact(throughSeq, keep))
  }

  const locked = (stream, run) => withLock(stream, (query) => run(streamOps(query, codec, tables, stream)))

  return {
    append,
    redact,
    transaction,
    loadEvents: async (stream, afterSeq) => onPool(stream).loadEvents(afterSeq),
    loadProjection: async (stream) => onPool(stream).loadProjection(),
    // Every write takes the stream's lock, so none lands in the middle of a
    // transaction's read-modify-write. Reads do not: they see what is committed.
    saveProjection,
    maxSeq: async (stream) => onPool(stream).maxSeq(),
    compact,
  }
}

export { createEventStore }
