// An in-memory store for running without a database: unit tests, and harnesses
// that run end to end without one. It has the interface and the contract of
// createEventStore, so code that uses a store runs unchanged on either.

import { readStream, lockKey, readAfterSeq } from '../stream-id.js'
import { encodeAppend, checkExpectedSeq, checkNextSeq } from '../append.js'
import { checkProjection } from '../save-check.js'
import { pruneUpToSeq, checkCompaction } from '../compaction/compaction.js'
import { bindTransaction, endedError, checkWork } from '../transaction/transaction.js'
import { heldLocks as locks } from '../transaction/held-locks.js'
import { resolveCodec, eventContext, projectionContext, encodeWith, decodeWith } from '../codec/codec.js'
import { readRedaction, tombstoneOf, markRedacted, checkSeqsFound } from '../redaction/redaction.js'
import { toIso } from '../instant.js'
import { readTables } from '../schema/schema.js'

const EPOCH = '1970-01-01T00:00:00.000Z'

async function noSql() {
  throw new Error('the mock event store runs no SQL: tx.query exists only on a store from createEventStore')
}

function createMockEventStore(options) {
  const codec = resolveCodec(options)
  readTables(options) // unused in memory, but a bad name fails here as in the pg store
  const streams = new Map()
  const tails = new Map()

  // The data is keyed by the exact tuple, as the rows are. Only the lock is
  // keyed by lockKey, as in Postgres: two streams whose parts join to the
  // same text there share a lock and wait for each other, but never data.
  function stateAt(stream) {
    const key = JSON.stringify([stream.ownerId, stream.stream, stream.subjectId])
    if (!streams.has(key)) streams.set(key, { events: [], projection: null, dedupeKeys: new Map() })
    return streams.get(key)
  }

  // The map holds what jsonb would hand back (the encoded value, through JSON),
  // and every read decodes a copy of it under the caller's stream. So a codec
  // bug (bytes, a BigInt, a decode that throws) fails in a no-DB test too, and
  // mutating a value on either side of a call never changes the stream.
  async function encodeToStore(value, context) {
    return JSON.parse(JSON.stringify(await encodeWith(codec, value, context)))
  }

  // The mock's per-stream lock and transaction: units of work on one stream run
  // one at a time, one that throws leaves the stream as it found it, and once
  // it has settled its operations refuse, as the pg store's do.
  function withLock(stream, run) {
    const key = lockKey(stream)
    locks.refuseIfHeld(key, stream)
    const result = (tails.get(key) ?? Promise.resolve()).then(() => locks.holding(key, () => restoringOnThrow(stream, run)))
    const tail = result.catch(() => {}).then(() => {
      if (tails.get(key) === tail) tails.delete(key)
    })
    tails.set(key, tail)
    return result
  }

  async function restoringOnThrow(stream, run) {
    const st = stateAt(stream)
    const before = { events: [...st.events], projection: st.projection, dedupeKeys: new Map(st.dedupeKeys) }
    const scope = { open: true }
    try {
      return await run(scope)
    } catch (err) {
      Object.assign(st, before)
      throw err
    } finally {
      scope.open = false
    }
  }

  // The same operations as the pg store's streamOps, on the map.
  function streamOps(stream, scope = { open: true }) {
    const streamData = () => {
      if (!scope.open) throw endedError(stream)
      return stateAt(stream)
    }

    async function readEvent(stored) {
      const event = structuredClone(stored)
      event.payload = await decodeWith(codec, event.payload, eventContext(stream, event.type), event.seq)
      return markRedacted(event)
    }

    async function maxSeq() {
      const events = streamData().events
      return events.length ? events[events.length - 1].seq : 0
    }

    // A key maps to its seq, not the event, so a hit reads the event as it is
    // now — a redacted one as its tombstone.
    async function eventByDedupeKey(dedupeKey) {
      const st = streamData()
      if (!st.dedupeKeys.has(dedupeKey)) return null
      const seq = st.dedupeKeys.get(dedupeKey)
      return readEvent(st.events.find((e) => e.seq === seq))
    }

    async function appendEncoded(event, payload, { dedupeKey, expectedSeq }) {
      const st = streamData()
      const existing = dedupeKey && await eventByDedupeKey(dedupeKey)
      if (existing) return { event: existing, appended: false }
      const current = await maxSeq()
      checkExpectedSeq(stream, expectedSeq, current)
      checkNextSeq(stream, current)
      const stored = {
        id: `mock-${current + 1}`, ownerId: stream.ownerId, stream: stream.stream, subjectId: stream.subjectId,
        seq: current + 1, type: event.type, payload,
        occurredAt: toIso(event.occurredAt ?? EPOCH), source: event.source ?? null,
      }
      st.events.push(stored)
      if (dedupeKey) st.dedupeKeys.set(dedupeKey, stored.seq)
      return { event: await readEvent(stored), appended: true }
    }

    // Replaces the redacted events rather than mutating them, so a rollback's
    // copy of the event list still holds the originals.
    async function redact(redaction) {
      const st = streamData()
      const found = st.events.filter((e) => redaction.seqs.includes(e.seq))
      checkSeqsFound(stream, redaction.seqs, found.map((e) => e.seq))
      const replaced = new Map()
      for (const event of found) {
        const payload = await encodeToStore(tombstoneOf(redaction, EPOCH), eventContext(stream, event.type))
        replaced.set(event.seq, { ...event, payload })
      }
      st.events = st.events.map((e) => replaced.get(e.seq) ?? e)
      return redaction.seqs
    }

    async function loadEvents(afterSeq) {
      afterSeq = readAfterSeq(afterSeq)
      return Promise.all(streamData().events.filter((e) => e.seq > afterSeq).map(readEvent))
    }

    async function loadProjection() {
      const projection = structuredClone(streamData().projection)
      if (projection) projection.state = await decodeWith(codec, projection.state, projectionContext(stream))
      return projection
    }

    async function saveProjection(state, throughSeq) {
      const stored = await encodeToStore(state, projectionContext(stream))
      streamData().projection = {
        ownerId: stream.ownerId, stream: stream.stream, subjectId: stream.subjectId,
        state: stored, throughSeq, updatedAt: EPOCH,
      }
    }

    // Deletes as the pg store does, so code that needs the oldest events fails
    // here too. A deleted event's dedupe key goes with it, as with the row.
    async function compact(throughSeq, keep) {
      const { projection } = streamData()
      const folded = projection ? Math.min(throughSeq, projection.throughSeq) : throughSeq
      const cutoff = pruneUpToSeq({ throughSeq: folded, maxSeq: await maxSeq(), keep })
      if (cutoff === null) return 0
      const st = streamData()
      const kept = st.events.filter((e) => e.seq > cutoff)
      const deleted = st.events.length - kept.length
      st.events = kept
      for (const [key, seq] of st.dedupeKeys) if (seq <= cutoff) st.dedupeKeys.delete(key)
      return deleted
    }

    return { appendEncoded, eventByDedupeKey, redact, loadEvents, loadProjection, saveProjection, maxSeq, compact }
  }

  const ops = (stream) => streamOps(readStream(stream))
  const locked = (stream, run) => withLock(stream, (scope) => run(streamOps(stream, scope)))

  async function append(stream, event, options) {
    stream = readStream(stream)
    const { payload, appendOptions } = await encodeAppend(stream, event, options, encodeToStore)
    return withLock(stream, (scope) => streamOps(stream, scope).appendEncoded(event, payload, appendOptions))
  }

  async function redact(stream, seqs, options) {
    stream = readStream(stream)
    const redaction = readRedaction(seqs, options)
    return locked(stream, (o) => o.redact(redaction))
  }

  async function saveProjection(stream, state, throughSeq) {
    stream = readStream(stream)
    checkProjection(state, throughSeq)
    return locked(stream, (o) => o.saveProjection(state, throughSeq))
  }

  async function compact(stream, throughSeq, keep) {
    stream = readStream(stream)
    checkCompaction(throughSeq, keep)
    return locked(stream, (o) => o.compact(throughSeq, keep))
  }

  async function transaction(stream, work) {
    stream = readStream(stream)
    checkWork(work)
    return withLock(stream, (scope) => work(bindTransaction(stream, streamOps(stream, scope), encodeToStore, noSql)))
  }

  return {
    append,
    redact,
    transaction,
    loadEvents: async (stream, afterSeq) => ops(stream).loadEvents(afterSeq),
    loadProjection: async (stream) => ops(stream).loadProjection(),
    // Writes take the stream's lock, as in the pg store, so a transaction that
    // rolls back never undoes a write that was not part of it.
    saveProjection,
    maxSeq: async (stream) => ops(stream).maxSeq(),
    compact,
  }
}

export { createMockEventStore }
