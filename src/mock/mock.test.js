import { test } from 'node:test'
import assert from 'node:assert/strict'
import { createMockEventStore } from './mock.js'
import { WrongExpectedSeqError } from '../append.js'

// The mock mirrors the pg store's contract so no-DB tests exercise the
// behaviour production gets.

const STREAM = { ownerId: 'a1', stream: 'ledger', subjectId: 'a1' }

test('mock store: append assigns sequential seq and loadEvents returns them in order', async () => {
  const s = createMockEventStore()
  const first = await s.append(STREAM, { type: 'A', payload: { n: 1 } })
  const second = await s.append(STREAM, { type: 'B', payload: { n: 2 } })
  assert.deepEqual([first.event.seq, first.appended, second.event.seq], [1, true, 2])
  assert.deepEqual((await s.loadEvents(STREAM)).map((e) => e.type), ['A', 'B'])
  assert.deepEqual((await s.loadEvents(STREAM, 1)).map((e) => e.type), ['B'])
})

test('mock store: projection round-trips and is isolated per stream', async () => {
  const s = createMockEventStore()
  assert.equal(await s.loadProjection(STREAM), null)
  await s.saveProjection(STREAM, { total: 3 }, 1)
  const p = await s.loadProjection(STREAM)
  assert.deepEqual(p.state, { total: 3 })
  assert.equal(p.throughSeq, 1)
  assert.equal(await s.loadProjection({ ownerId: 'other', stream: 'ledger', subjectId: 'other' }), null)
})

test('mock store: maxSeq tracks the latest', async () => {
  const s = createMockEventStore()
  assert.equal(await s.maxSeq(STREAM), 0)
  await s.append(STREAM, { type: 'A', payload: {} })
  assert.equal(await s.maxSeq(STREAM), 1)
})

// Compaction deletes in the mock as in Postgres, so code that relies on the
// oldest events fails in a no-DB test too, not first in production.
test('mock store: compact deletes folded events past the keep and frees their dedupe keys, as rows go in Postgres', async () => {
  const s = createMockEventStore()
  for (let n = 1; n <= 5; n++) await s.append(STREAM, { type: 'A', payload: { n } }, { dedupeKey: `req-${n}` })
  assert.equal(await s.compact(STREAM, 5, 100), 0, 'nothing past a keep of 100')
  assert.equal(await s.compact(STREAM, 3, 1), 3, 'never past throughSeq')
  assert.deepEqual((await s.loadEvents(STREAM)).map((e) => e.seq), [4, 5])
  assert.equal(await s.compact(STREAM, 5, 0), 1, 'the newest event always stays')
  assert.deepEqual((await s.loadEvents(STREAM)).map((e) => e.seq), [5])
  assert.equal(await s.maxSeq(STREAM), 5)
  const retried = await s.append(STREAM, { type: 'A', payload: { n: 1 } }, { dedupeKey: 'req-1' })
  assert.deepEqual([retried.appended, retried.event.seq], [true, 6], 'the key went with its event')
})

test('mock store: a compaction inside a transaction that throws is rolled back', async () => {
  const s = createMockEventStore()
  for (let n = 1; n <= 3; n++) await s.append(STREAM, { type: 'A', payload: { n } })
  await assert.rejects(s.transaction(STREAM, async (tx) => {
    await tx.compact(3, 0)
    throw new Error('boom')
  }), /boom/)
  assert.deepEqual((await s.loadEvents(STREAM)).map((e) => e.seq), [1, 2, 3])
})

test('mock store: a repeated dedupeKey is a no-op — appended:false, same event, no second seq', async () => {
  const s = createMockEventStore()
  const first = await s.append(STREAM, { type: 'Snapshot', payload: { n: 1 } }, { dedupeKey: 'req-1' })
  const second = await s.append(STREAM, { type: 'Snapshot', payload: { n: 2 } }, { dedupeKey: 'req-1' })
  assert.equal(first.appended, true)
  assert.equal(second.appended, false)
  assert.equal(second.event.seq, first.event.seq)
  assert.equal(second.event.payload.n, 1, 'the retried payload is never written')
  assert.equal(await s.maxSeq(STREAM), 1, 'only one event exists in the stream')
})

test('mock store: a dedupeKey is scoped to its stream', async () => {
  const s = createMockEventStore()
  await s.append(STREAM, { type: 'A', payload: {} }, { dedupeKey: 'req-1' })
  const other = await s.append({ ...STREAM, subjectId: 'b2' }, { type: 'A', payload: {} }, { dedupeKey: 'req-1' })
  assert.equal(other.appended, true)
})

test('mock store: expectedSeq appends on a match and throws WrongExpectedSeqError on a mismatch', async () => {
  const s = createMockEventStore()
  assert.equal((await s.append(STREAM, { type: 'A', payload: {} }, { expectedSeq: 0 })).event.seq, 1)
  await assert.rejects(() => s.append(STREAM, { type: 'B', payload: {} }, { expectedSeq: 0 }), (err) => {
    assert.ok(err instanceof WrongExpectedSeqError)
    assert.deepEqual([err.expectedSeq, err.actualSeq], [0, 1])
    return true
  })
  assert.equal(await s.maxSeq(STREAM), 1, 'nothing was written')
})

test('mock store: a deduped retry with a stale expectedSeq is answered as a retry', async () => {
  const s = createMockEventStore()
  await s.append(STREAM, { type: 'A', payload: {} }, { dedupeKey: 'req-1', expectedSeq: 0 })
  const retry = await s.append(STREAM, { type: 'A', payload: {} }, { dedupeKey: 'req-1', expectedSeq: 0 })
  assert.deepEqual([retry.event.seq, retry.appended], [1, false])
})

// Postgres hands back a fresh copy of what it stores, so mutating a value on
// either side of a call can never change the stream. The mock must not alias.
test('mock store: mutating what append was given or returned leaves the stored event alone', async () => {
  const s = createMockEventStore()
  const payload = { items: [1] }
  const { event } = await s.append(STREAM, { type: 'A', payload })
  payload.items.push(2)
  event.payload.items.push(3)
  ;(await s.loadEvents(STREAM))[0].payload.items.push(4)
  assert.deepEqual((await s.loadEvents(STREAM))[0].payload, { items: [1] })
})

test('mock store: mutating a saved or loaded state leaves the stored projection alone', async () => {
  const s = createMockEventStore()
  const state = { totals: { a: 1 } }
  await s.saveProjection(STREAM, state, 1)
  state.totals.a = 2
  const loaded = await s.loadProjection(STREAM)
  loaded.state.totals.a = 3
  loaded.throughSeq = 9
  assert.deepEqual(await s.loadProjection(STREAM), { ...STREAM, state: { totals: { a: 1 } }, throughSeq: 1, updatedAt: '1970-01-01T00:00:00.000Z' })
})

test('mock store: a payload and a state come back as a jsonb column would return them', async () => {
  const s = createMockEventStore()
  const at = new Date('2026-10-08T13:20:00.123Z')
  const { event } = await s.append(STREAM, { type: 'A', payload: { at, gone: undefined } })
  await s.saveProjection(STREAM, { at }, 1)
  assert.deepEqual(event.payload, { at: '2026-10-08T13:20:00.123Z' })
  assert.deepEqual((await s.loadProjection(STREAM)).state, { at: '2026-10-08T13:20:00.123Z' })
})

test('mock store: streams whose parts join to the same text around ":" stay separate, as rows do', async () => {
  const s = createMockEventStore()
  const a = { ownerId: 'o', stream: 'ledger:eur', subjectId: 'x' }
  const b = { ownerId: 'o', stream: 'ledger', subjectId: 'eur:x' }
  await s.append(a, { type: 'A', payload: {} })
  await s.saveProjection(a, { n: 1 }, 1)
  assert.equal(await s.maxSeq(b), 0)
  assert.deepEqual(await s.loadEvents(b), [])
  assert.equal(await s.loadProjection(b), null)
  assert.equal((await s.append(b, { type: 'B', payload: {} })).event.seq, 1)
})
