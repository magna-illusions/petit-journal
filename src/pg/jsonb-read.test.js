import { test } from 'node:test'
import assert from 'node:assert/strict'
import { createEventStore } from './store.js'
import { defineProjection } from '../projection/define.js'

// pg's default type parser hands a jsonb column back already parsed: a JSON
// string arrives as a JS string, indistinguishable from JSON text. This fake
// keeps each value as JSON text, as the column does, and returns it the way
// pg would: parsed for a bare column, text for column::text.

const STREAM = { ownerId: 'a1', stream: 'ledger', subjectId: 'a1' }

function jsonbPool() {
  const events = []
  let projection = null
  const asPgReturns = (text, sql, column) => (sql.includes(`${column}::text`) ? text : JSON.parse(text))
  const query = async (sql, p) => {
    if (sql.includes('MAX(seq)')) return { rows: [{ max: String(events.length) }], rowCount: 1 }
    const read = (r) => ({ ...r, payload: asPgReturns(r.payload, sql, 'payload') })
    if (sql.includes('dedupe_key = $4')) return { rows: events.filter((e) => e.dedupe_key === p[3]).map(read), rowCount: 1 }
    if (sql.startsWith('INSERT INTO "domain_events"')) {
      const row = { id: `e${p[3]}`, owner_id: p[0], stream: p[1], subject_id: p[2], seq: String(p[3]), type: p[4], payload: p[5], occurred_at: new Date(0), source: null, dedupe_key: p[8] }
      events.push(row)
      return { rows: [read(row)], rowCount: 1 }
    }
    if (sql.includes('seq = ANY')) return { rows: events.filter((e) => p[3].includes(Number(e.seq))), rowCount: 1 }
    if (sql.startsWith('UPDATE "domain_events"')) { events.find((e) => Number(e.seq) === p[3]).payload = p[4]; return { rows: [], rowCount: 1 } }
    if (sql.includes('FROM "domain_events"')) return { rows: events.filter((e) => Number(e.seq) > p[3]).map(read), rowCount: 1 }
    if (sql.startsWith('INSERT INTO "domain_projections"')) { projection = { state: p[3], through_seq: String(p[4]), updated_at: new Date(0) }; return { rows: [], rowCount: 1 } }
    if (sql.includes('FROM "domain_projections"')) {
      return { rows: projection ? [{ ...projection, state: asPgReturns(projection.state, sql, 'state') }] : [], rowCount: projection ? 1 : 0 }
    }
    return { rows: [], rowCount: 0 }
  }
  return { query, connect: async () => ({ query, release() {} }) }
}

const VALUES = ['hello', '123', 'true', 'null', '', { k: 'v' }, [1, '2'], 42, false, null]

test('pg: every JSON value round-trips through append, loadEvents and a dedupe hit, strings included', async () => {
  const store = createEventStore(jsonbPool())
  for (const [i, value] of VALUES.entries()) {
    const { event } = await store.append(STREAM, { type: 'A', payload: value }, { dedupeKey: `k${i}` })
    assert.deepEqual(event.payload, value, `append returns ${JSON.stringify(value)}`)
    assert.deepEqual((await store.append(STREAM, { type: 'A', payload: 'other' }, { dedupeKey: `k${i}` })).event.payload, value, `dedupe hit for ${JSON.stringify(value)}`)
  }
  assert.deepEqual((await store.loadEvents(STREAM)).map((e) => e.payload), VALUES)
  assert.deepEqual((await store.transaction(STREAM, (tx) => tx.loadEvents())).map((e) => e.payload), VALUES)
})

test('pg: a projection state that is a string, or looks like one, round-trips', async () => {
  const store = createEventStore(jsonbPool())
  for (const state of ['abc', '123', 'true', { balance: 1 }, 0]) {
    await store.saveProjection(STREAM, state, 1)
    assert.deepEqual((await store.loadProjection(STREAM)).state, state, JSON.stringify(state))
    assert.deepEqual((await store.transaction(STREAM, (tx) => tx.loadProjection())).state, state, `tx: ${JSON.stringify(state)}`)
  }
})

test('pg: a codec whose encode returns a string reads back, through the store and defineProjection', async () => {
  const base64 = {
    encode: (value) => Buffer.from(JSON.stringify(value)).toString('base64'),
    decode: (stored) => JSON.parse(Buffer.from(stored, 'base64').toString()),
  }
  const store = createEventStore(jsonbPool(), { codec: base64 })
  const ledger = defineProjection(store, { initial: { balance: 0 }, reduce: (s, e) => ({ balance: s.balance + e.payload.amount }) })
  await ledger.append(STREAM, [{ type: 'Deposited', payload: { amount: 5 } }, { type: 'Deposited', payload: { amount: 7 } }])
  assert.deepEqual(await ledger.state(STREAM), { balance: 12 })
  assert.deepEqual((await store.loadEvents(STREAM)).map((e) => e.payload), [{ amount: 5 }, { amount: 7 }])
  await store.redact(STREAM, [1], { reason: 'r' })
  assert.deepEqual(await ledger.rebuild(STREAM), { balance: 7 })
})
