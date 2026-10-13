import { test } from 'node:test'
import assert from 'node:assert/strict'
import { openEventStore } from './open.js'
import { createEventStore } from './store.js'

// openEventStore is the route for an application that lets the store own its
// tables: the store only exists once they do, so no write can reach a table
// that is not there yet.

const STREAM = { ownerId: 'a1', stream: 'ledger', subjectId: 'a1' }

// A pool whose schema query waits until the test lets it finish.
function slowSchemaPool() {
  const calls = []
  let finish
  const schemaDone = new Promise((resolve) => { finish = resolve })
  const query = async (text, params) => {
    calls.push(text)
    if (text.includes('CREATE TABLE')) { await schemaDone; return { rows: [], rowCount: 0 } }
    if (text.includes('MAX(seq)')) return { rows: [{ max: '0' }], rowCount: 1 }
    if (text.startsWith('INSERT INTO "domain_events"')) {
      return { rows: [{ id: 'e1', owner_id: 'a1', stream: 'ledger', subject_id: 'a1', seq: '1', type: params[4], payload: params[5], occurred_at: new Date(0), source: null }], rowCount: 1 }
    }
    return { rows: [], rowCount: 0 }
  }
  return { calls, finish, query, connect: async () => ({ query, release() {} }) }
}

test('openEventStore resolves to the store only after the schema setup has finished', async () => {
  const pool = slowSchemaPool()
  let store
  const opening = openEventStore(pool).then((opened) => { store = opened })
  await new Promise((resolve) => setTimeout(resolve, 10))
  assert.equal(store, undefined, 'no store while the tables may not exist')
  assert.equal(pool.calls.length, 1)
  pool.finish()
  await opening
  assert.equal((await store.append(STREAM, { type: 'A', payload: {} })).event.seq, 1)
  assert.deepEqual(Object.keys(store).sort(), Object.keys(createEventStore(pool)).sort(), 'the same store createEventStore makes')
})

test('openEventStore rejects at boot when the setup fails', async () => {
  const denied = Object.assign(new Error('permission denied for schema public'), { code: '42501', severity: 'ERROR' })
  const pool = { query: async () => { throw denied }, connect: async () => { throw new Error('no client needed') } }
  await assert.rejects(openEventStore(pool), /could not set up the tables "domain_events" and "domain_projections": permission denied/)
})

test('openEventStore refuses a bad pool, codec or table name before any SQL', async () => {
  const calls = []
  const pool = { query: async (text) => { calls.push(text) }, connect: async () => ({}) }
  await assert.rejects(openEventStore({ query: pool.query }), /createEventStore needs a pool shaped like pg\.Pool/)
  await assert.rejects(openEventStore(pool, { codec: { encode() {} } }), /codec must provide both encode/)
  await assert.rejects(openEventStore(pool, { tables: { events: 'a b' } }), /tables\.events must be a lowercase identifier/)
  assert.deepEqual(calls, [])
})
