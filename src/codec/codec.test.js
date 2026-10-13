import { test } from 'node:test'
import assert from 'node:assert/strict'
import { createEventStore } from '../pg/store.js'
import { createMockEventStore } from '../mock/mock.js'

// The codec is the caller's: the store must apply it on every write and read
// of a payload or a state, hand it the stream it belongs to, and never paper
// over a failure — a codec that seals data and silently falls back to clear
// text on error is worse than none. Without a codec, nothing may change.

const STREAM = { ownerId: 'a1', stream: 'ledger', subjectId: 's1' }

// Reversible and owner-bound, like a per-owner key would be: decoding under a
// different owner than the one that encoded fails.
const sealing = {
  encode: async (value, ctx) => ({ sealed: Buffer.from(JSON.stringify(value)).toString('base64'), owner: ctx.ownerId }),
  decode: async (stored, ctx) => {
    if (stored.owner !== ctx.ownerId) throw new Error(`sealed for ${stored.owner}, read as ${ctx.ownerId}`)
    return JSON.parse(Buffer.from(stored.sealed, 'base64').toString())
  },
}

// A fake pool that keeps what the store writes and returns it the way pg does:
// jsonb comes back as a string here, which is the harder of the two cases; a
// uuid owner_id comes back lowercased; and a repeated dedupe key trips the
// unique index, as it would if the store did not look the key up first.
function persistingPool() {
  const events = []
  let projection = null
  const calls = []
  let connected = 0
  const query = async (text, params) => {
    calls.push({ text, params })
    if (text.includes('MAX(seq)')) return { rows: [{ max: String(events.length) }], rowCount: 1 }
    if (text.includes('INSERT INTO "domain_events"')) {
      const [owner_id, stream, subject_id, seq, type, payload, , , dedupe_key] = params
      if (dedupe_key && events.some((e) => e.dedupe_key === dedupe_key)) throw new Error('duplicate key value violates unique constraint')
      const row = {
        id: `e${seq}`, owner_id: owner_id.toLowerCase(), stream, subject_id, seq: String(seq), type, payload,
        occurred_at: new Date(0), source: null, dedupe_key,
      }
      events.push(row)
      return { rows: [row], rowCount: 1 }
    }
    if (text.includes('dedupe_key = $4')) return { rows: events.filter((e) => e.dedupe_key === params[3]), rowCount: 1 }
    if (text.includes('FROM "domain_events"')) return { rows: events.filter((e) => Number(e.seq) > params[3]), rowCount: events.length }
    if (text.includes('INSERT INTO "domain_projections"')) { projection = { state: params[3], through_seq: String(params[4]), updated_at: new Date(0) }; return { rows: [], rowCount: 1 } }
    if (text.includes('FROM "domain_projections"')) return { rows: projection ? [projection] : [], rowCount: projection ? 1 : 0 }
    return { rows: [], rowCount: 0 }
  }
  return {
    calls, events, get projection() { return projection }, get connected() { return connected },
    query,
    connect: async () => { connected++; return { query, release() {} } },
  }
}

const insertedPayload = (pool) => pool.calls.find((c) => c.text.includes('INSERT INTO "domain_events"')).params[5]
const savedState = (pool) => pool.calls.find((c) => c.text.includes('INSERT INTO "domain_projections"')).params[3]

test('pg store: a codec round-trips event payloads and projection state, and only the encoded form is stored', async () => {
  const pool = persistingPool()
  const store = createEventStore(pool, { codec: sealing })

  const appended = await store.append(STREAM, { type: 'Amended', payload: { balance: 2 } })
  await store.saveProjection(STREAM, { total: 9 }, 1)

  assert.deepEqual(appended.event.payload, { balance: 2 }, 'append hands back the decoded payload')
  assert.deepEqual((await store.loadEvents(STREAM)).map((e) => e.payload), [{ balance: 2 }])
  assert.deepEqual((await store.loadProjection(STREAM)).state, { total: 9 })
  assert.ok(!insertedPayload(pool).includes('balance'), 'the clear payload never reaches the INSERT')
  assert.ok(!savedState(pool).includes('total'), 'the clear state never reaches the upsert')
  assert.equal(JSON.parse(insertedPayload(pool)).owner, 'a1', 'what is stored is the codec\'s JSON value')
})

test('pg store: without a codec the stored bytes are exactly JSON.stringify of the value', async () => {
  const pool = persistingPool()
  const store = createEventStore(pool)
  const payload = { balance: 2, nested: { at: '2026-10-02' } }

  const appended = await store.append(STREAM, { type: 'Amended', payload })
  await store.saveProjection(STREAM, { total: 9 }, 1)

  assert.equal(insertedPayload(pool), JSON.stringify(payload))
  assert.equal(savedState(pool), JSON.stringify({ total: 9 }))
  assert.deepEqual(appended.event.payload, payload)
})

test('decode and encode receive the stream tuple, the kind, and the event type', async () => {
  const seen = []
  const recording = {
    encode: (value, ctx) => { seen.push(['encode', ctx]); return value },
    decode: (stored, ctx) => { seen.push(['decode', ctx]); return stored },
  }
  const store = createEventStore(persistingPool(), { codec: recording })
  await store.append(STREAM, { type: 'Amended', payload: {} })
  await store.loadEvents(STREAM)
  await store.saveProjection(STREAM, {}, 1)
  await store.loadProjection(STREAM)

  const event = { kind: 'event', ownerId: 'a1', stream: 'ledger', subjectId: 's1', type: 'Amended' }
  const projection = { kind: 'projection', ownerId: 'a1', stream: 'ledger', subjectId: 's1' }
  assert.deepEqual(seen, [
    ['encode', event], ['decode', event], ['decode', event],
    ['encode', projection], ['decode', projection],
  ])
})

test('pg store: an encode failure rejects before a connection is taken', async () => {
  const pool = persistingPool()
  const failing = { encode: async () => { throw new Error('no key') }, decode: (s) => s }
  const store = createEventStore(pool, { codec: failing })

  await assert.rejects(() => store.append(STREAM, { type: 'A', payload: {} }), /no key/)
  await assert.rejects(() => store.saveProjection(STREAM, {}, 1), /no key/)
  assert.equal(pool.connected, 0, 'no transaction was opened for a payload that could not be encoded')
  assert.equal(pool.calls.length, 0, 'nothing was written in clear instead')
})

test('pg store: a decode failure rejects the read rather than returning the stored form', async () => {
  const pool = persistingPool()
  await createEventStore(pool, { codec: sealing }).append(STREAM, { type: 'A', payload: { n: 1 } })
  await createEventStore(pool, { codec: sealing }).saveProjection(STREAM, { n: 1 }, 1)
  const failing = { encode: (v) => v, decode: async () => { throw new Error('key destroyed') } }
  const store = createEventStore(pool, { codec: failing })

  await assert.rejects(() => store.loadEvents(STREAM), /key destroyed/)
  await assert.rejects(() => store.loadProjection(STREAM), /key destroyed/)
})

test('an encode that returns undefined is refused, naming where it came from', async () => {
  const silent = { encode: () => undefined, decode: (s) => s }
  for (const store of [createEventStore(persistingPool(), { codec: silent }), createMockEventStore({ codec: silent })]) {
    await assert.rejects(() => store.append(STREAM, { type: 'A', payload: {} }), /codec\.encode returned undefined for event on ledger\/s1/)
    await assert.rejects(() => store.saveProjection(STREAM, {}, 1), /returned undefined for projection/)
  }
})

test('a codec missing encode or decode is refused at construction', () => {
  for (const codec of [{}, { encode: (v) => v }, { decode: (s) => s }]) {
    assert.throws(() => createEventStore(persistingPool(), { codec }), /encode\(value, context\) and decode\(stored, context\)/)
    assert.throws(() => createMockEventStore({ codec }), /encode\(value, context\) and decode\(stored, context\)/)
  }
})

test('mock store: a codec round-trips and is bound to the owner, as in the pg store', async () => {
  const s = createMockEventStore({ codec: sealing })
  const fresh = await s.append(STREAM, { type: 'A', payload: { n: 1 } }, { dedupeKey: 'req-1' })
  const retry = await s.append(STREAM, { type: 'A', payload: { n: 2 } }, { dedupeKey: 'req-1' })
  await s.saveProjection(STREAM, { total: 1 }, 1)

  assert.deepEqual(fresh.event.payload, { n: 1 })
  assert.deepEqual(retry.event.payload, { n: 1 }, 'a deduped retry decodes the first call\'s payload')
  assert.deepEqual((await s.loadEvents(STREAM)).map((e) => e.payload), [{ n: 1 }])
  assert.deepEqual((await s.loadProjection(STREAM)).state, { total: 1 })
})

test('mock store: codec errors propagate on write and on read', async () => {
  let failDecode = false
  const flaky = {
    encode: (v, ctx) => { if (ctx.type === 'Poison') throw new Error('no key'); return v },
    decode: (s) => { if (failDecode) throw new Error('key destroyed'); return s },
  }
  const s = createMockEventStore({ codec: flaky })
  await assert.rejects(() => s.append(STREAM, { type: 'Poison', payload: {} }), /no key/)
  assert.equal(await s.maxSeq(STREAM), 0, 'a failed encode writes nothing')

  await s.append(STREAM, { type: 'A', payload: {} })
  await s.saveProjection(STREAM, {}, 1)
  failDecode = true
  await assert.rejects(() => s.loadEvents(STREAM), /key destroyed/)
  await assert.rejects(() => s.loadProjection(STREAM), /key destroyed/)
})

test('pg store: a deduped retry with a codec returns the first call\'s payload, decoded', async () => {
  const pool = persistingPool()
  const store = createEventStore(pool, { codec: sealing })
  const fresh = await store.append(STREAM, { type: 'A', payload: { n: 1 } }, { dedupeKey: 'req-1' })
  const retry = await store.append(STREAM, { type: 'A', payload: { n: 2 } }, { dedupeKey: 'req-1' })

  assert.equal(fresh.appended, true)
  assert.equal(retry.appended, false)
  assert.deepEqual(retry.event.payload, { n: 1 }, 'the stored payload, decoded — not the retried one')
  assert.equal(pool.events.length, 1)
})

test('pg store: a decode failure on append\'s own return rolls back instead of committing', async () => {
  // A stored event reported as a failure is the worst of both: the caller's
  // message is redelivered and the event is appended a second time.
  const pool = persistingPool()
  const unreadable = { encode: (v) => v, decode: () => { throw new Error('key destroyed') } }
  const store = createEventStore(pool, { codec: unreadable })

  await assert.rejects(() => store.append(STREAM, { type: 'A', payload: {} }), /key destroyed/)
  const texts = pool.calls.map((c) => c.text)
  assert.ok(texts.includes('ROLLBACK'))
  assert.ok(!texts.includes('COMMIT'), 'nothing is committed that the caller was told failed')
})

test('pg store: an owner-bound codec reads back what was appended under an uppercase ownerId', async () => {
  // owner_id is a uuid column, so pg hands it back lowercased. Decoding under
  // the row's id rather than the caller's would make that data unreadable.
  const upper = { ownerId: 'A1B2C3D4-0000-4000-8000-00000000000A', stream: 'ledger', subjectId: 's1' }
  const store = createEventStore(persistingPool(), { codec: sealing })

  const appended = await store.append(upper, { type: 'A', payload: { n: 1 } })
  assert.deepEqual(appended.event.payload, { n: 1 })
  assert.deepEqual((await store.loadEvents(upper)).map((e) => e.payload), [{ n: 1 }])
})

test('mock store: a decode failure on append stores nothing and leaves the dedupe key free', async () => {
  let failDecode = true
  const flaky = { encode: (v) => v, decode: (s) => { if (failDecode) throw new Error('key destroyed'); return s } }
  const s = createMockEventStore({ codec: flaky })

  await assert.rejects(() => s.append(STREAM, { type: 'A', payload: { n: 1 } }, { dedupeKey: 'req-1' }), /key destroyed/)
  assert.equal(await s.maxSeq(STREAM), 0)
  failDecode = false
  const retry = await s.append(STREAM, { type: 'A', payload: { n: 1 } }, { dedupeKey: 'req-1' })
  assert.equal(retry.appended, true, 'the failed attempt did not claim the key')
})

test('both stores: an encoded value that is not JSON fails as it would in jsonb', async () => {
  // The mock stores what a jsonb column would hand back, so a codec returning
  // bytes or a BigInt fails in a no-DB test, not first in production.
  const bytes = {
    encode: (v) => new TextEncoder().encode(JSON.stringify(v)),
    decode: (s) => JSON.parse(new TextDecoder().decode(s)),
  }
  const bigint = { encode: () => ({ n: 1n }), decode: (s) => s }
  for (const store of [createEventStore(persistingPool(), { codec: bytes }), createMockEventStore({ codec: bytes })]) {
    await assert.rejects(() => store.append(STREAM, { type: 'A', payload: { n: 1 } }), /could not decode the A event at seq 1/)
  }
  for (const store of [createEventStore(persistingPool(), { codec: bigint }), createMockEventStore({ codec: bigint })]) {
    await assert.rejects(() => store.append(STREAM, { type: 'A', payload: {} }), /BigInt/)
    await assert.rejects(() => store.saveProjection(STREAM, {}, 1), /BigInt/)
  }
})

test('both stores: a transaction applies the codec to every payload and state it touches', async () => {
  const pool = persistingPool()
  for (const store of [createEventStore(pool, { codec: sealing }), createMockEventStore({ codec: sealing })]) {
    const read = await store.transaction(STREAM, async (tx) => {
      const { event } = await tx.append({ type: 'A', payload: { n: 1 } })
      await tx.saveProjection({ total: 1 }, event.seq)
      return { appended: event.payload, events: await tx.loadEvents(), projection: (await tx.loadProjection()).state }
    })
    assert.deepEqual(read, { appended: { n: 1 }, events: [read.events[0]], projection: { total: 1 } })
    assert.deepEqual(read.events[0].payload, { n: 1 })
  }
  assert.ok(!insertedPayload(pool).includes('"n"'), 'the clear payload never reaches the INSERT')
  assert.ok(!savedState(pool).includes('total'), 'the clear state never reaches the upsert')
})

// A decode that fails names what it was reading, so the one bad row among
// thousands can be found; the codec's own error is kept as the cause.
test('both stores: a decode failure names the stream, and the event\'s seq and type or the projection', async () => {
  const broken = new Error('unable to authenticate data')
  let failing = false
  const flaky = { encode: (v) => v, decode: (s) => { if (failing) throw broken; return s } }
  for (const [name, store] of [['pg', createEventStore(persistingPool(), { codec: flaky })], ['mock', createMockEventStore({ codec: flaky })]]) {
    failing = false
    await store.append(STREAM, { type: 'Deposited', payload: { n: 1 } })
    await store.saveProjection(STREAM, { n: 1 }, 1)
    failing = true
    await assert.rejects(store.loadEvents(STREAM), (err) => {
      assert.match(err.message, /could not decode the Deposited event at seq 1 on ledger\/s1: unable to authenticate data/, name)
      assert.equal(err.cause, broken, name)
      return true
    })
    await assert.rejects(store.loadProjection(STREAM), /could not decode the projection state on ledger\/s1: unable to authenticate data/, name)
  }
})
