import { test } from 'node:test'
import assert from 'node:assert/strict'
import { createEventStore } from '../pg/store.js'
import { createMockEventStore } from '../mock/mock.js'
import { UnknownSeqError } from './redaction.js'

// redact overwrites the payload of events already in the log with a tombstone
// and keeps everything else, so the log's order and the folds over it survive
// the erasure of one fact. Both stores are held to the same contract; the pg
// store runs against a fake pool that keeps rows the way pg hands them back.

const STREAM = { ownerId: 'a1', stream: 'ledger', subjectId: 's1' }
const LOCK = 'SELECT pg_advisory_xact_lock(hashtextextended($1, 0))'

// Rows are matched on the full (owner_id, stream, subject_id) tuple, as the
// table does, so a statement that drops part of its scope misses or hits the
// wrong stream here too.
function persistingPool() {
  const rows = []
  const calls = []
  const inStream = (params) => (r) => r.owner_id === params[0] && r.stream === params[1] && r.subject_id === params[2]
  const query = async (text, params) => {
    calls.push({ text, params })
    if (text.includes('MAX(seq)')) return { rows: [{ max: String(rows.filter(inStream(params)).length) }], rowCount: 1 }
    if (text.includes('INSERT INTO "domain_events"')) {
      const [owner_id, stream, subject_id, seq, type, payload, occurred_at, source] = params
      const row = { id: `e${seq}`, owner_id, stream, subject_id, seq: String(seq), type, payload, occurred_at: new Date(occurred_at ?? 0), source }
      rows.push(row)
      return { rows: [row], rowCount: 1 }
    }
    if (text.includes('seq = ANY($4')) {
      const found = rows.filter(inStream(params)).filter((r) => params[3].includes(Number(r.seq)))
      return { rows: found.map(({ seq, type }) => ({ seq, type })), rowCount: found.length }
    }
    if (text.startsWith('UPDATE "domain_events"')) {
      const row = rows.filter(inStream(params)).find((r) => Number(r.seq) === params[3])
      if (row) row.payload = params[4]
      return { rows: [], rowCount: row ? 1 : 0 }
    }
    if (text.includes('FROM "domain_events"')) {
      const found = rows.filter(inStream(params)).filter((r) => Number(r.seq) > params[3])
      return { rows: found, rowCount: found.length }
    }
    return { rows: [], rowCount: 0 }
  }
  return { rows, calls, query, connect: async () => ({ query, release() {} }) }
}

const texts = (pool) => pool.calls.map((c) => c.text)
const updates = (pool) => pool.calls.filter((c) => c.text.startsWith('UPDATE "domain_events"'))
const bothStores = (options) => [
  ['pg', createEventStore(persistingPool(), options)],
  ['mock', createMockEventStore(options)],
]

async function seed(store, payloads) {
  for (const [i, payload] of payloads.entries()) {
    await store.append(STREAM, { type: `T${i + 1}`, payload, source: 'test', occurredAt: `2026-01-0${i + 1}T00:00:00.000Z` })
  }
}

test('both stores: redacting the middle event keeps every seq, the order and the other events untouched', async () => {
  for (const [name, store] of bothStores()) {
    await seed(store, [{ n: 1 }, { n: 2, note: 'private' }, { n: 3 }])
    const before = await store.loadEvents(STREAM)

    const redacted = await store.redact(STREAM, [2], { reason: 'erasure request 42' })

    const after = await store.loadEvents(STREAM)
    assert.deepEqual(redacted, [2], name)
    assert.deepEqual(after.map((e) => e.seq), [1, 2, 3], name)
    assert.deepEqual([after[0], after[2]], [before[0], before[2]], `${name}: the others are untouched`)
    const { payload, redacted: flag, ...kept } = after[1]
    const { payload: _, ...keptBefore } = before[1]
    assert.deepEqual(kept, keptBefore, `${name}: id, seq, type, occurredAt and source are kept`)
    assert.equal(flag, true, `${name}: loadEvents marks the tombstone`)
    assert.equal(payload.$redacted, true, name)
    assert.equal(payload.reason, 'erasure request 42', name)
    assert.equal(typeof payload.at, 'string', name)
    assert.ok(!JSON.stringify(after).includes('private'), `${name}: the fact is gone`)
  }
})

test('both stores: redact returns the seqs it redacted, sorted and de-duplicated', async () => {
  for (const [name, store] of bothStores()) {
    await seed(store, [{ n: 1 }, { n: 2 }, { n: 3 }])
    assert.deepEqual(await store.redact(STREAM, [3, 1, 3], { reason: 'r' }), [1, 3], name)
  }
})

test('both stores: your own tombstone keeps the fields a fold still needs', async () => {
  for (const [name, store] of bothStores()) {
    await seed(store, [{ amount: 5, note: 'private' }])
    await store.redact(STREAM, [1], { reason: 'r', tombstone: { amount: 5 } })
    const [event] = await store.loadEvents(STREAM)
    assert.equal(event.payload.amount, 5, name)
    assert.equal(event.payload.$redacted, true, name)
    assert.equal(event.redacted, true, name)
    assert.equal(event.payload.note, undefined, name)
  }
})

test('both stores: an unknown seq rejects with UnknownSeqError and changes nothing', async () => {
  for (const [name, store] of bothStores()) {
    await seed(store, [{ n: 1 }, { n: 2 }])
    const before = await store.loadEvents(STREAM)
    await assert.rejects(store.redact(STREAM, [1, 7], { reason: 'r' }), (err) => {
      assert.ok(err instanceof UnknownSeqError, name)
      assert.deepEqual(err.seqs, [7], name)
      return true
    })
    assert.deepEqual(await store.loadEvents(STREAM), before, `${name}: seq 1 was not redacted either`)
  }
})

test('both stores: malformed input is refused before the lock is taken', async () => {
  const pool = persistingPool()
  for (const store of [createEventStore(pool), createMockEventStore()]) {
    await assert.rejects(store.redact(STREAM, [], { reason: 'r' }), TypeError)
    await assert.rejects(store.redact(STREAM, [1], {}), TypeError)
    await assert.rejects(store.redact({ stream: 'ledger', subjectId: 's1' }, [1], { reason: 'r' }), /missing ownerId/)
  }
  assert.equal(pool.calls.length, 0)
})

test('both stores: the tombstone goes through the codec under the event\'s own context', async () => {
  const seen = []
  const sealing = {
    encode: (value, ctx) => { seen.push(ctx.type); return { sealed: Buffer.from(JSON.stringify(value)).toString('base64') } },
    decode: (stored) => JSON.parse(Buffer.from(stored.sealed, 'base64').toString()),
  }
  const pool = persistingPool()
  for (const [name, store] of [['pg', createEventStore(pool, { codec: sealing })], ['mock', createMockEventStore({ codec: sealing })]]) {
    seen.length = 0
    await seed(store, [{ n: 1 }, { n: 2 }])
    await store.redact(STREAM, [2], { reason: 'r' })
    assert.deepEqual(seen, ['T1', 'T2', 'T2'], `${name}: encoded as a T2 payload`)
    const [, event] = await store.loadEvents(STREAM)
    assert.equal(event.payload.$redacted, true, `${name}: decoded on the way out`)
    assert.equal(event.redacted, true, name)
  }
  const stored = updates(pool)[0].params[4]
  assert.ok(!stored.includes('$redacted'), 'the tombstone is stored sealed, as any payload is')
})

test('both stores: tx.redact inside a transaction is rolled back by a later error', async () => {
  const mock = createMockEventStore()
  await seed(mock, [{ n: 1, note: 'private' }])
  await assert.rejects(mock.transaction(STREAM, async (tx) => {
    assert.deepEqual(await tx.redact([1], { reason: 'r' }), [1])
    assert.equal((await tx.loadEvents())[0].redacted, true, 'the transaction sees its own redaction')
    throw new Error('later failure')
  }), /later failure/)
  assert.deepEqual((await mock.loadEvents(STREAM))[0].payload, { n: 1, note: 'private' }, 'mock: the payload is restored')

  const pool = persistingPool()
  const pg = createEventStore(pool)
  await seed(pg, [{ n: 1 }])
  pool.calls.length = 0
  await assert.rejects(pg.transaction(STREAM, async (tx) => {
    await tx.redact([1], { reason: 'r' })
    throw new Error('later failure')
  }), /later failure/)
  assert.deepEqual(texts(pool).filter((t) => /^(BEGIN|COMMIT|ROLLBACK)$/.test(t)), ['BEGIN', 'ROLLBACK'])
  assert.equal(updates(pool).length, 1, 'pg: the UPDATE ran inside the transaction that rolled back')
})

test('mock: redact, re-fold and save the projection as one transaction — the README pattern', async () => {
  const reduce = (total, event) => (event.redacted ? total : total + event.payload.amount)
  const store = createMockEventStore()
  await seed(store, [{ amount: 1 }, { amount: 10 }, { amount: 100 }])
  await store.transaction(STREAM, async (tx) => {
    await tx.redact([2], { reason: 'r' })
    const events = await tx.loadEvents()
    await tx.saveProjection(events.reduce(reduce, 0), events.at(-1).seq)
  })
  const projection = await store.loadProjection(STREAM)
  assert.deepEqual([projection.state, projection.throughSeq], [101, 3])
})

test('pg: redact takes the stream lock, checks every seq, then updates only the payload', async () => {
  const pool = persistingPool()
  const store = createEventStore(pool)
  await seed(store, [{ n: 1 }, { n: 2 }])
  pool.calls.length = 0

  await store.redact(STREAM, [2, 1], { reason: 'r' })

  const sql = texts(pool)
  assert.deepEqual(sql.slice(0, 2), ['BEGIN', LOCK])
  assert.equal(sql.at(-1), 'COMMIT')
  const select = pool.calls.findIndex((c) => c.text.includes('seq = ANY($4'))
  assert.deepEqual(pool.calls[select].params, ['a1', 'ledger', 's1', [1, 2]])
  assert.ok(select < sql.findIndex((t) => t.startsWith('UPDATE')), 'every seq is checked before any is written')
  assert.deepEqual(updates(pool).map((c) => c.params[3]), [1, 2])
  assert.ok(updates(pool).every((c) => /^UPDATE "domain_events" SET payload = \$5 WHERE/.test(c.text)), 'nothing but the payload changes')
})

test('pg: the UPDATE is scoped to the stream tuple, so the same seq in another stream is untouched', async () => {
  const pool = persistingPool()
  const store = createEventStore(pool)
  const neighbours = [{ ...STREAM, subjectId: 's2' }, { ...STREAM, ownerId: 'b2' }, { ...STREAM, stream: 'other' }]
  for (const stream of [STREAM, ...neighbours]) await store.append(stream, { type: 'A', payload: { note: 'kept' } })

  await store.redact(STREAM, [1], { reason: 'r' })

  const [update] = updates(pool)
  assert.deepEqual(update.params.slice(0, 3), [STREAM.ownerId, STREAM.stream, STREAM.subjectId])
  assert.match(update.text, /WHERE owner_id = \$1 AND stream = \$2 AND subject_id = \$3 AND seq = \$4$/)
  for (const stream of neighbours) {
    assert.deepEqual((await store.loadEvents(stream))[0].payload, { note: 'kept' }, `${JSON.stringify(stream)} untouched`)
  }
  assert.equal((await store.loadEvents(STREAM))[0].redacted, true)
})

test('pg: an unknown seq rolls back without an UPDATE', async () => {
  const pool = persistingPool()
  await assert.rejects(createEventStore(pool).redact(STREAM, [1], { reason: 'r' }), UnknownSeqError)
  assert.ok(texts(pool).includes('ROLLBACK'))
  assert.equal(updates(pool).length, 0)
})

const tick = () => new Promise((resolve) => setTimeout(resolve, 5))

test('mock: a redact waits for an append in flight under the lock, so it sees the event', async () => {
  const store = createMockEventStore()
  const appending = store.transaction(STREAM, async (tx) => {
    await tick()
    return tx.append({ type: 'A', payload: { note: 'private' } })
  })
  const redacting = store.redact(STREAM, [1], { reason: 'r' })
  await appending
  assert.deepEqual(await redacting, [1])
  assert.equal((await store.loadEvents(STREAM))[0].redacted, true)
})

test('mock: an append waits for a redact in flight, and a deduped retry returns the tombstone', async () => {
  const store = createMockEventStore()
  await store.append(STREAM, { type: 'A', payload: { note: 'private' } }, { dedupeKey: 'req-1' })
  let release
  const redacting = store.transaction(STREAM, async (tx) => {
    await tx.redact([1], { reason: 'r' })
    await new Promise((resolve) => { release = resolve })
  })
  const appending = store.append(STREAM, { type: 'B', payload: {} }, { expectedSeq: 1 })
  await tick()
  release()
  await Promise.all([redacting, appending])
  const retry = await store.append(STREAM, { type: 'A', payload: { note: 'private' } }, { dedupeKey: 'req-1' })
  assert.equal(retry.appended, false)
  assert.equal(retry.event.redacted, true, 'the retry does not resurrect the redacted payload')
  assert.deepEqual((await store.loadEvents(STREAM)).map((e) => e.seq), [1, 2])
})

test('both stores: every append path refuses a payload that would read as a tombstone, before encode', async () => {
  // An application spreading request JSON into a payload must not be able to
  // forge a tombstone that its reducer would then skip.
  let encoded = 0
  const counting = { encode: (v) => { encoded++; return v }, decode: (s) => s }
  const forged = { type: 'Deposited', payload: { amount: 5, $redacted: true } }
  const pool = persistingPool()
  for (const [name, store] of [['pg', createEventStore(pool, { codec: counting })], ['mock', createMockEventStore({ codec: counting })]]) {
    await assert.rejects(store.append(STREAM, forged), /\$redacted is reserved/, `${name}: append`)
    await assert.rejects(store.transaction(STREAM, (tx) => tx.append(forged)), /\$redacted is reserved/, `${name}: tx.append`)
    assert.equal(await store.maxSeq(STREAM), 0, name)
  }
  assert.equal(encoded, 0, 'refused before the codec ran')
  assert.ok(!texts(pool).some((t) => t.includes('INSERT')), 'pg: nothing reached the INSERT')
})
