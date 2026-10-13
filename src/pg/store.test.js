import { test } from 'node:test'
import assert from 'node:assert/strict'
import { createEventStore } from './store.js'
import { createMockEventStore } from '../mock/mock.js'
import { WrongExpectedSeqError } from '../append.js'

// The store is a handful of SQL statements with no logic of their own, which
// is where a SQL type or parameter bug hides. These tests drive
// createEventStore against a fake pool to lock the glue: transaction
// sequence, advisory lock, seq assignment, row→event mapping, jsonb parsing,
// dedupe, expected seq and compaction delegation.

function makePool(onQuery) {
  const calls = []
  const client = {
    released: false,
    query: async (text, params) => { calls.push({ text, params, via: 'client' }); return onQuery(text, params) },
    release() { this.released = true },
  }
  const pool = {
    calls,
    client,
    query: async (text, params) => { calls.push({ text, params, via: 'pool' }); return onQuery(text, params) },
    connect: async () => client,
  }
  return pool
}

const STREAM = { ownerId: 'a1', stream: 'ledger', subjectId: 'a1' }

// pg's default parser hands a timestamptz back as a Date.
const T0 = new Date('2026-10-08T13:20:00.123Z')

const row = (seq, payload = '{}', source = null) => ({
  id: `e${seq}`, owner_id: 'a1', stream: 'ledger', subject_id: 'a1',
  seq: String(seq), type: 'Snapshot', payload, occurred_at: T0, source,
})

// A stream at `max`, optionally holding an event under a dedupe key.
function streamAt(max, { deduped } = {}) {
  return makePool((text, params) => {
    if (text.includes('dedupe_key = $4')) return deduped ? { rows: [deduped], rowCount: 1 } : { rows: [], rowCount: 0 }
    if (text.includes('MAX(seq)')) return { rows: [{ max: String(max) }], rowCount: 1 }
    if (text.includes('INSERT INTO "domain_events"')) return { rows: [row(params[3], params[5], params[7])], rowCount: 1 }
    return { rows: [], rowCount: 0 }
  })
}

const texts = (pool) => pool.calls.map((c) => c.text)
const insertOf = (pool) => pool.calls.find((c) => c.text.includes('INSERT INTO "domain_events"'))

test('createEventStore refuses a pool without query() and connect(), rather than failing on first use', () => {
  for (const pool of [undefined, null, {}, { query() {} }, { connect() {} }, { query: 'SELECT', connect() {} }]) {
    assert.throws(() => createEventStore(pool), /createEventStore needs a pool shaped like pg\.Pool/, JSON.stringify(pool))
  }
  assert.doesNotThrow(() => createEventStore({ query() {}, connect() {} }))
})

test('every statement names the configured tables, quoted, and no other', async () => {
  const pool = makePool((text, params) => {
    if (text.includes('MAX(seq)')) return { rows: [{ max: '3' }], rowCount: 1 }
    if (text.includes('INSERT INTO') && text.includes('payload')) return { rows: [row(params[3], params[5])], rowCount: 1 }
    if (text.includes('seq = ANY')) return { rows: [{ seq: '1', type: 'A' }], rowCount: 1 }
    return { rows: [], rowCount: 1 }
  })
  const store = createEventStore(pool, { tables: { events: 'app.ledger_events', projections: 'app.ledger_state' } })
  await store.append(STREAM, { type: 'A', payload: {} }, { dedupeKey: 'req-1' })
  await store.loadEvents(STREAM)
  await store.loadProjection(STREAM)
  await store.saveProjection(STREAM, {}, 1)
  await store.maxSeq(STREAM)
  await store.compact(STREAM, 3, 0)
  await store.redact(STREAM, [1], { reason: 'r' })
  const statements = texts(pool).filter((t) => !/^(BEGIN|COMMIT|ROLLBACK)$/.test(t) && !t.includes('pg_advisory'))
  assert.equal(statements.length, 12)
  for (const text of statements) {
    assert.ok(!text.includes('domain_'), text)
    assert.match(text, /"app"\."ledger_(events|state)"/)
  }
  assert.throws(() => createEventStore(pool, { tables: { events: 'events; DROP TABLE x' } }), /tables\.events must be a lowercase identifier/)
})

test('append assigns the next seq under the stream lock, maps the row, commits and releases', async () => {
  const pool = streamAt(3)
  const { event, appended } = await createEventStore(pool).append(STREAM, { type: 'Snapshot', payload: { k: 1 }, source: 'writer' })

  assert.equal(appended, true)
  assert.equal(event.seq, 4)
  assert.equal(event.ownerId, 'a1')
  assert.deepEqual(event.payload, { k: 1 }, 'jsonb string parsed')
  assert.equal(event.source, 'writer')
  assert.deepEqual(texts(pool).filter((t) => /^(BEGIN|COMMIT|ROLLBACK)$/.test(t)), ['BEGIN', 'COMMIT'])
  assert.equal(pool.calls[1].text, 'SELECT pg_advisory_xact_lock(hashtextextended($1, 0))')
  assert.equal(pool.client.released, true)
})

test('the advisory lock key is owner:stream:subject, unchanged across versions', async () => {
  // Writers on different releases of the package must take one lock for one
  // stream, or they append concurrently and race for the same seq.
  const pool = streamAt(0)
  await createEventStore(pool).append({ ownerId: 'o1', stream: 'ledger', subjectId: 's1' }, { type: 'A', payload: {} })
  assert.deepEqual(pool.calls[1].params, ['o1:ledger:s1'])
})

test('append rolls back and rethrows on failure, still releasing the client', async () => {
  const pool = makePool((text) => {
    if (text.includes('INSERT INTO "domain_events"')) throw new Error('boom')
    return { rows: [{ max: '0' }], rowCount: 1 }
  })
  await assert.rejects(() => createEventStore(pool).append(STREAM, { type: 'X', payload: {} }), /boom/)
  assert.ok(texts(pool).includes('ROLLBACK'))
  assert.equal(pool.client.released, true)
})

test('loadProjection parses jsonb state and throughSeq, or null when absent', async () => {
  const present = createEventStore(makePool(() => ({ rows: [{ state: '{"total":9}', through_seq: '7', updated_at: T0 }], rowCount: 1 })))
  const proj = await present.loadProjection(STREAM)
  assert.deepEqual(proj.state, { total: 9 })
  assert.equal(proj.throughSeq, 7)
  assert.equal(proj.ownerId, 'a1')

  const absent = createEventStore(makePool(() => ({ rows: [], rowCount: 0 })))
  assert.equal(await absent.loadProjection(STREAM), null)
})

// The declarations say string. pg's default parser returns a Date, and with
// that parser switched off it returns Postgres's own text, which is not ISO.
// Either way the caller gets one ISO 8601 instant.
test('timestamps come back as ISO strings whether pg hands over a Date or its own text', async () => {
  for (const at of [T0, '2026-10-08 15:20:00.123456+02']) {
    const pool = makePool((text) => {
      if (text.includes('FROM "domain_projections"')) return { rows: [{ state: '{}', through_seq: '1', updated_at: at }], rowCount: 1 }
      if (text.includes('FROM "domain_events"')) return { rows: [{ ...row(1), occurred_at: at }], rowCount: 1 }
      return { rows: [], rowCount: 0 }
    })
    const store = createEventStore(pool)
    const [event] = await store.loadEvents(STREAM)
    const proj = await store.loadProjection(STREAM)
    assert.equal(event.occurredAt, '2026-10-08T13:20:00.123Z', `occurredAt from ${typeof at}`)
    assert.equal(proj.updatedAt, '2026-10-08T13:20:00.123Z', `updatedAt from ${typeof at}`)
  }
})

test('append returns occurredAt as an ISO string, the fresh event and a dedupe hit alike', async () => {
  const fresh = await createEventStore(streamAt(3)).append(STREAM, { type: 'Snapshot', payload: {} })
  const deduped = await createEventStore(streamAt(4, { deduped: row(4) })).append(STREAM, { type: 'Snapshot', payload: {} }, { dedupeKey: 'k' })
  assert.equal(fresh.event.occurredAt, '2026-10-08T13:20:00.123Z')
  assert.equal(deduped.event.occurredAt, '2026-10-08T13:20:00.123Z')
})

test('both stores: an occurredAt given in another ISO form is returned normalised', async () => {
  const pg = createEventStore(makePool((text, params) => {
    if (text.includes('MAX(seq)')) return { rows: [{ max: '0' }], rowCount: 1 }
    if (text.includes('INSERT INTO "domain_events"')) return { rows: [{ ...row(1), occurred_at: new Date(params[6]) }], rowCount: 1 }
    return { rows: [], rowCount: 0 }
  }))
  for (const [name, store] of [['pg', pg], ['mock', createMockEventStore()]]) {
    const { event } = await store.append(STREAM, { type: 'A', payload: {}, occurredAt: '2026-10-08T15:20:00+02:00' })
    assert.equal(event.occurredAt, '2026-10-08T13:20:00.000Z', name)
  }
})

test('saveProjection upserts the JSON-encoded state at through_seq', async () => {
  const pool = makePool(() => ({ rows: [], rowCount: 1 }))
  await createEventStore(pool).saveProjection(STREAM, { total: 3 }, 7)
  const call = pool.calls.find((c) => c.text.includes('INSERT INTO "domain_projections"'))
  assert.equal(call.params[3], JSON.stringify({ total: 3 }))
  assert.equal(call.params[4], 7)
})

test('compact sends the prune cutoff to the DELETE and returns its row count', async () => {
  // maxSeq=200, throughSeq=200, keep=100 → cutoff 100 (delete seq<=100).
  const pool = makePool((text) => {
    if (text.includes('MAX(seq)')) return { rows: [{ max: '200' }], rowCount: 1 }
    if (text.includes('DELETE FROM "domain_events"')) return { rows: [], rowCount: 100 }
    return { rows: [], rowCount: 0 }
  })
  assert.equal(await createEventStore(pool).compact(STREAM, 200, 100), 100)
  assert.equal(pool.calls.find((c) => c.text.includes('DELETE FROM "domain_events"')).params[3], 100, 'deletes seq <= cutoff')
})

test('compact is a no-op when nothing is prunable', async () => {
  const pool = streamAt(10)
  assert.equal(await createEventStore(pool).compact(STREAM, 10, 100), 0)
  assert.ok(!texts(pool).some((t) => t.includes('DELETE FROM "domain_events"')))
})

test('both stores: compact refuses a throughSeq or keep that is not an integer >= 0', async () => {
  // A missing keep used to make the cutoff NaN, and compact silently deleted nothing.
  const pool = streamAt(200)
  for (const [name, store] of [['pg', createEventStore(pool)], ['mock', createMockEventStore()]]) {
    await assert.rejects(store.compact(STREAM, 200), /keep must be an integer >= 0/, `${name}: no keep`)
    for (const bad of [undefined, null, -1, 1.5, '3', Number.NaN]) {
      await assert.rejects(store.compact(STREAM, 200, bad), /keep must be an integer >= 0/, `${name}: keep ${bad}`)
      await assert.rejects(store.compact(STREAM, bad, 100), /throughSeq must be an integer >= 0/, `${name}: throughSeq ${bad}`)
    }
    await assert.rejects(store.transaction(STREAM, (tx) => tx.compact(200)), /keep must be an integer >= 0/, `${name}: tx`)
    assert.equal(typeof await store.compact(STREAM, 0, 0), 'number', `${name}: 0 and 0 are valid`)
  }
  assert.ok(!texts(pool).some((t) => t.includes('DELETE')), 'pg: nothing was deleted')
})

// seq = MAX(seq)+1 stops concurrent racers, not a sequential retry of the same
// logical event: a second call with the same dedupeKey must write nothing new.

test('append with a fresh dedupeKey inserts it and reports appended:true', async () => {
  const pool = streamAt(3)
  const result = await createEventStore(pool).append(STREAM, { type: 'Snapshot', payload: { k: 1 } }, { dedupeKey: 'req-1' })

  assert.equal(result.appended, true)
  assert.equal(result.event.seq, 4)
  assert.equal(insertOf(pool).params[8], 'req-1', 'dedupeKey is parameterized into the insert')
})

test('append with a dedupeKey already used returns the stored event, appended:false, and inserts nothing', async () => {
  const pool = streamAt(4, { deduped: row(4, '{"k":1}') })
  const result = await createEventStore(pool).append(STREAM, { type: 'Snapshot', payload: { k: 2 } }, { dedupeKey: 'req-1' })

  assert.equal(result.appended, false, 'a retried dedupeKey must not report a fresh append')
  assert.equal(result.event.seq, 4, 'the caller gets back the event the first attempt wrote, not a new seq')
  assert.equal(result.event.payload.k, 1, 'the original payload stands — the retried payload was never written')
  assert.equal(insertOf(pool), undefined)
})

test('append without a dedupeKey skips the lookup and inserts a NULL key', async () => {
  const pool = streamAt(0)
  const result = await createEventStore(pool).append(STREAM, { type: 'Amended', payload: {} })

  assert.deepEqual({ seq: result.event.seq, appended: result.appended }, { seq: 1, appended: true })
  assert.ok(!texts(pool).some((t) => t.includes('dedupe_key = $4')))
  assert.equal(insertOf(pool).params[8], null)
})

// expectedSeq is optimistic concurrency: append only if the stream is still at
// the seq the caller decided against.

// seq is a JavaScript number once read: past 2^53 - 1 it would round, and two
// events would share a seq. Refused before anything is written.
test('append refuses a seq past Number.MAX_SAFE_INTEGER', async () => {
  const pool = streamAt(Number.MAX_SAFE_INTEGER)
  await assert.rejects(createEventStore(pool).append(STREAM, { type: 'A', payload: {} }), /ledger\/a1 has reached seq 9007199254740991, the largest a JavaScript number holds exactly/)
  assert.ok(!texts(pool).some((t) => t.includes('INSERT')))
})

test('append with the right expectedSeq appends at expectedSeq + 1', async () => {
  const pool = streamAt(7)
  const { event } = await createEventStore(pool).append(STREAM, { type: 'A', payload: {} }, { expectedSeq: 7 })
  assert.equal(event.seq, 8)
})

test('expectedSeq 0 means an empty stream', async () => {
  const { event } = await createEventStore(streamAt(0)).append(STREAM, { type: 'A', payload: {} }, { expectedSeq: 0 })
  assert.equal(event.seq, 1)
  await assert.rejects(() => createEventStore(streamAt(1)).append(STREAM, { type: 'A', payload: {} }, { expectedSeq: 0 }), WrongExpectedSeqError)
})

test('append with a stale expectedSeq throws WrongExpectedSeqError, writes nothing and rolls back', async () => {
  const pool = streamAt(8)
  await assert.rejects(() => createEventStore(pool).append(STREAM, { type: 'A', payload: {} }, { expectedSeq: 7 }), (err) => {
    assert.ok(err instanceof WrongExpectedSeqError)
    assert.equal(err.name, 'WrongExpectedSeqError')
    assert.deepEqual({ expectedSeq: err.expectedSeq, actualSeq: err.actualSeq }, { expectedSeq: 7, actualSeq: 8 })
    assert.deepEqual(err.stream, STREAM)
    assert.match(err.message, /ledger\/a1 expected the stream at seq 7, but it is at seq 8/)
    return true
  })
  assert.equal(insertOf(pool), undefined)
  assert.ok(texts(pool).includes('ROLLBACK'))
})

test('a deduped retry is answered as a retry even though its first attempt moved the stream', async () => {
  // The first attempt appended at 8 and its caller was never told; the retry
  // still says expectedSeq 7. That is the same request, not a conflict.
  const pool = streamAt(8, { deduped: row(8) })
  const result = await createEventStore(pool).append(STREAM, { type: 'A', payload: {} }, { dedupeKey: 'req-1', expectedSeq: 7 })
  assert.deepEqual({ seq: result.event.seq, appended: result.appended }, { seq: 8, appended: false })
})

test('both stores: malformed append options are refused before anything is written', async () => {
  for (const store of [createEventStore(streamAt(0)), createMockEventStore()]) {
    const append = (options) => store.append(STREAM, { type: 'A', payload: {} }, options)
    await assert.rejects(() => append('req-1'), /append options must be an object such as \{ dedupeKey, expectedSeq \}, got a string/)
    await assert.rejects(() => append({ dedupeKey: '' }), /dedupeKey must be a non-empty string/)
    await assert.rejects(() => append({ dedupeKey: 7 }), /dedupeKey must be a non-empty string/)
    for (const expectedSeq of [-1, 1.5, '3', Number.NaN]) {
      await assert.rejects(() => append({ expectedSeq }), /expectedSeq must be an integer >= 0/)
    }
    assert.equal(await store.maxSeq(STREAM), 0)
  }
})

test('both stores: an event without a type or a payload is refused before anything is written', async () => {
  // Postgres would refuse it at the INSERT; the mock must not store it.
  const pool = streamAt(0)
  for (const [name, store] of [['pg', createEventStore(pool)], ['mock', createMockEventStore()]]) {
    for (const event of [null, 'A', { payload: {} }, { type: '', payload: {} }, { type: 7, payload: {} }]) {
      await assert.rejects(store.append(STREAM, event), /an event needs a type, a non-empty string/, `${name}: ${JSON.stringify(event)}`)
    }
    await assert.rejects(store.append(STREAM, { type: 'A' }), /an event needs a payload/, name)
    await assert.rejects(store.transaction(STREAM, (tx) => tx.append({ type: 'A' })), /an event needs a payload/, `${name}: tx.append`)
    assert.equal(await store.maxSeq(STREAM), 0, name)
  }
  assert.ok(!texts(pool).some((t) => t.includes('INSERT')), 'pg: nothing reached the INSERT')
  assert.deepEqual((await createMockEventStore().append(STREAM, { type: 'A', payload: null })).event.payload, null, 'null is a JSON value')
})

test('both stores: an event carrying dedupeKey or expectedSeq is refused, since the options argument is where they work', async () => {
  // On the event they would be ignored, and a retried request would append twice.
  const pool = streamAt(0)
  for (const [name, store] of [['pg', createEventStore(pool)], ['mock', createMockEventStore()]]) {
    for (const option of ['dedupeKey', 'expectedSeq']) {
      const event = { type: 'A', payload: {}, [option]: option === 'dedupeKey' ? 'req-1' : 0 }
      const named = new RegExp(`an event may not carry ${option}: pass it in append's options`)
      await assert.rejects(store.append(STREAM, event), named, `${name}: append`)
      await assert.rejects(store.transaction(STREAM, (tx) => tx.append(event)), named, `${name}: tx.append`)
    }
    assert.equal(await store.maxSeq(STREAM), 0, name)
  }
  assert.ok(!texts(pool).some((t) => t.includes('INSERT')), 'pg: nothing reached the INSERT')
})

// The two stores used to disagree on what occurredAt means: Postgres reads a
// zone-less time in its own time zone, and words such as 'now' at the moment
// each retry lands. Only an instant is taken: a Date, or ISO 8601 with a zone.
test('both stores: occurredAt must be a Date or an ISO 8601 string with Z or an offset', async () => {
  const pool = streamAt(0)
  for (const [name, store] of [['pg', createEventStore(pool)], ['mock', createMockEventStore()]]) {
    for (const occurredAt of ['2026-10-08', '2026-10-08T15:20:00', '2026-10-08 15:20:00', 'now', 'yesterday', '2026-13-45T00:00:00Z', '2026-02-30T00:00:00Z', 1760000000000, new Date('nope')]) {
      await assert.rejects(store.append(STREAM, { type: 'A', payload: {}, occurredAt }), /occurredAt must be a Date or an ISO 8601 string with Z or an offset/, `${name}: ${occurredAt}`)
    }
    assert.equal(await store.maxSeq(STREAM), 0, name)
  }
  for (const occurredAt of [new Date('2026-10-08T13:20:00Z'), '2026-10-08T13:20:00Z', '2026-10-08T15:20:00.5+02:00', '2026-10-08T15:20+0200']) {
    assert.equal((await createMockEventStore().append(STREAM, { type: 'A', payload: {}, occurredAt })).event.occurredAt.slice(0, 16), '2026-10-08T13:20', String(occurredAt))
  }
})

// pg serialises a Date in the process's local time with a whole-minute offset,
// which shifts an instant before standard time (an LMT offset has seconds).
// A Date is bound as its UTC ISO string instead.
test('pg: an occurredAt Date is bound as its ISO string in UTC', async () => {
  const pool = streamAt(0)
  await createEventStore(pool).append(STREAM, { type: 'A', payload: {}, occurredAt: new Date('1880-01-01T00:00:00Z') })
  assert.equal(insertOf(pool).params[6], '1880-01-01T00:00:00.000Z')
})

test('both stores: occurredAt in year 0000, which Postgres refuses, is refused up front, and a long value is not echoed', async () => {
  for (const store of [createEventStore(streamAt(0)), createMockEventStore()]) {
    for (const occurredAt of ['0000-06-01T00:00:00Z', new Date('0000-06-01T00:00:00Z'), new Date(Date.UTC(10000, 0, 1))]) {
      await assert.rejects(store.append(STREAM, { type: 'A', payload: {}, occurredAt }), /occurredAt must be a Date or an ISO 8601 string/, String(occurredAt))
    }
    await assert.rejects(store.append(STREAM, { type: 'A', payload: {}, occurredAt: `secret-${'x'.repeat(500)}` }), (err) => {
      assert.ok(err.message.length < 200, err.message)
      assert.match(err.message, /got 'secret-x+…'$/)
      return true
    })
  }
})

// jsonb stores neither \u0000 nor half a surrogate pair, so Postgres refuses
// such a string where the mock would keep it. Both refuse it, naming where.
test('both stores: a payload or state string with \\u0000 or a lone surrogate is refused, naming where it is', async () => {
  const pool = streamAt(0)
  for (const [name, store] of [['pg', createEventStore(pool)], ['mock', createMockEventStore()]]) {
    await assert.rejects(store.append(STREAM, { type: 'A', payload: { note: 'a\u0000b' } }), /event payload\.note contains \\u0000, which Postgres cannot store/, name)
    await assert.rejects(store.append(STREAM, { type: 'A', payload: { list: ['ok', 'x\ud800'] } }), /event payload\.list\[1\] contains a lone surrogate/, name)
    await assert.rejects(store.append(STREAM, { type: 'A', payload: { ['k\u0000']: 1 } }), /event payload has a key that contains \\u0000/, name)
    await assert.rejects(store.saveProjection(STREAM, { s: '\udc00' }, 0), /projection state\.s contains a lone surrogate/, name)
    await assert.rejects(store.transaction(STREAM, (tx) => tx.append({ type: 'A', payload: '\u0000' })), /event payload contains \\u0000/, `${name}: tx`)
    assert.equal((await store.append(STREAM, { type: 'A', payload: { emoji: '😀' } })).appended, true, `${name}: a whole pair is fine`)
  }
  assert.equal(texts(pool).filter((t) => t.includes('INSERT')).length, 1, 'pg: only the valid append reached the INSERT')
})

// The text columns refuse \u0000 too, as a raw database error; and pg writes
// half a surrogate pair as U+FFFD, so a type would come back changed and fail
// its codec context. Every string the store writes is checked up front.
test('both stores: \\u0000 or a lone surrogate in a stream id, type, source or dedupeKey is refused, naming which', async () => {
  const pool = streamAt(0)
  for (const [name, store] of [['pg', createEventStore(pool)], ['mock', createMockEventStore()]]) {
    const cases = [
      [() => store.append({ ...STREAM, ownerId: 'a\u0000' }, { type: 'A', payload: {} }), /StreamId ownerId contains \\u0000/],
      [() => store.loadEvents({ ...STREAM, stream: 'l\ud800' }), /StreamId stream contains a lone surrogate/],
      [() => store.maxSeq({ ...STREAM, subjectId: 's\u0000' }), /StreamId subjectId contains \\u0000/],
      [() => store.append(STREAM, { type: 'A\ud800', payload: {} }), /event type contains a lone surrogate/],
      [() => store.append(STREAM, { type: 'A', payload: {}, source: 'w\u0000' }), /event source contains \\u0000/],
      [() => store.append(STREAM, { type: 'A', payload: {} }, { dedupeKey: 'k\u0000' }), /dedupeKey contains \\u0000/],
      [() => store.transaction(STREAM, (tx) => tx.eventByDedupeKey('k\udc00')), /dedupeKey contains a lone surrogate/],
    ]
    for (const [call, pattern] of cases) await assert.rejects(call(), pattern, `${name}: ${pattern}`)
  }
  assert.ok(!texts(pool).some((t) => t.includes('INSERT')), 'pg: nothing reached the INSERT')
})

test('both stores: saveProjection refuses an undefined state or a throughSeq that is not an integer >= 0', async () => {
  const pool = streamAt(0)
  for (const [name, store] of [['pg', createEventStore(pool)], ['mock', createMockEventStore()]]) {
    for (const throughSeq of [undefined, null, -1, 1.5, '3', Number.NaN]) {
      await assert.rejects(store.saveProjection(STREAM, {}, throughSeq), /throughSeq must be an integer >= 0/, `${name}: ${throughSeq}`)
    }
    await assert.rejects(store.transaction(STREAM, (tx) => tx.saveProjection({})), /throughSeq must be an integer >= 0/, `${name}: tx`)
    await assert.rejects(store.saveProjection(STREAM, undefined, 1), /a projection needs a state/, name)
    await store.saveProjection(STREAM, {}, 0)
  }
  assert.equal(pool.calls.filter((c) => c.text.includes('INSERT INTO "domain_projections"')).length, 1, 'pg: only the valid save')
})

test('both stores: one uuid owner spelled two ways is one stream, under one lock and one codec context', async () => {
  const id = '0b6c2f1e-8a4d-4c1b-9f3e-2d7a5c8e1b40'
  const lower = { ownerId: id, stream: 'ledger', subjectId: 'main' }
  const upper = { ...lower, ownerId: id.toUpperCase() }
  const seen = []
  const codec = { encode: (v, c) => { seen.push(c.ownerId); return v }, decode: (s, c) => { seen.push(c.ownerId); return s } }
  const pool = makePool((text, params) => {
    if (text.includes('MAX(seq)')) return { rows: [{ max: '0' }], rowCount: 1 }
    if (text.includes('INSERT INTO "domain_events"')) return { rows: [{ ...row(1), owner_id: params[0] }], rowCount: 1 }
    return { rows: [], rowCount: 0 }
  })
  await createEventStore(pool, { codec }).append(upper, { type: 'A', payload: {} })
  const locks = pool.calls.filter((c) => c.text.includes('pg_advisory_xact_lock')).map((c) => c.params[0])
  assert.deepEqual(locks, [`${id}:ledger:main`], 'the lowercase key a lowercase writer takes')
  assert.equal(insertOf(pool).params[0], id)
  const mock = createMockEventStore({ codec })
  await mock.append(upper, { type: 'A', payload: {} })
  const { event } = await mock.append(lower, { type: 'A', payload: {} })
  assert.equal(event.seq, 2, 'mock: one stream')
  assert.equal(event.ownerId, id)
  assert.deepEqual([...new Set(seen)], [id], 'the codec always sees one spelling')
  assert.deepEqual((await mock.transaction(upper, async (tx) => tx.stream)).ownerId, id)
})

// pg read a null afterSeq as 0 and NaN as nothing at all, the mock the other way
// round. Both take undefined or an integer >= 0, and refuse the rest.
test('both stores: loadEvents takes afterSeq as undefined or an integer >= 0', async () => {
  const pool = streamAt(0)
  for (const [name, store] of [['pg', createEventStore(pool)], ['mock', createMockEventStore()]]) {
    for (const afterSeq of [null, Number.NaN, -1, 1.5, '2']) {
      await assert.rejects(store.loadEvents(STREAM, afterSeq), /afterSeq must be an integer >= 0, or left out for every event/, `${name}: ${afterSeq}`)
      await assert.rejects(store.transaction(STREAM, (tx) => tx.loadEvents(afterSeq)), /afterSeq must be an integer >= 0/, `${name}: tx ${afterSeq}`)
    }
    for (const afterSeq of [undefined, 0, 3]) assert.deepEqual(await store.loadEvents(STREAM, afterSeq), [], `${name}: ${afterSeq}`)
  }
})

test('both stores: a StreamId missing a part throws on every method, naming what is missing', async () => {
  // Unchecked, a missing part reaches the SQL as NULL and matches nothing, or
  // the lock key as "undefined" and serializes unrelated streams.
  const stores = [createEventStore(makePool(() => ({ rows: [], rowCount: 0 }))), createMockEventStore()]
  for (const store of stores) {
    const ownerless = { stream: 'ledger', subjectId: 'a1' }
    await assert.rejects(() => store.append(ownerless, { type: 'A', payload: {} }), /StreamId is missing ownerId/)
    await assert.rejects(() => store.loadEvents(ownerless), /missing ownerId/)
    await assert.rejects(() => store.loadProjection(ownerless), /missing ownerId/)
    await assert.rejects(() => store.saveProjection(ownerless, {}, 1), /missing ownerId/)
    await assert.rejects(() => store.maxSeq(ownerless), /missing ownerId/)
    await assert.rejects(() => store.compact(ownerless, 1, 1), /missing ownerId/)
    await assert.rejects(() => store.maxSeq({ ownerId: 'a1', stream: '' }), /missing stream, subjectId/)
  }
})
