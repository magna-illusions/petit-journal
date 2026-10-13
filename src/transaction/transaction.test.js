import { test } from 'node:test'
import assert from 'node:assert/strict'
import { createEventStore } from '../pg/store.js'
import { createMockEventStore } from '../mock/mock.js'
import { WrongExpectedSeqError } from '../append.js'

// store.transaction(stream, work) is what lets a caller load, decide, append,
// save and run its own SQL as one unit under the stream's lock. These lock the
// guarantees a caller builds on: one client, one lock, all-or-nothing, and no
// operation escaping the transaction after it ended.

const STREAM = { ownerId: 'a1', stream: 'ledger', subjectId: 'a1' }
const LOCK = 'SELECT pg_advisory_xact_lock(hashtextextended($1, 0))'

// A fake pool whose stream is at seq 0 and whose client records every query.
function fakePool() {
  const calls = []
  let released = 0
  const onQuery = async (text, params, via) => {
    calls.push({ text, params, via })
    if (text.includes('MAX(seq)')) return { rows: [{ max: '0' }], rowCount: 1 }
    if (text.includes('INSERT INTO "domain_events"')) {
      return { rows: [{ id: 'e1', owner_id: 'a1', stream: 'ledger', subject_id: 'a1', seq: String(params[3]), type: params[4], payload: params[5], occurred_at: new Date(0), source: null }], rowCount: 1 }
    }
    return { rows: [], rowCount: 0 }
  }
  return {
    calls,
    get released() { return released },
    query: (text, params) => onQuery(text, params, 'pool'),
    connect: async () => ({ query: (text, params) => onQuery(text, params, 'client'), release: () => { released++ } }),
  }
}

const texts = (pool) => pool.calls.map((c) => c.text)

test('pg: a transaction runs every operation and the caller\'s SQL on one client, between one lock and one COMMIT', async () => {
  const pool = fakePool()
  const result = await createEventStore(pool).transaction(STREAM, async (tx) => {
    await tx.loadProjection()
    const { event } = await tx.append({ type: 'A', payload: {} })
    await tx.saveProjection({ n: 1 }, event.seq)
    await tx.query('UPDATE side_table SET x = $1', [event.seq])
    await tx.compact(event.seq, 100)
    return 'done'
  })

  assert.equal(result, 'done', 'the work function\'s value is returned')
  assert.ok(pool.calls.every((c) => c.via === 'client'), 'nothing went to the pool')
  assert.equal(texts(pool)[0], 'BEGIN')
  assert.equal(texts(pool)[1], LOCK)
  assert.deepEqual(pool.calls[1].params, ['a1:ledger:a1'])
  assert.equal(texts(pool).at(-1), 'COMMIT')
  assert.equal(texts(pool).filter((t) => t === 'BEGIN' || t === LOCK).length, 2, 'append inside does not lock again')
  assert.ok(texts(pool).indexOf('UPDATE side_table SET x = $1') < texts(pool).indexOf('COMMIT'))
  assert.equal(pool.released, 1)
})

test('pg: a throw from the work function rolls everything back and rethrows', async () => {
  const pool = fakePool()
  await assert.rejects(createEventStore(pool).transaction(STREAM, async (tx) => {
    await tx.append({ type: 'A', payload: {} })
    throw new Error('decided against it')
  }), /decided against it/)
  assert.ok(texts(pool).includes('ROLLBACK'))
  assert.ok(!texts(pool).includes('COMMIT'))
  assert.equal(pool.released, 1)
})

test('pg: a WrongExpectedSeqError writes nothing, so the caller may catch it and still commit', async () => {
  const pool = fakePool()
  const outcome = await createEventStore(pool).transaction(STREAM, async (tx) => {
    try {
      await tx.append({ type: 'A', payload: {} }, { expectedSeq: 5 })
      return 'appended'
    } catch (err) {
      if (err instanceof WrongExpectedSeqError) return `stream moved to ${err.actualSeq}`
      throw err
    }
  })
  assert.equal(outcome, 'stream moved to 0')
  assert.ok(texts(pool).includes('COMMIT'))
  assert.ok(!texts(pool).some((t) => t.includes('INSERT')))
})

test('pg: a call that outlives the work function fails instead of running after COMMIT', async () => {
  const pool = fakePool()
  let escaped
  await createEventStore(pool).transaction(STREAM, async (tx) => { escaped = tx })
  await assert.rejects(escaped.append({ type: 'A', payload: {} }), /transaction on ledger\/a1 has already ended/)
  await assert.rejects(escaped.query('SELECT 1'), /already ended/)
  assert.equal(texts(pool).at(-1), 'COMMIT', 'nothing reached the client after COMMIT')
})

test('pg: a forgotten await fails rather than appending outside the transaction', async () => {
  const pool = fakePool()
  let dangling
  await createEventStore(pool).transaction(STREAM, async (tx) => { dangling = tx.append({ type: 'A', payload: {} }) })
  await assert.rejects(dangling, /already ended/)
  assert.ok(!texts(pool).some((t) => t.includes('INSERT')))
})

test('pg: a work function that swallowed a SQL error does not pass for committed', async () => {
  // Postgres answers COMMIT on an aborted transaction with a ROLLBACK, not an
  // error: without this check the caller would be told its writes were saved.
  const pool = fakePool()
  const connect = pool.connect
  pool.connect = async () => {
    const client = await connect()
    const query = client.query
    return { ...client, query: async (text, params) => (text === 'COMMIT' ? (await query(text, params), { command: 'ROLLBACK', rows: [], rowCount: null }) : query(text, params)) }
  }
  await assert.rejects(createEventStore(pool).transaction(STREAM, async () => 'looked fine'), /rolled back instead of committing/)
})

test('pg: a connection whose ROLLBACK failed is released as broken', async () => {
  const released = []
  const pool = {
    query: async () => ({ rows: [], rowCount: 0 }),
    connect: async () => ({
      query: async (text) => { if (text === 'ROLLBACK') throw new Error('connection lost'); return { rows: [], rowCount: 0 } },
      release: (err) => released.push(err),
    }),
  }
  await assert.rejects(createEventStore(pool).transaction(STREAM, async () => { throw new Error('work failed') }), /work failed/)
  assert.match(String(released[0]), /connection lost/, 'the pool is told to discard it')
})

test('pg: standalone saveProjection and compact take the stream lock too', async () => {
  const pool = fakePool()
  const store = createEventStore(pool)
  await store.saveProjection(STREAM, { n: 1 }, 1)
  await store.compact(STREAM, 1, 100)
  assert.equal(texts(pool).filter((t) => t === LOCK).length, 2)
  assert.ok(pool.calls.every((c) => c.via === 'client'))
})

test('mock: a standalone write waits for a transaction rather than being undone by its rollback', async () => {
  const store = createMockEventStore()
  let release
  const failing = store.transaction(STREAM, async () => { await new Promise((resolve) => { release = resolve }); throw new Error('nope') })
  const saved = store.saveProjection(STREAM, { kept: true }, 0)
  await tick()
  release()
  await assert.rejects(failing, /nope/)
  await saved
  assert.deepEqual((await store.loadProjection(STREAM)).state, { kept: true })
})

test('both stores: tx is bound to its stream and hands it back', async () => {
  for (const store of [createEventStore(fakePool()), createMockEventStore()]) {
    const seen = await store.transaction(STREAM, async (tx) => {
      const { event } = await tx.append({ type: 'A', payload: { n: 1 } })
      return { stream: tx.stream, event, max: await tx.maxSeq() }
    })
    assert.deepEqual(seen.stream, STREAM)
    assert.equal(seen.event.seq, 1)
  }
})

test('both stores: a bad stream or work is refused before a connection is taken', async () => {
  const pool = fakePool()
  for (const store of [createEventStore(pool), createMockEventStore()]) {
    await assert.rejects(store.transaction({ stream: 'ledger', subjectId: 'a1' }, async () => {}), /missing ownerId/)
    await assert.rejects(store.transaction(STREAM), /needs work to be a function/)
  }
  assert.equal(pool.calls.length, 0)
})

// The mock gives the same guarantees in memory, so a no-DB test of a
// read-modify-write sees the same outcome production does.

const tick = () => new Promise((resolve) => setTimeout(resolve, 5))

async function loadFoldSave(tx, label) {
  const projection = await tx.loadProjection()
  await tick() // the window in which an unserialized writer would interleave
  const { event } = await tx.append({ type: label, payload: {} })
  await tx.saveProjection([...(projection?.state ?? []), label], event.seq)
}

test('mock: transactions on one stream run one at a time, so no write is lost', async () => {
  const store = createMockEventStore()
  await Promise.all(['A', 'B', 'C'].map((label) => store.transaction(STREAM, (tx) => loadFoldSave(tx, label))))
  const projection = await store.loadProjection(STREAM)
  assert.deepEqual(projection.state, ['A', 'B', 'C'])
  assert.equal(projection.throughSeq, 3)
})

test('mock: a standalone append waits for a transaction on its stream, as it waits for the pg lock', async () => {
  const store = createMockEventStore()
  const running = store.transaction(STREAM, (tx) => loadFoldSave(tx, 'A'))
  await store.append(STREAM, { type: 'B', payload: {} })
  await running
  assert.deepEqual((await store.loadEvents(STREAM)).map((e) => e.type), ['A', 'B'])
})

test('mock: transactions on different streams do not wait for each other', async () => {
  const store = createMockEventStore()
  let release
  const blocked = store.transaction(STREAM, () => new Promise((resolve) => { release = resolve }))
  await store.transaction({ ...STREAM, subjectId: 'b2' }, (tx) => tx.append({ type: 'A', payload: {} }))
  release()
  await blocked
})

test('mock: a throw restores the stream — events, projection and dedupe keys', async () => {
  const store = createMockEventStore()
  await store.append(STREAM, { type: 'A', payload: {} })
  await assert.rejects(store.transaction(STREAM, async (tx) => {
    await tx.append({ type: 'B', payload: {} }, { dedupeKey: 'req-1' })
    await tx.saveProjection({ n: 2 }, 2)
    throw new Error('decided against it')
  }), /decided against it/)

  assert.deepEqual((await store.loadEvents(STREAM)).map((e) => e.type), ['A'])
  assert.equal(await store.loadProjection(STREAM), null)
  const retry = await store.append(STREAM, { type: 'B', payload: {} }, { dedupeKey: 'req-1' })
  assert.equal(retry.appended, true, 'the rolled-back append did not claim its dedupe key')
})

test('mock: a call that outlives the work function fails, and tx.query says there is no SQL', async () => {
  const store = createMockEventStore()
  let escaped
  await assert.rejects(store.transaction(STREAM, async (tx) => { escaped = tx; await tx.query('SELECT 1') }), /mock event store runs no SQL/)
  await assert.rejects(escaped.append({ type: 'A', payload: {} }), /already ended/)
  assert.equal(await store.maxSeq(STREAM), 0)
})

// pg emits 'error' on a client whose backend dies (terminated, or an
// idle-in-transaction timeout). With no listener that is an uncaught exception
// that takes the process down; the store listens while it holds the client.
test('pg: a backend that dies mid-transaction rejects the call and discards the connection, without crashing', async () => {
  const { EventEmitter } = await import('node:events')
  const terminated = new Error('terminating connection due to administrator command')
  const client = new EventEmitter()
  let dead = false
  let releasedWith = 'not released'
  client.query = async (text) => {
    if (dead) throw new Error('Client was closed and is not queryable')
    return { rows: [], rowCount: 0, command: text }
  }
  client.release = (err) => { releasedWith = err }
  const pool = { query: client.query, connect: async () => client }
  const work = async (tx) => {
    await tx.query('SELECT 1')
    setImmediate(() => { dead = true; client.emit('error', terminated) }) // between statements, as a timeout fires
    await new Promise((resolve) => setTimeout(resolve, 5))
    await tx.query('SELECT 2')
  }
  await assert.rejects(createEventStore(pool).transaction(STREAM, work), (err) => {
    assert.match(err.message, /lost the connection during the transaction on ledger\/a1: terminating connection due to administrator command/)
    assert.equal(err.cause, terminated)
    return true
  })
  assert.equal(releasedWith, terminated, 'released as broken, so the pool discards it')
  assert.equal(client.listenerCount('error'), 0, 'the listener goes with the checkout')
})

// A timeout that ends the session while work awaits something that never
// settles must still end the call, with the reason, and free the connection.
test('pg: a backend that dies while work is stuck rejects the call at once, with the cause, and releases the client', async () => {
  const { EventEmitter } = await import('node:events')
  const timedOut = new Error('terminating connection due to idle-in-transaction timeout')
  const client = new EventEmitter()
  let releasedWith = 'not released'
  client.query = async (text) => ({ rows: [], rowCount: 0, command: text })
  client.release = (err) => { releasedWith = err }
  const pool = { query: client.query, connect: async () => client }
  setTimeout(() => client.emit('error', timedOut), 20)
  const outcome = await Promise.race([
    createEventStore(pool).transaction(STREAM, () => new Promise(() => {})).then(() => 'resolved', (err) => err),
    new Promise((resolve) => setTimeout(() => resolve('still pending'), 500)),
  ])
  assert.ok(outcome instanceof Error, String(outcome))
  assert.match(outcome.message, /lost the connection during the transaction on ledger\/a1: terminating connection due to idle-in-transaction timeout/)
  assert.equal(outcome.cause, timedOut)
  assert.equal(releasedWith, timedOut)
})

// A store write to the stream a transaction holds waits for that transaction's
// lock, which waits for the write: a deadlock. It is refused at once instead.
test('both stores: a store write to the same stream inside its transaction throws at once instead of waiting forever', async () => {
  const hang = new Promise((resolve) => setTimeout(() => resolve('hung'), 500))
  for (const store of [createEventStore(fakePool()), createMockEventStore()]) {
    const inside = (call) => Promise.race([store.transaction(STREAM, () => call()), hang])
    for (const call of [
      () => store.append(STREAM, { type: 'A', payload: {} }),
      () => store.saveProjection(STREAM, {}, 0),
      () => store.compact(STREAM, 1, 1),
      () => store.redact(STREAM, [1], { reason: 'r' }),
      () => store.transaction(STREAM, async () => {}),
    ]) {
      await assert.rejects(inside(call), /inside a transaction on ledger\/a1 would wait for that transaction's own lock: it takes the stream's lock/)
    }
    const other = { ...STREAM, subjectId: 'b2' }
    assert.equal((await store.transaction(STREAM, () => store.append(other, { type: 'A', payload: {} }))).appended, true, 'another stream is fine')
  }
})

// The guard is for writes while the transaction holds its lock, not for work it
// merely scheduled: async work started inside `work` keeps its context after
// the COMMIT, and must then write freely.
test('both stores: the same-stream guard ends with its transaction', async () => {
  for (const [name, store] of [['pg', createEventStore(fakePool())], ['mock', createMockEventStore()]]) {
    let later
    await store.transaction(STREAM, async () => {
      later = new Promise((resolve) => setTimeout(() => resolve(store.append(STREAM, { type: 'A', payload: {} })), 10))
    })
    assert.equal((await later).appended, true, `${name}: a timer set inside work writes after the COMMIT`)
    const after = await store.transaction(STREAM, async () => () => store.append(STREAM, { type: 'A', payload: {} }))
    assert.equal((await after()).appended, true, `${name}: a write made after the transaction returned`)
    const other = { ...STREAM, subjectId: 'b2' }
    await store.transaction(STREAM, () => store.transaction(other, async (tx) => tx.append({ type: 'A', payload: {} })))
    await assert.rejects(store.transaction(STREAM, () => store.transaction(other, () => store.append(STREAM, { type: 'A', payload: {} }))),
      /inside a transaction on ledger\/a1 would wait/, `${name}: still refused while the outer one holds its lock`)
  }
})

// The lock lives in the database, not in a store object: a second store on the
// same pool takes the same advisory lock, and so would wait just the same.
test('pg: a second store writing the same stream inside the first one\'s transaction is refused too', async () => {
  const pool = fakePool()
  const [first, second] = [createEventStore(pool), createEventStore(pool, { tables: { events: 'other_events', projections: 'other_projections' } })]
  await assert.rejects(first.transaction(STREAM, () => second.append(STREAM, { type: 'A', payload: {} })),
    /inside a transaction on ledger\/a1 would wait for that transaction's own lock/)
})

// Without the guard this deadlocks on the mock (and on Postgres), so it runs
// under a timeout: a broken guard fails the test instead of hanging the run.
test('both stores: a write to the outer stream from a nested transaction on another is refused', { timeout: 2000 }, async () => {
  for (const store of [createEventStore(fakePool()), createMockEventStore()]) {
    const other = { ...STREAM, subjectId: 'b2' }
    await assert.rejects(store.transaction(STREAM, () => store.transaction(other, () => store.append(STREAM, { type: 'A', payload: {} }))),
      /a store call inside a transaction on ledger\/a1 would wait for that transaction's own lock/)
  }
})
