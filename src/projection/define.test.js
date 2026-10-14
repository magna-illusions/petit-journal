import { test } from 'node:test'
import assert from 'node:assert/strict'
import { createEventStore } from '../pg/store.js'
import { createMockEventStore } from '../mock/mock.js'
import { WrongExpectedSeqError } from '../append.js'
import { defineProjection } from './define.js'
import { CompactedLogError } from '../compaction/compaction.js'

// defineProjection is the load → fold → append → save → compact cycle every
// caller otherwise writes by hand. It is built on the public store methods
// only, so each behaviour is held on both stores: the pg store over a fake
// pool that keeps rows (and deletes them, so compaction is observable), and
// the mock.

const STREAM = { ownerId: 'a1', stream: 'ledger', subjectId: 'a1' }
const deposit = (amount) => ({ type: 'Deposited', payload: { amount } })
const ledger = { initial: { balance: 0 }, reduce: (state, event) => ({ balance: state.balance + event.payload.amount }) }

// Rows in two arrays, matched on the full stream tuple, returned the way pg
// returns them (jsonb as text). BEGIN snapshots and ROLLBACK restores.
function tablePool() {
  let events = []
  let projections = []
  let snapshot
  const inStream = (p) => (r) => r.owner_id === p[0] && r.stream === p[1] && r.subject_id === p[2]
  const query = async (text, p) => {
    if (text === 'BEGIN') snapshot = [[...events], [...projections]]
    if (text === 'ROLLBACK') [events, projections] = snapshot
    if (text.includes('MAX(seq)')) return { rows: [{ max: String(Math.max(0, ...events.filter(inStream(p)).map((r) => Number(r.seq)))) }] }
    if (text.includes('dedupe_key = $4')) return { rows: events.filter(inStream(p)).filter((r) => r.dedupe_key === p[3]) }
    if (text.startsWith('INSERT INTO "domain_events"')) {
      const row = { id: `e${p[3]}`, owner_id: p[0], stream: p[1], subject_id: p[2], seq: String(p[3]), type: p[4], payload: p[5], occurred_at: new Date(0), source: p[7], dedupe_key: p[8] }
      events.push(row)
      return { rows: [row] }
    }
    if (text.includes('seq = ANY')) return { rows: events.filter(inStream(p)).filter((r) => p[3].includes(Number(r.seq))) }
    if (text.startsWith('UPDATE "domain_events"')) {
      // Replaced, not mutated, so a ROLLBACK's snapshot still holds the original.
      events = events.map((r) => (inStream(p)(r) && Number(r.seq) === p[3] ? { ...r, payload: p[4] } : r))
      return { rows: [], rowCount: 1 }
    }
    if (text.startsWith('DELETE FROM "domain_events"')) {
      const before = events.length
      events = events.filter((r) => !(inStream(p)(r) && Number(r.seq) <= p[3]))
      return { rows: [], rowCount: before - events.length }
    }
    if (text.includes('FROM "domain_events"')) return { rows: events.filter(inStream(p)).filter((r) => Number(r.seq) > p[3]) }
    if (text.startsWith('INSERT INTO "domain_projections"')) {
      projections = projections.filter((r) => !inStream(p)(r))
      projections.push({ owner_id: p[0], stream: p[1], subject_id: p[2], state: p[3], through_seq: String(p[4]), updated_at: new Date(0) })
      return { rows: [], rowCount: 1 }
    }
    if (text.includes('FROM "domain_projections"')) return { rows: projections.filter(inStream(p)) }
    return { rows: [], rowCount: 0 }
  }
  return { query, connect: async () => ({ query, release() {} }) }
}

const bothStores = () => [['pg', createEventStore(tablePool())], ['mock', createMockEventStore()]]
const seqs = async (store, stream = STREAM) => (await store.loadEvents(stream)).map((e) => e.seq)

test('append folds one event onto the state, saves it at its seq and returns what it appended', async () => {
  for (const [name, store] of bothStores()) {
    const result = await defineProjection(store, ledger).append(STREAM, deposit(5))
    assert.deepEqual(result.state, { balance: 5 }, name)
    assert.deepEqual(Object.keys(result).sort(), ['duplicate', 'events', 'redacted', 'state'], name)
    assert.equal(result.duplicate, false, name)
    assert.deepEqual(result.events.map((e) => [e.seq, e.type]), [[1, 'Deposited']], name)
    const saved = await store.loadProjection(STREAM)
    assert.deepEqual([saved.state, saved.throughSeq], [{ balance: 5 }, 1], name)
  }
})

test('append folds a batch in order in one transaction and saves at the last seq', async () => {
  for (const [name, store] of bothStores()) {
    const ledgerOf = defineProjection(store, ledger)
    await ledgerOf.append(STREAM, deposit(1))
    const result = await ledgerOf.append(STREAM, [deposit(2), deposit(3), deposit(4)])
    assert.deepEqual(result.state, { balance: 10 }, name)
    assert.deepEqual(result.events.map((e) => e.seq), [2, 3, 4], name)
    assert.equal((await store.loadProjection(STREAM)).throughSeq, 4, name)
  }
})

test('append of an empty batch writes nothing and returns the current state', async () => {
  for (const [name, store] of bothStores()) {
    const ledgerOf = defineProjection(store, ledger)
    assert.deepEqual(await ledgerOf.append(STREAM, []), { state: { balance: 0 }, events: [], duplicate: false, redacted: [] }, name)
    assert.equal(await store.loadProjection(STREAM), null, name)
    await ledgerOf.append(STREAM, deposit(5))
    assert.deepEqual(await ledgerOf.append(STREAM, []), { state: { balance: 5 }, events: [], duplicate: false, redacted: [] }, name)
    assert.deepEqual(await seqs(store), [1], name)
  }
})

test('an empty batch with dedupeKey or expectedSeq is a no-op: nothing written, the key left unused, duplicate: false', async () => {
  for (const [name, store] of bothStores()) {
    const ledgerOf = defineProjection(store, ledger)
    const nothing = { state: { balance: 0 }, events: [], duplicate: false, redacted: [] }
    assert.deepEqual(await ledgerOf.append(STREAM, [], { dedupeKey: 'k' }), nothing, name)
    assert.deepEqual(await ledgerOf.append(STREAM, [], { expectedSeq: 7 }), nothing, `${name}: a stale expectedSeq is not checked`)
    assert.deepEqual(await ledgerOf.update(STREAM, () => [], { dedupeKey: 'k' }), nothing, `${name}: update`)
    assert.deepEqual(await seqs(store), [], name)
    assert.equal((await ledgerOf.append(STREAM, deposit(5), { dedupeKey: 'k' })).events.length, 1, `${name}: the key is still free`)
  }
})

test('a dedupe hit appends and folds nothing, for one event and for a batch', async () => {
  for (const [name, store] of bothStores()) {
    const ledgerOf = defineProjection(store, ledger)
    await ledgerOf.append(STREAM, deposit(5), { dedupeKey: 'one' })
    const again = await ledgerOf.append(STREAM, deposit(5), { dedupeKey: 'one' })
    assert.deepEqual(again, { state: { balance: 5 }, events: [], duplicate: true, redacted: [] }, name)
    await ledgerOf.append(STREAM, [deposit(1), deposit(2)], { dedupeKey: 'batch' })
    const batchAgain = await ledgerOf.append(STREAM, [deposit(1), deposit(2)], { dedupeKey: 'batch' })
    assert.deepEqual(batchAgain, { state: { balance: 8 }, events: [], duplicate: true, redacted: [] }, name)
    assert.deepEqual(await seqs(store), [1, 2, 3], name)
    assert.deepEqual((await store.loadProjection(STREAM)).state, { balance: 8 }, name)
  }
})

test('expectedSeq is the stream\'s seq before the batch; a conflict writes nothing', async () => {
  for (const [name, store] of bothStores()) {
    const ledgerOf = defineProjection(store, ledger)
    await ledgerOf.append(STREAM, [deposit(1), deposit(2)], { expectedSeq: 0 })
    await assert.rejects(ledgerOf.append(STREAM, [deposit(3), deposit(4)], { expectedSeq: 1 }), (err) => {
      assert.ok(err instanceof WrongExpectedSeqError, name)
      assert.deepEqual([err.expectedSeq, err.actualSeq], [1, 2], name)
      return true
    })
    assert.deepEqual(await seqs(store), [1, 2], name)
    assert.deepEqual((await store.loadProjection(STREAM)).state, { balance: 3 }, name)
    assert.deepEqual((await ledgerOf.append(STREAM, deposit(3), { expectedSeq: 2 })).state, { balance: 6 }, name)
  }
})

test('keep compacts after each save: unset keeps every event, 0 and 1 keep the newest, 2 the newest two', async () => {
  for (const [keep, left] of [[undefined, [1, 2, 3, 4, 5]], [0, [5]], [1, [5]], [2, [4, 5]]]) {
    for (const [name, store] of bothStores()) {
      const ledgerOf = defineProjection(store, { ...ledger, keep })
      await ledgerOf.append(STREAM, [deposit(1), deposit(2), deposit(3), deposit(4), deposit(5)])
      assert.deepEqual(await seqs(store), left, `${name}: keep ${keep}`)
      assert.deepEqual(await ledgerOf.state(STREAM), { balance: 15 }, `${name}: keep ${keep}`)
    }
  }
})

// The store's own compact trusts the throughSeq it is given; past the saved
// projection's, it would delete events the projection never folded and leave
// the stream unable to catch up. With a projection row, the cutoff stops there.
test('store: compact never deletes past the saved projection\'s throughSeq, so a projection can always catch up', async () => {
  for (const [name, store] of bothStores()) {
    for (let n = 1; n <= 5; n++) await store.append(STREAM, deposit(n))
    await store.saveProjection(STREAM, { balance: 3 }, 2)
    assert.equal(await store.compact(STREAM, 5, 0), 2, `${name}: clamped to the projection at seq 2`)
    assert.deepEqual(await seqs(store), [3, 4, 5], name)
    assert.deepEqual(await defineProjection(store, ledger).state(STREAM), { balance: 15 }, `${name}: it still catches up`)
    assert.equal(await store.transaction(STREAM, (tx) => tx.compact(5, 0)), 0, `${name}: tx.compact too`)
  }
})

// What jsonb receives is checked, not what the caller handed in: a sealed
// value may hold any string, and a codec's own output may not.
test('store: the jsonb check reads the encoded value, so a sealed \\u0000 is fine and a codec emitting one is not', async () => {
  const base64 = { encode: (v) => Buffer.from(JSON.stringify(v)).toString('base64'), decode: (s) => JSON.parse(Buffer.from(s, 'base64').toString()) }
  const leaky = { encode: (v) => ({ sealed: `${JSON.stringify(v)}\u0000` }), decode: (s) => JSON.parse(s.sealed.slice(0, -1)) }
  for (const [name, make] of [['pg', (codec) => createEventStore(tablePool(), { codec })], ['mock', (codec) => createMockEventStore({ codec })]]) {
    const sealed = make(base64)
    await sealed.append(STREAM, { type: 'A', payload: { note: 'a\u0000b' } })
    assert.deepEqual((await sealed.loadEvents(STREAM))[0].payload, { note: 'a\u0000b' }, `${name}: sealed, it round-trips`)
    await sealed.saveProjection(STREAM, { s: '\udc00' }, 1)
    const leak = make(leaky)
    await assert.rejects(leak.append(STREAM, { type: 'A', payload: {} }), /event payload\.sealed contains \\u0000/, `${name}: the codec's output`)
    await assert.rejects(leak.saveProjection(STREAM, {}, 0), /projection state\.sealed contains \\u0000/, name)
    const plain = make(undefined)
    await plain.append(STREAM, deposit(1))
    await assert.rejects(plain.redact(STREAM, [1], { reason: 'r\u0000' }), /event payload\.reason contains \\u0000/, `${name}: a tombstone as stored`)
  }
})

test('state is initial for a stream never written, and the folded state after', async () => {
  for (const [name, store] of bothStores()) {
    const ledgerOf = defineProjection(store, ledger)
    assert.deepEqual(await ledgerOf.state(STREAM), { balance: 0 }, name)
    await ledgerOf.append(STREAM, [deposit(2), deposit(3)])
    assert.deepEqual(await ledgerOf.state(STREAM), { balance: 5 }, name)
  }
})

// Lock-free reads can race a compaction and see a gap no matter how often they
// retry under load; the last attempt reads under the lock, where no write can
// land between the two reads.
test('state reads under the lock when its lock-free reads keep racing, and refuses only a real gap', async () => {
  for (const [name, store] of bothStores()) {
    const ledgerOf = defineProjection(store, ledger)
    await ledgerOf.append(STREAM, [deposit(1), deposit(2), deposit(3)])
    await store.append(STREAM, deposit(4))
    // Outside a transaction this store always shows a projection at seq 1 and a log from seq 3: a gap.
    const racing = {
      ...store,
      loadProjection: async (s) => ({ ...(await store.loadProjection(s)), throughSeq: 1 }),
      loadEvents: async (s) => (await store.loadEvents(s)).filter((e) => e.seq >= 3),
    }
    assert.deepEqual(await defineProjection(racing, ledger).state(STREAM), { balance: 10 }, name)
  }
})

test('state reads without the lock: it resolves while another transaction holds the stream', { timeout: 2000 }, async () => {
  for (const [name, store] of bothStores()) {
    const ledgerOf = defineProjection(store, ledger)
    await ledgerOf.append(STREAM, deposit(5))
    let release
    const holding = store.transaction(STREAM, () => new Promise((resolve) => { release = resolve }))
    await new Promise((resolve) => setTimeout(resolve, 10))
    assert.deepEqual(await ledgerOf.state(STREAM), { balance: 5 }, name)
    release()
    await holding
  }
})

test('state falls back to a locked read when its lock-free read meets a gap once', async () => {
  for (const [name, store] of bothStores()) {
    const ledgerOf = defineProjection(store, ledger)
    await ledgerOf.append(STREAM, [deposit(1), deposit(2)])
    await store.append(STREAM, deposit(3))
    await store.append(STREAM, deposit(4))
    let reads = 0
    // The first read outside a transaction misses seq 3, as one racing a compaction would.
    const once = { ...store, loadEvents: async (s, after) => (await store.loadEvents(s, after)).filter((e) => reads++ > 0 || e.seq !== 3) }
    assert.deepEqual(await defineProjection(once, ledger).state(STREAM), { balance: 10 }, name)
  }
})

// state's locked re-read takes the stream's lock; inside a transaction on that
// stream it would wait for that transaction. It says so, rather than blaming a
// write.
test('state inside a transaction on its stream, needing its locked re-read, says why it cannot', async () => {
  for (const [name, store] of bothStores()) {
    const ledgerOf = defineProjection(store, ledger)
    await ledgerOf.append(STREAM, [deposit(1), deposit(2)])
    await store.append(STREAM, deposit(3))
    const gappy = { ...store, loadEvents: async (s, after) => (await store.loadEvents(s, after)).filter((e) => e.seq !== 3) }
    await store.append(STREAM, deposit(4))
    await assert.rejects(store.transaction(STREAM, () => defineProjection(gappy, ledger).state(STREAM)),
      /would wait for that transaction's own lock: it takes the stream's lock \(a write, a nested transaction, or a projection's locked re-read\)/, name)
  }
})

test('a projection behind its log is caught up before folding, by state and by append', async () => {
  for (const [name, store] of bothStores()) {
    const ledgerOf = defineProjection(store, ledger)
    await ledgerOf.append(STREAM, deposit(1))
    await store.append(STREAM, deposit(10)) // a low-level append the projection has not folded
    assert.deepEqual(await ledgerOf.state(STREAM), { balance: 11 }, name)
    assert.deepEqual((await ledgerOf.append(STREAM, deposit(100))).state, { balance: 111 }, name)
    assert.equal((await store.loadProjection(STREAM)).throughSeq, 3, name)
  }
})

test('decide gets a copy: mutating it changes neither the result nor the saved state', async () => {
  for (const [name, store] of bothStores()) {
    const ledgerOf = defineProjection(store, ledger)
    await ledgerOf.append(STREAM, deposit(5))
    const result = await ledgerOf.update(STREAM, (state) => { state.balance = 1000; return [deposit(1)] })
    assert.deepEqual(result.state, { balance: 6 }, name)
    assert.deepEqual(await ledgerOf.state(STREAM), { balance: 6 }, name)
  }
})

test('initial is never mutated: each stream starts from a fresh copy, even with a reducer that mutates', async () => {
  for (const [name, store] of bothStores()) {
    const initial = { seen: [] }
    const pushing = defineProjection(store, { initial, reduce: (state, event) => { state.seen.push(event.payload.amount); return state } })
    await pushing.append(STREAM, deposit(1))
    await pushing.append({ ...STREAM, subjectId: 'b2' }, deposit(2))
    assert.deepEqual(initial, { seen: [] }, name)
    assert.deepEqual(await pushing.state({ ...STREAM, subjectId: 'c3' }), { seen: [] }, name)
    assert.deepEqual(await pushing.state({ ...STREAM, subjectId: 'b2' }), { seen: [2] }, name)
  }
})

test('update appends what decide returns, folded and saved; an empty result writes nothing', async () => {
  for (const [name, store] of bothStores()) {
    const ledgerOf = defineProjection(store, ledger)
    const topUp = (state) => (state.balance < 10 ? [deposit(10 - state.balance)] : [])
    assert.deepEqual((await ledgerOf.update(STREAM, topUp)).state, { balance: 10 }, name)
    assert.deepEqual(await ledgerOf.update(STREAM, topUp), { state: { balance: 10 }, events: [], duplicate: false, redacted: [] }, name)
    assert.deepEqual((await ledgerOf.update(STREAM, async () => [deposit(1)])).state, { balance: 11 }, `${name}: async, one event`)
    assert.deepEqual(await seqs(store), [1, 2], name)
  }
})

// A note-taking projection. Its forget plan appends a Forgot event and redacts
// the notes that contain a given word, in one transaction.
const note = (text) => ({ type: 'Noted', payload: { text } })
const notes = {
  initial: {},
  reduce: (state, event) => {
    if (event.type === 'Noted') return { ...state, [event.seq]: event.payload.text }
    return Object.fromEntries(Object.entries(state).filter(([seq]) => !event.payload.seqs.includes(Number(seq))))
  },
}
const forget = (word) => async (state, ctx) => {
  const holding = (await ctx.loadEvents()).filter((e) => e.type === 'Noted' && !e.redacted && e.payload.text.includes(word)).map((e) => e.seq)
  return {
    append: [{ type: 'Forgot', payload: { seqs: holding } }],
    redact: holding.map((seq) => ({ seqs: [seq], reason: `forget ${word}`, tombstone: { note: seq } })),
  }
}

test('update can append and redact earlier events in one transaction, choosing them through ctx.loadEvents', async () => {
  for (const [name, store] of bothStores()) {
    const notesOf = defineProjection(store, notes)
    await notesOf.append(STREAM, [note('a secret'), note('another secret'), note('public')])
    const result = await notesOf.update(STREAM, forget('secret'))
    assert.deepEqual(result.state, { 3: 'public' }, name)
    assert.deepEqual([result.events.map((e) => e.seq), result.duplicate, result.redacted], [[4], false, [1, 2]], name)
    const events = await store.loadEvents(STREAM)
    assert.deepEqual(events.map((e) => [e.seq, e.type, e.redacted ?? false]), [[1, 'Noted', true], [2, 'Noted', true], [3, 'Noted', false], [4, 'Forgot', false]], name)
    assert.deepEqual(events.slice(0, 2).map((e) => [e.payload.note, e.payload.reason]), [[1, 'forget secret'], [2, 'forget secret']], `${name}: a tombstone per group`)
    assert.ok(!JSON.stringify(events).includes('a secret') && !JSON.stringify(events).includes('another secret'), `${name}: no redacted text is left`)
    assert.deepEqual((await store.loadProjection(STREAM)).state, { 3: 'public' }, name)
  }
})

test('an update that fails at a redaction writes nothing: neither its append nor its other redactions', async () => {
  for (const [name, store] of bothStores()) {
    const notesOf = defineProjection(store, notes)
    await notesOf.append(STREAM, note('a secret'))
    // The second group is malformed, so it fails after the append and the first redaction.
    const failing = () => ({ append: [{ type: 'Forgot', payload: { seqs: [1] } }], redact: [{ seqs: [1], reason: 'r' }, { seqs: [1], reason: '' }] })
    await assert.rejects(notesOf.update(STREAM, failing), /redact needs options with a non-empty reason/, name)
    assert.deepEqual((await store.loadEvents(STREAM)).map((e) => [e.seq, e.redacted ?? false]), [[1, false]], name)
    assert.deepEqual((await store.loadProjection(STREAM)).state, { 1: 'a secret' }, name)
  }
})

// A plan that redacts re-folds the whole log from initial after its
// redactions, so the saved state never holds a redacted fact.
test('a plan that only redacts re-folds the projection without the fact: the atomic forget', async () => {
  for (const [name, store] of bothStores()) {
    const notesOf = defineProjection(store, notes)
    await notesOf.append(STREAM, [note('a secret'), note('public')])
    const result = await notesOf.update(STREAM, () => ({ redact: [{ seqs: [1], reason: 'r' }] }))
    assert.deepEqual(result, { state: { 2: 'public' }, events: [], duplicate: false, redacted: [1] }, name)
    assert.equal((await store.loadEvents(STREAM))[0].redacted, true, name)
    const saved = await store.loadProjection(STREAM)
    assert.deepEqual([saved.state, saved.throughSeq], [{ 2: 'public' }, 2], name)
  }
})

test('the re-fold after a redaction honours redacted: \'fold\', keeping the entry as forgotten', async () => {
  for (const [name, store] of bothStores()) {
    const kept = defineProjection(store, {
      initial: {},
      redacted: 'fold',
      reduce: (state, event) => ({ ...state, [event.seq]: event.redacted ? `(forgotten: ${event.payload.reason})` : event.payload.text }),
    })
    await kept.append(STREAM, [note('a secret'), note('public')])
    const result = await kept.update(STREAM, () => ({ redact: [{ seqs: [1], reason: 'erasure request 42' }] }))
    assert.deepEqual(result.state, { 1: '(forgotten: erasure request 42)', 2: 'public' }, name)
    assert.deepEqual((await store.loadProjection(STREAM)).state, result.state, name)
  }
})

test('a plan may redact only earlier events: one past the stream\'s seq before the update is refused, nothing written', async () => {
  for (const [name, store] of bothStores()) {
    const notesOf = defineProjection(store, notes)
    await notesOf.append(STREAM, note('a secret'))
    const ahead = () => ({ append: [note('another secret')], redact: [{ seqs: [2], reason: 'r' }] })
    await assert.rejects(notesOf.update(STREAM, ahead), /a plan redacts only earlier events: seq 2 is past seq 1, where the stream was before this update/, name)
    assert.deepEqual(await seqs(store), [1], name)
    await assert.rejects(defineProjection(createMockEventStore(), notes).update(STREAM, () => ({ redact: [{ seqs: [1], reason: 'r' }] })), /seq 1 is past seq 0/, `${name}: an empty stream`)
  }
})

test('a plan that redacts is refused on a log compacted by hand, which cannot be re-folded without the fact', async () => {
  for (const [name, store] of bothStores()) {
    const notesOf = defineProjection(store, notes)
    await notesOf.append(STREAM, [note('a secret'), note('another secret'), note('public')])
    await store.compact(STREAM, 3, 1)
    await assert.rejects(notesOf.update(STREAM, () => ({ redact: [{ seqs: [3], reason: 'r' }] })),
      /cannot redact in an update on ledger\/a1: compaction removed seq 1 to 2.*store\.transaction/, name)
    assert.equal((await store.loadEvents(STREAM))[0].redacted, undefined, `${name}: nothing redacted`)
  }
})

// A retried update must not be planned again: its decide may now see a state
// the first attempt moved, and refuse what it already did.
test('an update whose dedupeKey is already stored returns duplicate without calling decide', async () => {
  for (const [name, store] of bothStores()) {
    const ledgerOf = defineProjection(store, ledger)
    let decided = 0
    const once = (state) => { decided++; return state.balance === 0 ? [deposit(5)] : [] }
    await ledgerOf.update(STREAM, once, { dedupeKey: 'req-1' })
    const retry = await ledgerOf.update(STREAM, once, { dedupeKey: 'req-1' })
    assert.deepEqual(retry, { state: { balance: 5 }, events: [], duplicate: true, redacted: [] }, name)
    assert.equal(decided, 1, `${name}: decide ran once`)
  }
})

test('tx.eventByDedupeKey reads the event a key wrote, or null, on both stores', async () => {
  for (const [name, store] of bothStores()) {
    await store.append(STREAM, deposit(5), { dedupeKey: 'req-1' })
    const [hit, miss] = await store.transaction(STREAM, async (tx) => [await tx.eventByDedupeKey('req-1'), await tx.eventByDedupeKey('req-2')])
    assert.deepEqual([hit.seq, hit.payload, miss], [1, { amount: 5 }, null], name)
    await assert.rejects(store.transaction(STREAM, (tx) => tx.eventByDedupeKey('')), /dedupeKey must be a non-empty string/, name)
  }
})

test('a dedupe hit skips the plan\'s redactions too: the first attempt made them', async () => {
  for (const [name, store] of bothStores()) {
    const notesOf = defineProjection(store, notes)
    await notesOf.append(STREAM, [note('a secret'), note('kept')])
    await notesOf.update(STREAM, forget('secret'), { dedupeKey: 'forget-1' })
    const retry = await notesOf.update(STREAM, () => ({ append: [{ type: 'Forgot', payload: { seqs: [2] } }], redact: [{ seqs: [2], reason: 'r' }] }), { dedupeKey: 'forget-1' })
    assert.deepEqual([retry.duplicate, retry.redacted], [true, []], name)
    assert.equal((await store.loadEvents(STREAM))[1].redacted, undefined, `${name}: seq 2 untouched`)
  }
})

test('a retried plan is answered as a dedupe hit even after the log it redacted was compacted', async () => {
  for (const [name, store] of bothStores()) {
    const notesOf = defineProjection(store, notes)
    await notesOf.append(STREAM, note('a secret'))
    const plan = () => ({ append: [{ type: 'Forgot', payload: { seqs: [1] } }], redact: [{ seqs: [1], reason: 'r' }] })
    const first = await notesOf.update(STREAM, plan, { dedupeKey: 'f1' })
    await store.compact(STREAM, 2, 1)
    assert.deepEqual([first.events.map((e) => e.seq), first.redacted, await seqs(store)], [[2], [1], [2]], name)
    const retry = await notesOf.update(STREAM, plan, { dedupeKey: 'f1' })
    assert.deepEqual(retry, { state: {}, events: [], duplicate: true, redacted: [] }, name)
  }
})

// keep compacts after every save, so a projection with keep would refuse its
// first redacting plan only once compaction had run: in production, not in a
// short test. It is refused from the first call instead.
test('a redacting plan is refused on a projection with keep, from the first call', async () => {
  for (const [name, store] of bothStores()) {
    const notesOf = defineProjection(store, { ...notes, keep: 100 })
    await notesOf.append(STREAM, note('a secret'))
    const plan = () => ({ append: [{ type: 'Forgot', payload: { seqs: [1] } }], redact: [{ seqs: [1], reason: 'r' }] })
    await assert.rejects(notesOf.update(STREAM, plan), /a plan that redacts cannot run on a projection with keep/, name)
    assert.deepEqual((await store.loadEvents(STREAM)).map((e) => [e.seq, e.redacted ?? false]), [[1, false]], name)
    assert.deepEqual((await notesOf.update(STREAM, () => ({ append: [note('more')] }))).events.length, 1, `${name}: a plan that only appends is fine`)
  }
})

test('a plan with an unknown key, or a redact that is not an array of groups, is refused and writes nothing', async () => {
  for (const [name, store] of bothStores()) {
    const notesOf = defineProjection(store, notes)
    await notesOf.append(STREAM, note('a secret'))
    await assert.rejects(notesOf.update(STREAM, () => ({ append: [], redacts: [] })), /unknown key redacts: decide returns an array of events, or a plan \{ append, redact \}/, name)
    await assert.rejects(notesOf.update(STREAM, () => ({ redact: { seqs: [1], reason: 'r' } })), /a plan's redact must be an array of \{ seqs, reason, tombstone\? \}/, name)
    await assert.rejects(notesOf.update(STREAM, () => ({ redact: [7] })), /a plan's redact must be an array of \{ seqs, reason, tombstone\? \}/, name)
    for (const group of [{ seqs: 'x', reason: 'r' }, { seqs: [1] }, { seqs: [], reason: 'r' }]) {
      await assert.rejects(defineProjection(createMockEventStore(), notes).update(STREAM, () => ({ redact: [group] })),
        /redact needs (a non-empty array of seqs|options with a non-empty reason)/, `${name}: ${JSON.stringify(group)} on an empty stream`)
    }
    assert.deepEqual((await store.loadEvents(STREAM)).map((e) => e.redacted ?? false), [false], name)
  }
})

// An array is events and an object is a plan: nothing is guessed from keys.
test('update refuses a decide that returns neither an array of events nor a plan, and writes nothing', async () => {
  for (const [name, store] of bothStores()) {
    const ledgerOf = defineProjection(store, ledger)
    for (const result of [undefined, null, 'Deposited', 7]) {
      await assert.rejects(ledgerOf.update(STREAM, () => result), /decide must return an array of events \(\[\] appends nothing\), or a plan \{ append, redact \}/, `${name}: ${result}`)
    }
    await assert.rejects(ledgerOf.update(STREAM, () => deposit(1)), /decide returned one event: wrap a single event in an array, as in \[event\]/, `${name}: one bare event`)
    await assert.rejects(ledgerOf.update(STREAM, () => ({ append: deposit(1) })), /a plan's append must be an array of events/, `${name}: a plan's bare event`)
    await assert.rejects(ledgerOf.update(STREAM, 'not a function'), /update needs decide to be a function/, name)
    assert.deepEqual(await seqs(store), [], name)
  }
})

test('rebuild re-folds the whole log from initial and saves it; on an empty stream it writes nothing', async () => {
  for (const [name, store] of bothStores()) {
    const ledgerOf = defineProjection(store, ledger)
    assert.deepEqual(await ledgerOf.rebuild(STREAM), { balance: 0 }, name)
    assert.equal(await store.loadProjection(STREAM), null, name)
    await ledgerOf.append(STREAM, [deposit(1), deposit(2)])
    await store.saveProjection(STREAM, { balance: 999 }, 2) // a projection gone wrong
    assert.deepEqual(await ledgerOf.rebuild(STREAM), { balance: 3 }, name)
    assert.deepEqual((await store.loadProjection(STREAM)).state, { balance: 3 }, name)
  }
})

test('rebuild refuses a compacted log rather than save a state missing its oldest events', async () => {
  for (const [name, store] of bothStores()) {
    const ledgerOf = defineProjection(store, { ...ledger, keep: 1 })
    await ledgerOf.append(STREAM, [deposit(1), deposit(2), deposit(3)])
    await assert.rejects(ledgerOf.rebuild(STREAM), /cannot rebuild ledger\/a1 from initial: compaction removed seq 1 to 2/, name)
    assert.deepEqual((await store.loadProjection(STREAM)).state, { balance: 6 }, name)
  }
})

test('a redacted event is skipped before reduce, in rebuild and in catching up', async () => {
  for (const [name, store] of bothStores()) {
    const seen = []
    const ledgerOf = defineProjection(store, { ...ledger, reduce: (state, event) => { seen.push(event.seq); return ledger.reduce(state, event) } })
    await ledgerOf.append(STREAM, [deposit(1), deposit(2), deposit(3)])
    await store.redact(STREAM, [2], { reason: 'erasure request 42' })
    seen.length = 0
    assert.deepEqual(await ledgerOf.rebuild(STREAM), { balance: 4 }, name)
    assert.deepEqual(seen, [1, 3], `${name}: reduce never saw the tombstone`)
    await store.append(STREAM, deposit(4))
    await store.redact(STREAM, [4], { reason: 'erasure request 43' })
    assert.deepEqual(await ledgerOf.state(STREAM), { balance: 4 }, `${name}: catching up skips it too`)
  }
})

test('redacted: \'fold\' hands reduce the tombstoned event, in rebuild and in catching up', async () => {
  for (const [name, store] of bothStores()) {
    const seen = []
    const kept = defineProjection(store, {
      initial: [],
      reduce: (entries, event) => { seen.push(event.seq); return [...entries, event.redacted ? { seq: event.seq, forgotten: event.payload.reason } : { seq: event.seq }] },
      redacted: 'fold',
    })
    await kept.append(STREAM, [deposit(1), deposit(2)])
    await store.redact(STREAM, [2], { reason: 'forget 2' })
    seen.length = 0
    assert.deepEqual(await kept.rebuild(STREAM), [{ seq: 1 }, { seq: 2, forgotten: 'forget 2' }], name)
    assert.deepEqual(seen, [1, 2], `${name}: reduce saw the tombstone`)
    await store.append(STREAM, deposit(3))
    await store.redact(STREAM, [3], { reason: 'forget 3' })
    assert.deepEqual((await kept.state(STREAM)).at(-1), { seq: 3, forgotten: 'forget 3' }, `${name}: catching up folds it too`)
  }
})

// Defence in depth: should anything let a write land on a stream the fold has
// not seen (a writer under another lock, say), refuse rather than save a state
// that skips events.
test('a write that lands past the state it folded onto is refused and rolled back', async () => {
  const store = createMockEventStore()
  await store.append(STREAM, deposit(1))
  await store.append(STREAM, deposit(2))
  const blind = { ...store, transaction: (stream, work) => store.transaction(stream, (tx) => work({ ...tx, loadEvents: async () => [] })) }
  const ledgerOf = defineProjection(blind, ledger)
  await assert.rejects(ledgerOf.append(STREAM, deposit(3)), /ledger\/a1 moved under this write: the state it folded is at seq 0, but the append landed at seq 3/)
  assert.deepEqual(await seqs(store), [1, 2])
  assert.equal(await store.loadProjection(STREAM), null)
})

// An async reducer folds the second event onto a promise; one that forgets to
// return folds onto undefined. Either would save a wrong state, so both throw.
test('a reduce that returns undefined or a promise is refused, and nothing is written', async () => {
  for (const [name, store] of bothStores()) {
    for (const [what, reduce] of [['undefined', () => undefined], ['a promise', async (s) => s]]) {
      const bad = defineProjection(store, { initial: {}, reduce })
      const pattern = new RegExp(`reduce must return the next state, synchronously: it returned ${what} for Deposited at seq 1`)
      await assert.rejects(bad.append(STREAM, [deposit(1), deposit(2)]), pattern, `${name}: ${what}`)
    }
    assert.deepEqual(await seqs(store), [], name)
  }
})

test('every refusal of a compacted log is a CompactedLogError naming the stream and the oldest seq left', async () => {
  for (const [name, store] of bothStores()) {
    for (let n = 1; n <= 3; n++) await store.append(STREAM, deposit(n))
    await store.compact(STREAM, 3, 1) // by hand, with no projection saved: seq 3 is all that is left
    const ledgerOf = defineProjection(store, ledger)
    const compacted = (err) => {
      assert.ok(err instanceof CompactedLogError, `${name}: ${err.message}`)
      assert.deepEqual([err.name, err.stream, err.firstSeq], ['CompactedLogError', STREAM, 3], name)
      return true
    }
    await assert.rejects(ledgerOf.state(STREAM), compacted)
    await assert.rejects(ledgerOf.rebuild(STREAM), compacted)
    await store.saveProjection(STREAM, { balance: 6 }, 3)
    await assert.rejects(ledgerOf.update(STREAM, () => ({ redact: [{ seqs: [3], reason: 'r' }] })), compacted)
    await assert.rejects(ledgerOf.update(STREAM, () => ({ redact: [{ seqs: [1], reason: 'r' }] })), compacted, `${name}: a seq compaction removed`)
  }
})

test('a definition without reduce, with a bad keep or redacted, or without initial is refused', () => {
  const store = createMockEventStore()
  assert.throws(() => defineProjection(store, { initial: {} }), /needs reduce to be a function/)
  for (const keep of [-1, 1.5, '2', null]) {
    assert.throws(() => defineProjection(store, { ...ledger, keep }), /keep must be an integer >= 0/, String(keep))
  }
  assert.throws(() => defineProjection(store, { reduce: ledger.reduce }), /needs an initial state/)
  assert.throws(() => defineProjection({}, ledger), /needs a store from createEventStore or createMockEventStore/)
  for (const redacted of ['keep', true, null]) {
    assert.throws(() => defineProjection(store, { ...ledger, redacted }), /redacted must be 'skip' \(the default\) or 'fold'/, String(redacted))
  }
})

test('a log compacted past its projection is refused by state and append, not folded from the wrong start', async () => {
  for (const [name, store] of bothStores()) {
    for (const amount of [1, 2, 3]) await store.append(STREAM, deposit(amount))
    await store.compact(STREAM, 3, 1) // low-level: seq 1 and 2 gone, and no projection ever saved
    const ledgerOf = defineProjection(store, ledger)
    await assert.rejects(ledgerOf.state(STREAM), /the log of ledger\/a1 was compacted past its projection/, name)
    await assert.rejects(ledgerOf.append(STREAM, deposit(4)), /compacted past its projection/, name)
    assert.deepEqual(await seqs(store), [3], name)
  }
})
