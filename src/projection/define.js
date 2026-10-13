// defineProjection(store, { initial, reduce, keep, redacted }) — the cycle every caller
// otherwise writes by hand: load the projection, append, fold, save it at the
// last seq, compact. Built only on the store's public methods, so it runs on
// either store. Every fold that starts from `initial` starts from a fresh copy
// of it, and redacted events are skipped before `reduce` sees them, so neither
// a mutating reducer nor a tombstone can corrupt a state. With redacted:
// 'fold', reduce gets them instead, for a state that must show what was
// forgotten.

import { checkDefinition, checkReduced } from './check.js'
import { readDecision, toEvents } from './decision.js'
import { compactedPlanError, gapError, rebuildError, movedError, keptPlanError } from './errors.js'

// The seq a saved projection has folded through, 0 for none.
const foldedThrough = (projection) => projection?.throughSeq ?? 0

// The first seq a plan names that is past `before`, if any: a plan redacts only
// events the stream held before it.
const firstSeqAfter = (redactions, before) =>
  redactions.flatMap(({ seqs }) => (Array.isArray(seqs) ? seqs : [])).find((seq) => seq > before)

function defineProjection(store, definition) {
  checkDefinition(store, definition)
  const { initial, reduce, keep, redacted = 'skip' } = definition
  const reduceOne = (state, event) => checkReduced(reduce(state, event), event)
  const step = redacted === 'fold' ? reduceOne : (state, event) => (event.redacted ? state : reduceOne(state, event))
  const fold = (state, events) => events.reduce(step, state)

  // The state as of the newest event, and that event's seq: the saved
  // projection, caught up with any event appended past it. null when the
  // events do not continue from where the projection stopped.
  function catchUp(projection, events) {
    const from = foldedThrough(projection)
    if (events.length > 0 && events[0].seq !== from + 1) return null
    return { state: fold(projection ? projection.state : structuredClone(initial), events), seq: events.at(-1)?.seq ?? from }
  }

  async function load(tx) {
    const projection = await tx.loadProjection()
    const events = await tx.loadEvents(foldedThrough(projection))
    const loaded = catchUp(projection, events)
    if (loaded === null) throw gapError(tx.stream, events[0].seq, foldedThrough(projection))
    return loaded
  }

  // Options apply to the batch as a whole: expectedSeq is the stream's seq
  // before it, and a dedupeKey goes on its first event. The batch commits as
  // one, so a hit there means the whole write, any redactions included, is
  // already in, and nothing more happens (duplicate: true). With nothing to
  // append, no option applies (duplicate: false).
  async function write(tx, { state, seq: loadedSeq }, { events, redactions }, options) {
    const nothing = { state, events: [], duplicate: false, redacted: [] }
    if (redactions.length > 0 && keep !== undefined) throw keptPlanError()
    const before = redactions.length > 0 ? await tx.maxSeq() : 0
    const appended = []
    if (events.length > 0) {
      const first = await tx.append(events[0], options)
      if (!first.appended) return { ...nothing, duplicate: true }
      if (first.event.seq !== loadedSeq + 1) throw movedError(tx.stream, loadedSeq, first.event.seq)
      appended.push(first.event)
      for (const event of events.slice(1)) appended.push((await tx.append(event)).event)
    }
    if (redactions.length > 0) return redactAndRefold(tx, appended, redactions, before)
    if (appended.length === 0) return nothing
    const next = fold(state, appended)
    await saveAndCompact(tx, next, appended.at(-1).seq)
    return { state: next, events: appended, duplicate: false, redacted: [] }
  }

  async function saveAndCompact(tx, state, seq) {
    await tx.saveProjection(state, seq)
    if (keep !== undefined) await tx.compact(seq, keep)
  }

  // Runs after the append, so a retried plan meets its dedupe hit first, even
  // once compaction has removed what it redacted. A throw from here rolls the
  // append back. A plan redacts only events that were there before it; then
  // the whole log is re-folded from initial, so the saved state never holds a
  // redacted fact, and a log compaction has cut cannot be.
  async function redactAndRefold(tx, appended, redactions, before) {
    const ahead = firstSeqAfter(redactions, before)
    if (ahead !== undefined) {
      throw new TypeError(`a plan redacts only earlier events: seq ${ahead} is past seq ${before}, where the stream was before this update`)
    }
    // Checked before redacting, so a seq compaction removed is reported as
    // compaction, not as an unknown seq. The log is read again after: the
    // re-fold needs the tombstones.
    const [first] = await tx.loadEvents()
    if (first.seq !== 1) throw compactedPlanError(tx.stream, first.seq)
    const redacted = []
    for (const { seqs, reason, tombstone } of redactions) redacted.push(...(await tx.redact(seqs, { reason, tombstone })))
    const log = await tx.loadEvents()
    const next = fold(structuredClone(initial), log)
    await saveAndCompact(tx, next, log.at(-1).seq)
    return { state: next, events: appended, duplicate: false, redacted }
  }

  async function append(stream, eventOrEvents, options) {
    const events = toEvents(eventOrEvents, 'append needs an event or an array of events')
    return store.transaction(stream, async (tx) => write(tx, await load(tx), { events, redactions: [] }, options))
  }

  // decide runs under the stream's lock, so every other write to the stream
  // waits for it: keep it quick. It gets a copy, so it cannot change the state,
  // and ctx.loadEvents to read the log inside the same transaction.
  async function update(stream, decide, options) {
    if (typeof decide !== 'function') throw new TypeError('update needs decide to be a function (state, ctx) => events or a plan')
    return store.transaction(stream, async (tx) => {
      const loaded = await load(tx)
      // A retry is answered before it is planned again: decide would see the
      // state its first attempt moved, and might refuse what it already did.
      if (options?.dedupeKey != null && await tx.eventByDedupeKey(options.dedupeKey)) {
        return { state: loaded.state, events: [], duplicate: true, redacted: [] }
      }
      const ctx = { loadEvents: (afterSeq) => tx.loadEvents(afterSeq) }
      return write(tx, loaded, readDecision(await decide(structuredClone(loaded.state), ctx)), options)
    })
  }

  // A first read takes no lock, so readers do not queue behind writers. A
  // compaction landing between its two reads can open a gap; then the read is
  // made again inside a transaction, under the lock, where nothing lands in
  // between: a gap there is a log compacted past its projection.
  async function state(stream) {
    const projection = await store.loadProjection(stream)
    const folded = catchUp(projection, await store.loadEvents(stream, foldedThrough(projection)))
    if (folded !== null) return folded.state
    return store.transaction(stream, async (tx) => (await load(tx)).state)
  }

  // Re-folds the whole log from initial, as after a redact or a reducer
  // change. Refuses a compacted log: the state would lack its oldest events.
  async function rebuild(stream) {
    return store.transaction(stream, async (tx) => {
      const events = await tx.loadEvents()
      if (events.length === 0) return structuredClone(initial)
      if (events[0].seq !== 1) throw rebuildError(tx.stream, events[0].seq)
      const rebuilt = fold(structuredClone(initial), events)
      await tx.saveProjection(rebuilt, events.at(-1).seq)
      return rebuilt
    })
  }

  return { append, update, state, rebuild }
}

export { defineProjection }
