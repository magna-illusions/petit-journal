// defineProjection(store, definition) — checked at definition, so a mistake
// fails where the projection is declared, not at its first write.
function checkDefinition(store, { initial, reduce, keep, redacted = 'skip' } = {}) {
  if (typeof store?.transaction !== 'function' || typeof store?.loadProjection !== 'function') {
    throw new TypeError('defineProjection needs a store from createEventStore or createMockEventStore')
  }
  if (typeof reduce !== 'function') {
    throw new TypeError('defineProjection needs reduce to be a function (state, event) => state')
  }
  if (initial === undefined) {
    throw new TypeError('defineProjection needs an initial state, any JSON value')
  }
  if (keep !== undefined && !(Number.isSafeInteger(keep) && keep >= 0)) {
    throw new TypeError(`keep must be an integer >= 0 (how many of the newest events compaction keeps), got ${keep}`)
  }
  if (redacted !== 'skip' && redacted !== 'fold') {
    throw new TypeError(`redacted must be 'skip' (the default) or 'fold', got ${JSON.stringify(redacted)}`)
  }
}

// An async reducer would fold the next event onto a promise, and one that
// forgets to return onto undefined: either saves a wrong state.
function checkReduced(next, event) {
  if (next === undefined || typeof next?.then === 'function') {
    const what = next === undefined ? 'undefined' : 'a promise'
    throw new TypeError(`reduce must return the next state, synchronously: it returned ${what} for ${event.type} at seq ${event.seq}`)
  }
  return next
}

export { checkDefinition, checkReduced }
