// saveProjection(stream, state, throughSeq) — checked up front. Postgres
// refuses a NULL state or through_seq, so the mock must too, and throughSeq is
// what compaction trusts: a value that is not a seq is refused, not stored.
function checkProjection(state, throughSeq) {
  if (state === undefined) {
    throw new TypeError('a projection needs a state, any JSON value')
  }
  if (!(Number.isSafeInteger(throughSeq) && throughSeq >= 0)) {
    throw new TypeError(`throughSeq must be an integer >= 0 (the highest seq the state embodies), got ${throughSeq}`)
  }
}

export { checkProjection }
