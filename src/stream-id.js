import { checkStorableText } from './storable-text.js'

// A stream is addressed by the tuple (ownerId, stream, subjectId). Every call
// checks it, because a missing part would otherwise reach the SQL as NULL and
// match nothing, or reach the lock key as the string "undefined" and serialize
// unrelated streams.
const PARTS = ['ownerId', 'stream', 'subjectId']

// The standard uuid form, 8-4-4-4-12 hex digits, with or without braces.
const UUID = /^\{?([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\}?$/i

// A uuid owner in a UUID column reaches the rows as one value whatever its case
// or braces, but the lock key is text: two spellings would take two locks for
// one stream, and writers through each would fold onto the same state. So an
// owner in the standard form is written lowercase and without braces, which
// leaves a lowercase id, and its lock key, exactly as it was. Anything else is
// an ordinary TEXT id and is kept as given: 32 bare hex digits (an md5, say)
// are not taken for a uuid.
function canonicalOwner(ownerId) {
  const match = typeof ownerId === 'string' && ownerId.startsWith('{') === ownerId.endsWith('}') ? UUID.exec(ownerId) : null
  return match ? match[1].toLowerCase() : ownerId
}

// The stream every operation then uses: checked, and a copy with the owner in
// its one spelling.
function readStream(stream) {
  const missing = PARTS.filter((part) => stream?.[part] === undefined || stream?.[part] === null || stream?.[part] === '')
  if (missing.length > 0) {
    throw new TypeError(`StreamId is missing ${missing.join(', ')}: a stream is addressed by { ownerId, stream, subjectId }`)
  }
  for (const part of PARTS) checkStorableText(stream[part], `StreamId ${part}`)
  return { ownerId: canonicalOwner(stream.ownerId), stream: stream.stream, subjectId: stream.subjectId }
}

// An owner on its own, for eraseOwner: checked as a StreamId's part is, and in
// the same one spelling, or it would miss the rows the writes made.
function readOwner(ownerId) {
  if (ownerId === undefined || ownerId === null || ownerId === '') {
    throw new TypeError('eraseOwner needs an ownerId')
  }
  checkStorableText(ownerId, 'ownerId')
  return canonicalOwner(ownerId)
}

// The per-stream advisory lock key, in Postgres and in the mock. Kept
// byte-identical across versions so writers on different releases of this
// package still take one lock for one stream. Parts that contain ':' can join
// to one key ('ledger:eur' + 'x' and 'ledger' + 'eur:x'): such streams share a
// lock and wait for each other, but their rows never mix. The key names no
// table, so one stream tuple in two stores on different tables shares a lock.
function lockKey(stream) {
  return `${stream.ownerId}:${stream.stream}:${stream.subjectId}`
}

// How a stream is named in messages: its kind and its subject. The owner is
// left out, since it often names a person or a tenant.
const describeStream = ({ stream, subjectId }) => `${stream}/${subjectId}`

// loadEvents' afterSeq: left out for every event, or a seq. Anything else is
// refused, since Postgres and the mock would each read it their own way.
function readAfterSeq(afterSeq) {
  if (afterSeq === undefined) return 0
  if (Number.isSafeInteger(afterSeq) && afterSeq >= 0) return afterSeq
  throw new TypeError(`afterSeq must be an integer >= 0, or left out for every event, got ${afterSeq}`)
}

export { readStream, readOwner, lockKey, describeStream, readAfterSeq }
