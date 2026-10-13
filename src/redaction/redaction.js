// redact(stream, seqs, { reason, tombstone }) — the one sanctioned exception to
// append-only: it overwrites the payload of events already in the log with a
// tombstone, and keeps everything else (id, seq, type, occurredAt, source), so
// ordering and history stay intact. It is for forgetting one fact; forgetting a
// whole subject is crypto-shredding its key.
//
// The input is checked up front, as append's options are: a seq list or a
// reason that is silently misread would redact the wrong thing, or nothing.

import { describeStream } from '../stream-id.js'
import { isPlainObject } from '../plain-object.js'

class UnknownSeqError extends Error {
  constructor(stream, seqs) {
    super(`redact: ${describeStream(stream)} has no event at seq ${seqs.join(', ')}`)
    this.name = 'UnknownSeqError'
    this.stream = { ownerId: stream.ownerId, stream: stream.stream, subjectId: stream.subjectId }
    this.seqs = seqs
  }
}

function readRedaction(seqs, options) {
  if (!Array.isArray(seqs) || seqs.length === 0 || !seqs.every((seq) => Number.isSafeInteger(seq) && seq >= 1)) {
    throw new TypeError(`redact needs a non-empty array of seqs, each an integer >= 1, got ${JSON.stringify(seqs)}`)
  }
  if (!isPlainObject(options) || typeof options.reason !== 'string' || options.reason === '') {
    throw new TypeError('redact needs options with a non-empty reason, such as { reason: \'erasure request 42\' }')
  }
  const { reason, tombstone } = options
  if (tombstone !== undefined && !isPlainObject(tombstone)) {
    throw new TypeError('a redact tombstone must be a plain object')
  }
  return { seqs: [...new Set(seqs)].sort((a, b) => a - b), reason, tombstone }
}

// The payload that replaces a redacted one. The caller's tombstone may keep
// fields a fold still needs; $redacted, reason and at are always the store's,
// so a tombstone is always recognisable and always says why and when.
function tombstoneOf({ reason, tombstone }, at) {
  return { ...tombstone, $redacted: true, reason, at }
}

// Read side: an event whose decoded payload is a tombstone carries
// redacted: true. Other events are returned as they are, without the field.
function markRedacted(event) {
  return isPlainObject(event.payload) && event.payload.$redacted === true ? { ...event, redacted: true } : event
}

// Write side: append refuses a payload that would read as a tombstone, so an
// application that spreads request JSON into a payload cannot forge one — a
// forged tombstone is a real event its reducer would skip. Checked before
// encode.
function checkNotTombstone(payload) {
  if (isPlainObject(payload) && Object.hasOwn(payload, '$redacted')) {
    throw new TypeError('an event payload may not carry $redacted: $redacted is reserved for the tombstones redact writes')
  }
}

// Checked under the stream's lock, before anything is written.
function checkSeqsFound(stream, wanted, found) {
  const present = new Set(found)
  const missing = wanted.filter((seq) => !present.has(seq))
  if (missing.length > 0) throw new UnknownSeqError(stream, missing)
}

export { UnknownSeqError, readRedaction, tombstoneOf, markRedacted, checkSeqsFound, checkNotTombstone }
