import { describeStream } from './stream-id.js'
import { eventContext } from './codec/codec.js'
import { checkNotTombstone } from './redaction/redaction.js'
import { checkStorableText } from './storable-text.js'

// append(stream, event, { dedupeKey, expectedSeq }) — checked up front, because
// a malformed option fails open: a dedupeKey that is silently ignored appends a
// retry twice, an expectedSeq that is silently ignored loses the guard it was
// passed for.

class WrongExpectedSeqError extends Error {
  constructor(stream, expectedSeq, actualSeq) {
    super(`append to ${describeStream(stream)} expected the stream at seq ${expectedSeq}, but it is at seq ${actualSeq}`)
    this.name = 'WrongExpectedSeqError'
    this.stream = { ownerId: stream.ownerId, stream: stream.stream, subjectId: stream.subjectId }
    this.expectedSeq = expectedSeq
    this.actualSeq = actualSeq
  }
}

function checkDedupeKey(dedupeKey) {
  if (typeof dedupeKey !== 'string' || dedupeKey === '') throw new TypeError('dedupeKey must be a non-empty string')
  checkStorableText(dedupeKey, 'dedupeKey')
}

function readAppendOptions(options) {
  if (options === undefined || options === null) return {}
  if (typeof options !== 'object') {
    throw new TypeError(`append options must be an object such as { dedupeKey, expectedSeq }, got a ${typeof options}`)
  }
  const { dedupeKey, expectedSeq } = options
  if (dedupeKey != null) checkDedupeKey(dedupeKey)
  if (expectedSeq != null && !(Number.isSafeInteger(expectedSeq) && expectedSeq >= 0)) {
    throw new TypeError(`expectedSeq must be an integer >= 0 (0 for an empty stream), got ${expectedSeq}`)
  }
  return { dedupeKey: dedupeKey ?? undefined, expectedSeq: expectedSeq ?? undefined }
}

// The event is checked up front too. Postgres refuses a missing type or
// payload at the INSERT, so the mock must refuse it as well.
function checkEvent(event) {
  if (typeof event?.type !== 'string' || event.type === '') {
    throw new TypeError('an event needs a type, a non-empty string, as in { type, payload }')
  }
  if (event.payload === undefined) {
    throw new TypeError('an event needs a payload, any JSON value (null included)')
  }
  // On the event an option would be ignored, and a retry would append twice.
  const option = ['dedupeKey', 'expectedSeq'].find((key) => Object.hasOwn(event, key))
  if (option) {
    throw new TypeError(`an event may not carry ${option}: pass it in append's options, as in append(stream, event, { ${option} })`)
  }
  checkStorableText(event.type, 'event type')
  checkStorableText(event.source, 'event source')
  checkOccurredAt(event.occurredAt)
  checkNotTombstone(event.payload)
}

// An instant, and only an instant: Postgres reads a time without a zone in its
// own time zone, and words such as 'now' at the moment each retry lands, so a
// retried event would not be the same event. The calendar check catches what
// Date would roll over (2026-02-30) and Postgres refuses, and so is the year
// range: Postgres has no year 0000, and a Date past 9999 is not ISO it reads.
// The value is echoed only in part, since it is the caller's data.
const INSTANT = /^(\d{4})-(\d{2})-(\d{2})T\d{2}:\d{2}(:\d{2}(\.\d+)?)?(Z|[+-]\d{2}:?\d{2})$/i

function checkOccurredAt(occurredAt) {
  if (occurredAt === undefined || occurredAt === null) return
  if (occurredAt instanceof Date ? inYears(occurredAt.getUTCFullYear()) : isInstant(occurredAt)) return
  throw new TypeError(`occurredAt must be a Date or an ISO 8601 string with Z or an offset, such as '2026-10-08T13:20:00Z', got ${echo(occurredAt)}`)
}

const inYears = (year) => year >= 1 && year <= 9999

function echo(value) {
  if (value instanceof Date) return Number.isNaN(value.getTime()) ? 'an invalid Date' : `a Date in year ${value.getUTCFullYear()}`
  const text = String(value)
  return `'${text.length > 40 ? `${text.slice(0, 40)}…` : text}'`
}

function isInstant(text) {
  const match = typeof text === 'string' && INSTANT.exec(text)
  if (!match || Number.isNaN(Date.parse(text))) return false
  const [year, month, day] = match.slice(1, 4).map(Number)
  const date = new Date(Date.UTC(year, month - 1, day))
  return inYears(year) && date.getUTCMonth() === month - 1 && date.getUTCDate() === day
}

// append's input, checked and then encoded under the event's own context: by
// a standalone append before it takes the stream's lock, by tx.append under it.
async function encodeAppend(stream, event, options, encode) {
  const appendOptions = readAppendOptions(options)
  checkEvent(event)
  return { payload: await encode(event.payload, eventContext(stream, event.type)), appendOptions }
}

// Checked under the stream's lock, after a dedupe hit has already returned: a
// retried request whose first attempt landed moved the stream itself, and is
// answered as a retry, not as a conflict.
function checkExpectedSeq(stream, expectedSeq, actualSeq) {
  if (expectedSeq !== undefined && expectedSeq !== actualSeq) {
    throw new WrongExpectedSeqError(stream, expectedSeq, actualSeq)
  }
}

// seq is a JavaScript number once read: past 2^53 - 1 it would round, and two
// events would share one.
function checkNextSeq(stream, current) {
  if (!Number.isSafeInteger(current + 1)) {
    throw new RangeError(`${describeStream(stream)} has reached seq ${current}, the largest a JavaScript number holds exactly`)
  }
}

export { WrongExpectedSeqError, encodeAppend, checkExpectedSeq, checkNextSeq, checkDedupeKey }
