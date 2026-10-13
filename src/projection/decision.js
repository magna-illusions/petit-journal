// What append takes, and what update's decide returns: append takes an event
// or an array of them; decide returns an array of events, or a plan { append,
// redact } that also redacts earlier events in the same transaction. An array
// is events and an object is a plan: nothing is guessed from an object's keys.

import { isPlainObject } from '../plain-object.js'
import { readRedaction } from '../redaction/redaction.js'

function toEvents(value, message) {
  if (Array.isArray(value)) return value
  if (isPlainObject(value)) return [value]
  throw new TypeError(message)
}

function readDecision(decided) {
  if (Array.isArray(decided)) return { events: decided, redactions: [] }
  if (!isPlainObject(decided)) {
    throw new TypeError('decide must return an array of events ([] appends nothing), or a plan { append, redact }')
  }
  if ('type' in decided) throw new TypeError('decide returned one event: wrap a single event in an array, as in [event]')
  const unknown = Object.keys(decided).find((key) => key !== 'append' && key !== 'redact')
  if (unknown) throw new TypeError(`unknown key ${unknown}: decide returns an array of events, or a plan { append, redact }`)
  const { append = [], redact = [] } = decided
  if (!Array.isArray(append)) throw new TypeError('a plan\'s append must be an array of events')
  if (!Array.isArray(redact) || !redact.every(isPlainObject)) {
    throw new TypeError('a plan\'s redact must be an array of { seqs, reason, tombstone? }')
  }
  for (const { seqs, ...options } of redact) readRedaction(seqs, options)
  return { events: append, redactions: redact }
}

export { readDecision, toEvents }
