import { describeStream } from '../stream-id.js'
import { checkStorableText } from '../storable-text.js'

// The optional payload codec: the caller's encode/decode, applied to every
// event payload and projection state on its way into and out of storage. The
// store knows nothing about what the codec does (encryption, compression,
// versioning), only that it gets a JSON value back to store and hands the
// stored JSON value back to decode. No codec means no call at all: the value is
// stored as plain JSON.
//
// The context carries the stream tuple, so a caller can choose per owner (a
// per-tenant key, say) without the store knowing that owners have keys.

function resolveCodec(options) {
  const codec = options?.codec
  if (codec === undefined || codec === null) return null
  if (typeof codec.encode !== 'function' || typeof codec.decode !== 'function') {
    throw new TypeError('codec must provide both encode(value, context) and decode(stored, context)')
  }
  return codec
}

function eventContext(stream, type) {
  return { kind: 'event', ownerId: stream.ownerId, stream: stream.stream, subjectId: stream.subjectId, type }
}

function projectionContext(stream) {
  return { kind: 'projection', ownerId: stream.ownerId, stream: stream.stream, subjectId: stream.subjectId }
}

// undefined is the one value JSON.stringify drops silently: it would reach the
// NOT NULL column as a bare NULL (or, in the mock, store nothing at all), so it
// is refused here with the context that produced it.
// What reaches jsonb is checked, the encoded value or, with no codec, the
// value itself: a sealed value may hold any string, and a codec's own output
// may hold one Postgres refuses.
async function encodeWith(codec, value, context) {
  const encoded = codec ? await codec.encode(value, context) : value
  if (encoded === undefined) {
    throw new TypeError(`codec.encode returned undefined for ${context.kind} on ${describeStream(context)}`)
  }
  checkStorableText(encoded, context.kind === 'event' ? 'event payload' : 'projection state')
  return encoded
}

// A failure names what was being read, so the one bad row can be found; the
// codec's own error is the cause. An event's seq is not part of the context
// the codec sees, so it is passed beside it.
async function decodeWith(codec, stored, context, seq) {
  if (!codec) return stored
  try {
    return await codec.decode(stored, context)
  } catch (err) {
    const what = context.kind === 'event' ? `the ${context.type} event at seq ${seq}` : 'the projection state'
    throw new Error(`could not decode ${what} on ${describeStream(context)}: ${err.message}`, { cause: err })
  }
}

export { resolveCodec, eventContext, projectionContext, encodeWith, decodeWith }
