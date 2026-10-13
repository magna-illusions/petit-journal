import { describeStream } from '../stream-id.js'
import { encodeAppend, checkDedupeKey } from '../append.js'
import { checkProjection } from '../save-check.js'
import { checkCompaction } from '../compaction/compaction.js'
import { readRedaction } from '../redaction/redaction.js'

// What store.transaction(stream, work) hands `work`: the stream's operations,
// bound to the one client that holds the stream's lock — without the stream
// argument, because a transaction is on one stream — plus that client's query
// for the caller's own SQL. Shared by the pg and the mock store, so the two
// cannot drift in shape.
function bindTransaction(stream, ops, encode, query) {
  return {
    stream: { ownerId: stream.ownerId, stream: stream.stream, subjectId: stream.subjectId },
    async append(event, options) {
      const { payload, appendOptions } = await encodeAppend(stream, event, options, encode)
      return ops.appendEncoded(event, payload, appendOptions)
    },
    async redact(seqs, options) {
      return ops.redact(readRedaction(seqs, options))
    },
    async eventByDedupeKey(dedupeKey) {
      checkDedupeKey(dedupeKey)
      return ops.eventByDedupeKey(dedupeKey)
    },
    loadEvents: (afterSeq) => ops.loadEvents(afterSeq),
    loadProjection: () => ops.loadProjection(),
    async saveProjection(state, throughSeq) {
      checkProjection(state, throughSeq)
      return ops.saveProjection(state, throughSeq)
    },
    maxSeq: () => ops.maxSeq(),
    async compact(throughSeq, keep) {
      checkCompaction(throughSeq, keep)
      return ops.compact(throughSeq, keep)
    },
    query,
  }
}

// A call that lands after `work` returned would run outside the transaction and
// without the lock — typically a missing await. It fails instead.
function endedError(stream) {
  return new Error(
    `the transaction on ${describeStream(stream)} has already ended: ` +
    'await every call inside the work function and do not use the transaction after it returns',
  )
}

function checkWork(work) {
  if (typeof work !== 'function') throw new TypeError('transaction(stream, work) needs work to be a function (tx) => Promise')
}

export { bindTransaction, endedError, checkWork }
