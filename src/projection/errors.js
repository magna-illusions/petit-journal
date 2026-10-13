// The errors defineProjection throws, each with what it needs to say why.

import { describeStream } from '../stream-id.js'
import { CompactedLogError } from '../compaction/compaction.js'

const compactedPlanError = (stream, firstSeq) => new CompactedLogError(stream, firstSeq,
  `cannot redact in an update on ${describeStream(stream)}: compaction removed seq 1 to ${firstSeq - 1}, so the state cannot be ` +
  're-folded without the redacted facts. Use store.transaction: tx.redact, then correct the state and tx.saveProjection yourself',
)

// Defence in depth: the lock makes this impossible, unless a writer took
// another lock for the same rows. Saving would skip the events in between.
const movedError = (stream, folded, landed) => new Error(
  `${describeStream(stream)} moved under this write: the state it folded is at seq ${folded}, but the append landed at seq ${landed}. ` +
  'Another writer may be taking a different lock for this stream',
)

// Refused from the first call: with keep, compaction would soon remove the
// events a re-fold needs, and the plan would start failing only in production.
const keptPlanError = () => new TypeError(
  'a plan that redacts cannot run on a projection with keep: compaction removes the events its re-fold needs. ' +
  'Define the projection without keep, or redact with store.transaction and correct the state yourself',
)

const gapError = (stream, firstSeq, throughSeq) => new CompactedLogError(stream, firstSeq,
  `the log of ${describeStream(stream)} was compacted past its projection: it starts at seq ${firstSeq}, ` +
  `the projection stops at seq ${throughSeq}, and the events between are gone`,
)

const rebuildError = (stream, firstSeq) => new CompactedLogError(stream, firstSeq,
  `cannot rebuild ${describeStream(stream)} from initial: compaction removed seq 1 to ${firstSeq - 1}`,
)

export { compactedPlanError, gapError, rebuildError, movedError, keptPlanError }
