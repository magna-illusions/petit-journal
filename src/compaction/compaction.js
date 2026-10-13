// Bounded history. Events the projection has already folded (seq <= throughSeq)
// are redundant for reads, since the projection embodies them, so they can be
// pruned once they fall outside a trailing audit window: the newest `keep`
// events stay as an audit and replay trail, and older folded ones go. The log
// stays bounded while the projection row remains the current snapshot.
//
// The newest event is never pruned, whatever `keep` is: append assigns
// seq = MAX(seq)+1, so an emptied log would restart at seq 1, behind the
// projection's throughSeq, and every later event would be invisible to it.

// Highest seq that may be deleted (inclusive), or null when nothing is prunable.
// Prune seq <= min(throughSeq, oldestSeqToKeep - 1), keep counted as at least 1.
function pruneUpToSeq({ throughSeq, maxSeq, keep }) {
  const oldestSeqToKeep = maxSeq - Math.max(keep, 1) + 1
  const cutoff = Math.min(throughSeq, oldestSeqToKeep - 1)
  return cutoff >= 1 ? cutoff : null
}

// compact(stream, throughSeq, keep) — checked up front: a missing keep makes
// the cutoff NaN, and compact would silently delete nothing.
function checkCompaction(throughSeq, keep) {
  for (const [name, value] of [['throughSeq', throughSeq], ['keep', keep]]) {
    if (!(Number.isSafeInteger(value) && value >= 0)) {
      throw new TypeError(`compact's ${name} must be an integer >= 0, got ${value}`)
    }
  }
}

// What defineProjection throws when it needs events compaction has removed: a
// projection behind a log that now starts past it, a rebuild from initial, a
// plan's re-fold. firstSeq is the oldest seq the log still holds.
class CompactedLogError extends Error {
  constructor(stream, firstSeq, message) {
    super(message)
    this.name = 'CompactedLogError'
    this.stream = { ownerId: stream.ownerId, stream: stream.stream, subjectId: stream.subjectId }
    this.firstSeq = firstSeq
  }
}

export { pruneUpToSeq, checkCompaction, CompactedLogError }
