import { test } from 'node:test'
import assert from 'node:assert/strict'
import { pruneUpToSeq } from './compaction.js'

// pruneUpToSeq returns the highest seq that may be deleted (inclusive), or null
// when nothing is prunable. It never prunes an event the projection has not yet
// folded (seq > throughSeq), and always keeps the most-recent `keep` events.

test('nothing to prune when the stream is shorter than the audit keep', () => {
  assert.equal(pruneUpToSeq({ throughSeq: 10, maxSeq: 10, keep: 100 }), null)
})

test('nothing to prune when the stream is exactly the audit keep', () => {
  assert.equal(pruneUpToSeq({ throughSeq: 100, maxSeq: 100, keep: 100 }), null)
})

test('prunes folded events older than the audit keep', () => {
  // 200 events, all folded, keep last 100 → prune seq 1..100.
  assert.equal(pruneUpToSeq({ throughSeq: 200, maxSeq: 200, keep: 100 }), 100)
})

test('never prunes beyond what the projection has folded', () => {
  // 200 events but only 50 folded → cutoff bounded by throughSeq.
  assert.equal(pruneUpToSeq({ throughSeq: 50, maxSeq: 200, keep: 100 }), 50)
})

test('one past the keep prunes exactly the single oldest event', () => {
  assert.equal(pruneUpToSeq({ throughSeq: 101, maxSeq: 101, keep: 100 }), 1)
})

test('keep of zero prunes everything the projection has folded', () => {
  assert.equal(pruneUpToSeq({ throughSeq: 30, maxSeq: 40, keep: 0 }), 30)
})

test('never prunes the newest event, even with keep 0 and everything folded', () => {
  // append assigns seq = MAX(seq)+1: an empty log would restart at seq 1, behind
  // the projection's throughSeq, and every later event would be invisible to it.
  assert.equal(pruneUpToSeq({ throughSeq: 30, maxSeq: 30, keep: 0 }), 29)
  assert.equal(pruneUpToSeq({ throughSeq: 1, maxSeq: 1, keep: 0 }), null)
})

test('empty stream (maxSeq 0) is not prunable', () => {
  assert.equal(pruneUpToSeq({ throughSeq: 0, maxSeq: 0, keep: 100 }), null)
})
