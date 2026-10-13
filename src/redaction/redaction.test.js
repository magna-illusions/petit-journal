import { test } from 'node:test'
import assert from 'node:assert/strict'
import { readRedaction, tombstoneOf, markRedacted, checkSeqsFound, checkNotTombstone, UnknownSeqError } from './redaction.js'

// redact(stream, seqs, { reason, tombstone }) is the one write that changes an
// event already in the log, so its input is checked before anything is
// touched: a malformed seq list or a missing reason must not reach the lock.

const STREAM = { ownerId: 'a1', stream: 'ledger', subjectId: 'a1' }

test('readRedaction sorts and de-duplicates the seqs', () => {
  assert.deepEqual(readRedaction([3, 1, 3], { reason: 'gdpr' }).seqs, [1, 3])
  assert.deepEqual(readRedaction([1], { reason: 'gdpr' }).seqs, [1])
})

test('readRedaction refuses anything but a non-empty array of seqs >= 1', () => {
  for (const seqs of [undefined, null, 3, '3', [], [0], [-1], [1.5], ['2'], [Number.MAX_SAFE_INTEGER + 1]]) {
    assert.throws(() => readRedaction(seqs, { reason: 'gdpr' }), TypeError, `accepted ${JSON.stringify(seqs)}`)
  }
})

test('readRedaction requires a non-empty reason', () => {
  for (const options of [undefined, null, 'gdpr', {}, { reason: '' }, { reason: 7 }]) {
    assert.throws(() => readRedaction([1], options), TypeError, `accepted ${JSON.stringify(options)}`)
  }
})

test('readRedaction takes a tombstone only as a plain object', () => {
  assert.deepEqual(readRedaction([1], { reason: 'gdpr', tombstone: { amount: 5 } }).tombstone, { amount: 5 })
  for (const tombstone of [null, [], 'gone', 1]) {
    assert.throws(() => readRedaction([1], { reason: 'gdpr', tombstone }), TypeError, `accepted ${JSON.stringify(tombstone)}`)
  }
})

test('tombstoneOf is { $redacted, reason, at } by default', () => {
  assert.deepEqual(tombstoneOf({ reason: 'gdpr' }, 'T1'), { $redacted: true, reason: 'gdpr', at: 'T1' })
})

test('tombstoneOf keeps the caller\'s tombstone fields, but $redacted, reason and at are the store\'s', () => {
  const own = { amount: 5, reason: 'spoofed', $redacted: false }
  assert.deepEqual(tombstoneOf({ reason: 'gdpr', tombstone: own }, 'T1'), { amount: 5, $redacted: true, reason: 'gdpr', at: 'T1' })
})

test('markRedacted flags an event whose decoded payload is a tombstone, and leaves the rest alone', () => {
  const tomb = { seq: 2, payload: { $redacted: true, reason: 'gdpr', at: 'T1' } }
  assert.equal(markRedacted(tomb).redacted, true)
  for (const payload of [{ n: 1 }, { $redacted: 'yes' }, null, 'text', [true]]) {
    assert.ok(!('redacted' in markRedacted({ seq: 1, payload })), `flagged ${JSON.stringify(payload)}`)
  }
})

test('checkSeqsFound throws UnknownSeqError naming the seqs the stream does not hold', () => {
  checkSeqsFound(STREAM, [1, 2], [1, 2])
  assert.throws(() => checkSeqsFound(STREAM, [1, 4, 9], [1]), (err) => {
    assert.ok(err instanceof UnknownSeqError)
    assert.equal(err.name, 'UnknownSeqError')
    assert.deepEqual(err.seqs, [4, 9])
    assert.deepEqual(err.stream, STREAM)
    assert.match(err.message, /ledger\/a1 has no event at seq 4, 9/)
    return true
  })
})

test('checkNotTombstone refuses a payload with its own $redacted key, whatever the value', () => {
  for (const payload of [{ $redacted: true }, { $redacted: false, n: 1 }, { $redacted: undefined }]) {
    assert.throws(() => checkNotTombstone(payload), /\$redacted is reserved/, `accepted ${JSON.stringify(payload)}`)
  }
  for (const payload of [{ n: 1 }, { nested: { $redacted: true } }, [{ $redacted: true }], null, 'text', 3, Object.create({ $redacted: true })]) {
    checkNotTombstone(payload)
  }
})
