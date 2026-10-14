import { test } from 'node:test'
import assert from 'node:assert/strict'
import { readStream, readOwner, lockKey } from './stream-id.js'

// Postgres reads a uuid in any of its accepted spellings as one value, so the
// rows of one owner are one set however the id is written. The lock key is
// text, so one owner must also be written one way, or two writers take two
// locks for one stream.

const LOWER = '0b6c2f1e-8a4d-4c1b-9f3e-2d7a5c8e1b40'
const at = (ownerId) => ({ ownerId, stream: 'ledger', subjectId: 'main' })

test('readStream writes a hyphenated uuid owner, in any case and with or without braces, lowercase and bare', () => {
  for (const spelling of [LOWER, LOWER.toUpperCase(), `{${LOWER}}`, `{${LOWER.toUpperCase()}}`]) {
    assert.deepEqual(readStream(at(spelling)), at(LOWER), spelling)
  }
})

// owner_id is TEXT: an id that only looks like a uuid without its hyphens, an
// md5 say, is an ordinary id, and two cases of it are two owners.
test('readStream leaves 32 bare hex digits, and other hyphenations, exactly as given', () => {
  for (const ownerId of ['d41d8cd98f00b204e9800998ecf8427e', 'D41D8CD98F00B204E9800998ECF8427E', LOWER.replaceAll('-', ''), '0b6c-2f1e-8a4d-4c1b-9f3e-2d7a-5c8e-1b40']) {
    assert.equal(readStream(at(ownerId)).ownerId, ownerId, ownerId)
  }
})

test('readStream leaves a lowercase uuid as it is, so its lock key never changes', () => {
  assert.equal(lockKey(readStream(at(LOWER))), `${LOWER}:ledger:main`)
})

test('readStream leaves an owner that is not a uuid exactly as given', () => {
  for (const ownerId of ['Tenant-A', 'acct_42', `${LOWER}x`, `{${LOWER}`, LOWER.slice(1), 7]) {
    assert.equal(readStream(at(ownerId)).ownerId, ownerId, String(ownerId))
  }
})

test('readStream returns a copy and refuses a missing part', () => {
  const given = { ...at(LOWER), extra: 1 }
  assert.deepEqual(readStream(given), at(LOWER))
  assert.throws(() => readStream({ stream: 'ledger', subjectId: 'main' }), /StreamId is missing ownerId/)
})

// eraseOwner takes an owner on its own. It must reach the rows a write made, so
// it spells the owner exactly as readStream does.
test('readOwner spells an owner as readStream does', () => {
  for (const ownerId of [LOWER, LOWER.toUpperCase(), `{${LOWER}}`, 'Tenant-A', 'd41d8cd98f00b204e9800998ecf8427e']) {
    assert.equal(readOwner(ownerId), readStream(at(ownerId)).ownerId, ownerId)
  }
})

test('readOwner refuses a missing or unstorable owner', () => {
  for (const ownerId of [undefined, null, '']) {
    assert.throws(() => readOwner(ownerId), /eraseOwner needs an ownerId/, String(ownerId))
  }
  assert.throws(() => readOwner('a\u0000b'), TypeError)
})
