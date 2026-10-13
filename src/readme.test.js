import { test } from 'node:test'
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import { readTables, schemaSql } from './schema/schema.js'

// `npm run typecheck` checks examples/quickstart.js, so it must be the README's
// quick start exactly, below the file's own comment. examples/encrypting.js is
// linked from the README and run below.
const read = (path) => readFileSync(new URL(path, import.meta.url), 'utf8')

test('the quick start the README shows is the one the typecheck checks', () => {
  const shown = read('../README.md').match(/```js\n([\s\S]*?)```/)[1]
  const checked = read('../examples/quickstart.js').replace(/^(\/\/.*\n)+\n/, '')
  assert.equal(checked, shown)
})

test('every example the README links to exists', () => {
  const linked = [...read('../README.md').matchAll(/\/blob\/main\/(examples\/[\w.-]+)\)/g)].map((m) => m[1])
  assert.deepEqual([...new Set(linked)].sort(), ['examples/encrypting.js', 'examples/quickstart.js'])
  for (const path of linked) assert.ok(read(`../${path}`).length > 0, path)
})

test('the schema the README shows is exactly what ensureSchema creates', () => {
  const shown = read('../README.md').match(/```sql\n([\s\S]*?)```/)[1]
  assert.equal(shown, `${schemaSql(readTables())}\n`)
})

// The README's codec example is code people copy, so it is held to its claims.
test('the README codec refuses a truncated tag, a key that is not 32 bytes, and a value it did not seal', async () => {
  const { encryptingCodec } = await import('../examples/encrypting.js')
  const { createMockEventStore } = await import('./mock/mock.js')
  const { randomBytes } = await import('node:crypto')
  const stream = { ownerId: 'o', stream: 'ledger', subjectId: 'main' }
  const context = { kind: 'event', ...stream, type: 'A' }
  const codec = encryptingCodec(() => randomBytes(32).fill(7))
  const sealed = await codec.encode({ amount: 5 }, context)
  assert.deepEqual(await codec.decode(sealed, context), { amount: 5 })
  const truncated = { ...sealed, tag: Buffer.from(sealed.tag, 'base64').subarray(0, 4).toString('base64') }
  await assert.rejects(codec.decode(truncated, context), /a 12-byte iv and a 16-byte tag/)
  for (const unsealed of [{ amount: 5 }, 'plain text', { ...sealed, v: 2 }]) {
    await assert.rejects(codec.decode(unsealed, context), /not a value this codec sealed/, JSON.stringify(unsealed))
  }
  for (const key of ['k'.repeat(32), randomBytes(16)]) {
    await assert.rejects(encryptingCodec(() => key).encode({}, context), /keyFor must return a 32-byte Buffer/)
  }
  const store = createMockEventStore({ codec })
  await store.append(stream, { type: 'A', payload: { amount: 5 } })
  assert.deepEqual((await store.loadEvents(stream))[0].payload, { amount: 5 })
})
