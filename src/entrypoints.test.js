import { describe, it } from 'node:test'
import assert from 'node:assert/strict'
import { createRequire } from 'node:module'
import * as esm from 'petit-journal'

const require = createRequire(import.meta.url)
const cjs = require('petit-journal')

// The package imports itself by name, so this goes through the `exports` map
// as a consumer's import does. src/ is ESM, so require() of it is a require()
// of an ES module — a resolution path that is warning-free only from Node
// 22.13 (the `engines` floor) and one the ESM consumers never exercise.
// Assert both hand back the very same functions, and nothing beyond the
// documented surface.
const EXPORTS = [
  'createEventStore', 'openEventStore', 'ensureSchema', 'createMockEventStore', 'defineProjection',
  'WrongExpectedSeqError', 'UnknownSeqError', 'CompactedLogError',
]

describe('published entrypoints', () => {
  it('exposes the whole API to both import and require', () => {
    for (const name of EXPORTS) {
      assert.equal(typeof esm[name], 'function', `import is missing ${name}`)
      assert.equal(typeof cjs[name], 'function', `require is missing ${name}`)
    }
  })

  it('resolves import and require to one implementation', () => {
    for (const name of EXPORTS) {
      assert.equal(esm[name], cjs[name], `${name} differs between import and require`)
    }
  })

  it('publishes nothing beyond the documented surface', () => {
    // A stray export is API by accident: it ships, someone imports it, and it
    // can no longer be removed without a major.
    assert.deepEqual(Object.keys(cjs).sort(), [...EXPORTS].sort())
  })
})
