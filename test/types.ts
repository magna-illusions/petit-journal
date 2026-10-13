// `npm run typecheck` compiles this file and nothing ships it. It holds the
// hand-written declarations to the code: every runtime export is declared and
// every declaration exists, and the code's inferred shapes fit the declared
// ones. examples/ holds the README's examples to them.
import * as declared from 'petit-journal'
import * as code from '../src/index.js'

type Same<A, B> = [A] extends [B] ? ([B] extends [A] ? true : false) : false
const exportsMatch: Same<keyof typeof declared, keyof typeof code> = true
const storeMethodsMatch: Same<keyof ReturnType<typeof code.createEventStore>, keyof declared.EventStore> = true
const mockMethodsMatch: Same<keyof ReturnType<typeof code.createMockEventStore>, keyof declared.EventStore> = true
const projectionMethodsMatch: Same<keyof ReturnType<typeof code.defineProjection>, keyof declared.DefinedProjection<unknown>> = true

// The error classes set `name` in their constructors, which the code's types
// infer as a string; everything else about them must match.
type Errors = 'WrongExpectedSeqError' | 'UnknownSeqError' | 'CompactedLogError'
const surface: Omit<typeof declared, Errors> = code
const stream = { ownerId: 'o', stream: 's', subjectId: 'o' }
const errors: [Omit<declared.WrongExpectedSeqError, 'name'>, Omit<declared.UnknownSeqError, 'name'>, Omit<declared.CompactedLogError, 'name'>] =
  [new code.WrongExpectedSeqError(stream, 1, 2), new code.UnknownSeqError(stream, [3]), new code.CompactedLogError(stream, 3, 'compacted')]

// The forget path a consumer writes: an update that appends and redacts in one
// transaction, reading the log through ctx, on a projection that folds tombstones.
async function forgetting(store: declared.EventStore, stream: declared.StreamId) {
  const notes = declared.defineProjection(store, { initial: {} as Record<string, string>, reduce: (state) => state, redacted: 'fold' })
  const result = await notes.update(stream, async (_state, ctx) => {
    const seqs = (await ctx.loadEvents()).filter((event) => !event.redacted).map((event) => event.seq)
    return { append: [{ type: 'Forgot', payload: { seqs } }], redact: seqs.map((seq) => ({ seqs: [seq], reason: 'forget', tombstone: { seq } })) }
  })
  const outcome: [number, boolean, number[]] = [result.events.length, result.duplicate, result.redacted]
  return outcome
}

export { forgetting, exportsMatch, storeMethodsMatch, mockMethodsMatch, projectionMethodsMatch, surface, errors }
