import { describeStream } from '../stream-id.js'
import { AsyncLocalStorage } from 'node:async_hooks'

// The stream locks the current async context holds. A store write that needs
// one of them would wait for the transaction holding it, while that
// transaction waits for the write: a deadlock. It is refused at once instead,
// with the way out.
//
// One record for the whole process, not one per store: the lock is in the
// database, so a second store on the same pool, or on other tables (the lock
// key is the stream tuple alone), would wait just the same.
//
// Async work started inside a transaction keeps its context after the COMMIT,
// so each record says whether its lock is still held: `holding` marks it
// released when `run` settles, and only live records refuse.
const held = new AsyncLocalStorage()

const heldLocks = {
  refuseIfHeld(key, stream) {
    if (held.getStore()?.some((record) => record.live && record.key === key)) {
      throw new TypeError(
        `a store call inside a transaction on ${describeStream(stream)} would wait for that transaction's own lock: ` +
        "it takes the stream's lock (a write, a nested transaction, or a projection's locked re-read). Use the transaction's tx",
      )
    }
  },
  // eraseOwner deletes rows the transaction may have locked, and the
  // transaction's work would write past the erase: refused the same way.
  refuseIfOwnerHeld(ownerId) {
    const record = held.getStore()?.find((r) => r.live && r.stream.ownerId === ownerId)
    if (record) {
      throw new TypeError(
        `eraseOwner inside a transaction on ${describeStream(record.stream)} would erase that transaction's owner: ` +
        'erase once the transaction has ended, with writes for that owner stopped',
      )
    }
  },
  async holding(key, stream, run) {
    const record = { key, stream, live: true }
    try {
      return await held.run([...(held.getStore() ?? []), record], run)
    } finally {
      record.live = false
    }
  },
}

export { heldLocks }
