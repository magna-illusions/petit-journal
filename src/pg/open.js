import { createEventStore } from './store.js'
import { ensureSchema } from '../schema/schema.js'

// openEventStore(pool, options) — for an application that lets the store own
// its tables. The store is made first, so a bad option fails before any SQL,
// and handed out only once the tables exist: no write can reach a table that
// is not there yet. An application that runs its own migrations uses
// createEventStore, which never creates anything.
async function openEventStore(pool, options) {
  const store = createEventStore(pool, options)
  await ensureSchema(pool, options)
  return store
}

export { openEventStore }
