// @ts-check
// The README's quick start, character for character: `npm run typecheck`
// checks it against the declarations and pg's own types, and readme.test.js
// fails if the two drift apart. Nothing ships it.

import pg from 'pg'
import { openEventStore, defineProjection } from 'petit-journal'

const pool = new pg.Pool({ connectionString: process.env.DATABASE_URL })
pool.on('error', (err) => console.error('idle connection lost', err.message)) // pg crashes the process otherwise
const store = await openEventStore(pool) // creates the tables if they are missing, then the store

const ledger = defineProjection(store, {
  initial: { balance: 0 },
  reduce: (state, event) =>
    event.type === 'Deposited' ? { balance: state.balance + event.payload.amount } : state,
})

const account = { ownerId: 'acme', stream: 'ledger', subjectId: 'main' }
await ledger.append(account, { type: 'Deposited', payload: { amount: 100 } })
console.log(await ledger.state(account)) // { balance: 100 }
await pool.end()
