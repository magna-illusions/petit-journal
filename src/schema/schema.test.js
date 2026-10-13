import { test } from 'node:test'
import assert from 'node:assert/strict'
import { readTables, schemaSql, ensureSchema } from './schema.js'
import { createMockEventStore } from '../mock/mock.js'

// Table names reach the SQL as text, never as parameters, so they are checked
// at construction against a strict pattern and quoted as identifiers.

test('readTables defaults to domain_events and domain_projections, quoted', () => {
  for (const options of [undefined, null, {}, { tables: {} }]) {
    assert.deepEqual(readTables(options), {
      events: '"domain_events"', projections: '"domain_projections"', dedupeIndex: '"domain_events_dedupe_key"',
    })
  }
})

test('readTables takes either name, optionally schema-qualified; the index lives beside its table', () => {
  assert.deepEqual(readTables({ tables: { events: 'ledger_events' } }), {
    events: '"ledger_events"', projections: '"domain_projections"', dedupeIndex: '"ledger_events_dedupe_key"',
  })
  assert.deepEqual(readTables({ tables: { events: 'app.events', projections: 'app.projections' } }), {
    events: '"app"."events"', projections: '"app"."projections"', dedupeIndex: '"events_dedupe_key"',
  })
})

test('readTables refuses a name that is not a plain lowercase identifier', () => {
  const bad = ['', 'Events', '1events', 'my-events', 'events;drop table x', 'a.b.c', '.events', 'events.', '"events"', ' events', 7]
  for (const name of bad) {
    assert.throws(() => readTables({ tables: { events: name } }), /tables\.events must be a lowercase identifier/, String(name))
    assert.throws(() => readTables({ tables: { projections: name } }), /tables\.projections must be a lowercase identifier/, String(name))
  }
})

test('readTables refuses the system schemas, pg_* and information_schema', () => {
  for (const name of ['pg_catalog.events', 'pg_temp.events', 'pg_toast.events', 'information_schema.events']) {
    assert.throws(() => readTables({ tables: { events: name } }), /tables\.events may not live in a system schema/, name)
    assert.throws(() => readTables({ tables: { projections: name } }), /tables\.projections may not live in a system schema/, name)
  }
  assert.doesNotThrow(() => readTables({ tables: { events: 'app.pg_events', projections: 'pgx.state' } }))
})

test('readTables allows 52 characters per part, so the derived index name is never truncated', () => {
  const longest = 'e'.repeat(52)
  assert.equal(readTables({ tables: { events: longest } }).dedupeIndex, `"${longest}_dedupe_key"`)
  assert.equal(readTables({ tables: { events: `s.${longest}` } }).events, `"s"."${longest}"`)
  assert.throws(() => readTables({ tables: { events: 'e'.repeat(53) } }), /at most 52 characters/)
  assert.throws(() => readTables({ tables: { events: `${'s'.repeat(53)}.events` } }), /at most 52 characters/)
})

test('readTables refuses a misspelt key, a non-object and one table for both', () => {
  assert.throws(() => readTables({ tables: { event: 'x' } }), /unknown key event: tables takes \{ events, projections \}/)
  assert.throws(() => readTables({ tables: 'domain_events' }), /tables must be an object such as \{ events, projections \}/)
  assert.throws(() => readTables({ tables: { events: 'same', projections: 'same' } }), /events and projections must be different tables/)
})

// An unqualified name resolves through the search_path, so it may be either
// schema: two names that end alike are refused unless both name a schema.
test('readTables refuses names that may land on the same relation, the dedupe index included', () => {
  const clash = /events and projections must be different tables/
  assert.throws(() => readTables({ tables: { events: 'evy', projections: 'public.evy' } }), clash)
  assert.throws(() => readTables({ tables: { events: 'app.evy', projections: 'evy' } }), clash)
  assert.doesNotThrow(() => readTables({ tables: { events: 'a.evy', projections: 'b.evy' } }))
  const index = /tables\.projections would be the events table's dedupe index, "evx_dedupe_key"/
  assert.throws(() => readTables({ tables: { events: 'evx', projections: 'evx_dedupe_key' } }), index)
  assert.throws(() => readTables({ tables: { events: 'app.evx', projections: 'app.evx_dedupe_key' } }), index)
  assert.throws(() => readTables({ tables: { events: 'app.evx', projections: 'evx_dedupe_key' } }), index)
  assert.doesNotThrow(() => readTables({ tables: { events: 'app.evx', projections: 'other.evx_dedupe_key' } }))
})

test('schemaSql creates both tables and the dedupe index only if they do not exist, under the configured names', () => {
  const sql = schemaSql(readTables({ tables: { events: 'app.ledger_events', projections: 'app.ledger_state' } }))
  assert.match(sql, /^CREATE TABLE IF NOT EXISTS "app"\."ledger_events" \(/)
  assert.match(sql, /CREATE UNIQUE INDEX IF NOT EXISTS "ledger_events_dedupe_key"\n {2}ON "app"\."ledger_events" /)
  assert.match(sql, /CREATE TABLE IF NOT EXISTS "app"\."ledger_state" \(/)
  assert.ok(!sql.includes('domain_'))
})

test('ensureSchema runs the schema as one query behind an advisory lock, so concurrent boots do not race', async () => {
  const calls = []
  const pool = { query: async (text, params) => { calls.push({ text, params }); return { rows: [], rowCount: 0 } } }
  const tables = { events: 'ledger_events', projections: 'ledger_state' }
  assert.equal(await ensureSchema(pool, { tables }), undefined)
  assert.deepEqual(calls, [{
    text: `SELECT pg_advisory_xact_lock(hashtextextended('petit-journal:schema', 0));\n${schemaSql(readTables({ tables }))}\n${shapeCheck}`,
    params: undefined,
  }])
})

// CREATE TABLE IF NOT EXISTS skips a name that something else holds already: an
// index Postgres named itself, or an older table of another shape. Reading
// every column the store uses, from both names, fails the setup at boot then.
const shapeCheck = [
  'SELECT id, owner_id, stream, subject_id, seq, type, payload, occurred_at, source, dedupe_key FROM "ledger_events" LIMIT 0;',
  'SELECT owner_id, stream, subject_id, state, through_seq, updated_at FROM "ledger_state" LIMIT 0;',
].join('\n')

// pg's server errors carry a severity and a SQLSTATE; a connection that never
// opened does not, and may carry no message at all (an AggregateError of every
// address tried). Each failure says which it was.
const pgError = (message, code) => Object.assign(new Error(message), { code, severity: 'ERROR' })

test('ensureSchema rejects naming the tables it could not set up, with the database error as the cause', async () => {
  const failed = pgError('relation "ev3_pkey" is an index', '42809')
  const pool = { query: async () => { throw failed } }
  await assert.rejects(ensureSchema(pool, { tables: { events: 'ledger_events', projections: 'ledger_state' } }), (err) => {
    assert.match(err.message, /could not set up the tables "ledger_events" and "ledger_state": relation "ev3_pkey" is an index$/)
    assert.equal(err.cause, failed)
    return true
  })
})

test('ensureSchema on a role without CREATE points to createEventStore', async () => {
  const pool = { query: async () => { throw pgError('permission denied for schema public', '42501') } }
  await assert.rejects(ensureSchema(pool), /could not set up the tables .*: permission denied for schema public\. A role without CREATE can use createEventStore, with the tables made by your own migrations/)
})

test('ensureSchema says when it could not reach the database, whatever shape the error has', async () => {
  const refused = new AggregateError([new Error('connect ECONNREFUSED ::1:5432'), new Error('connect ECONNREFUSED 127.0.0.1:5432')], '')
  const unnamed = Object.assign(new Error(''), { code: 'ECONNREFUSED' })
  const login = pgError('password authentication failed for user "app"', '28P01')
  const noDatabase = pgError('database "app" does not exist', '3D000')
  for (const [error, reason] of [
    [refused, 'connect ECONNREFUSED ::1:5432; connect ECONNREFUSED 127.0.0.1:5432'], [unnamed, 'ECONNREFUSED'],
    [new Error('getaddrinfo ENOTFOUND db'), 'getaddrinfo ENOTFOUND db'], [login, login.message], [noDatabase, noDatabase.message],
  ]) {
    await assert.rejects(ensureSchema({ query: async () => { throw error } }), (err) => {
      assert.equal(err.message, `petit-journal could not reach the database: ${reason}`)
      assert.equal(err.cause, error)
      return true
    })
  }
})

test('ensureSchema refuses a pool without query, and a bad table name, before any SQL', async () => {
  const calls = []
  const pool = { query: async (text) => { calls.push(text) } }
  for (const bad of [undefined, {}, { query: 'SELECT' }]) {
    await assert.rejects(ensureSchema(bad), /ensureSchema needs a pool with query\(text\), such as a pg\.Pool/)
  }
  await assert.rejects(ensureSchema(pool, { tables: { events: 'Events' } }), /tables\.events must be a lowercase identifier/)
  assert.deepEqual(calls, [])
})

test('mock: a bad table name is refused as in the pg store', () => {
  assert.throws(() => createMockEventStore({ tables: { events: 'Events' } }), /tables\.events must be a lowercase identifier/)
})
