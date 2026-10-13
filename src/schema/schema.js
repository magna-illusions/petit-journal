// The two tables, by default domain_events and domain_projections. A name
// cannot be bound as a parameter, so it reaches the SQL as text: it is checked
// at construction against a strict pattern, then quoted as an identifier. The
// 52-character limit leaves room for the dedupe index's name, derived from the
// events table's, within Postgres's 63.
const DEFAULTS = { events: 'domain_events', projections: 'domain_projections' }
const PART = /^[a-z_][a-z0-9_]*$/
const MAX_PART = 52

const quote = (parts) => parts.map((part) => `"${part}"`).join('.')

function readName(key, name) {
  const parts = typeof name === 'string' ? name.split('.') : []
  if (parts.length === 0 || parts.length > 2 || !parts.every((part) => PART.test(part))) {
    throw new TypeError(`tables.${key} must be a lowercase identifier such as '${DEFAULTS[key]}' or 'app.${DEFAULTS[key]}', got ${JSON.stringify(name)}`)
  }
  if (parts.some((part) => part.length > MAX_PART)) {
    throw new TypeError(`tables.${key}: each part of the name must be at most ${MAX_PART} characters`)
  }
  // Postgres reserves these schemas for itself.
  if (parts.length === 2 && (parts[0].startsWith('pg_') || parts[0] === 'information_schema')) {
    throw new TypeError(`tables.${key} may not live in a system schema (pg_*, information_schema), got ${JSON.stringify(name)}`)
  }
  return parts
}

function readTables(options) {
  const tables = options?.tables ?? {}
  if (typeof tables !== 'object' || Array.isArray(tables)) {
    throw new TypeError('tables must be an object such as { events, projections }')
  }
  const unknown = Object.keys(tables).find((key) => !(key in DEFAULTS))
  if (unknown) throw new TypeError(`unknown key ${unknown}: tables takes { events, projections }`)
  const events = readName('events', tables.events ?? DEFAULTS.events)
  const projections = readName('projections', tables.projections ?? DEFAULTS.projections)
  const dedupeIndex = `${events.at(-1)}_dedupe_key`
  if (mayCoincide(events, projections)) {
    throw new TypeError('tables: events and projections must be different tables')
  }
  // The index lives in the events table's schema, beside it.
  if (mayCoincide([...events.slice(0, -1), dedupeIndex], projections)) {
    throw new TypeError(`tables.projections would be the events table's dedupe index, "${dedupeIndex}": choose another name`)
  }
  return { events: quote(events), projections: quote(projections), dedupeIndex: quote([dedupeIndex]) }
}

// Whether two names may resolve to one relation. An unqualified name goes
// through the search_path, so it may be in any schema.
function mayCoincide(a, b) {
  if (a.at(-1) !== b.at(-1)) return false
  return a.length === 1 || b.length === 1 || a[0] === b[0]
}

// What ensureSchema creates, and what the README shows for an application's own
// migrations. Every statement is IF NOT EXISTS, so it is safe on every boot.
function schemaSql({ events, projections, dedupeIndex }) {
  return `CREATE TABLE IF NOT EXISTS ${events} (
  id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  owner_id    TEXT NOT NULL,
  stream      TEXT NOT NULL,
  subject_id  TEXT NOT NULL,
  seq         BIGINT NOT NULL,
  type        TEXT NOT NULL,
  payload     JSONB NOT NULL,
  occurred_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  source      TEXT,
  dedupe_key  TEXT,
  UNIQUE (owner_id, stream, subject_id, seq)
);

CREATE UNIQUE INDEX IF NOT EXISTS ${dedupeIndex}
  ON ${events} (owner_id, stream, subject_id, dedupe_key)
  WHERE dedupe_key IS NOT NULL;

CREATE TABLE IF NOT EXISTS ${projections} (
  owner_id    TEXT NOT NULL,
  stream      TEXT NOT NULL,
  subject_id  TEXT NOT NULL,
  state       JSONB NOT NULL,
  through_seq BIGINT NOT NULL,
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (owner_id, stream, subject_id)
);`
}

// IF NOT EXISTS is not enough on its own: two sessions creating the same table
// at once both pass the check, and one fails on a unique violation in
// pg_type. So the statements run behind an advisory lock, as one query: pg
// sends a query without parameters as a simple query, and Postgres runs its
// statements as one transaction, which holds the lock to the end.
const SCHEMA_LOCK = "SELECT pg_advisory_xact_lock(hashtextextended('petit-journal:schema', 0))"

// CREATE TABLE IF NOT EXISTS skips a name that something else holds already:
// an index Postgres named itself (ev_pkey), or an older table of another
// shape. Reading every column the store uses, from both names, turns either
// into a failed setup at boot rather than a failed write later.
const shapeCheckSql = ({ events, projections }) => [
  `SELECT id, owner_id, stream, subject_id, seq, type, payload, occurred_at, source, dedupe_key FROM ${events} LIMIT 0;`,
  `SELECT owner_id, stream, subject_id, state, through_seq, updated_at FROM ${projections} LIMIT 0;`,
].join('\n')

// The setup, as a plain function for migration scripts and tests; also what
// openEventStore runs before it hands out a store. A failure names the tables
// and keeps the database's error as its cause.
async function ensureSchema(pool, options) {
  if (typeof pool?.query !== 'function') {
    throw new TypeError('ensureSchema needs a pool with query(text), such as a pg.Pool')
  }
  const tables = readTables(options)
  try {
    await pool.query(`${SCHEMA_LOCK};\n${schemaSql(tables)}\n${shapeCheckSql(tables)}`)
  } catch (err) {
    throw setupError(err, tables)
  }
}

// Classes of SQLSTATE that mean the session never started: a connection
// exception, a failed login, a missing database, too many connections, a
// server shutting down.
const NOT_REACHED = ['08', '28', '3D', '53', '57']

// pg's server errors carry a severity and a SQLSTATE; a connection that never
// opened does not, and may have no message of its own (an AggregateError of
// every address tried). Either way the reason is never empty.
function setupError(err, { events, projections }) {
  const reason = err.message || err.errors?.map((inner) => inner.message).join('; ') || err.code || String(err)
  const reached = typeof err.severity === 'string' && !NOT_REACHED.includes(String(err.code).slice(0, 2))
  if (!reached) return new Error(`petit-journal could not reach the database: ${reason}`, { cause: err })
  const hint = err.code === '42501' ? '. A role without CREATE can use createEventStore, with the tables made by your own migrations' : ''
  return new Error(`petit-journal could not set up the tables ${events} and ${projections}: ${reason}${hint}`, { cause: err })
}

export { readTables, schemaSql, ensureSchema }
