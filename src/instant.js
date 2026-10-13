// pg returns a timestamptz as a Date with its default parser, and as Postgres's
// own text ('2026-10-08 15:20:00.123456+02') with that parser switched off. The
// caller always gets one form, whatever the pool is configured to do: an ISO
// 8601 instant in UTC, to the millisecond. What reaches it is an instant
// already: a timestamptz from Postgres, or an occurredAt append has checked.
const toIso = (value) => new Date(value).toISOString()

export { toIso }
