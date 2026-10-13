# Security

petit-journal writes your data to Postgres, holds the locks that keep a stream's writers in turn,
and decides what a redaction erases, so a flaw in it can lose events, mix streams, or leave a
forgotten fact readable. Please report one privately.

## Reporting

Use GitHub's private vulnerability reporting:
[report a vulnerability](https://github.com/magna-illusions/petit-journal/security/advisories/new).
Please do not open a public issue. Include the version, a minimal script or the calls that show
it, and what you expected instead.

## In scope

- SQL and identifiers: any input that reaches a statement as anything but a bound parameter, or a
  table name the checks accept that is not the table you named.
- The lock: two writers to one stream that do not wait for each other, a write that lands between
  a transaction's read and its save, or a store call that waits forever where it should be refused.
- Isolation between streams: one stream's events, projection or dedupe keys read or changed
  through another.
- The codec example in the README: a stored value it accepts that it did not seal, or one it
  decodes under another owner, stream or event type.
- Redaction: a redacted payload, or the fact in a projection re-folded after one, still returned
  by the store; a redact that changes more than the payloads it names.
- A call that throws on input it documents as valid, or writes a value that does not read back as
  written.

Out of scope: what the package documents as yours or as beyond it, such as your codec and keys,
your own SQL in `tx.query`, copies of a payload that redaction does not reach (dead row versions,
WAL, backups, CDC consumers; see the design notes), and the pool's own configuration.

## Supported versions

The latest 1.x release.
