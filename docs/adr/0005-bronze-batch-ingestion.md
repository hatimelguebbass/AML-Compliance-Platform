# ADR 0005: Bronze batch ingestion of the file feeds

- Status: accepted

## Context

Three sources arrive as files: the daily payments hub CSV with its manifest,
the daily card processor file with its trailer, and the UN and OFAC sanctions
snapshots. Each carries a completeness signal and a contract (ADR 0004). The
bronze layer must hold what was delivered, be safe to re-run after any failure,
and never let consumers of `main` see a partial load (ADR 0001).

## Decision

1. **Spark jobs write only through the `lakehouse_etl` catalog**, i.e. the Nessie
   branch `etl`. A run is published by merging `etl` into `main` after the audit
   step passes (`make bronze`: reset `etl`, ingest, audit, publish).
2. **Bronze schemas come from the contracts.** Types are exact: amounts are
   `DECIMAL(p,s)` (a number without declared precision is refused), source
   timestamps without zone are `TIMESTAMP_NTZ`, nested XML lists are arrays of
   structs. Values are kept as delivered; normalisation is silver's job. Every
   row carries lineage columns: `_source_file`, `_ingest_run_id`, `_ingested_at`,
   plus `_business_date` (daily feeds) or `_list_published` / `_retrieved_at`
   (sanctions snapshots).
3. **A file is accepted or rejected as a whole.** The driver checks what is
   cheap and file-level: name, manifest (name, SHA-256), CSV header, card
   header and trailer presence. Spark then reads every value as text, casts it
   with `try_` functions and counts violations in one pass: malformed records,
   values not of their type, missing required values, pattern and allowed-value
   breaches, record count against manifest or trailer, repeated keys. Any
   violation rejects the file: a copy and a rejection report go to
   `data/quarantine/`, the rejection is recorded, and the other files continue.
4. **The landing zone is mounted read-only.** Files are never moved; a corrected
   redelivery is recognised by its new checksum.
5. **Re-running is idempotent, per feed semantics.** Payments: a file replaces
   its business-date partition. Cards: records are inserted only if their
   `txn_ref` is not already in the table, so resends within a file or across
   files add nothing. Sanctions: a snapshot replaces the rows with the same
   `_retrieved_at`. In addition, a file whose `(name, SHA-256)` is already in
   the registry is skipped.
6. **The registry is `audit.ingestion_file` on the same branch as the data**, with
   one row per file and target table, written right after the data commit.
   Discarding a staging run therefore also forgets the files it processed.
   `audit.ingestion_run` holds one row per run and feed.
7. **The audit step checks each bronze table as a whole** before publishing: keys
   are unique across files, and the rows of every source file match what the
   registry says was written. Results are kept in `audit.publish_check`.
8. **The Spark image's Python 3.10 is the jobs' interpreter.** The ingestion
   package and the modules it imports are held to 3.10 by a per-file ruff
   target; the only added dependency, PyYAML, is installed from a pinned wheel.

## Alternatives considered

| Option | Why not |
|---|---|
| `inferSchema` / schema-on-read | A producer changing a type silently widens the column instead of being caught |
| All-string bronze, typing in silver | The contracts already declare the types; deferring the cast only moves the failure to a layer that cannot reject the file |
| Load valid rows, quarantine invalid rows | A partial business day passes downstream as complete; file exchanges between banks and processors are accepted or rejected whole |
| `FAILFAST` read mode | Stops at the first bad record and cannot say how many records are affected |
| Structured Streaming file source (`availableNow`) | Its checkpoint is a second registry outside the Nessie branch, and it has no notion of a manifest or trailer gating a whole file |
| Moving processed files out of the landing zone | Needs write access to the sender's drop zone and is not atomic across mounts; checksums already identify redeliveries |
| A data quality framework (Great Expectations, Soda) | The checks are derived from the contracts in about a hundred lines; a framework is reconsidered for the reliability step |

## Consequences

- One bad record holds back a whole business day until the sender redelivers.
  This is the expected operating model and shows in `audit.ingestion_run`.
- The first ingestion of the real UN list was rejected: nine `LISTED_ON` values
  carry an `xs:date` zone offset (`2015-07-01-04:00`). The contract was wrong,
  not the data; it now declares the field as text with a pattern, and silver
  takes the first ten characters as the date.
- Loading the full history showed the Trino container being killed by the
  kernel on large scans: the image sizes the heap at 80% of the container
  limit. The heap is now 60%, so a too-large query fails with a memory error
  instead of taking the server down.
- The card anti-join reads every `txn_ref` already in bronze; at 100,000 records
  a day this stays small, and it can be bounded by business date if volumes grow.
