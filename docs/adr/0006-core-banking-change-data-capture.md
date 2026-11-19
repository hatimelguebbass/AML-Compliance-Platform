# ADR 0006: Change data capture of the core banking into bronze

- Status: accepted

## Context

Customers, accounts and cards live in the core banking PostgreSQL database
(ADR 0004). AML rules need their state at the time of each transaction:
a customer re-rated to high risk, designated as politically exposed, or an
account closed the day after a transfer. Bronze must therefore keep **every
version** of these rows, including deletes, in commit order, without loading
the production database, and with the same guarantees as the file feeds:
re-runs add nothing, and `main` never shows a partial load (ADR 0001, ADR 0005).

## Options

| | Full extract | Incremental on `updated_at` | Log-based CDC (Debezium) |
|---|---|---|---|
| Load on the source | Whole tables every run | Index range scan every run | Reads the WAL already written; one snapshot at start |
| Deletes | Only by comparing two extracts | Invisible | Captured, with the deleted row |
| Intermediate versions | Lost (one image per run) | Lost (one image per run) | Every committed version |
| Late commits | — | A transaction that commits after the poll but stamped before the watermark is skipped for good | Ordered by commit (LSN) |
| Depends on application discipline | No | Yes: every writer must maintain `updated_at` | No |
| Run cost as tables grow | Grows with table size | Grows with changes, plus the scan | Grows with changes |
| Moving parts | None | A watermark store | Kafka, Kafka Connect, a replication slot |

A full extract stays the right tool for small reference tables and for a
periodic reconciliation of the lakehouse against the source. Polling `updated_at` is rejected for an audit
trail: it misses deletes and intermediate states by construction.

## Decision

1. **Debezium PostgreSQL connector, `pgoutput` plugin, on Kafka Connect**
   (ADR 0003). `pgoutput` is built into PostgreSQL 10+; no extension to install
   on a bank's database server.
2. **The database owner prepares the capture; Debezium gets least privilege.**
   `aml-cdc setup` creates the role `debezium` (`LOGIN REPLICATION`, `SELECT`
   on the captured tables only), the publication with an explicit table list
   (`publication.autocreate.mode=disabled`), and sets
   **`REPLICA IDENTITY FULL`** on the captured tables. With the default
   identity a delete carries only the primary key and Debezium fills the other
   `NOT NULL` columns of the old image with zero values (an empty card token);
   a full identity puts the real deleted row in the event. The extra WAL is
   negligible on master data.
3. **No secret in connector configs.** They reference `${env:CDC_DB_PASSWORD}`,
   resolved by the worker's `EnvVarConfigProvider`, which is restricted to an
   allow-list of variables.
4. **Topics are declared** (`aml_platform.cdc.topics`; broker auto-creation is
   off): `corebanking.core.{customer,account,card}`, 3 partitions each, keyed by
   primary key, `cleanup.policy=compact` with `min.compaction.lag.ms` and
   `delete.retention.ms` of 7 days. Every change stays in the topic for a week,
   the time bronze ingestion has to catch up, after which the topic keeps the
   latest version of each row, enough to rebuild state without a new snapshot.
   The partition count is never increased: a row's later changes would land in
   another partition and lose their order. A heartbeat topic lets the slot's
   position advance while the tables are idle, so PostgreSQL can recycle WAL.
5. **JSON events without inline schema; the contract is the schema.** The
   ingestion job derives the Debezium wire types from the contract (dates as
   epoch days, `timestamptz` as ISO-8601 strings) and converts them to the
   contract's types.
6. **Bronze is an append-only change log per table** (`bronze.corebanking_*`):
   the contract columns of the row image (the new row, or the deleted row) plus
   `_op` (`r` snapshot, `c`, `u`, `d`), `_snapshot`, `_source_lsn`,
   `_source_tx_id`, `_source_committed_at`, `_kafka_topic`, `_kafka_partition`,
   `_kafka_offset`, `_ingest_run_id`, `_ingested_at`. Envelope unwrapping
   (`ExtractNewRecordState`) is not used: it drops the operation and the source
   position that silver needs to order and interpret versions.
7. **The consumed position is stored in the data.** A run starts each partition
   after the highest `_kafka_offset` in the bronze table and reads up to the end
   offsets observed at the start (the read is materialised, so checks and write
   see the same events). Data and position are written in one Iceberg commit:
   no crash can separate them, and resetting the `etl` branch rewinds both.
   `audit.change_batch` records each partition range read.
8. **A contract breach stops the line.** A change event cannot be quarantined
   like a file: later events of the same row build on it. The job counts, per
   topic: unparseable events, unknown operations, events from another table,
   events whose field set differs from the contract (a column added or dropped
   at the source), values of the wrong type, missing required values, and
   Debezium's placeholder for unavailable TOAST values. Any of them fails the
   run before writing; the events stay in Kafka until the contract (or the
   source) is fixed.
9. **Audit before publish** adds, per change log: the position
   `(_kafka_topic, _kafka_partition, _kafka_offset)` is unique, and row counts
   reconcile with `audit.change_batch` per partition.

## Alternatives considered

| Option | Why not |
|---|---|
| Avro with a schema registry | One more service and its RAM on a 16 GB workstation; schema drift is already caught against the contract, which is the reviewed definition |
| Spark Structured Streaming with a checkpoint | The checkpoint is a second position store outside the Nessie branch: a discarded `etl` run would leave it ahead of the data (same reason as ADR 0005) |
| Kafka Connect Iceberg sink | Writes straight to the tables, outside write-audit-publish and without the contract checks |
| Debezium Server (no Kafka) | No retained, replayable log: a failed downstream write cannot be re-read |
| Deduplicating in bronze | Bronze keeps what was delivered; silver keeps the last event per key and position |

## Consequences

- **Delivery is at least once.** If the Connect worker dies during the initial
  snapshot, Debezium restarts the snapshot and the topic holds rows twice
  (observed: a worker killed mid-snapshot produced 335,696 customer events for
  167,848 rows). Silver keeps one version per key and LSN.
- **Truncates are not captured** (Debezium default). Reloading the source
  (`make data-corebanking REPLACE=1`) requires `make cdc-reset` then `make cdc`,
  which takes a new snapshot; bronze rows of the old load must be dropped with it.
- **A stopped connector holds WAL.** The slot keeps WAL until Debezium confirms
  it; `make cdc-status` shows the lag in bytes. In production
  `max_slot_wal_keep_size` caps it and the lag is alerted on.
- **Lag beyond a week loses intermediate versions** (compaction), never the
  latest one. Ingestion logs a warning when a range it expected is gone.
- **Change timestamps are processing time.** `_source_committed_at` is when the
  change was committed in the source, which silver uses as the start of a
  version's validity.
