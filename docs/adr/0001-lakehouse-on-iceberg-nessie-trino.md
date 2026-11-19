# ADR 0001: Lakehouse on Iceberg, Nessie and Trino with branch-based write-audit-publish

- Status: accepted

## Context

The compliance team consumes alerts, a customer 360 view and sanctions hits. Two
requirements drive the storage design:

1. **Auditability.** An examiner can ask why a customer was flagged on a given
   date. The platform must reproduce the exact data that existed at that time.
2. **No partially published data.** A run that loads half of a card file, or
   fails a quality check, must never reach the dashboard. A false negative in AML
   is a regulatory finding; a false positive costs analyst hours.

The platform runs on-premise. Customer data cannot leave the bank's data centre
(law 09-08), which rules out managed cloud warehouses for this workload.

## Decision

- **Table format: Apache Iceberg.** ACID commits, snapshot history for time
  travel, schema and partition evolution without rewrites. Open and read by
  Spark, Trino, Snowflake, BigQuery and Databricks, so the data is not locked to
  one engine.
- **Catalog: Nessie**, exposed through its Iceberg REST endpoint. Nessie adds
  Git-like branches and commits spanning all tables.
- **Write-audit-publish on a fixed staging branch `etl`.** Each run resets `etl`
  to `main`, writes there, runs audits there, and merges into `main` only if
  every audit passes. The merge is atomic across tables, so readers see either
  the previous run or the complete new one. Pipelines run one at a time
  (`max_active_runs=1`), so a single named branch is enough and keeps engine
  configuration static.
- **Engines.** Spark for heavy ingestion and entity resolution, Trino for SQL
  transformations (dbt) and BI access. Both expose two catalogs over the same
  repository: `lakehouse` (main, published) and `lakehouse_etl` (staging).
- **Layout.** Namespaces `bronze` (raw, replayable), `silver` (cleaned,
  conformed), `gold` (business-facing), `audit` (quality results and run
  metadata).

## Alternatives considered

| Option | Why not |
|---|---|
| PostgreSQL as the warehouse | Single engine, storage and compute coupled, weak at multi-million-row scans; no table-level time travel |
| Delta Lake | Strong on Databricks; weaker multi-engine support on-premise and no multi-table branching without Databricks features |
| Hive Metastore or JDBC catalog | No branches; WAP would need per-table snapshot tricks and could not publish several tables atomically |
| Iceberg branches (per table) | Branching per table does not give a consistent multi-table publish |
| lakeFS | Branches at the object-store level work, but add a service; Nessie is already the catalog |
| Per-run branch names | More isolation than needed with serialized runs, and requires dynamic catalog configuration in Trino |

## Consequences

- Nessie sets `gc.enabled=false` on tables because data files can be shared
  between branches. `DROP TABLE ... PURGE` is refused; unreferenced files are
  removed by Nessie GC, to be scheduled as a maintenance job.
- `main` must have at least one commit before the first merge. The bootstrap
  step creates the namespaces on `main`, which provides it.
- Nessie authentication is disabled in the local stack. A production
  deployment enables OIDC and restricts merges to `main` to the pipeline
  service account.
