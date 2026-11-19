# ADR 0008: Silver layer with dbt on Trino

- Status: accepted

## Context

Bronze holds the feeds as delivered (ADR 0005) and the core banking as a log of
changes (ADR 0006). Nothing downstream can use them as they are: bank ids are
written three ways, card amounts are minor units of a numeric currency code, a
customer is a stream of change events rather than a row, the same person can hold
two customer records, and every table carries names and identity documents that
analysts must not see (law 09-08, CNDP). Silver turns bronze into conformed,
tested tables that the AML rules (gold) and the compliance reports read. It must
be rebuilt from bronze at any time, load one business date per pipeline run, and
never publish a date whose silver failed its checks.

## Decision

1. **dbt (dbt-core 1.11, dbt-trino 1.11) on Trino** in `transform/`. SQL models,
   tests and documentation in one versioned project. dbt runs in its own image
   (`infra/dbt`, hash-pinned requirements), started by Airflow like the Spark jobs,
   with the project mounted read-only.
2. **dbt only writes the staging branch.** Its single target is the
   `lakehouse_etl` catalog (`etl`). The daily DAG becomes:

   ```
   ingest_* -> audit_bronze -> build_silver (seeds, models) -> audit_silver (data tests) -> publish
   ```

   Building and testing are separate tasks for the same reason ingestion and audit
   are: models retry, tests do not. Bronze is audited before silver reads it.
3. **Dated tables load one business date per run.** `payment_transfer` and
   `card_transaction` are incremental, partitioned by `business_date`; a run
   deletes its date (a partition delete, no rewrite) and inserts it again, so a
   replay replaces. Without a date (`make silver`) every date is rebuilt. Their
   uniqueness tests cover the loaded date only: earlier dates were tested when
   they were loaded.
4. **Core banking history is derived from the change log, not snapshotted.**
   `customer_history`, `account_history` and `card_history` are type 2 histories
   rebuilt from bronze on every run (one macro, `scd2_versions`):
   - grain is the business day: a version is the row's state at the end of
     `valid_from`, until `valid_to` (exclusive, null while current);
   - a change's business date is the business date of the pipeline run that
     captured it, recorded in `audit.change_batch.business_date`. Commit times are
     the wall clock of the capture, which during a replay is years after the business date. A
     snapshot read dates from the row's `updated_at`;
   - a key's events are ordered by Kafka offset (a key never changes partition:
     change topics are never repartitioned, ADR 0006);
   - a version starts only when a tracked column or the deleted state changes,
     which folds the duplicates of an at-least-once delivery or a repeated
     snapshot, and changes to columns silver does not keep.
5. **Personal data is split by zone.** `silver` keeps internal keys
   (`customer_id`, `account_id`) and analytical attributes only: birth year, not
   birth date; city, not address; no names, documents, phone, email, RIB or PAN
   digits. `restricted.customer_identity` holds the direct identifiers for
   sanctions screening and investigations. Trino file-based access control
   (`infra/trino/rules.json`) enforces it: `analyst` and `bi` read published
   `silver` and `reference` only; `compliance` also reads `restricted`; neither
   reaches `lakehouse_etl`, so unpublished data stays invisible; unknown users get
   nothing.
6. **Party resolution (customer 360) in SQL, deterministic, measured.**
   `customer_match` links two current customer records with explainable rules:
   persons with the same CIN or passport and a similar name or the same birth
   date; persons with a similar name (normalized, Levenshtein similarity >= 0.8),
   the same city and the same birth date, also with day and month swapped;
   organizations with the same tax id (ICE). Trade register numbers are not a rule:
   they are unique only within one commercial court. `party_member` takes the
   connected components by label propagation over a fixed number of rounds, with a
   test that fails if they did not converge; `party` is one row per party.
   `make evaluate-parties` scores the published result against the ground truth
   (pairwise precision and recall).
7. **Contracts on every model.** Column names and types are enforced by dbt at
   build time (`contract: enforced`), so a change that would alter what gold reads
   fails the build instead of the consumer.

## Alternatives considered

| | Kept | Rejected because |
|---|---|---|
| Transformation tool | dbt on Trino | Spark SQL jobs: no tests, documentation or lineage without building them; the silver logic is joins and windows that Trino runs in seconds. SQLMesh: fewer teams use it. |
| Customer history | Derived from the change log | dbt snapshots compare the current state at each run: they miss the intermediate states of a day, depend on when dbt ran, and cannot be rebuilt. The change log has every committed state. |
| Effective date of a change | Business date of the capturing run | Commit time: right in production, wrong during any replay, and SCD2 versions dated by the wall clock would never apply to the replayed transactions. |
| Party resolution | SQL rules in dbt | Spark job with Splink (probabilistic linkage): justified at tens of millions of records or with no shared identifiers; here 168k records, and deterministic rules reached precision 1.00 and recall 1.00 on the 1,641 planted duplicates, each match explainable to an investigator. |
| Pseudonymisation | Zones + access rules | Hashed or tokenised identifiers in silver: a hash of a CIN is reversible by brute force over the CIN space, and a token vault adds a stateful service for a key (`customer_id`) that is already meaningless outside the bank. Trino column masks: the identifiers would still sit in tables analysts can list. |
| Incremental strategy | Delete the date, append | `merge` on the transfer id: a full join of the date against the whole table. dbt `microbatch`: its batches run up to the current day, more than four years of empty batches here. |

## Consequences

- One dbt thread: two rewrites of the large dated tables at once exceeded Trino's
  memory. A full rebuild takes about 2 minutes 40 seconds, one business date about
  45 seconds.
- Rewriting the 5 million transfers keeps a Parquet writer and an S3 upload per
  business date open. With 2 GB and a 60% heap the kernel killed Trino; SeaweedFS
  was killed at 512 MB. Trino now has 3 GB with half as heap and 700 MB per query,
  without the image's in-heap file cache; SeaweedFS has 1 GB.
- The first party resolution scored precision 0.15: shared trade register numbers
  merged unrelated companies, and CIN numbers shared by different people merged
  them too. Requiring corroboration fixed both. The rules are measured against the
  defects the source generator plants; a new kind of defect needs a new measure.
- Access rules apply to Trino only. Spark writes through Nessie and S3 directly;
  in production the same split needs Nessie authorization and storage policies.
- A fresh environment can build silver before any delivery: `make bronze-tables`
  creates the bronze and audit tables empty.
