# Runbook: daily AML pipeline

For whoever is on duty for the platform. Each section starts from what you see
(a failed task, a check, a symptom) and ends with the date published or handed
over. Commands run from the repository root, with the stack and Airflow up.

## What a healthy day looks like

The run of business date *D* starts at midnight UTC, waits for the payments
manifest and the card file of *D*, and ends with `publish`: `main` then holds
*D* in every layer, and the compliance report shows it after its next refresh.
A published date has passed every audit; nothing partial ever reaches `main`
([ADR 0001](adr/0001-lakehouse-on-iceberg-nessie-trino.md)).

If a run fails, `main` keeps the previous date. The report stays correct but
late: tell compliance that *D* is not published yet, and when you expect it.
Never publish `etl` by hand to make a date appear.

## Where to look

| Question | Where |
|---|---|
| Which task failed, and its log | Airflow, http://localhost:8081, run of the date; `make airflow-status` |
| Files accepted, rejected, skipped per run | `audit.ingestion_run`, `audit.ingestion_file` (on `lakehouse_etl` for the failed run) |
| Why a file was rejected | `data/quarantine/<feed>/<sha256 prefix>_<file>.rejection.json` |
| Which bronze check failed | `audit.publish_check` where `NOT passed` |
| Which dbt test failed | log of the `audit_*` task; `audit.dbt_node_run` |
| Volumes, reconciliations, drift of a date | `observability.*`, or the report's Data reliability page |
| Change data capture | `make cdc-status` |
| A container that disappeared | `dmesg \| grep "Killed process"` (the kernel kills on memory limits, without a log line in the service) |

```sql
-- make trino, then:
SELECT run_id, source, file_name, status, reason
FROM lakehouse_etl.audit.ingestion_file
WHERE run_id LIKE '<airflow run id>#%';

SELECT check_name, target_table, detail
FROM lakehouse_etl.audit.publish_check
WHERE NOT passed ORDER BY checked_at DESC;
```

## Resuming a run

Ingestion is idempotent and every step works on the staging branch `etl`, so a
failed run resumes where it stopped:

| To | Do |
|---|---|
| Re-run the failed step and what follows | Clear the failed task with its downstream tasks (Airflow UI, or `make airflow-clear DATE=<D> TASK=<task>`) |
| Re-read a corrected delivery | Clear the ingestion task of that feed with its downstream tasks: files already accepted are skipped, the new one is read |
| Start the date again from `main` | Clear the whole run: `open_staging` resets `etl` to `main` |
| Reload a published date | `make airflow-backfill FROM=<D> TO=<D> REPROCESS=completed` |

Audit tasks never retry: a failed audit means the data is wrong and needs a
person. Build and ingestion tasks retry with backoff, since their failures are
mostly transient.

## Procedures

### 1. `wait_for_deliveries` still waiting, or timed out after 12 hours

A feed of *D* has not landed completely. The task log names what is missing:
the payments file, its manifest, or the card file.

1. Look in `data/landing/payments` and `data/landing/cards`. A payments file
   without its manifest is still being transferred; never write a manifest
   yourself.
2. Ask the sender (payments hub, card processor) for the delivery, and tell
   compliance that *D* will be late.
3. Once delivered, nothing to do while the sensor waits. After a timeout,
   clear `wait_for_deliveries`.

### 2. A file is rejected

Seen as: `audit_bronze` fails on `business_date_delivered:<feed>` with
`<file> rejected: <reason>`. The ingestion task itself succeeded: it recorded
the rejection and copied the file to quarantine. A file is accepted or rejected
as a whole ([ADR 0005](adr/0005-bronze-batch-ingestion.md)), so nothing of it
is in bronze.

1. Read the reason in the check detail or in the `.rejection.json` next to the
   quarantined copy:

   | Reason | Meaning |
   |---|---|
   | `<column>:not_<type>=<n>` | `n` values do not parse as the contract's type: a format change at the sender |
   | `<column>:missing=<n>`, `pattern_mismatch`, `invalid_value` | Values break the contract's rules |
   | `trailer missing: the transfer was cut short` | Incomplete transfer |
   | `trailer declares … file holds …` | Records lost or added after the trailer was written |
   | `unsupported format version` | The sender announced a new layout |
   | `sha256 … differs from manifest` | Payments file changed after its manifest was written |
   | `<key>:duplicate_key=<n>` | Same key with different content within the file |

2. Confirm on the quarantined copy, never on the landing zone, which belongs
   to the sender: `sed -n 1,3p data/quarantine/<feed>/<copy>` shows the header
   and the first records.
3. Do not fix the file. Ask the sender for a corrected delivery, quoting the
   reason and the count. If the change is intended on their side (new layout),
   it needs a new contract version first, then a new format version in the
   files ([ADR 0004](adr/0004-source-feeds-and-data-contracts.md)).
4. When the corrected file lands (same name or next sequence number), clear the
   ingestion task of the feed with its downstream tasks. The registry skips the
   rejected checksum and reads the new one; the check passes once a file of *D*
   is accepted after the rejection.
5. Write a postmortem if the date was published late
   ([template](postmortems/README.md)).

### 3. `audit_bronze` fails on another check

| Check | Meaning | Action |
|---|---|---|
| `pipeline_run_complete` | A feed of this run has no successful ingestion on `etl`: the branch was reset by another run, or an ingestion task was skipped | Clear the whole run |
| `business_date_delivered:<feed>` with `no accepted <feed> file` | Nothing of the feed was accepted for *D* | Procedure 1 or 2 |
| `unique_key` | A key appears twice in a bronze table | Stop. A defect in ingestion: find the files involved in `_source_file`, open an issue, do not publish |
| `matches_registry` | A table holds a different number of rows than the registry says it wrote | Stop, same as above. Usually a write interrupted between table and registry; clearing the whole run is safe |
| `matches_change_batches` | Change events stored differ from the Kafka ranges read | Clear the whole run. If it persists, see procedure 6 |

### 4. A dbt build fails (`build_silver`, `build_gold`, `build_observability`)

Retried twice by Airflow. If the last attempt fails:

- A container killed (`exit code 137`, or nothing at the end of the log): check
  `dmesg`. Trino and SeaweedFS have hard memory limits on a 16 GB workstation;
  a heavier date than usual can exceed them. Clear the task once nothing else
  runs. If it fails again, the date needs the limits in `docker-compose.yml`
  reviewed.
- A SQL or contract error: a model no longer matches its upstream (column
  renamed, type changed). Fix in a pull request; the CI runs the dbt build on
  the stack. Then clear the task.

### 5. A dbt audit fails (`audit_silver`, `audit_gold`, `audit_observability`)

The data built for *D* is wrong or suspicious; never clear the audit without
understanding it.

| Failed test | Action |
|---|---|
| `no_blocking_volume_anomaly` | Volume of payments or cards for *D* outside [0.5, 2] times the median of comparable days. Check the feed first (procedure 2 if a file was rejected). If the data is right (holiday, migration at the sender), record it in [acknowledged_anomaly.csv](../transform/seeds/acknowledged_anomaly.csv) with the reason and your name, in a reviewed pull request, then clear `build_silver` with downstream so the seed is reloaded |
| Layer reconciliation (`is_reconciled`) | Records or amount totals differ between file, bronze and silver for *D*: a defect in a model. Do not publish; compare `layer_reconciliation` with the registry |
| Core banking reconciliation | Silver differs from the core banking database as of the last captured change. Check `make cdc-status` (procedure 6); if capture is healthy, a history model is wrong |
| Unique keys, relationships, accepted values in silver or gold | The upstream data changed shape. Find the rows with the test's compiled SQL in the task log |

Warnings (`no_volume_anomaly` on alerts, `distribution_drift`) do not block the
publish. Read them the next morning with compliance: a shift in alerts by rule
can be real activity or a rule that no longer fits.

### 6. Change data capture

Symptoms: `ingest_corebanking` writes nothing for several days, the core banking
reconciliation fails, or the replication slot lag grows.

1. `make cdc-status`: connector and task state, events per topic, slot lag.
2. Connector `FAILED`: read the task trace in `docker logs aml-kafka-connect`.
   A Kafka Connect container killed by its memory limit restarts its snapshot,
   which writes rows twice; silver removes duplicates by position, so
   downstream stays right.
3. Slot lag growing while the connector is stopped: PostgreSQL keeps the write-
   ahead log for the slot and fills its disk. Restart the connector before
   anything else; `make cdc-reset` drops the slot as a last resort.
4. After `make cdc-reset` and `make cdc`, the next run ingests a full new
   snapshot. Expect the core banking reconciliation of that date to pass and
   the history models to show no change for rows that did not change.

### 7. `open_staging` waiting

Another active run, often a backfill, holds `etl`. It opens when that run
publishes or fails. A failed run that nobody resumes holds nothing; one stuck
in `running` does: mark it failed in the UI if it is not coming back.

### 8. The compliance report does not refresh

- Credentials refused: the `bi` password changed in `.env`. Update it in
  Power BI (File, Options and settings, Data source settings).
- Certificate error: the Trino key pair was regenerated (new `trino-security`
  volume). Same place: clear the permissions of `localhost:8443` and connect
  again.
- Figures differ from `make bi-figures` for the same dates: refresh the model;
  if they still differ, `make bi-check` tells whether the report and the dbt
  contracts disagree.

## After an incident

Every incident that delays or corrects a published date gets a blameless
postmortem in [docs/postmortems](postmortems), from the
[template](postmortems/README.md): timeline in UTC, impact on published data,
detection, root cause, what went well, and actions with an owner. Add any
procedure you had to invent here.
