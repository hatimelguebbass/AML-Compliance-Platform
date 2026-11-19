# Card processor release changes the transaction time format

| | |
|---|---|
| Business dates affected | 2022-09-08 |
| Severity | SEV2: date published late, no wrong data published |
| Status | Resolved; two actions open |
| Kind | Game day: the defect was injected with `aml-sources cards --defect 2022-09-08:date-format` |

## Summary

A card processor release wrote the transaction local time as
`YYYY-MM-DD HH:MM:SS` instead of `YYYYMMDDHHMMSS`, without changing the format
version in the header. Ingestion rejected the file of 2022-09-08 as the
contract requires, but recorded the rejection as a successful run: the date
went through silver and gold without card transactions and was only held at
the volume check, at the end of the pipeline, with a message about volumes
rather than about the file. Nothing wrong reached `main`. The bronze audit now
fails on the rejected file itself, 1 minute 31 seconds after it is read, before
any model is built.

## Impact

- Published data: none affected. `main` kept 2022-09-07 until the corrected
  file was published.
- Business date 2022-09-08 published 23 minutes after its run started, instead
  of about 5 minutes; most of that time was the fix of the audit.
- Had it been published, the date would have held none of its 100,000 card
  transactions, and every later volume baseline would have counted that day.

## Timeline (UTC)

| Time | Event |
|---|---|
| 17:24:40 | `CARDTXN_20220908_001.dat` lands, header unchanged (`H\|CARDTXN\|01\|…`), 100,000 records with the new time format |
| 17:24:43 | Run of 2022-09-08 starts; deliveries complete, `etl` reset to `main` |
| 17:25:47 | `ingest_cards` rejects the file: `txn_local_time:not_timestamp=100000`; copy and reason in `data/quarantine/cards/`. The task succeeds |
| 17:27:21 | `audit_bronze` passes: every feed has a successful ingestion run |
| 17:27:52–17:28:57 | Silver, gold and the observability models build 2022-09-08 without card transactions |
| 17:29:07 | `audit_observability` fails: `silver.card_transaction` holds 0 records against a median of 100,000 (ratio 0.0). `publish` does not run. **Detected**, 3 min 20 s after the rejection |
| 17:30 | On-duty reads `observability.volume_check`, then `audit.ingestion_file`: the card file was rejected, not missing. `layer_reconciliation` has no row for cards on that date |
| 17:30–17:40 | Bronze audit extended to check each dated feed of the run's date; deployed |
| 17:40:21 | Whole run cleared, to confirm the new check on the same file |
| 17:41:12 | File rejected again |
| 17:42:43 | `audit_bronze` fails: `business_date_delivered:cards`, `CARDTXN_20220908_001.dat rejected: txn_local_time:not_timestamp=100000`. Nothing built after bronze |
| 17:43:18 | Processor redelivers the file in the agreed format |
| 17:44:25 | `make airflow-clear DATE=2022-09-08 TASK=ingest_cards` |
| 17:44:53 | File accepted, 100,000 records |
| 17:47:39 | 2022-09-08 published; volume ratio 1.0, cards reconcile from file to silver |

## Detection

The defect was caught at the first possible place, the contract check of the
file, but that check only quarantines: the run that held the rejection
succeeded, and so did the bronze audit, which looked at whether every feed had
run, not at whether the date had data. The first failing control was the volume
check, three steps later, and its message pointed at a volume, not at a file.
On a day when the card feed is small, or for a feed without a volume check, the
date would have been published incomplete.

With the change, detection is the bronze audit of the same run, 1 min 31 s after
the file is read, and the failure names the file and the contract violation.

## Root cause

1. The processor changed a field format without a new format version or
   contract version; the header announced version 01.
2. A rejected file is, by design, recorded and skipped so that one bad file
   does not stop the other files of a run ([ADR 0005](../adr/0005-bronze-batch-ingestion.md)).
   For the dated feeds of an orchestrated run, that leaves the business date
   without the feed and nothing failing.
3. The bronze audit of a pipeline run checked that each feed had a successful
   ingestion, which a run that rejected every file still is. Root cause: no
   control asserted that the business date being published holds an accepted
   file of each dated feed.

## What went well

- The contract rejected the file as a whole: no record with a wrong or null
  time reached bronze, and no partial day.
- Write-audit-publish held: `main` never saw 2022-09-08 without cards.
- The quarantine copy and its `.rejection.json` gave the exact reason without
  reading logs.
- Resume worked as designed: the redelivered file, with a new checksum, was read;
  the rejected one stayed in the registry as the audit trail.

## What did not

- The first signal was a volume anomaly, three builds after the cause.
- `observability.layer_reconciliation` reconciles accepted files only, so it
  reported nothing for a feed with no accepted file.
- The first `make airflow-clear` matched no task: Airflow 3.3 did not match an
  anchored task regex (`^ingest_cards$`). The target now passes the task id as
  is. A clear that matches nothing prints nothing; check the task states after.

## Actions

| Action | Type | Owner | Status |
|---|---|---|---|
| Bronze audit of a pipeline run fails when a dated feed has no accepted file for the business date, or when the run rejected one after the last accepted ([checks.py](../../src/aml_platform/ingestion/checks.py)) | Detect | Data platform | Done |
| `make airflow-clear` and runbook procedure for a rejected file ([runbook](../runbook.md#2-a-file-is-rejected)) | Mitigate | Data platform | Done |
| Integration test: date-format release rejected, date held, redelivery published | Prevent regression | Data platform | Done |
| Agree with the processor: any layout change ships with a new format version and a test file two weeks before release; the contract is versioned first | Prevent | Data platform with the card processor | Open |
| Route failed audit tasks to the on-duty channel (Airflow failure callback) | Detect | Data platform | Open |
