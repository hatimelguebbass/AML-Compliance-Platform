# ADR 0007: Orchestration of the daily load with Airflow

- Status: accepted

## Context

Until now an operator ran `make bronze`: reset `etl`, ingest every landed file and
change event, audit, publish. A bank needs this to happen every day without
anyone at a keyboard, and needs to know, for each business date, whether it was
published, when, and what went wrong if not. Three properties matter more than
the scheduler itself:

- **A business date is published complete or not at all.** The payments feed
  signals completeness with a manifest; ingestion skips a file whose manifest is
  missing and still succeeds (ADR 0005). Started too early, a run would publish
  a date without its payments.
- **Recovery is routine.** A transient failure (a container that cannot start,
  Kafka restarting) must be retried; a failed audit must stop the run and wait
  for a person. Resuming must not reload what already succeeded.
- **History can be replayed.** A corrected redelivery of an old date, or a new
  environment, needs past business dates loaded again, in order, with the same
  code path as the daily run.

The platform runs on one host (ADR 0001): every service is a container, and the
Spark jobs run in the Spark image with its own Python 3.10 (ADR 0005).

## Decision

1. **Apache Airflow 3.3** (scheduler, DAG processor and API server, LocalExecutor,
   metadata in the platform PostgreSQL), under the compose profile `airflow`.
   3.3.2 rather than 3.4.0: the newer minor was a week old when this was written.
2. **One DAG, `aml_daily`, per business date.** The timetable is a daily cron with
   data intervals; the run whose logical date is D processes business date D,
   just after D ends:

   ```
   wait_for_deliveries -> open_staging -> ingest_{payments,cards,sanctions,corebanking} -> audit_bronze -> publish
   ```

   The payments and card jobs take `--business-date D` and read only that date's
   files. Sanctions snapshots and change events are not dated; each run takes
   whatever arrived since the last publish.
3. **A sensor waits for the completeness signals** of D (payments manifest, card
   file) in `reschedule` mode, so a waiting run holds no worker slot. After 12
   hours it fails: missing files are an incident, not something to wait out.
4. **Spark jobs run as containers started by Airflow** (Docker operator), with the
   same image, mounts and memory limit as the compose `spark` service. The
   Airflow image stays the official one, without Spark, Java or the platform's
   dependencies; the job code cannot conflict with Airflow's. Airflow reaches the
   Docker API through a socket proxy that only exposes containers, images and the
   version endpoint. The `spark` pool has one slot: one 2 GB driver at a time.
5. **Retries by failure type.** Ingestion tasks retry twice with exponential
   backoff: they are idempotent (ADR 0005, ADR 0006), so a retry can only finish
   the work. The audit never retries: a failed check is a data problem.
6. **Exclusive use of the staging branch.** `max_active_runs=1` does not cover
   it: Airflow counts backfill runs apart from scheduled and manual ones, so a
   backfill and the daily run can be active together, and one would reset `etl`
   under the other. `open_staging` is a sensor that resets `etl` only when no
   other active run sits between `open_staging` and `publish`; otherwise it
   waits. The rule lives in `aml_platform.orchestration.staging`, unit tested.
7. **The audit checks that the branch holds the whole run.** Ingestion runs record
   `<airflow run id>#<attempt>` as run id; `audit --pipeline-run` fails unless every
   feed of that Airflow run has a successful ingestion on `etl`. This catches the
   case the guard cannot: an operator resuming a failed run after another run has
   reset the branch, which would otherwise publish a date with feeds missing.
   With `--business-date D`, it also fails unless D holds an accepted payments
   file and an accepted card file, and no file the run rejected came after them:
   a rejected file leaves its ingestion successful and D without the feed
   (added after the card file format game day, see
   [the postmortem](../postmortems/card-date-format.md)).
8. **Backfill with Airflow's own backfill** (`make airflow-backfill`), one run at
   a time. Each date goes through the same tasks and the same audit as a daily run.

## Alternatives considered

| | Kept | Rejected because |
|---|---|---|
| Scheduler | Airflow | Dagster: asset-centric model fits well, but fewer bank and ESN teams run it, and the DAG here is a fixed pipeline, not an asset graph. Cron + Makefile: no retries per step, no history per business date, no backfill. |
| Executor | LocalExecutor | CeleryExecutor needs Redis and workers for a single host. KubernetesExecutor needs a cluster (not part of this platform). |
| Running Spark | Container per job | SparkSubmitOperator: Airflow image would need Java and Spark, and a standalone Spark master would hold memory all day. Spark Connect: the jobs use the driver's local filesystem (landing zone, quarantine). Installing PySpark in Airflow: Python 3.12 vs the image's 3.10, and two copies of the job dependencies. |
| Docker access | Socket proxy | Mounting `/var/run/docker.sock` in Airflow: every endpoint exposed, and the socket's group differs per host, so the `airflow` user often cannot open it. |
| Serialising runs | Guard on the branch | One task for the whole run: loses per-feed retries and resume. A Nessie branch per run: concurrent publishes would then conflict at merge time instead, and ADR 0001 fixed one staging branch. |
| Partial publish | Refuse | Publishing the feeds that arrived and the rest later: consumers would see a date half loaded, which is exactly what write-audit-publish exists to prevent. |

## Consequences

- Starting containers through the Docker API is equivalent to root on the host,
  proxy or not. This is acceptable on a single analytics host that only runs this
  platform; on a shared cluster the jobs would run as Kubernetes pods
  (KubernetesPodOperator) with a service account limited to one namespace.
- Unpausing the DAG starts the daily schedule: the run for the day that just ended
  waits for files that will never come for the current date and fails after 12 hours. Replays
  of the 2022 dates go through backfill.
- Airflow is the single writer of `etl` once the DAG is unpaused. `make bronze`
  remains for development and must not run while a pipeline run is active.
- The LocalExecutor runs task processes inside the scheduler container, about
  300 MB per running task. With a 1 GB limit the kernel killed task supervisors
  during the first backfill, leaving tasks marked running with no process behind
  them until Airflow's heartbeat timeout. The scheduler has 2 GB and
  `parallelism` is 4.
- Airflow's metadata database is in the platform PostgreSQL, next to Nessie's,
  with its own role and database. Run history, retries and task logs (volume
  `airflow-logs`) are the operational record; `audit.*` tables remain the data
  record, linked to Airflow by run id.
- Silver and gold transformations (dbt, later steps) become tasks between the
  ingestions and the audit; the audit then covers them before the same publish.
