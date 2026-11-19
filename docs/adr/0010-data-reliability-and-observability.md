# ADR 0010: Data reliability: reconciliation, volume and drift checks, run metrics, regeneration

- Status: accepted

## Context

Every audit so far checks a layer against its own rules: a file against its
manifest or trailer and its contract (ADR 0005), a change log against the
offsets it read (ADR 0006), a model against its dbt tests (ADR 0008, 0009). None
of them catches a day that is valid at every step and still wrong: a feed
delivered complete but cut at the source, a join that stops matching and drops
rows between two layers, a silver table that no longer agrees with the core
banking it mirrors, or a rebuild that would not give back what was published.
In AML monitoring these failures are silent and costly: fewer transfers mean
fewer alerts, and nobody files a ticket for an alert that was never raised.

The IBM payments history shows two such days. Its first date carries 481,056
REINVESTMENT transfers, the opening positions, that no other date has. After
2022-09-10 it holds only the last transfers of chains already started: 11 to 396
a day against about 480,000 on a weekday. Every file of those dates passes its
contract and its manifest.

## Decision

1. **A fourth set of dbt models, `observability`, built and audited last.** The
   DAG runs `build_observability` then `audit_observability` after
   `audit_gold`, before `publish`. Their output is published with the date it
   checks, so `main` holds the checks of every date it serves. `make
   observability [DATE=]` runs them outside Airflow.
2. **Layer reconciliation** (`observability.layer_reconciliation`). For each
   business date and dated feed: records landed (accepted files, from
   `audit.ingestion_file`), in bronze and in silver, and the amount control
   totals of bronze and silver (payments: amount paid; cards: minor units, the
   unit of the processor's trailer). Any difference fails the audit.
3. **Core banking reconciliation** (`observability.corebanking_reconciliation`).
   Customers, accounts and cards in the core banking database against the
   current versions of their silver histories, compared on every column silver
   keeps, with the same derivations (birth year, normalized bank id). Trino reads
   the database through a `corebanking` catalog that only `dbt` may use,
   read-only, logged in as a role granted only those columns: names, identity
   documents and contact details stay out of reach (law 09-08). The lakehouse is
   as current as the last captured change, so source rows modified after that
   watermark less a 5-minute grace are counted in flight and left out. Missing,
   unexpected or mismatched rows fail the audit.
4. **Volume checks** (`observability.volume_check`). Each dataset's record count
   on the loaded date against the median of its baseline: the previous 5 loaded
   dates of the same kind, weekday or weekend, needing at least 2. Payments clear
   on working days, so a Saturday is half a weekday and compared with weekends.
   Outside [0.5, 2] × the median, payments and cards are a blocking anomaly that
   holds the publish; alerts, outside [0.25, 4], a warning. A dataset with no
   record on a loaded date counts as zero, so a rule that stops firing shows.
5. **Distribution drift** (`observability.distribution_drift`). The population
   stability index of each profiled dimension (payment format, currency, amount
   band in USD, resolved accounts; card channel, type, approval; alerts by rule)
   against the pooled baseline dates. From 0.25 it is a drift, reported as a
   warning only: a mix can change while the data is right, and the answer may be
   to retune a rule, not to hold the date.
6. **Acknowledged anomalies are data under review.** The seed
   `acknowledged_anomaly` lists investigated anomalies per dataset and date
   range, with the reason and who accepted them. A check reports them as
   ACKNOWLEDGED instead of failing, and leaves those dates out of later
   baselines, so a known outage does not become the new normal. Adding one is a
   reviewed commit; rows are never edited.
7. **Profiles keep the history cheap.** `observability.dataset_profile` stores
   record counts per date, dimension and bin (a few hundred rows a day). The
   checks read their baselines there instead of rescanning silver and gold.
8. **Run metrics.** A dbt hook records every node of every invocation (status,
   failures, rows, duration) in `audit.dbt_node_run` on the staging branch;
   `observability.pipeline_run` joins it with `audit.ingestion_run` into one row
   per run and step, attempts folded. Being on the branch, they are published
   with the data they built; a failed run's branch is discarded, and its record
   is the orchestrator's.
9. **Regeneration check.** `make regeneration-check` drops the derived tables of
   the staging branch, rebuilds silver and gold from bronze as from nothing, and
   compares every table with `main` by row count and an order-insensitive
   checksum of every column but the lineage ones (`aml-reliability compare`). It
   never publishes. Bronze is the boundary: the change data capture topics are
   compacted, so bronze holds the only full history of the core banking changes.
   It runs before a release or after a model change; on this workstation it takes
   about 7 minutes and holds the staging branch, so it is not part of the daily
   run.

## Results

All 18 business dates, published `main`.

| Check | Result |
|---|---|
| Layer reconciliation | 36 feed-dates, every one reconciled: 5,078,345 transfers and 1,800,000 card transactions landed, in bronze and in silver, with equal control totals |
| Core banking reconciliation | 167,848 customers, 518,581 accounts and 87,451 cards matched column by column; 0 missing, unexpected or mismatched |
| Volume checks | 54 dataset-dates: 24 OK, 16 ACKNOWLEDGED (the opening date and the tail after 2022-09-10, payments and alerts), 14 without a baseline yet (the first dates of each kind) |
| Distribution drift | Before the opening date was acknowledged, its REINVESTMENT transfers made the payment-format mix of 2022-09-05 to 2022-09-08 drift (index 2.1 down to 1.1) and the alert mix of 2022-09-05 (0.56). Acknowledged, every dimension from 2022-09-06 to 2022-09-10 is stable |
| Blocking | With the tail's acknowledgement removed, `audit_observability` fails for 2022-09-12 on payments (281 transfers, 0.0006 × the median) and warns on alerts |
| Daily run | Airflow run of 2022-09-12: `build_observability` 14 s, `audit_observability` 9 s, the whole run 4 minutes |

The regeneration check found a defect on its first run. `silver.fx_rate` took
the median of the rates the hub applied with `approx_percentile`, whose partial
digests merge in whatever order Trino's splits finish: two builds from the same
transfers disagreed in the tenth digit, and 426 of 127,187 alert evidence
amounts, in 346 alerts, moved by a cent. The alerts were the same. The rates are
now exact nearest-rank percentiles of the sorted quotes. The same run reported
`reference.acknowledged_anomaly` as different: the seed had changed since `main`
was built, which is the other case the check exists for. After the fix and a
republish, 19 tables of 19 were identical, and the rule backtest of ADR 0009
gave the same figures.

## Alternatives considered

| | Kept | Rejected because |
|---|---|---|
| Check framework | dbt models and tests | Great Expectations or Soda: a second rule language and runtime beside the dbt tests that already gate each layer. Elementary, the closest fit: a dbt package with anomaly tests, but its results and exclusions live in its own schema and test configuration, where an acknowledged anomaly is a filter rather than a reviewed record with a reason. |
| Volume baseline | Median of the same kind of day | Mean and standard deviation: one outage inflates both and hides the next. Same weekday of past weeks: ten dates of history give at most one. A forecast model: not with ten dates. |
| Drift statistic | Population stability index | Kolmogorov-Smirnov: per numeric column only, and with 480,000 transfers a day any difference is significant. PSI works on categories and bands, and has thresholds risk teams already use. |
| Drift gating | Warning | Blocking: a real change of business would hold every date until someone retunes the rules. |
| Source reconciliation | Trino catalog on the core banking, column grants | An export from the core banking compared offline: another file feed to secure and schedule. Counts only: a silver row with a stale risk rating would pass. |
| Run metrics | Tables in the lakehouse, read through Trino | Prometheus and Grafana: two more services on a 16 GB workstation for metrics that are a few rows a run. Airflow's StatsD metrics cover the scheduler, not rows and tests. |
| Regeneration | Full rebuild on the staging branch, checksum per table | A row-by-row diff (`EXCEPT`): it holds both tables in Trino's memory. Rebuilding on a separate Nessie branch: dbt writes only `etl` (ADR 0008), and a second writable catalog would widen that. |

## Consequences

- A blocking anomaly holds the date until the cause is fixed and the date is
  rerun, or the anomaly is acknowledged in a reviewed commit. The second is a
  decision with an owner, recorded next to the data.
- Bank holidays are not modelled: an expected low day is acknowledged like any
  other. With a year of history the baseline can move to the same weekday.
- The core banking reconciliation assumes no source transaction stays open
  longer than the grace period across a capture, and counts a row deleted after
  the watermark as unexpected. Under constant load it would run against a
  snapshot of the source at the captured position.
- Run metrics of failed runs live only in Airflow. A bank would also ship them
  to its monitoring stack; the tables here are what the compliance reporting
  (step 9) reads.
- Every model change that alters published rows needs the affected dates
  reprocessed; the regeneration check tells whether it was done.
