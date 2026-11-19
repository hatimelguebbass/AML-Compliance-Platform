# ADR 0009: Gold layer: AML rules and sanctions screening

- Status: accepted

## Context

Silver holds conformed transfers, customer histories, the customer 360 and the
sanctions lists (ADR 0008). Compliance needs two outputs from them: alerts on
transfers that look like money laundering, and customers whose name is on the UN
or OFAC list. Both must be explainable to an investigator and to a regulator, so
every alert has to say which rule fired, with which parameters, on which
transfers. Both must also be measured: the payments carry the IBM laundering label
and the core banking has customers planted on the sanctions lists, kept outside
the lakehouse (ADR 0004), so precision and recall can be computed instead of
asserted. Gold is built by the daily run for its business date, on the staging
branch, and published only once audited, like the layers below.

## Decision

1. **Rules are SQL models in the dbt project, parameters are versioned data.**
   Each rule is an ephemeral model under `transform/models/gold/rules/` that
   returns the transfers it flags for an account and a business date. Its
   parameters live in the `aml_rules` var of `dbt_project.yml`; the same block
   renders the catalogue `gold.aml_rule`, so the documented thresholds are the ones
   that ran, and changing one is a reviewed commit.
2. **Seven rules, each tied to a typology.**

   | Rule | Fires when | Typology |
   |---|---|---|
   | AML-001 fan-in | ≥ 3 distinct accounts credit an account by ACH in a day | gathering |
   | AML-002 fan-out | an account pays ≥ 5 distinct accounts by ACH in a day | scattering |
   | AML-003 pass-through | an account forwards 90-100% of an ACH credit to a third party within 72 h | mule, layering |
   | AML-004 round trip | funds an account sent come back from the same counterparty within 72 h | layering |
   | AML-005 volume spike | outflow ≥ 10,000 and ≥ 10 × the 7-day daily average, with ≥ 2 active days | unexpected activity |
   | AML-006 structuring | ≥ 2 payments of 9,000-9,999 in a day | threshold avoidance |
   | AML-007 high-risk jurisdiction | a customer resident in or national of a FATF-listed country moves ≥ 10,000 in a day | geographic risk |

   Amounts are compared in USD at the rates the payments hub itself applies,
   read off its cross-currency transfers (`silver.fx_rate`); a test fails the run
   if those rates start to vary. The FATF lists are a seed with the dates they
   were in force, and customer attributes are taken from the type 2 history as of
   the business date, so replaying a past date gives the alerts that date would
   have given.
3. **Alert grain: rule × account × business date, with its evidence.**
   `gold.aml_alert_transfer` holds every transfer behind an alert (with the
   earlier credit or payment a pass-through or round trip was matched to);
   `gold.aml_alert` adds what triage sorts on: holder, party, KYC risk rating and
   PEP flag on that date, counterparties, amounts. Alert ids are a hash of what
   the alert is about, so a rerun of a date keeps them. Both tables are
   partitioned by business date and replaced per date like the silver dated tables.
4. **Gold is built and audited after silver is audited.** The DAG becomes
   `... audit_silver -> build_gold -> audit_gold -> publish`, with layers selected
   by `transform/selectors.yml`. Silver tests that also read gold are kept out of
   the silver audit (cautious indirect selection).
5. **Sanctions screening in SQL with blocking.** `gold.sanctions_hit` compares
   current customers with every primary name and alias of the latest lists.
   Candidates share two name tokens or the whole compacted name; a candidate is a
   hit when the compacted names are equal (EXACT), when every token of the shorter
   name is in the longer one and covers at least 60% of it (TOKENS: a dropped
   middle name), or when the compacted names are at least 90% similar (FUZZY).
   Weak aliases only hit exactly. Persons are screened against individuals,
   organizations against entities.
6. **Measured, against the label the rules never see.** `make evaluate-rules`
   scores each rule on the transfers its alerts hold: alert precision (share of
   alerts holding a labelled transfer), transfer precision and recall, and lift
   over the base rate of 0.10%, on a range of business dates (`FROM`, `UNTIL`).
   `make evaluate-sanctions` scores hits per customer.
7. **Thresholds are chosen on one period and scored on the next.** Each threshold
   was set on business dates 2022-09-01 to 2022-09-05 by running the rule over a
   grid of values and keeping the one with the highest alert precision × recall;
   when two values score within 5% of each other, the one raising fewer alerts
   wins, since alerts are investigator time. The thresholds are then frozen and
   scored on 2022-09-06 to 2022-09-10, which played no part in choosing them.
8. **Alerts are confidential.** Trino access rules give `gold` to `compliance` and
   `bi` (the compliance dashboard), not to `analyst`: an alert disclosed outside
   compliance is a tipping-off risk.

## Results

Published `main`. Tuning dates 2022-09-01 to 2022-09-05: 2,766,832 transfers,
1,999 labelled. Validation dates 2022-09-06 to 2022-09-10: 2,310,405 transfers,
2,523 labelled (base rate 0.109%).

| Rule | Value kept on the tuning dates | Validation alerts | Alert precision | Recall | Transfer precision | Lift |
|---|---|---|---|---|---|---|
| AML-001 fan-in | 3 counterparties (2: 3.0% / 38.7%; 5: 7.6% / 13.0%) | 4,551 | 8.0% | 24.4% | 1.4% | 13.2 |
| AML-002 fan-out | 5 counterparties (3: 1.3% / 11.1%; 8: 9 alerts) | 217 | 15.2% | 6.2% | 13.5% | 123.2 |
| AML-003 pass-through | 90% within 72 h (80% / 72 h: 10.4% / 12.7%) | 876 | 38.5% | 21.2% | 27.3% | 250.3 |
| AML-004 round trip | 72 h (120 h within 5%, 16% more alerts) | 1,342 | 7.2% | 6.1% | 5.3% | 48.8 |
| AML-005 volume spike | 10 × the average (see below) | 533 | 16.1% | 5.2% | 9.0% | 82.5 |
| AML-006 structuring | policy, not tuned | 780 | 0.9% | 0.3% | 0.2% | 1.9 |
| AML-007 high-risk jurisdiction | policy, not tuned | 1,433 | 1.0% | 1.2% | 0.4% | 3.2 |
| All rules | | 9,732 | 9.7% | 54.3% | 2.3% | 20.7 |

The figures in brackets are alert precision / recall of the rejected values on
the tuning dates. On the tuning dates the kept thresholds score 6.4% alert
precision and 44.3% recall (lift 20.1): validation scores higher, because the
first dates have little account history for the rules that look back.

AML-005 failed validation. The grid kept 5 × the average (16.1% alert precision,
5.6% recall on the tuning dates against 14.4% and 3.3% for 10 ×), but on the
validation dates 5 × raised 6,095 alerts at 2.1% alert precision. On the tuning
dates few accounts have paid out on the two days the rule requires, so the rule
barely fires there and its threshold cannot be tuned on them. It is kept at the
10 × it ran with before the backtest; its validation figure was seen before that
choice and is not an unseen-data score.

Over all 18 dates: 19,395 alerts, recall 52.6%, lift 22.0.

Sanctions screening: every one of the 40 planted customers is hit, on the entry
it was planted from (recall 1.00); 49 other customers are hit (precision 0.45).

## Alternatives considered

| | Kept | Rejected because |
|---|---|---|
| Rule engine | SQL models in dbt | A Python rule engine or a commercial monitoring tool: the rules are set operations over transfers that Trino runs in seconds, and dbt already gives lineage, tests and contracts. A Drools-style engine adds a runtime to operate for no rule it could express better. |
| Detection method | Explainable rules | A supervised model trained on the label: the label then becomes training data, not an independent measure, and a score is harder to justify to a regulator than a rule and its threshold. A model can come later as a prioritisation of these alerts. |
| Longer cycles | Two-leg round trip | Cycles of three or more hops: the self-join explodes on hub accounts; graph cycle detection needs a graph engine for a typology a two-leg rule partly covers. |
| Parameters | `dbt_project.yml` var | A seed table: thresholds would be data a load can change without review. Hard-coded literals: the catalogue would drift from the code. |
| Currency conversion | Rates the hub applies | A reference rate feed (ECB, Bank Al-Maghrib): no feed covers every currency of the hub, and rates other than the applied ones would misstate what was settled. |
| Screening | Token-pair blocking + rules in SQL | A full comparison of every customer with every listed name: 168k × 46k pairs. A screening product or Elasticsearch fuzzy queries: an extra service to run for under a hundred hits. |
| Full rebuild | One business date per query | One query over every date: it exceeds the 700 MB query memory of the local Trino. `make gold` loops over the dates, as the daily DAG and a backfill do. |

## Consequences

- The label is IBM's: it knows the typologies its simulator planted (fan-in,
  fan-out, cycles, scatter-gather, stacks), not structuring or customer
  countries, which come from the core banking. AML-006 and AML-007 are measured
  at about the base rate: they are policy rules, kept for coverage, and the label
  cannot judge them.
- Five tuning dates and five validation dates are a short backtest: a bank
  tunes on months and confirms on the following quarter, and re-tunes as
  activity drifts. The method carries over unchanged to a longer history. The
  last eight dates of the IBM file are left out of both periods: they hold almost
  only the tails of laundering patterns and flatter every rule.
- Channel scope (ACH for fan-in, fan-out and pass-through) follows the label:
  87% of labelled transfers are ACH against 12% of all transfers. Another bank's
  history would scope them differently.
- Sanctions screening on names alone hits real people who share a listed name
  (Ahmed El Khatib, Hassan Ait Ali): every planted customer is found, and about
  half the hits are for compliance to discard. Date of birth and nationality of
  the listed entries would discard most of them automatically.
- Gold is rebuilt per date: a full rebuild of the 18 dates takes about 5 minutes
  30 seconds.
