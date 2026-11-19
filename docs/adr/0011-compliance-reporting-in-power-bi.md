# ADR 0011: Compliance reporting in Power BI, versioned as a project and served by Trino over HTTPS

- Status: accepted

## Context

Compliance officers triage alerts and sanctions hits, and the data team answers
for whether the data behind them is complete. Both read `main` through Trino:
`gold` for the first, `observability` for the second (ADR 0009, 0010). The bank's
reporting tool is Power BI Desktop, on Windows workstations.

Two things stood in the way. Trino accepted any user name a client declared over
HTTP: fine for services inside the compose network, not for a person's desktop
tool, whose user decides what it may read (`bi` reads `gold`, `analyst` does not,
ADR 0009). And a report saved as a `.pbix` file is a binary: no review, no diff,
and nothing CI can check when a dbt model changes under it.

## Decision

1. **People log in to Trino over HTTPS with a password.** A second listener on
   port 8443 serves TLS with a key pair generated at the first `up` (one-shot
   service `trino-init`, with the JDK of the Trino image: no extra image). The
   certificate names `localhost` and `trino`. Passwords of `bi`, `compliance` and
   `analyst` come from `.env`, hashed into Trino's password file with PBKDF2
   (HMAC-SHA1, the function Trino verifies, at OWASP's 1.3 million iterations).
   Services keep the HTTP listener inside the compose network, where Trino
   accepts a declared user (`allow-insecure-over-http`). The access rules of
   ADR 0008 and 0009 are unchanged: the password only proves who declares the
   name they apply to.
2. **Power BI connects with the open-source Trino connector.** The custom
   connector [PowerBITrinoConnector](https://github.com/CreativeDataEU/PowerBITrinoConnector)
   calls Trino's REST API (`/v1/statement`) directly, with no ODBC driver, and
   navigates catalog, schema and table. It logs in with a user name and password
   over HTTPS (decision 1) and supports Import. Desktop loads it from
   `Documents\Power BI Desktop\Custom Connectors` once uncertified extensions
   are allowed. It trusts the certificates Windows trusts, so the self-signed
   certificate is imported into the Windows root store; a bank would have it
   issued by its internal CA.
3. **The report is a Power BI project (PBIP) in [bi/](../../bi).** The semantic
   model is TMDL, one file per table; the report is PBIR, one JSON file per page
   and visual. Desktop opens and saves both; every change is a reviewable diff.
4. **Import mode, `gold` and `observability` only.** About 20,000 alerts and
   130,000 evidence transfers fit in memory and refresh in seconds; DirectQuery would send
   every click to a single-node Trino. The model never imports lineage
   columns, `silver` or `restricted`: it holds identifiers (account, customer,
   party), never names. Report export is limited to summarized data, so the
   rows behind an alert do not leave the report.
5. **The model is a star around a date table.** Facts: `Alert`, `Alert evidence`,
   `Sanctions hit`, and the five `observability` tables. Dimensions: `Rule` and
   a calculated `Date` spanning the dates the volume checks ran on, so a day
   without an alert still counts. Implicit measures are disabled: every number
   on the report is a DAX measure with a description. Auto date tables are off.
6. **CI checks what it can without Windows.** `make bi-check` validates every
   PBIR and PBIP file against the schema it declares, fetched from Microsoft,
   and checks that each field the report uses exists in the model, each DAX
   reference resolves, and each imported column exists in the dbt contract with
   the type the model declares. `make test-reporting` publishes the empty tables
   CI builds and reads every imported column as `bi` over HTTPS.
7. **The report's numbers are checked against SQL.** `make bi-figures` computes
   each card's figure in SQL, as `bi` over HTTPS, so a refreshed report can be
   compared number by number.

## Results

All 18 business dates published to `main`; `make bi-figures`:

| Page | Measure | All dates | 2022-09-06 to 2022-09-10 |
|---|---|---|---|
| Alerts | Alerts | 19,395 | 9,732 |
| Alerts | Alerted accounts | 12,865 | 6,929 |
| Alerts | Alerted parties | 9,624 | 5,434 |
| Alerts | PEP alerts | 13 | 4 |
| Alerts | High-risk customer alerts | 729 | 356 |
| Alert triage | Evidence transfers | 127,187 | |
| Sanctions screening | Sanctions hits / customers with a hit | 98 / 89 | |
| Data reliability | Last business date | 2022-09-18 | |
| Data reliability | Volume anomalies / acknowledged | 0 / 16 | |
| Data reliability | Unreconciled feeds, core banking differences, drifting dimensions | 0, 0, 0 | |

The model imports 19,395 alerts, 127,187 evidence transfers, 98 sanctions hits
and the checks of 18 dates; every imported column reads as `bi` over HTTPS
(`make test-reporting`, 9 tables). The 63 fields the report uses resolve in the
model, and the 47 project files that declare a schema match it; a visual broken
on purpose (unknown property, position as text, unknown title property) reports
its three errors.

## Alternatives considered

| | Kept | Rejected because |
|---|---|---|
| Connector | Open-source Trino custom connector | Starburst (built in, certified): its ODBC driver, v2 and v3, sends each statement wrapped in JSON (`{"query": ..., "preparedStatements": {}}`), which open-source Trino rejects with `mismatched input '{'`. The generic ODBC connector: needs a driver that speaks open-source Trino, has no navigation by catalog and schema, and a connection string to maintain by hand. |
| Authentication | Password file, TLS | LDAP: a directory to run for three users. OAuth with Keycloak: one more service and token renewal that Power BI handles poorly. Trusting the declared user over HTTP: any local process could claim `compliance`. |
| File format | PBIP (TMDL + PBIR) | `.pbix`: binary, no review. A `.pbit` template: still binary. |
| Storage mode | Import | DirectQuery: every interaction hits Trino, and the measures run as SQL Trino must plan; worth it only for data too large or too fresh to import. |
| Measures | DAX in the model | Pre-aggregated gold tables per visual: each new question needs a dbt model, and filters would not combine. |
| Date table | Calculated from the checked dates | Power BI's auto date tables: one hidden table per date column, and no notion of a business date. A dbt calendar: one more model for four columns. |

## Consequences

- Power BI Desktop, the custom connector and a refresh run on Windows only.
  The connector is uncertified: Desktop must allow any extension, and the
  on-premises gateway needs it installed for a scheduled refresh. It is pinned
  by commit and checksum in [docs/pipeline.md](../pipeline.md#compliance-reporting).
  CI catches structural and contract breaks; the visual rendering is checked by
  opening the project, and the figures by `make bi-figures`.
- The HTTP listener still trusts a declared user. It is bound to `127.0.0.1` and
  used by the services of the stack and the evaluation tools; a deployment would
  drop it and give each service a credential.
- The report reads with one service account. Per-viewer security needs the
  Power BI service: row-level security in the model, or DirectQuery with single
  sign-on so Trino's rules apply to each viewer.
- The certificate is valid 825 days and regenerated when the keystore password
  changes; clients that pinned it must take the new one.
- A dbt contract change that drops or retypes a column the model imports fails
  CI until the model follows.
