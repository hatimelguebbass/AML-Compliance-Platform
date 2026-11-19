"""The figures the report's cards must show, computed in SQL as the reporting user.

They are the measures of the semantic model restated in SQL over the same tables. A report showing
other numbers for the same dates has a broken measure, relationship or refresh, not different data.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Protocol


class Cursor(Protocol):
    def execute(self, operation: str, params: Any = ...) -> Any: ...
    def fetchall(self) -> list[Any]: ...


@dataclass(frozen=True)
class Figure:
    page: str
    measure: str
    value: object


# (page, measure, SQL over the lakehouse catalog). {dates} filters on business_date.
_FIGURES = [
    ("Alerts", "Alerts", "select count(*) from gold.aml_alert where {dates}"),
    ("Alerts", "Alerted accounts", "select count(distinct account_id) from gold.aml_alert where {dates}"),
    ("Alerts", "Alerted parties", "select count(distinct party_id) from gold.aml_alert where {dates}"),
    ("Alerts", "PEP alerts", "select count_if(is_pep) from gold.aml_alert where {dates}"),
    (
        "Alerts",
        "High-risk customer alerts",
        "select count_if(customer_risk_rating = 'HIGH') from gold.aml_alert where {dates}",
    ),
    ("Alerts", "Total credited (USD)", "select sum(credited_amount) from gold.aml_alert where {dates}"),
    ("Alerts", "Total debited (USD)", "select sum(debited_amount) from gold.aml_alert where {dates}"),
    (
        "Alert triage",
        "Evidence transfers",
        "select count(*) from gold.aml_alert_transfer where {dates}",
    ),
    ("Sanctions screening", "Sanctions hits", "select count(*) from gold.sanctions_hit"),
    ("Sanctions screening", "Customers with a hit", "select count(distinct customer_id) from gold.sanctions_hit"),
    ("Data reliability", "Last business date", "select max(business_date) from observability.volume_check"),
    (
        "Data reliability",
        "Volume anomalies",
        "select count_if(status = 'ANOMALY') from observability.volume_check where {dates}",
    ),
    (
        "Data reliability",
        "Acknowledged anomalies",
        "select count_if(status = 'ACKNOWLEDGED') from observability.volume_check where {dates}",
    ),
    (
        "Data reliability",
        "Unreconciled feeds",
        "select count_if(not is_reconciled) from observability.layer_reconciliation where {dates}",
    ),
    (
        "Data reliability",
        "Core banking differences",
        "select sum(missing_in_lakehouse + unexpected_in_lakehouse + mismatched)"
        " from observability.corebanking_reconciliation",
    ),
    (
        "Data reliability",
        "Drifting dimensions",
        "select count_if(status = 'DRIFT') from observability.distribution_drift where {dates}",
    ),
]


def compute(cursor: Cursor, first: date | None = None, last: date | None = None) -> list[Figure]:
    """Each card's figure over business dates ``first`` to ``last`` (both included, default all)."""
    bounds = [f"business_date >= date '{first.isoformat()}'"] if first else []
    bounds += [f"business_date <= date '{last.isoformat()}'"] if last else []
    dates = " and ".join(bounds) or "true"
    figures = []
    for page, measure, sql in _FIGURES:
        cursor.execute(sql.format(dates=dates))
        figures.append(Figure(page, measure, cursor.fetchall()[0][0]))
    return figures
