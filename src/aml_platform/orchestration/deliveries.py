"""Whether the dated feeds of a business date are complete, which is when its daily run may start.

The ingestion jobs skip a payments file whose manifest has not arrived yet and
succeed. Starting the run before every completeness signal is present would
therefore publish a business date without its payments, so the run waits here.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

from aml_platform.ingestion.files import CARDS_FILE, PAYMENTS_FILE, data_files
from aml_platform.sources.landing import Manifest


def missing_deliveries(landing_dir: Path, business_date: date) -> list[str]:
    """What is still missing for ``business_date``; an empty list means the run can start."""
    missing = []
    payments = data_files(landing_dir / "payments", PAYMENTS_FILE, business_date)
    if not payments:
        missing.append(f"payments file for {business_date}")
    elif not Manifest.path_for(payments[0]).is_file():
        missing.append(f"manifest of {payments[0].name}")
    # A card file is renamed into place once complete and carries its own trailer.
    if not data_files(landing_dir / "cards", CARDS_FILE, business_date):
        missing.append(f"card processor file for {business_date}")
    return missing
