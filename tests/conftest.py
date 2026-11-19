from __future__ import annotations

from pathlib import Path

import pytest

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def ibm_dir() -> Path:
    return FIXTURES / "ibm"


@pytest.fixture
def sanctions_dir() -> Path:
    return FIXTURES / "sanctions"
