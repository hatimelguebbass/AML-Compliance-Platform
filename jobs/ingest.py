"""spark-submit entry point; the logic lives in ``aml_platform.ingestion``."""

import sys

from aml_platform.ingestion.cli import main

if __name__ == "__main__":
    sys.exit(main())
