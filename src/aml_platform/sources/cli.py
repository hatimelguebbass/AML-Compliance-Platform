"""Command line entry point: ``aml-sources <command>``."""

from __future__ import annotations

import argparse
import logging
import os
from datetime import date
from pathlib import Path

from psycopg.conninfo import make_conninfo

from aml_platform.sources import cards, corebanking, ibm_aml, kyc_changes, payments, sanctions

log = logging.getLogger("aml_platform.sources")

OPERATOR_ERRORS: tuple[type[Exception], ...] = (
    corebanking.TargetNotEmptyError,
    ibm_aml.ChecksumMismatchError,
    ibm_aml.MissingCredentialsError,
    payments.SourceFormatError,
    sanctions.SanctionsValidationError,
    FileNotFoundError,
)
FIRST_BUSINESS_DATE = date(2022, 9, 1)
LAST_BUSINESS_DATE = date(2022, 9, 18)


class Layout:
    def __init__(self, root: Path) -> None:
        self.raw_ibm = root / "raw" / "ibm"
        self.payments = root / "landing" / "payments"
        self.cards = root / "landing" / "cards"
        self.sanctions = root / "landing" / "sanctions"
        self.ground_truth = root / "ground_truth"


def source_db_conninfo() -> str:
    return make_conninfo(
        host=os.environ.get("SOURCE_DB_HOST", "localhost"),
        port=os.environ.get("SOURCE_DB_PORT", "5432"),
        dbname=os.environ["SOURCE_DB_NAME"],
        user=os.environ["SOURCE_DB_USER"],
        password=os.environ["SOURCE_DB_PASSWORD"],
    )


def _ibm(args: argparse.Namespace, layout: Layout) -> None:
    if args.from_dir:
        landed = ibm_aml.copy_from_directory(args.from_dir, layout.raw_ibm)
    else:
        landed = ibm_aml.download_from_kaggle(layout.raw_ibm, auth_header=ibm_aml.kaggle_auth_header())
    for path in landed:
        log.info("verified %s", path)


def _payments(_: argparse.Namespace, layout: Layout) -> None:
    for source in ibm_aml.FILES:
        ibm_aml.verify(layout.raw_ibm / source.name, source)
    summary = payments.build_feed(
        layout.raw_ibm / ibm_aml.TRANSACTIONS.name,
        layout.raw_ibm / ibm_aml.PATTERNS.name,
        layout.payments,
        layout.ground_truth,
    )
    if summary.unmatched_pattern_rows:
        raise SystemExit(f"{summary.unmatched_pattern_rows} pattern rows match no transaction")
    log.info(
        "payments feed: %s files, %s transfers, %s labelled as laundering",
        len(summary.files),
        summary.record_count,
        summary.laundering_count,
    )


def _sanctions(args: argparse.Namespace, layout: Layout) -> None:
    names = sorted(sanctions.LISTS) if args.list == "all" else [args.list]
    for name in names:
        result = sanctions.fetch(sanctions.LISTS[name], layout.sanctions, allow_shrink=args.allow_shrink)
        log.info("%s: %s (%s entries)", name, result.path.name, result.entry_count)


def _corebanking(args: argparse.Namespace, layout: Layout) -> None:
    listed: list[sanctions.ListedPerson] = []
    for sanctions_list in sanctions.LISTS.values():
        manifest = sanctions.latest_manifest(layout.sanctions / sanctions_list.name)
        if manifest is None:
            raise SystemExit(f"no {sanctions_list.name} list landed yet: run `aml-sources sanctions` first")
        listed.extend(
            sanctions.listed_persons(layout.sanctions / sanctions_list.name / manifest.file_name, sanctions_list)
        )
    for source in (ibm_aml.ACCOUNTS, ibm_aml.TRANSACTIONS):
        ibm_aml.verify(layout.raw_ibm / source.name, source)
    data = corebanking.generate(
        corebanking.read_ibm_accounts(layout.raw_ibm / ibm_aml.ACCOUNTS.name),
        corebanking.account_currencies(layout.raw_ibm / ibm_aml.TRANSACTIONS.name),
        listed,
        corebanking.GenerationConfig(seed=args.seed),
    )
    corebanking.load(source_db_conninfo(), data, replace=args.replace)
    corebanking.write_ground_truth(data, layout.ground_truth)
    log.info(
        "core banking: %s customers, %s accounts, %s cards", len(data.customers), len(data.accounts), len(data.cards)
    )


def _kyc_changes(args: argparse.Namespace, _: Layout) -> None:
    applied = kyc_changes.apply(source_db_conninfo(), per_operation=args.per_operation, seed=args.seed)
    log.info("core banking changes applied: %s rows", sum(applied.values()))


def _parse_defect(value: str) -> tuple[date, cards.Defect]:
    day, _, kind = value.partition(":")
    try:
        return date.fromisoformat(day), cards.Defect(kind)
    except ValueError as error:
        choices = ", ".join(cards.Defect)
        raise argparse.ArgumentTypeError(f"expected YYYY-MM-DD:<{choices}>, got {value!r}") from error


def _cards(args: argparse.Namespace, layout: Layout) -> None:
    holders = cards.load_card_holders(source_db_conninfo())
    if not holders:
        raise SystemExit("no active cards in core banking: run `aml-sources corebanking` first")
    merchants = cards.build_merchants([holder.home_city for holder in holders], args.seed)
    defects: dict[date, set[cards.Defect]] = {}
    for day, kind in args.defect:
        defects.setdefault(day, set()).add(kind)
    for day in cards.business_dates(args.start, args.end):
        path = cards.generate_file(
            layout.cards,
            day,
            holders,
            merchants,
            count=args.per_day,
            seed=args.seed,
            defects=frozenset(defects.get(day, ())),
        )
        log.info("%s written%s", path.name, f" with {sorted(defects[day])}" if day in defects else "")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="aml-sources", description="Produce and land the source system feeds.")
    parser.add_argument("--data-dir", type=Path, default=Path(os.environ.get("AML_DATA_DIR", "data")))
    commands = parser.add_subparsers(dest="command", required=True)

    ibm = commands.add_parser("ibm", help="download and verify the IBM AML HI-Small files")
    ibm.add_argument("--from-dir", type=Path, help="verify files downloaded manually instead of calling the Kaggle API")
    ibm.set_defaults(handler=_ibm)

    commands.add_parser("payments", help="build the daily payments hub feed").set_defaults(handler=_payments)

    sanctions_cmd = commands.add_parser("sanctions", help="download and land the UN and OFAC lists")
    sanctions_cmd.add_argument("--list", choices=[*sorted(sanctions.LISTS), "all"], default="all")
    sanctions_cmd.add_argument("--allow-shrink", action="store_true", help="accept a list that lost >10%% of entries")
    sanctions_cmd.set_defaults(handler=_sanctions)

    core = commands.add_parser("corebanking", help="generate and load customers, accounts and cards")
    core.add_argument("--seed", type=int, default=corebanking.GenerationConfig.seed)
    core.add_argument("--replace", action="store_true", help="truncate and reload a populated database")
    core.set_defaults(handler=_corebanking)

    changes = commands.add_parser("kyc-changes", help="apply one day of back-office changes to the core banking")
    changes.add_argument("--per-operation", type=int, default=100)
    changes.add_argument("--seed", type=int, default=1)
    changes.set_defaults(handler=_kyc_changes)

    card = commands.add_parser("cards", help="write the card processor daily files")
    card.add_argument("--start", type=date.fromisoformat, default=FIRST_BUSINESS_DATE)
    card.add_argument("--end", type=date.fromisoformat, default=LAST_BUSINESS_DATE)
    card.add_argument("--per-day", type=int, default=100_000)
    card.add_argument("--seed", type=int, default=corebanking.GenerationConfig.seed)
    card.add_argument(
        "--defect",
        type=_parse_defect,
        action="append",
        default=[],
        metavar="DATE:KIND",
        help="inject a delivery defect, e.g. 2022-09-05:truncated (repeatable)",
    )
    card.set_defaults(handler=_cards)
    return parser


def main(argv: list[str] | None = None) -> None:
    args = build_parser().parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    try:
        args.handler(args, Layout(args.data_dir))
    except OPERATOR_ERRORS as error:
        # Expected refusals need a clear message for the operator, not a stack trace.
        log.error("%s", error)
        raise SystemExit(1) from error


if __name__ == "__main__":
    main()
