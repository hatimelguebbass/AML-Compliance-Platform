"""Reporting tools log in over HTTPS with a password; what they may read is unchanged (ADR 0011)."""

from __future__ import annotations

import ssl
from pathlib import Path

import httpx
import pytest
import trino

from tests.integration.reporting import PASSWORDS, TRINO_HOST, https_query, server_certificate

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def certificate(tmp_path_factory: pytest.TempPathFactory) -> Path:
    return server_certificate(tmp_path_factory.mktemp("trino"))


@pytest.mark.parametrize("user", sorted(PASSWORDS))
def test_people_log_in_with_their_password(user: str, certificate: Path) -> None:
    assert https_query(user, PASSWORDS[user], "SELECT current_user", certificate) == [[user]]


def test_a_wrong_password_is_refused(certificate: Path) -> None:
    with pytest.raises(trino.exceptions.HttpError, match="401"):
        https_query("bi", "not-the-password", "SELECT 1", certificate)


@pytest.mark.parametrize("user", ["dbt", "trino", "integration-test"])
def test_service_users_have_no_password(user: str, certificate: Path) -> None:
    with pytest.raises(trino.exceptions.HttpError, match="401"):
        https_query(user, "anything", "SELECT 1", certificate)


def test_the_certificate_names_the_host_clients_connect_to(certificate: Path) -> None:
    context = ssl.create_default_context(cafile=certificate)
    response = httpx.get(f"https://{TRINO_HOST}:8443/v1/info", verify=context)
    assert response.status_code == 200


def test_reporting_never_reads_the_staging_branch(certificate: Path) -> None:
    with pytest.raises(trino.exceptions.TrinoUserError, match="Access Denied"):
        https_query("bi", PASSWORDS["bi"], "SHOW SCHEMAS FROM lakehouse_etl", certificate)


def test_reporting_never_reads_identities(certificate: Path) -> None:
    with pytest.raises(trino.exceptions.TrinoUserError, match=r"Access Denied|does not exist"):
        https_query(
            "bi", PASSWORDS["bi"], "SELECT full_name FROM lakehouse.restricted.customer_identity LIMIT 1", certificate
        )
