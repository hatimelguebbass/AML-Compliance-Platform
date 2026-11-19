#!/usr/bin/env bash
# One database and one login role per platform service, so a compromised or
# misbehaving service cannot read another service's metadata.
#
# Runs on the first start of an empty volume (docker-entrypoint-initdb.d) and again
# on every `up` through the platform-db-init service, so a service added later gets
# its database on an existing volume too. Every statement is therefore idempotent.
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "${POSTGRES_USER:-postgres}" \
  -v nessie_password="$NESSIE_DB_PASSWORD" \
  -v airflow_password="$AIRFLOW_DB_PASSWORD" <<'SQL'
SELECT format('CREATE ROLE %I LOGIN', service)
FROM (VALUES ('nessie'), ('airflow')) AS services (service)
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = service)
\gexec

ALTER ROLE nessie PASSWORD :'nessie_password';
ALTER ROLE airflow PASSWORD :'airflow_password';

SELECT format('CREATE DATABASE %I OWNER %I', service, service)
FROM (VALUES ('nessie'), ('airflow')) AS services (service)
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = service)
\gexec
SQL
