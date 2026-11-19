#!/usr/bin/env bash
# Provisions the core banking schema and the read-only role the lakehouse reconciles it with.
#
# The role reads only the columns the lakehouse keeps in silver, never names, identity
# documents or contact details (Moroccan law 09-08, ADR 0010). Runs on every `up` through
# the source-db-init service; every statement is idempotent.
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
  -v role="$RECONCILIATION_DB_USER" -v password="$RECONCILIATION_DB_PASSWORD" <<'SQL'
CREATE SCHEMA IF NOT EXISTS core;
SET search_path TO core;
\i /schema/corebanking.sql

SELECT format('CREATE ROLE %I LOGIN', :'role')
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = :'role')
\gexec
ALTER ROLE :"role" PASSWORD :'password';
GRANT USAGE ON SCHEMA core TO :"role";
GRANT SELECT (customer_id, customer_type, gender, birth_date, legal_form, incorporation_date, nationality,
              residence_country, city, occupation, risk_rating, is_pep, customer_since, updated_at)
    ON core.customer TO :"role";
GRANT SELECT (account_id, customer_id, bank_id, account_number, account_type, currency, status, opened_on,
              closed_on, updated_at)
    ON core.account TO :"role";
GRANT SELECT (card_id, account_id, card_token, scheme, product, status, issued_on, expires_on, updated_at)
    ON core.card TO :"role";
SQL
