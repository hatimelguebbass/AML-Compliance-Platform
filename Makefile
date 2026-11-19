SHELL := /usr/bin/env bash
.DEFAULT_GOAL := help

COMPOSE := docker compose
NESSIE_URL ?= http://localhost:19120
SOURCES := uv run aml-sources
# Generators read the source database credentials from the same .env as compose.
# Exporting them globally would leak empty values into compose on a first run.
WITH_ENV := set -a && . ./.env && set +a &&
# The Spark container runs as the host user so that quarantined files stay owned by that user.
export HOST_UID := $(shell id -u)
export HOST_GID := $(shell id -g)
SPARK_SUBMIT := $(COMPOSE) --profile spark run --rm spark /opt/spark/bin/spark-submit jobs/ingest.py
# Airflow starts the Spark jobs as containers of the Docker host, whose bind mounts need host paths.
export AML_HOST_DIR := $(CURDIR)
export AML_LANDING_DIR := $(abspath $(or $(LANDING_DIR),data/landing))
export AML_QUARANTINE_DIR := $(abspath $(or $(QUARANTINE_DIR),data/quarantine))
AIRFLOW := docker exec aml-airflow-scheduler airflow
DBT := $(COMPOSE) --profile dbt run --rm dbt
DBT_VARS := $(if $(DATE),--vars '{"business_date": "$(DATE)"}')
ALL_PROFILES := --profile cdc --profile spark --profile dbt --profile airflow

.PHONY: help env up up-cdc down reset bootstrap ps logs lint typecheck test test-unit test-integration smoke-spark trino \
	data data-ibm data-payments data-sanctions data-corebanking data-cards \
	bronze bronze-tables ingest audit-bronze publish cdc cdc-reset cdc-status data-kyc-changes \
	up-airflow airflow-run airflow-backfill airflow-clear airflow-status \
	pipeline silver gold observability regeneration-check test-dbt dbt dbt-lock evaluate-parties evaluate-rules evaluate-sanctions \
	test-reporting bi-check bi-figures

help: ## List targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-18s %s\n", $$1, $$2}'

env: ## Create .env from the template, or add the variables introduced since it was created
	@test -f .env || cp .env.example .env
	@grep -E '^[A-Z_]+=' .env.example | while IFS='=' read -r key _; do \
	  grep -q "^$$key=" .env || { grep "^$$key=" .env.example >>.env && echo "added $$key to .env"; }; \
	done

# Compose would try to pull the local Spark and dbt images on first use instead of building them.
up: env ## Start storage, catalog, query engine and source database, build the Spark and dbt images, then bootstrap
	$(COMPOSE) up -d --wait
	$(COMPOSE) --profile spark --profile dbt build spark dbt
	$(MAKE) bootstrap

up-cdc: env ## Also start Kafka and Kafka Connect
	$(COMPOSE) --profile cdc up -d --wait

cdc: up-cdc ## Capture the core banking: role and publication, topics, Debezium connector (load the data first)
	$(WITH_ENV) uv run aml-cdc setup

cdc-reset: env ## Remove the capture (connector, offsets, slot, topics); the next `make cdc` takes a new snapshot
	$(WITH_ENV) uv run aml-cdc reset

cdc-status: env ## Connector state, events per topic, replication slot lag
	$(WITH_ENV) uv run aml-cdc status

up-airflow: env ## Also start Airflow (UI on http://localhost:8081); needs the Spark and dbt images, built here
	@mkdir -p data/landing data/quarantine
	$(COMPOSE) --profile spark --profile dbt build spark dbt
	$(COMPOSE) --profile airflow up -d --wait

# Runs of a paused DAG stay queued, so both targets unpause it, which also starts its daily schedule.
airflow-run: ## Run the daily pipeline for one business date, e.g. make airflow-run DATE=2022-09-01
	$(AIRFLOW) dags unpause aml_daily
	$(AIRFLOW) dags trigger aml_daily --logical-date $(DATE)

airflow-backfill: ## Run the daily pipeline over a date range: FROM=2022-09-01 TO=2022-09-18 (REPROCESS=completed to redo)
	$(AIRFLOW) dags unpause aml_daily
	$(AIRFLOW) backfill create --dag-id aml_daily --from-date $(FROM) --to-date $(TO) --max-active-runs 1 \
	  $(if $(REPROCESS),--reprocess-behavior $(REPROCESS))

airflow-clear: ## Resume a failed run from one task and its downstream tasks: DATE=2022-09-08 TASK=ingest_cards
	$(AIRFLOW) tasks clear aml_daily --task-regex '$(TASK)' --start-date $(DATE) --end-date $(DATE) --downstream --yes

airflow-status: ## Latest runs of the daily pipeline
	$(AIRFLOW) dags list-runs aml_daily

down: ## Stop all services, keep data
	$(COMPOSE) $(ALL_PROFILES) down

reset: ## Stop all services and delete all data volumes
	$(COMPOSE) $(ALL_PROFILES) down --volumes --remove-orphans

bootstrap: ## Create lakehouse namespaces and the staging branch (idempotent)
	uv run python -m aml_platform.lakehouse.bootstrap --nessie-url $(NESSIE_URL)

ps: ## Show service status
	$(COMPOSE) --profile cdc --profile airflow ps

logs: ## Follow logs, e.g. make logs s=nessie
	$(COMPOSE) logs -f $(s)

lint: ## Lint and check formatting
	uv run ruff check .
	uv run ruff format --check .

typecheck: ## Static type check
	uv run mypy

test-unit: ## Fast tests, no containers needed
	uv run pytest -m "not integration and not reporting"

test-integration: ## Tests against the running stack (make up first)
	uv run pytest -m integration

test-reporting: ## bi reads every column the Power BI model imports, over HTTPS (after a publish of gold and observability)
	uv run pytest -m reporting

test: lint typecheck test-unit ## Everything that runs without containers

smoke-spark: ## Spark reads and writes Iceberg tables through Nessie
	$(COMPOSE) --profile spark run --rm spark /opt/spark/bin/spark-sql -e \
	  "DROP TABLE IF EXISTS lakehouse_etl.audit.spark_smoke; \
	   CREATE TABLE lakehouse_etl.audit.spark_smoke (id INT) USING iceberg; \
	   INSERT INTO lakehouse_etl.audit.spark_smoke VALUES (1); \
	   SELECT count(*) FROM lakehouse_etl.audit.spark_smoke; \
	   DROP TABLE lakehouse_etl.audit.spark_smoke;"

trino: ## Open the Trino CLI
	docker exec -it aml-trino trino

data: data-ibm data-payments data-sanctions data-corebanking data-cards ## Produce every source feed (make up first)

data-ibm: ## Download and verify the IBM AML files (Kaggle credentials, or IBM_DIR=<folder with the files>)
	$(SOURCES) ibm $(if $(IBM_DIR),--from-dir $(IBM_DIR))

data-payments: ## Build the daily payments feed and its ground truth
	$(SOURCES) payments

data-sanctions: ## Download and validate the UN and OFAC sanctions lists
	$(SOURCES) sanctions

data-corebanking: env ## Load customers, accounts and cards into the core banking database
	$(WITH_ENV) $(SOURCES) corebanking $(if $(REPLACE),--replace)

data-kyc-changes: env ## Apply one day of back-office changes to the core banking (SEED=n for another day)
	$(WITH_ENV) $(SOURCES) kyc-changes $(if $(SEED),--seed $(SEED))

data-cards: env ## Write the card processor daily files from the core banking cards
	$(WITH_ENV) $(SOURCES) cards

bronze: bootstrap ingest audit-bronze publish ## Reset etl, ingest landed files and change events, audit, then publish to main

pipeline: bootstrap ingest audit-bronze silver gold observability publish ## Same as bronze, with silver, gold and the checks before the publish

ingest: ## Ingest into bronze on the etl branch (SOURCE=payments|cards|sanctions|files|corebanking, default all)
	@mkdir -p data/landing data/quarantine
	$(SPARK_SUBMIT) $(or $(SOURCE),all)

bronze-tables: ## Create the bronze and audit tables, empty, where absent (silver builds before the first delivery)
	$(SPARK_SUBMIT) tables

audit-bronze: ## Table-level checks on the etl branch; fails if it must not be published
	$(SPARK_SUBMIT) audit

silver: ## Build and test silver on the etl branch (DATE=YYYY-MM-DD for one business date, default every date)
	$(DBT) build --selector silver --exclude-resource-type unit_test $(DBT_VARS)

# One query over every date exceeds the query memory of the local Trino: the dated models are
# rebuilt a business date at a time, as the daily DAG builds them, then tested once.
GOLD_DATES = $(or $(DATE),$(shell docker exec aml-trino trino --user dbt --output-format TSV \
  --execute "SELECT DISTINCT business_date FROM lakehouse_etl.silver.payment_transfer ORDER BY 1" 2>/dev/null))

gold: ## Build and test gold on the etl branch (DATE=YYYY-MM-DD for one business date, default every date)
	$(DBT) run --select path:models/gold,config.materialized:table
	$(if $(GOLD_DATES),$(foreach date,$(GOLD_DATES),$(DBT) run --select path:models/gold,config.materialized:incremental \
	  --vars '{"business_date": "$(date)"}' &&) true,$(DBT) run --select path:models/gold,config.materialized:incremental)
	$(DBT) test --selector gold --exclude-resource-type unit_test $(DBT_VARS)

observability: ## Reconcile and check the volumes and distributions on the etl branch (DATE=YYYY-MM-DD, default every date)
	$(DBT) build --selector observability --exclude-resource-type unit_test $(DBT_VARS)

# Never publishes: the staging branch is reset to main by the next run.
regeneration-check: bootstrap ## Rebuild silver and gold from bronze on the etl branch and compare every table with main
	uv run aml-reliability drop-derived
	$(MAKE) silver gold
	uv run aml-reliability compare

test-dbt: ## dbt unit tests, against Trino (after a first make silver, make gold and make observability)
	$(DBT) test --select test_type:unit

dbt: ## Any dbt command in the dbt container, e.g. make dbt ARGS="ls --select silver"
	$(DBT) $(ARGS)

dbt-lock: ## Re-pin the dbt image requirements
	uv pip compile --quiet --generate-hashes --universal --python-version 3.12 --custom-compile-command "make dbt-lock" \
	  infra/dbt/requirements.in -o infra/dbt/requirements.txt

evaluate-parties: ## Precision and recall of the party resolution on main, against the ground truth
	uv run aml-evaluate parties

evaluate-rules: ## Precision and recall of each AML rule on main, against the laundering label (FROM=, UNTIL=YYYY-MM-DD)
	uv run aml-evaluate rules $(if $(FROM),--from $(FROM)) $(if $(UNTIL),--until $(UNTIL))

evaluate-sanctions: ## Precision and recall of the sanctions screening on main, against the planted customers
	uv run aml-evaluate sanctions

bi-check: ## Power BI project: report, model and dbt contracts agree; files match Microsoft's schemas
	uv run aml-reporting check
	uv run aml-reporting validate

bi-figures: ## Figures the Power BI report's cards must show, read as bi over HTTPS (FROM=, UNTIL=YYYY-MM-DD)
	@mkdir -p data
	$(COMPOSE) cp trino:/etc/trino/security/trino.crt data/trino.crt
	$(WITH_ENV) uv run aml-reporting figures $(if $(FROM),--from $(FROM)) $(if $(UNTIL),--until $(UNTIL))

publish: ## Merge the etl branch into main
	uv run python -m aml_platform.lakehouse.publish --nessie-url $(NESSIE_URL) --message "publish the staging branch"
