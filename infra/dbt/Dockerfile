# syntax=docker/dockerfile:1.7
FROM python:3.12.16-slim-bookworm

# Hash-pinned: a re-published or tampered package fails the build. Regenerate with `make dbt-lock`.
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --no-deps --require-hashes -r /tmp/requirements.txt \
    && useradd --uid 1000 --create-home dbt

# The project is mounted read-only; dbt writes its compiled SQL, artifacts and logs elsewhere.
ENV DBT_PROFILES_DIR=/opt/dbt/project \
    DBT_TARGET_PATH=/tmp/dbt/target \
    DBT_LOG_PATH=/tmp/dbt/logs \
    DBT_SEND_ANONYMOUS_USAGE_STATS=false \
    DBT_VERSION_CHECK=false
USER dbt
WORKDIR /opt/dbt/project
ENTRYPOINT ["dbt"]
CMD ["--help"]
