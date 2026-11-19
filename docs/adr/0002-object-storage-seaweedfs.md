# ADR 0002: SeaweedFS as S3-compatible object storage

- Status: accepted

## Context

Iceberg data and metadata files need an object store. On-premise, it must speak
the S3 API so that Spark (`S3FileIO`), Trino (native S3) and Nessie use the same
code paths they would use against AWS S3.

MinIO was the default choice for this role. Its community edition stopped
shipping binaries and container images, and the repository was later
archived; the maintained product is commercial. Building on an
unmaintained storage layer is not acceptable for a system of record.

## Decision

Use **SeaweedFS** (Apache 2.0, active since 2012) in single-node mode with its
S3 gateway. Clients use path-style addressing against `http://seaweedfs:8333`.
Nothing outside `docker-compose.yml` references SeaweedFS directly.

## Alternatives considered

| Option | Why not |
|---|---|
| MinIO community edition | Archived; no security fixes |
| Third-party MinIO rebuilds | Unclear maintenance and provenance |
| Ceph RGW | The usual choice for a production on-premise cluster, but too heavy for a single workstation |
| Garage | Lightweight and maintained, AGPL; smaller ecosystem for analytics workloads |
| RustFS | Young project, API coverage still moving |

## Consequences

- Swapping the object store (Ceph RGW in production, AWS S3 in a cloud
  migration) changes the endpoint and credentials only.
- Local credentials come from `.env`; production would use per-service keys
  with bucket policies.
