"""Kafka topics that carry core banking change events.

Topics are declared here, never auto-created: a typo in a consumer must fail
instead of creating an empty topic, and every topic gets its retention and
partitioning on purpose. The Spark ingestion job imports this module too, so it
is limited to the Spark image's Python 3.10.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field

WEEK_MS = 7 * 24 * 3600 * 1000

# Partitions of a change topic are fixed for its whole life: Debezium routes a row by its primary
# key, and adding partitions would send later changes of a row to another partition, breaking the
# per-row ordering that consumers rely on.
CHANGE_TOPIC_PARTITIONS = 3

CHANGE_TOPIC_CONFIG: Mapping[str, str] = {
    # Compaction keeps the latest event of every key forever, so the topic stays a complete image of
    # the source that a new consumer can rebuild from without a new snapshot.
    "cleanup.policy": "compact",
    # No event becomes eligible for compaction before a week: bronze ingestion has that long to
    # record every intermediate version of a row before older versions may disappear.
    "min.compaction.lag.ms": str(WEEK_MS),
    # Tombstones stay as long, so a consumer that is behind still learns about the delete.
    "delete.retention.ms": str(WEEK_MS),
}

HEARTBEAT_TOPIC_CONFIG: Mapping[str, str] = {
    "cleanup.policy": "delete",
    "retention.ms": str(24 * 3600 * 1000),
}


@dataclass(frozen=True)
class Topic:
    name: str
    partitions: int
    config: Mapping[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class CdcSource:
    """One captured database schema, published by one connector as ``<prefix>.<schema>.<table>`` topics."""

    topic_prefix: str
    schema: str
    tables: tuple[str, ...]

    @property
    def connector_name(self) -> str:
        return f"{self.topic_prefix}-source"

    @property
    def slot_name(self) -> str:
        # Replication slot and publication names are SQL identifiers: no dashes.
        return f"debezium_{self.topic_prefix}".replace("-", "_")

    @property
    def publication_name(self) -> str:
        return self.slot_name

    @property
    def heartbeat_topic(self) -> str:
        return f"__debezium-heartbeat.{self.topic_prefix}"

    def change_topic(self, table: str) -> str:
        if table not in self.tables:
            raise KeyError(f"{table!r} is not captured from {self.schema}")
        return f"{self.topic_prefix}.{self.schema}.{table}"

    def topics(self) -> list[Topic]:
        return [
            *(Topic(self.change_topic(t), CHANGE_TOPIC_PARTITIONS, CHANGE_TOPIC_CONFIG) for t in self.tables),
            # One partition: heartbeats only advance the replication slot, nobody consumes them in order.
            Topic(self.heartbeat_topic, 1, HEARTBEAT_TOPIC_CONFIG),
        ]


CORE_BANKING = CdcSource(topic_prefix="corebanking", schema="core", tables=("customer", "account", "card"))
