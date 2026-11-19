"""Topic administration through the Kafka command line tools inside the broker container.

The platform has no Kafka client library on the host; the broker image ships the
official tools, which are also what an operator would use on a bank's cluster.
"""

from __future__ import annotations

import logging
import re
import subprocess
from collections.abc import Iterable, Sequence

from aml_platform.cdc.topics import Topic

log = logging.getLogger(__name__)

CONTAINER = "aml-kafka"
BOOTSTRAP = "localhost:9092"
BIN = "/opt/kafka/bin"
# One broker locally; a production cluster declares 3 with min.insync.replicas=2 (ADR 0003).
REPLICATION_FACTOR = 1

_PARTITION_COUNT = re.compile(r"PartitionCount:\s*(\d+)")


class TopicMismatchError(RuntimeError):
    """An existing topic differs from its declaration in a way that cannot be altered in place."""


def _kafka(tool: str, *args: str) -> str:
    command = ["docker", "exec", CONTAINER, f"{BIN}/{tool}", "--bootstrap-server", BOOTSTRAP, *args]
    result = subprocess.run(command, capture_output=True, text=True, check=False, timeout=120)  # noqa: S603
    if result.returncode != 0:
        raise RuntimeError(f"{tool} {' '.join(args)} failed: {result.stderr.strip() or result.stdout.strip()}")
    return result.stdout


def ensure_topics(topics: Iterable[Topic]) -> None:
    """Create missing topics and converge the configuration of existing ones (idempotent)."""
    for topic in topics:
        config_args = [arg for key, value in sorted(topic.config.items()) for arg in ("--config", f"{key}={value}")]
        _kafka(
            "kafka-topics.sh",
            "--create",
            "--if-not-exists",
            "--topic",
            topic.name,
            "--partitions",
            str(topic.partitions),
            "--replication-factor",
            str(REPLICATION_FACTOR),
            *config_args,
        )
        if topic.config:
            _kafka(
                "kafka-configs.sh",
                "--alter",
                "--entity-type",
                "topics",
                "--entity-name",
                topic.name,
                "--add-config",
                ",".join(f"{key}={value}" for key, value in sorted(topic.config.items())),
            )
        description = _kafka("kafka-topics.sh", "--describe", "--topic", topic.name)
        match = _PARTITION_COUNT.search(description)
        actual = int(match.group(1)) if match else None
        if actual != topic.partitions:
            raise TopicMismatchError(f"{topic.name} has {actual} partitions, declared {topic.partitions}")
        log.info("topic %s ready (%s partitions)", topic.name, topic.partitions)


def end_offsets(topic: str) -> dict[int, int]:
    """Offset of the next event to be written, per partition."""
    output = _kafka("kafka-get-offsets.sh", "--topic", topic, "--time", "latest")
    offsets: dict[int, int] = {}
    for line in output.splitlines():
        _, partition, offset = line.rsplit(":", 2)
        offsets[int(partition)] = int(offset)
    return offsets


def delete_topics(names: Sequence[str]) -> None:
    for name in names:
        _kafka("kafka-topics.sh", "--delete", "--if-exists", "--topic", name)
