# ADR 0003: Apache Kafka (KRaft) as the change data capture bus

- Status: accepted

## Context

Customer and KYC changes in the core banking database are captured with
Debezium. Debezium runs on Kafka Connect and needs a Kafka-protocol broker that
retains events long enough to replay a consumer after an outage.

## Decision

Run **Apache Kafka 4.x in KRaft mode**, one combined broker/controller node in
the local stack, with Debezium on **Kafka Connect**. Automatic topic creation is
disabled so every topic is declared with its retention and partitioning.

## Alternatives considered

| Option | Why not |
|---|---|
| Redpanda | Kafka API compatible and lighter, but since Kafka 4.0 removed ZooKeeper the footprint gap is small (~0.5 GB heap here); Kafka is what bank platforms operate |
| RabbitMQ | Queue semantics; no retained, replayable log per partition |
| Polling with `updated_at` | Misses deletes and intermediate states; unsafe for an audit trail |

## Consequences

- Replication factors are 1 locally. A production cluster uses three brokers,
  replication factor 3 and `min.insync.replicas=2`.
