# syntax=docker/dockerfile:1.7
ARG SPARK_IMAGE=apache/spark:4.1.4-scala2.13-java17-python3-ubuntu

# The image's interpreter is Python 3.10; the ingestion jobs only add PyYAML to it,
# installed from a pinned wheel because the build has no package index to resolve from.
FROM ${SPARK_IMAGE} AS wheels-amd64
ADD --checksum=sha256:9c7708761fccb9397fe64bbc0395abcae8c4bf7b0eac081e12b809bf47700d0b \
    https://files.pythonhosted.org/packages/7a/1e/7acc4f0e74c4b3d9531e24739e0ab832a5edf40e64fbae1a9c01941cabd7/pyyaml-6.0.3-cp310-cp310-manylinux2014_x86_64.manylinux_2_17_x86_64.manylinux_2_28_x86_64.whl \
    /wheels/

FROM ${SPARK_IMAGE} AS wheels-arm64
ADD --checksum=sha256:b30236e45cf30d2b8e7b3e85881719e98507abed1011bf463a8fa23e9c3e98a8 \
    https://files.pythonhosted.org/packages/43/f7/0e6a5ae5599c838c696adb4e6330a59f463265bfa1e116cfd1fbb0abaaae/pyyaml-6.0.3-cp310-cp310-manylinux2014_aarch64.manylinux_2_17_aarch64.manylinux_2_28_aarch64.whl \
    /wheels/

FROM wheels-${TARGETARCH} AS wheels

FROM ${SPARK_IMAGE}

ARG MAVEN_ICEBERG=https://repo1.maven.org/maven2/org/apache/iceberg

# Checksums pin the exact artifacts: a re-published or tampered jar fails the build.
ADD --chmod=0644 --checksum=sha256:3123c3798af642dd537230923548e013ef41cc28b6626e5225808cf5670a0f82 \
    ${MAVEN_ICEBERG}/iceberg-spark-runtime-4.1_2.13/1.12.0/iceberg-spark-runtime-4.1_2.13-1.12.0.jar \
    /opt/spark/jars/
ADD --chmod=0644 --checksum=sha256:dd7e49ffa4ac9b215bed846581991259e3aec9bb2fa08d6cec029c46cd66d957 \
    ${MAVEN_ICEBERG}/iceberg-aws-bundle/1.12.0/iceberg-aws-bundle-1.12.0.jar \
    /opt/spark/jars/

# Kafka source for the change data capture ingestion, with the client version Spark 4.1.4 is built
# against. zstd, lz4 and snappy codecs already ship with Spark.
ARG MAVEN=https://repo1.maven.org/maven2
ADD --chmod=0644 --checksum=sha256:94f9514f64b319aa31c3b2061bf2fb75700f82e14845fda71337a13696855710 \
    ${MAVEN}/org/apache/spark/spark-sql-kafka-0-10_2.13/4.1.4/spark-sql-kafka-0-10_2.13-4.1.4.jar \
    /opt/spark/jars/
ADD --chmod=0644 --checksum=sha256:b641257bf720279865206a9cd8d69691c8004bc18e653829c6e1aa1d748c6716 \
    ${MAVEN}/org/apache/spark/spark-token-provider-kafka-0-10_2.13/4.1.4/spark-token-provider-kafka-0-10_2.13-4.1.4.jar \
    /opt/spark/jars/
ADD --chmod=0644 --checksum=sha256:7568b998572d256f0b7bc0afdc1b7a2588b8b08415c62ce314c864a6851ae9d9 \
    ${MAVEN}/org/apache/kafka/kafka-clients/3.9.1/kafka-clients-3.9.1.jar \
    /opt/spark/jars/
ADD --chmod=0644 --checksum=sha256:5273c8bc8c0dca2211175c0d27efbd7228efae99689aac001a8e1ef8e872e9ef \
    ${MAVEN}/org/apache/commons/commons-pool2/2.12.1/commons-pool2-2.12.1.jar \
    /opt/spark/jars/

USER root
RUN --mount=from=wheels,source=/wheels,target=/wheels \
    python3 -m pip install --no-index --no-deps --no-cache-dir /wheels/*.whl

COPY spark-defaults.conf log4j2.properties /opt/spark/conf/
COPY --chmod=0755 run-as-host-user.sh /opt/run-as-host-user.sh

USER spark
WORKDIR /opt/spark/work-dir
ENTRYPOINT ["/usr/bin/tini", "-s", "--", "/opt/run-as-host-user.sh"]
