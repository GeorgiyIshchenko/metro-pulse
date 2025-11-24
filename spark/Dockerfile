ARG BASE_IMAGE=bde2020/spark-master:3.3.0-hadoop3.3
FROM ${BASE_IMAGE}

ARG HADOOP_AWS_VERSION=3.3.0
ARG AWS_SDK_BUNDLE_VERSION=1.11.901
ARG GUAVA_VERSION=27.0-jre
ARG POSTGRES_JDBC_VERSION=42.7.4
ARG CLICKHOUSE_JDBC_VERSION=0.6.3

ENV SPARK_EXTRA_JARS_DIR=/spark/jars
ENV HADOOP_COMMON_LIB_DIR=/usr/local/hadoop/share/hadoop/common/lib

RUN set -eux; \
    mkdir -p "$SPARK_EXTRA_JARS_DIR" "$HADOOP_COMMON_LIB_DIR" && \
    \
    rm -f "$SPARK_EXTRA_JARS_DIR"/guava-*.jar || true; \
    rm -f "$HADOOP_COMMON_LIB_DIR"/guava-*.jar || true; \
    \
    curl -fSL "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
      -o "${SPARK_EXTRA_JARS_DIR}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" && \
    \
    curl -fSL "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_BUNDLE_VERSION}/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar" \
      -o "${SPARK_EXTRA_JARS_DIR}/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar" && \
    \
    curl -fSL "https://repo1.maven.org/maven2/com/google/guava/guava/${GUAVA_VERSION}/guava-${GUAVA_VERSION}.jar" \
      -o "${SPARK_EXTRA_JARS_DIR}/guava-${GUAVA_VERSION}.jar" && \
    cp "${SPARK_EXTRA_JARS_DIR}/guava-${GUAVA_VERSION}.jar" \
       "${HADOOP_COMMON_LIB_DIR}/guava-${GUAVA_VERSION}.jar" && \
   \
    curl -fSL "https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar" \
      -o "${SPARK_EXTRA_JARS_DIR}/postgresql-${POSTGRES_JDBC_VERSION}.jar" && \
    \
    curl -fSL "https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/${CLICKHOUSE_JDBC_VERSION}/clickhouse-jdbc-${CLICKHOUSE_JDBC_VERSION}-all.jar" \
      -o "${SPARK_EXTRA_JARS_DIR}/clickhouse-jdbc-${CLICKHOUSE_JDBC_VERSION}.jar"
