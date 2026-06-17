# Use a lightweight Debian-based image as the base
FROM debian:bookworm-slim

# Define arguments for versions to make them easy to update
# CHECK COMPATIBILITY: https://delta-docs-incubator.netlify.app/releases/
ARG SPARK_VERSION="3.5.3"
ARG HADOOP_VERSION="3"
ARG SCALA_VERSION="2.12"
ARG DELTA_LAKE_VERSION="3.3.2"
ARG DELTA_SPARK_JAR_URL="https://repo1.maven.org/maven2/io/delta/delta-spark_${SCALA_VERSION}/${DELTA_LAKE_VERSION}/delta-spark_${SCALA_VERSION}-${DELTA_LAKE_VERSION}.jar"
ARG DELTA_STORAGE_JAR_URL="https://repo1.maven.org/maven2/io/delta/delta-storage/${DELTA_LAKE_VERSION}/delta-storage-${DELTA_LAKE_VERSION}.jar"

# Set non-interactive mode for Debian package manager
ENV DEBIAN_FRONTEND=noninteractive

# Update package lists and install dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    wget curl openjdk-17-jdk python3 python3-pip python3-setuptools \
    python3-wheel procps bash git \
    && rm -rf /var/lib/apt/lists/*

# Download and extract the Apache Spark tarball
WORKDIR /tmp
RUN wget -q "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" && \
    tar xzf "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" && \
    mv "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}" /opt/spark && \
    rm "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz"

# Create a non-root user for security best practices
ARG USER_UID=1000
ARG USER_GID=1000
RUN groupadd --gid $USER_GID sparkuser \
    && useradd --uid $USER_UID --gid $USER_GID -m -s /bin/bash sparkuser \
    && chown -R sparkuser:sparkuser /opt/spark

# Switch user
USER sparkuser
WORKDIR /home/sparkuser

# Set up environment variables for Spark
ENV SPARK_HOME="/opt/spark"
ENV PATH="${PATH}:${SPARK_HOME}/bin:${SPARK_HOME}/sbin"
ENV JAVA_HOME="/usr/lib/jvm/java-17-openjdk-arm64"

# Download the Delta Lake jars
RUN wget -q -O "${SPARK_HOME}/jars/delta-spark_${SCALA_VERSION}-${DELTA_LAKE_VERSION}.jar" "${DELTA_SPARK_JAR_URL}" && \
    wget -q -O "${SPARK_HOME}/jars/delta-storage-${DELTA_LAKE_VERSION}.jar" "${DELTA_STORAGE_JAR_URL}"

# Install Python packages
RUN python3 -m pip install --no-cache-dir --break-system-packages \
    pyspark==${SPARK_VERSION} delta-spark==${DELTA_LAKE_VERSION} \
    ipykernel chispa pytest

# Configure PySpark to use Delta Lake by default
ENV PYSPARK_SUBMIT_ARGS="--packages io.delta:delta-spark_${SCALA_VERSION}:${DELTA_LAKE_VERSION} --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog pyspark-shell"

# Fix git autocrlf and keep container running
RUN git config --global core.autocrlf input
CMD ["tail", "-f", "/dev/null"]