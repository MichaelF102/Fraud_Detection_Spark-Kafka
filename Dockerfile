FROM python:3.11-slim-bookworm

LABEL maintainer="Data Engineering Team"
LABEL description="Real-Time Fraud Detection Pipeline Runtime"

# Install OpenJDK 17 headless for PySpark, procps, and networking tools
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    procps \
    curl \
    libpq5 \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

WORKDIR /app

# Install pinned Python dependencies
COPY requirements.txt requirements-spark.txt ./
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt && \
    pip install --no-cache-dir -r requirements-spark.txt

# Download Spark Kafka connector & PostgreSQL JDBC drivers
COPY scripts/download_jars.py scripts/
RUN python scripts/download_jars.py

# Copy project code
COPY . .

# Ensure storage directories exist
RUN mkdir -p /app/checkpoints /app/lake/bronze /app/lake/silver /app/data

ENV PYTHONPATH=/app
