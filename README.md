# Real-Time Fraud Detection Pipeline

A production-grade, end-to-end real-time financial fraud detection pipeline built with **Apache Kafka (KRaft mode)**, **Apache Spark Structured Streaming (PySpark 3.5.8)**, **PostgreSQL 16**, and a **Streamlit Dark-Themed Operations Dashboard**, running locally on Linux with Docker Compose.

---

## 🏛️ Architecture Overview

```mermaid
flowchart LR
    A[Synthetic Producer] -->|JSON Events| B[Apache Kafka<br>financial_transactions]
    B -->|Structured Streaming| C[PySpark Processor<br>Spark 3.5.8]
    C -->|Rule Engine| D{Fraud Rules}
    D -->|Valid Tx & Alerts| E[(PostgreSQL 16<br>Idempotent Upsert)]
    D -->|Raw & Clean Data| F[Parquet Lake<br>Bronze & Silver]
    D -->|Malformed Records| G[(Quarantine Dead-Letter)]
    E -->|Forensics & Telemetry| H[Streamlit Dashboard<br>Port 8501]
```

### Components

1. **Synthetic Producer (`producers/transaction_simulator.py`)**:
   - Generates realistic banking transactions (normal log-normal spend distribution, benign bursts, everyday merchants).
   - Injects controlled fraud patterns (high-value purchases, rapid burst transfers, unusual spending spikes, repeated card declines, duplicate transaction events).
   - Simulates schema violations and malformed events for dead-letter testing.
   - Configurable event rates, seeds, and fraud ratios.

2. **Apache Kafka 3.9.1 (KRaft Mode)**:
   - High-throughput distributed event broker operating in ZooKeeper-less KRaft mode.
   - Persistent volume storage and automated cluster ID initialization.
   - Dual-listener networking: internal container access (`kafka:9092`) and host access (`localhost:29092`).

3. **PySpark Structured Streaming (`spark/transaction_stream.py`)**:
   - Explicit JSON parsing schema (`TRANSACTION_RAW_SCHEMA`).
   - Strict field and range validation; invalid events are quarantined.
   - Configurable rule engine (`config/fraud_rules.yaml`):
     - **High-Value Transactions**: Amount $\ge$ $5,000.
     - **Rapid Repeated Transfers**: $\ge 3$ transfers within 60 seconds from the same account.
     - **Unusual Spending Spikes**: Amount $\ge 3.5\times$ account baseline average and $\ge \$1,500$.
     - **Repeated Failed Transactions**: $\ge 3$ failed/declined attempts within 5 minutes.
     - **Duplicate Transaction Events**: Exact duplicate IDs or duplicate bursts.
   - Stateful event-time windowing with watermark support.
   - Pre-installed connector jars (`spark-sql-kafka-0-10_2.12:3.5.8` and PostgreSQL JDBC).

4. **Storage & Data Lake**:
   - **PostgreSQL 16**: Idempotent batch upserts using `ON CONFLICT (transaction_id) DO NOTHING` and `ON CONFLICT (transaction_id, rule_name) DO UPDATE`.
   - **Bronze Lake (`lake/bronze/`)**: Preserves raw incoming payloads with Kafka metadata partitioned by `ingestion_date`.
   - **Silver Lake (`lake/silver/`)**: Cleaned, typed, normalized transactions partitioned by `event_date`.

5. **Operations Dashboard (`dashboard/app.py`)**:
   - Dark-themed financial monitoring cockpit built with Streamlit and Plotly.
   - **Page 1: Live Monitoring**: Real-time throughput (TPS), alert rate, severity breakdown, timeseries volume, recent transaction stream.
   - **Page 2: Fraud Investigation**: Search by Account ID or Transaction ID, severity filters, rule filters, forensic account transaction history, related alerts.
   - **Page 3: Pipeline Performance**: Micro-batch latency, processing rates, quarantine counts, local CPU/RAM utilization.

---

## 📋 Prerequisites & Hardware Requirements

- **OS**: Linux (tested on Ubuntu 24.04 LTS / Debian)
- **RAM**: Minimum 8 GB, 16 GB recommended.
- **Docker**: Docker Engine 24+ and Docker Compose v2.

Verify prerequisites:
```bash
docker --version
docker compose version
free -h
```

---

## 🚀 Quick Start Guide

### 1. Clone & Configure Environment

```bash
cd /home/michaelfernandes/Desktop/Projects/DE_Fraud_Detection

# Copy sample environment configuration
cp .env.example .env
```

### 2. Build Docker Images

```bash
docker compose build
```

### 3. Start Core Pipeline Services

Start Kafka, PostgreSQL, the Spark Streaming processor, and the Streamlit Dashboard:
```bash
docker compose up -d kafka postgres spark-stream dashboard
```

Verify service health:
```bash
docker compose ps
```

### 4. Start the Synthetic Transaction Producer

Start the transaction generator in background using Compose profiles:
```bash
docker compose --profile producer up -d producer
```

Or run interactively from the host or container:
```bash
# Via Docker:
docker compose run --rm producer python producers/transaction_simulator.py --rate 15 --seed 42

# Or directly on host:
python3 producers/transaction_simulator.py --bootstrap-servers localhost:29092 --rate 10
```

### 5. Access the Web Dashboard

Open your browser and navigate to:
```
http://localhost:8501
```

---

## 🛠️ Operational Commands

### Viewing Logs

```bash
# Follow Spark Streaming logs
docker compose logs -f spark-stream

# Follow Producer logs
docker compose logs -f producer

# Follow Kafka logs
docker compose logs -f kafka

# Follow PostgreSQL logs
docker compose logs -f postgres
```

### Inspecting Kafka Topics

```bash
# List topics
docker exec fraud_kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:29092 --list

# Consume live messages from transactions topic
docker exec fraud_kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:29092 \
  --topic financial_transactions \
  --from-beginning \
  --max-messages 10
```

### Querying PostgreSQL

```bash
# Connect to PostgreSQL CLI
docker exec -it fraud_postgres psql -U fraud_admin -d fraud_detection

# Check transaction and alert counts
docker exec fraud_postgres psql -U fraud_admin -d fraud_detection -c "
  SELECT 
    (SELECT COUNT(*) FROM transactions) AS total_transactions,
    (SELECT COUNT(*) FROM fraud_alerts) AS total_alerts,
    (SELECT COUNT(*) FROM quarantine_events) AS quarantined_events;
"

# View top triggered fraud rules
docker exec fraud_postgres psql -U fraud_admin -d fraud_detection -c "
  SELECT rule_name, severity, COUNT(*) AS count
  FROM fraud_alerts
  GROUP BY rule_name, severity
  ORDER BY count DESC;
"
```

### Inspecting Parquet Data Lake

```bash
# Check Bronze partitioned Parquet files
ls -la lake/bronze/

# Check Silver partitioned Parquet files
ls -la lake/silver/
```

---

## 🧪 Testing Suite

### Running Unit Tests

Unit tests execute without requiring running Docker containers:
```bash
# Inside the Docker test runner:
docker compose run --rm spark-stream pytest -v tests/

# Or locally (if pytest is installed in your python environment):
pytest -v tests/
```

The automated test suite verifies:
- `test_event_validation.py`: Schema enforcement, missing keys, negative amounts, invalid status codes.
- `test_fraud_rules.py`: High-value detection, rapid bursts, unusual spending spikes, repeated failures, multi-alert triggers.
- `test_deduplication.py`: Deterministic alert hashing, within-batch duplicate recognition, idempotent SQL queries.

---

## 📊 Benchmarking & Quality Evaluation

Run the benchmark script to test throughput across 10, 50, and 100 tx/s:
```bash
python3 benchmark.py --rates 10,50,100 --duration 15
```

The benchmark reports:
- **Throughput**: Events generated/s and events processed/s.
- **Latency**: End-to-end event timestamp to alert persistence latency (ms).
- **Micro-Batch Duration**: Time taken by PySpark to process each batch.
- **Detection Metrics**: Precision, Recall, and F1-Score evaluated strictly against synthetic ground truth labels (`is_fraud_ground_truth`).

---

## 🔧 Troubleshooting

### 1. Kafka Container Readiness
- **Symptom**: Spark or Producer reports `Connection refused` or `NoBrokersAvailable`.
- **Cause**: Kafka KRaft cluster takes 5–8 seconds to form quorum on initial startup.
- **Solution**: The healthcheck ensures services only start once Kafka is ready:
  ```bash
  docker compose ps kafka
  docker compose logs kafka --tail 50
  ```

### 2. Checkpoint Invalidation
- **Symptom**: Spark crashes with `Invalid checkpoint` after schema alterations.
- **Solution**: Checkpoints are stored in persistent volume `fraud_spark_checkpoints`. To reset checkpoint state without losing database data:
  ```bash
  docker compose stop spark-stream
  docker compose rm -f spark-stream
  docker volume rm fraud_spark_checkpoints
  docker compose up -d spark-stream
  ```

### 3. Port Conflicts
- **Symptom**: `Bind for 0.0.0.0:5432 failed: port is already allocated`.
- **Solution**: You may have a local PostgreSQL or Kafka running on the host. Modify the host port bindings in `.env` or `docker-compose.yml` (e.g. `127.0.0.1:5433:5432`).

### 4. Memory Optimization
- All services in `docker-compose.yml` are capped with explicit memory limits:
  - Spark: 3.0 GB
  - Kafka: 1.5 GB
  - PostgreSQL: 1.0 GB
  - Dashboard: 1.0 GB
  - Producer: 0.5 GB
  - Total allocation $\approx$ 7.0 GB, operating smoothly on 16 GB machines.

---

## 🛑 Teardown & Clean Up

### Stop Services (Preserving Data)
```bash
docker compose down
```

### Full Reset (WARNING: Erases all data, checkpoints, and databases)
```bash
docker compose down -v
```
