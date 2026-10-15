import datetime
import json
import logging
import os
import sys
import time
from typing import Any, Dict, List

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    date_format,
    expr,
    from_json,
    lit,
    to_date,
    to_timestamp,
    when,
)

# Add parent path to sys.path so storage and spark modules can be imported
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from spark.fraud_rules import FraudRulesEngine
from spark.schemas import ALLOWED_STATUSES, ALLOWED_TRANSACTION_TYPES, TRANSACTION_RAW_SCHEMA, validate_record_dict
from storage.postgres_writer import PostgresWriter

logger = logging.getLogger("TransactionStreamProcessor")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")


def create_spark_session() -> SparkSession:
    """Build Spark session configured with Kafka connector and optimized for 16GB host."""
    kafka_package = os.getenv("SPARK_KAFKA_PACKAGE", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8")
    postgres_driver = os.getenv("SPARK_POSTGRES_PACKAGE", "org.postgresql:postgresql:42.7.5")

    builder = (
        SparkSession.builder.appName("RealTimeFraudDetectionPipeline")
        .master("local[2]")  # 2 local cores suitable for lightweight streaming
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.jars.packages", f"{kafka_package},{postgres_driver}")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .config("spark.sql.session.timeZone", "UTC")
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    logger.info("Spark session created successfully.")
    return spark


class MicroBatchProcessor:
    """Processes each micro-batch: validation, deduplication, fraud detection, Parquet lake, and PostgreSQL storage."""

    def __init__(self, config_path: str, lake_bronze: str, lake_silver: str):
        self.rules_engine = FraudRulesEngine(config_path)
        self.lake_bronze = lake_bronze
        self.lake_silver = lake_silver
        self.db_writer = PostgresWriter()
        # Initialize database schema if not present
        try:
            self.db_writer.init_schema()
        except Exception as e:
            logger.warning("Could not automatically initialize DB schema: %s", e)

        os.makedirs(self.lake_bronze, exist_ok=True)
        os.makedirs(self.lake_silver, exist_ok=True)

    def process_batch(self, df, batch_id: int):
        start_time = time.time()
        count = df.count()
        if count == 0:
            logger.debug("Micro-batch %s is empty. Skipping.", batch_id)
            return

        logger.info("Starting processing for micro-batch %s with %d records...", batch_id, count)
        rows = df.collect()

        bronze_records = []
        silver_records = []
        quarantine_records = []
        seen_tx_ids_in_batch = set()
        alerts_to_write = []

        now_utc = datetime.datetime.now(datetime.timezone.utc)
        today_str = now_utc.strftime("%Y-%m-%d")

        for row in rows:
            raw_value_str = row["raw_payload"]
            kafka_offset = row["offset"]
            kafka_partition = row["partition"]

            # Save to bronze archive payload
            bronze_records.append({
                "raw_payload": raw_value_str,
                "kafka_offset": kafka_offset,
                "kafka_partition": kafka_partition,
                "ingestion_date": today_str,
                "ingested_at": now_utc.isoformat(),
            })

            # Parse JSON
            try:
                tx_dict = json.loads(raw_value_str)
            except Exception as parse_err:
                quarantine_records.append({
                    "raw_payload": raw_value_str,
                    "error_reason": f"JSON parse error: {str(parse_err)}",
                })
                continue

            # Validate schema and values
            is_valid, reason = validate_record_dict(tx_dict)
            if not is_valid:
                quarantine_records.append({
                    "raw_payload": raw_value_str,
                    "error_reason": f"Validation failed: {reason}",
                })
                continue

            tx_id = str(tx_dict["transaction_id"])
            # Evaluate fraud rules
            detected_alerts = self.rules_engine.evaluate_transaction(
                tx=tx_dict,
                seen_tx_ids_in_batch=seen_tx_ids_in_batch,
            )
            alerts_to_write.extend(detected_alerts)

            seen_tx_ids_in_batch.add(tx_id)

            # Format record for silver store & PostgreSQL
            event_ts = tx_dict["event_timestamp"]
            event_date = event_ts[:10] if len(event_ts) >= 10 else today_str

            silver_records.append({
                "transaction_id": tx_id,
                "account_id": str(tx_dict["account_id"]),
                "amount": float(tx_dict["amount"]),
                "currency": str(tx_dict["currency"]),
                "merchant_id": str(tx_dict["merchant_id"]),
                "transaction_type": str(tx_dict["transaction_type"]),
                "status": str(tx_dict["status"]),
                "event_timestamp": event_ts,
                "is_fraud_ground_truth": bool(tx_dict.get("is_fraud_ground_truth", False)),
                "event_date": event_date,
            })

        # 1. Write Quarantine records
        if quarantine_records:
            try:
                self.db_writer.write_quarantine(quarantine_records)
                logger.info("Batch %s: Quarantined %d invalid events.", batch_id, len(quarantine_records))
            except Exception as e:
                logger.error("Batch %s: Error writing quarantine records: %s", batch_id, e)

        # 2. Persist valid transactions in PostgreSQL
        if silver_records:
            try:
                self.db_writer.write_transactions(silver_records)
            except Exception as e:
                logger.error("Batch %s: Error persisting transactions to PostgreSQL: %s", batch_id, e)

        # 3. Persist fraud alerts in PostgreSQL
        if alerts_to_write:
            try:
                self.db_writer.write_fraud_alerts(alerts_to_write)
                logger.info("Batch %s: Generated and stored %d fraud alerts.", batch_id, len(alerts_to_write))
            except Exception as e:
                logger.error("Batch %s: Error persisting fraud alerts to PostgreSQL: %s", batch_id, e)

        # 4. Write Bronze Parquet partition
        if bronze_records:
            try:
                bronze_df = df.sparkSession.createDataFrame(bronze_records)
                (
                    bronze_df.write
                    .mode("append")
                    .partitionBy("ingestion_date")
                    .parquet(self.lake_bronze)
                )
            except Exception as e:
                logger.warning("Batch %s: Error writing Bronze Parquet lake: %s", batch_id, e)

        # 5. Write Silver Parquet partition
        if silver_records:
            try:
                silver_df = df.sparkSession.createDataFrame(silver_records)
                (
                    silver_df.write
                    .mode("append")
                    .partitionBy("event_date")
                    .parquet(self.lake_silver)
                )
            except Exception as e:
                logger.warning("Batch %s: Error writing Silver Parquet lake: %s", batch_id, e)

        # 6. Record batch processing metrics
        elapsed_ms = (time.time() - start_time) * 1000.0
        elapsed_sec = elapsed_ms / 1000.0 if elapsed_ms > 0 else 0.001
        processing_rate = count / elapsed_sec

        metric = {
            "batch_id": batch_id,
            "records_processed": len(silver_records),
            "alerts_generated": len(alerts_to_write),
            "batch_duration_ms": round(elapsed_ms, 2),
            "input_rate": round(count / max(1.0, elapsed_sec), 2),
            "processing_rate": round(processing_rate, 2),
            "quarantined_records": len(quarantine_records),
        }
        try:
            self.db_writer.write_processing_metrics(metric)
        except Exception as e:
            logger.warning("Batch %s: Error writing processing metrics: %s", batch_id, e)

        logger.info(
            "Batch %s completed in %.1fms | Clean: %d | Quarantined: %d | Alerts: %d",
            batch_id, elapsed_ms, len(silver_records), len(quarantine_records), len(alerts_to_write)
        )


def ensure_kafka_topics(bootstrap_servers: str, topics: List[str], max_retries: int = 10, retry_delay: float = 2.0) -> None:
    """Ensure required Kafka topics exist before Spark structured streaming begins reading."""
    try:
        from kafka.admin import KafkaAdminClient, NewTopic
    except ImportError:
        try:
            from kafka_python_ng.admin import KafkaAdminClient, NewTopic
        except ImportError:
            logger.warning("Kafka admin client not available to pre-create topics.")
            return

    for attempt in range(1, max_retries + 1):
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=bootstrap_servers.split(","),
                request_timeout_ms=5000,
            )
            existing = set(admin_client.list_topics())
            to_create = [
                NewTopic(name=t, num_partitions=3, replication_factor=1)
                for t in topics if t not in existing
            ]
            if to_create:
                admin_client.create_topics(new_topics=to_create, validate_only=False)
                logger.info("Auto-created Kafka topics: %s", [t.name for t in to_create])
            admin_client.close()
            return
        except Exception as e:
            if attempt == max_retries:
                logger.warning("Could not pre-create Kafka topics: %s", e)
            else:
                time.sleep(retry_delay)


def main():
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    topic = os.getenv("KAFKA_TOPIC_TRANSACTIONS", "financial_transactions")
    checkpoint_path = os.getenv("CHECKPOINT_PATH", "/app/checkpoints/spark_stream")
    lake_bronze = os.getenv("LAKE_BRONZE_PATH", "/app/lake/bronze")
    lake_silver = os.getenv("LAKE_SILVER_PATH", "/app/lake/silver")
    config_path = os.getenv("FRAUD_RULES_CONFIG_PATH", "/app/config/fraud_rules.yaml")
    trigger_interval = os.getenv("STREAM_TRIGGER_INTERVAL", "5 seconds")

    logger.info("Initializing Spark Streaming Pipeline...")
    logger.info("Kafka: %s | Topic: %s", bootstrap_servers, topic)
    logger.info("Checkpoints: %s", checkpoint_path)

    # Ensure topic exists in Kafka to prevent UnknownTopicOrPartitionException
    ensure_kafka_topics(bootstrap_servers, [topic, "transactions_dead_letter"])

    spark = create_spark_session()
    processor = MicroBatchProcessor(config_path, lake_bronze, lake_silver)

    # Read stream from Kafka
    kafka_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # Extract raw string value and metadata
    prepared_df = kafka_stream.selectExpr(
        "CAST(value AS STRING) AS raw_payload",
        "CAST(key AS STRING) AS message_key",
        "offset",
        "partition",
        "timestamp AS kafka_timestamp"
    )

    # Execute stream with micro-batch processing
    query = (
        prepared_df.writeStream
        .trigger(processingTime=trigger_interval)
        .foreachBatch(processor.process_batch)
        .option("checkpointLocation", checkpoint_path)
        .start()
    )

    logger.info("Streaming query started. Awaiting termination...")
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Termination signal received. Stopping streaming query gracefully...")
        query.stop()
        spark.stop()
        processor.db_writer.close()
        logger.info("Streaming query stopped.")


if __name__ == "__main__":
    main()
