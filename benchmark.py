import argparse
import datetime
import json
import logging
import os
import sys
import time
from typing import Any, Dict, List, Optional
import psycopg2
from psycopg2.extras import RealDictCursor

# Add project root to sys.path
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from producers.transaction_simulator import TransactionSimulator

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("BenchmarkRunner")


def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "fraud_detection"),
        user=os.getenv("POSTGRES_USER", "fraud_admin"),
        password=os.getenv("POSTGRES_PASSWORD", "fraud_secure_pass_2026"),
        connect_timeout=10,
    )


class PipelineBenchmark:
    """End-to-end benchmark suite for real-time throughput, latency, and detection quality."""

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        postgres_host: str = "localhost",
        postgres_port: int = 5432,
    ):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.pg_host = postgres_host
        self.pg_port = postgres_port

    def get_pipeline_snapshot(self) -> Dict[str, Any]:
        """Fetch current counts from database."""
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT 
                        (SELECT COUNT(*) FROM transactions) AS total_tx,
                        (SELECT COUNT(*) FROM transactions WHERE is_fraud_ground_truth = true) AS gt_fraud_tx,
                        (SELECT COUNT(*) FROM fraud_alerts) AS total_alerts,
                        (SELECT COUNT(*) FROM quarantine_events) AS quarantined,
                        (SELECT AVG(batch_duration_ms) FROM processing_metrics WHERE recorded_at >= NOW() - INTERVAL '2 minutes') AS avg_batch_ms
                """)
                return dict(cur.fetchone())

    def run_benchmark_rate(self, rate: float, duration_seconds: int = 20) -> Dict[str, Any]:
        """Run benchmark at a specific target event rate."""
        logger.info("==================================================================")
        logger.info("STARTING BENCHMARK RUN: Target Rate = %.1f tx/s | Duration = %ds", rate, duration_seconds)
        logger.info("==================================================================")

        sim = TransactionSimulator(
            bootstrap_servers=self.bootstrap_servers,
            topic=self.topic,
            rate=rate,
            seed=int(time.time()),
            fraud_ratio=0.15,
            malformed_ratio=0.02,
        )

        initial_snapshot = self.get_pipeline_snapshot()
        run_start_time = datetime.datetime.now(datetime.timezone.utc)
        start_ts = time.time()

        # Emit events for specified duration
        target_count = int(rate * duration_seconds)
        logger.info("Emitting %d events over ~%ds...", target_count, duration_seconds)
        sim.run(max_count=target_count)
        emission_time = time.time() - start_ts
        actual_emission_rate = target_count / emission_time if emission_time > 0 else 0

        # Wait for Spark micro-batches to catch up and persist all data
        wait_seconds = max(12, int(target_count / 20) + 8)
        logger.info("Waiting %ds for Spark streaming pipeline to process and persist all micro-batches...", wait_seconds)
        time.sleep(wait_seconds)

        final_snapshot = self.get_pipeline_snapshot()

        # Compute precision, recall, latency on this specific run's window
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Confusion matrix query
                cur.execute("""
                    SELECT 
                        COUNT(DISTINCT t.transaction_id) FILTER (WHERE t.is_fraud_ground_truth = true AND fa.alert_id IS NOT NULL) AS tp,
                        COUNT(DISTINCT t.transaction_id) FILTER (WHERE t.is_fraud_ground_truth = false AND fa.alert_id IS NOT NULL) AS fp,
                        COUNT(DISTINCT t.transaction_id) FILTER (WHERE t.is_fraud_ground_truth = false AND fa.alert_id IS NULL) AS tn,
                        COUNT(DISTINCT t.transaction_id) FILTER (WHERE t.is_fraud_ground_truth = true AND fa.alert_id IS NULL) AS fn,
                        AVG(EXTRACT(EPOCH FROM (fa.processing_timestamp - fa.event_timestamp))) * 1000.0 AS avg_latency_ms
                    FROM transactions t
                    LEFT JOIN fraud_alerts fa ON t.transaction_id = fa.transaction_id
                    WHERE t.event_timestamp >= %s
                """, (run_start_time,))
                perf_row = dict(cur.fetchone())

        tp = int(perf_row["tp"] or 0)
        fp = int(perf_row["fp"] or 0)
        tn = int(perf_row["tn"] or 0)
        fn = int(perf_row["fn"] or 0)
        avg_latency_ms = float(perf_row["avg_latency_ms"] or 0.0)

        precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
        recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
        f1 = (2 * precision * recall) / (precision + recall) if (precision + recall) > 0 else 0.0

        processed_tx = int(final_snapshot["total_tx"] - initial_snapshot["total_tx"])
        new_alerts = int(final_snapshot["total_alerts"] - initial_snapshot["total_alerts"])
        quarantined = int(final_snapshot["quarantined"] - initial_snapshot["quarantined"])

        result = {
            "target_rate_tps": rate,
            "actual_emission_rate_tps": round(float(actual_emission_rate), 1),
            "total_emitted": target_count,
            "total_processed": processed_tx,
            "new_alerts": new_alerts,
            "quarantined": quarantined,
            "avg_latency_ms": round(float(avg_latency_ms), 1),
            "tp": tp,
            "fp": fp,
            "tn": tn,
            "fn": fn,
            "precision": round(float(precision), 4),
            "recall": round(float(recall), 4),
            "f1_score": round(float(f1), 4),
        }

        logger.info("Run Results: %s", json.dumps(result, indent=2, default=str))
        return result


def main():
    parser = argparse.ArgumentParser(description="Real-Time Fraud Detection Pipeline Benchmark")
    parser.add_argument("--rates", default="10,50,100", help="Comma-separated rates to test (tx/s)")
    parser.add_argument("--duration", type=int, default=15, help="Duration in seconds per rate test")
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_HOST_BOOTSTRAP_SERVERS", "localhost:29092"))
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC_TRANSACTIONS", "financial_transactions"))
    args = parser.parse_args()

    rates = [float(r.strip()) for r in args.rates.split(",") if r.strip()]
    bench = PipelineBenchmark(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
    )

    all_results = []
    for r in rates:
        res = bench.run_benchmark_rate(rate=r, duration_seconds=args.duration)
        all_results.append(res)

    print("\n" + "=" * 95)
    print("                    PIPELINE BENCHMARK SUMMARY REPORT")
    print("=" * 95)
    headers = ["Target TPS", "Emission TPS", "Emitted", "Processed", "Alerts", "Avg Latency (ms)", "Precision", "Recall", "F1 Score"]
    print(f"{headers[0]:<12} {headers[1]:<14} {headers[2]:<10} {headers[3]:<11} {headers[4]:<8} {headers[5]:<18} {headers[6]:<11} {headers[7]:<8} {headers[8]:<8}")
    print("-" * 95)
    for res in all_results:
        print(f"{res['target_rate_tps']:<12.1f} {res['actual_emission_rate_tps']:<14.1f} {res['total_emitted']:<10} {res['total_processed']:<11} {res['new_alerts']:<8} {res['avg_latency_ms']:<18.1f} {res['precision']:<11.3f} {res['recall']:<8.3f} {res['f1_score']:<8.3f}")
    print("=" * 95)


if __name__ == "__main__":
    main()
