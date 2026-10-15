import logging
import os
import time
from contextlib import contextmanager
from typing import Any, Dict, List, Optional
import psycopg2
from psycopg2 import pool
from psycopg2.extras import execute_batch

logger = logging.getLogger("PostgresWriter")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")


class PostgresWriter:
    """Thread-safe, pooled, idempotent PostgreSQL writer for transactions, alerts, and metrics."""

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        dbname: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        minconn: int = 1,
        maxconn: int = 10,
    ):
        self.host = host or os.getenv("POSTGRES_HOST", "postgres")
        self.port = int(port or os.getenv("POSTGRES_PORT", "5432"))
        self.dbname = dbname or os.getenv("POSTGRES_DB", "fraud_detection")
        self.user = user or os.getenv("POSTGRES_USER", "fraud_admin")
        self.password = password or os.getenv("POSTGRES_PASSWORD", "fraud_secure_pass_2026")
        self.minconn = minconn
        self.maxconn = maxconn
        self._pool: Optional[pool.SimpleConnectionPool] = None
        self._init_pool()

    def _init_pool(self, max_retries: int = 5, retry_delay: float = 2.0) -> None:
        """Initialize connection pool with retry mechanism."""
        for attempt in range(1, max_retries + 1):
            try:
                self._pool = pool.SimpleConnectionPool(
                    self.minconn,
                    self.maxconn,
                    host=self.host,
                    port=self.port,
                    dbname=self.dbname,
                    user=self.user,
                    password=self.password,
                    connect_timeout=10,
                )
                logger.info("Connected to PostgreSQL at %s:%s/%s", self.host, self.port, self.dbname)
                return
            except psycopg2.OperationalError as exc:
                if attempt == max_retries:
                    logger.error("Failed to connect to PostgreSQL after %d attempts: %s", max_retries, exc)
                    raise
                logger.warning("PostgreSQL connection attempt %d failed. Retrying in %.1fs...", attempt, retry_delay)
                time.sleep(retry_delay)

    @contextmanager
    def get_connection(self):
        """Context manager for acquiring and releasing a pooled connection."""
        if self._pool is None:
            self._init_pool()
        conn = self._pool.getconn()
        try:
            yield conn
        finally:
            self._pool.putconn(conn)

    def init_schema(self, schema_file_path: Optional[str] = None) -> None:
        """Execute DDL statements from the schema file."""
        if not schema_file_path:
            schema_file_path = os.path.join(os.path.dirname(__file__), "schema.sql")

        if not os.path.exists(schema_file_path):
            logger.warning("Schema file not found at %s. Skipping DDL init.", schema_file_path)
            return

        with open(schema_file_path, "r", encoding="utf-8") as f:
            ddl_script = f.read()

        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(ddl_script)
            conn.commit()
        logger.info("Database schema initialized successfully from %s", schema_file_path)

    def write_transactions(self, records: List[Dict[str, Any]]) -> int:
        """Idempotently insert transactions batch (ON CONFLICT DO NOTHING)."""
        if not records:
            return 0

        query = """
            INSERT INTO transactions (
                transaction_id,
                account_id,
                amount,
                currency,
                merchant_id,
                transaction_type,
                status,
                event_timestamp,
                is_fraud_ground_truth
            ) VALUES (
                %(transaction_id)s,
                %(account_id)s,
                %(amount)s,
                %(currency)s,
                %(merchant_id)s,
                %(transaction_type)s,
                %(status)s,
                %(event_timestamp)s,
                %(is_fraud_ground_truth)s
            )
            ON CONFLICT (transaction_id) DO NOTHING;
        """
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                execute_batch(cur, query, records, page_size=500)
            conn.commit()
        return len(records)

    def write_fraud_alerts(self, alerts: List[Dict[str, Any]]) -> int:
        """Idempotently insert fraud alerts batch (ON CONFLICT DO UPDATE)."""
        if not alerts:
            return 0

        query = """
            INSERT INTO fraud_alerts (
                alert_id,
                transaction_id,
                account_id,
                rule_name,
                severity,
                explanation,
                event_timestamp,
                processing_timestamp
            ) VALUES (
                %(alert_id)s,
                %(transaction_id)s,
                %(account_id)s,
                %(rule_name)s,
                %(severity)s,
                %(explanation)s,
                %(event_timestamp)s,
                %(processing_timestamp)s
            )
            ON CONFLICT (transaction_id, rule_name) DO UPDATE SET
                processing_timestamp = EXCLUDED.processing_timestamp,
                explanation = EXCLUDED.explanation;
        """
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                execute_batch(cur, query, alerts, page_size=500)
            conn.commit()
        return len(alerts)

    def write_processing_metrics(self, metric: Dict[str, Any]) -> None:
        """Insert micro-batch metrics row."""
        query = """
            INSERT INTO processing_metrics (
                batch_id,
                records_processed,
                alerts_generated,
                batch_duration_ms,
                input_rate,
                processing_rate,
                quarantined_records
            ) VALUES (
                %(batch_id)s,
                %(records_processed)s,
                %(alerts_generated)s,
                %(batch_duration_ms)s,
                %(input_rate)s,
                %(processing_rate)s,
                %(quarantined_records)s
            );
        """
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, metric)
            conn.commit()

    def write_quarantine(self, events: List[Dict[str, Any]]) -> int:
        """Record malformed/invalid events to quarantine table."""
        if not events:
            return 0

        query = """
            INSERT INTO quarantine_events (raw_payload, error_reason)
            VALUES (%(raw_payload)s, %(error_reason)s);
        """
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                execute_batch(cur, query, events, page_size=200)
            conn.commit()
        return len(events)

    def close(self) -> None:
        """Close connection pool cleanly."""
        if self._pool is not None:
            self._pool.closeall()
            self._pool = None
            logger.info("PostgreSQL connection pool closed.")
