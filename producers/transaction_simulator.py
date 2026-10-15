import argparse
import datetime
import json
import logging
import os
import random
import signal
import sys
import time
import uuid
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger("TransactionSimulator")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")


class TransactionSimulator:
    """Synthetic financial transaction generator with normal patterns and injected anomalies."""

    MERCHANTS = [
        "MERCH_AMAZON", "MERCH_WALMART", "MERCH_UBER", "MERCH_STARBUCKS",
        "MERCH_NETFLIX", "MERCH_SHELL", "MERCH_TARGET", "MERCH_APPLE",
        "MERCH_AIRBNB", "MERCH_DELTA", "MERCH_BESTBUY", "MERCH_GROCERY"
    ]

    TRANSACTION_TYPES = ["PURCHASE", "PAYMENT", "TRANSFER", "WITHDRAWAL"]
    CURRENCIES = ["USD"]

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        rate: float = 10.0,
        seed: Optional[int] = 42,
        fraud_ratio: float = 0.15,
        malformed_ratio: float = 0.02,
        num_accounts: int = 50,
    ):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.rate = rate
        self.interval = 1.0 / rate if rate > 0 else 0.1
        self.seed = seed
        self.fraud_ratio = fraud_ratio
        self.malformed_ratio = malformed_ratio
        self.num_accounts = num_accounts
        self.running = True

        if self.seed is not None:
            random.seed(self.seed)

        self.accounts = [f"ACC_{1000 + i}" for i in range(self.num_accounts)]
        # Define account profiles (some accounts are low-spenders, some high-spenders)
        self.account_baselines = {
            acc: (random.uniform(20.0, 80.0) if i % 4 != 0 else random.uniform(200.0, 500.0))
            for i, acc in enumerate(self.accounts)
        }

        self.producer = self._init_kafka_producer()

    def _init_kafka_producer(self, max_retries: int = 15, retry_delay: float = 3.0):
        """Initialize Kafka producer with connection retries."""
        try:
            from kafka import KafkaProducer
        except ImportError:
            try:
                from kafka_python_ng import KafkaProducer
            except ImportError:
                logger.error("Neither kafka nor kafka-python-ng is installed.")
                raise

        logger.info("Connecting to Kafka at %s (topic: %s)...", self.bootstrap_servers, self.topic)
        for attempt in range(1, max_retries + 1):
            try:
                producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers.split(","),
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                    key_serializer=lambda k: k.encode("utf-8") if k else None,
                    retries=5,
                    acks="all",
                    request_timeout_ms=10000,
                )
                logger.info("Successfully connected to Kafka.")
                return producer
            except Exception as e:
                logger.warning("Kafka connection attempt %d/%d failed: %s. Retrying in %.1fs...",
                               attempt, max_retries, e, retry_delay)
                time.sleep(retry_delay)

        raise ConnectionError(f"Could not connect to Kafka at {self.bootstrap_servers} after {max_retries} attempts.")

    def generate_normal_transaction(self, account_id: Optional[str] = None) -> Dict[str, Any]:
        """Generate a realistic benign transaction."""
        acc = account_id or random.choice(self.accounts)
        baseline = self.account_baselines[acc]

        # 95% regular amount around baseline, 5% legitimate larger purchase (e.g. electronics)
        if random.random() < 0.95:
            amount = max(1.50, round(random.lognormvariate(mu=0.0, sigma=0.6) * baseline, 2))
        else:
            amount = round(random.uniform(800.0, 2500.0), 2)  # Benign high-value purchase

        now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
        status = "COMPLETED"
        if random.random() < 0.04:
            status = "FAILED" if random.random() < 0.5 else "DECLINED"

        return {
            "transaction_id": str(uuid.uuid4()),
            "account_id": acc,
            "amount": amount,
            "currency": random.choice(self.CURRENCIES),
            "merchant_id": random.choice(self.MERCHANTS),
            "transaction_type": random.choice(self.TRANSACTION_TYPES),
            "status": status,
            "event_timestamp": now_iso,
            "is_fraud_ground_truth": False,
        }

    def generate_fraud_pattern(self) -> List[Dict[str, Any]]:
        """Inject specific synthetic fraud patterns."""
        pattern_type = random.choice([
            "high_value",
            "rapid_transfers",
            "unusual_spending",
            "repeated_failed",
            "duplicate_event"
        ])
        acc = random.choice(self.accounts)
        now_dt = datetime.datetime.now(datetime.timezone.utc)
        events = []

        if pattern_type == "high_value":
            # Amount > $5000
            events.append({
                "transaction_id": str(uuid.uuid4()),
                "account_id": acc,
                "amount": round(random.uniform(5500.0, 14500.0), 2),
                "currency": "USD",
                "merchant_id": random.choice(self.MERCHANTS),
                "transaction_type": random.choice(["TRANSFER", "WITHDRAWAL", "PURCHASE"]),
                "status": "COMPLETED",
                "event_timestamp": now_dt.isoformat(),
                "is_fraud_ground_truth": True,
            })

        elif pattern_type == "rapid_transfers":
            # 3-4 rapid transfers within seconds
            num_txns = random.randint(3, 4)
            for i in range(num_txns):
                ts = (now_dt + datetime.timedelta(seconds=i * 2)).isoformat()
                events.append({
                    "transaction_id": str(uuid.uuid4()),
                    "account_id": acc,
                    "amount": round(random.uniform(400.0, 950.0), 2),
                    "currency": "USD",
                    "merchant_id": "MERCH_TRANSFER_BURST",
                    "transaction_type": "TRANSFER",
                    "status": "COMPLETED",
                    "event_timestamp": ts,
                    "is_fraud_ground_truth": True,
                })

        elif pattern_type == "unusual_spending":
            # Account with small baseline spikes significantly above $1,500
            baseline = self.account_baselines[acc]
            amount = round(max(1800.0, baseline * random.uniform(10.0, 25.0)), 2)
            events.append({
                "transaction_id": str(uuid.uuid4()),
                "account_id": acc,
                "amount": amount,
                "currency": "USD",
                "merchant_id": "MERCH_LUXURY_JEWELRY",
                "transaction_type": "PURCHASE",
                "status": "COMPLETED",
                "event_timestamp": now_dt.isoformat(),
                "is_fraud_ground_truth": True,
            })

        elif pattern_type == "repeated_failed":
            # 3 failed attempts in a row
            for i in range(3):
                ts = (now_dt + datetime.timedelta(seconds=i * 5)).isoformat()
                events.append({
                    "transaction_id": str(uuid.uuid4()),
                    "account_id": acc,
                    "amount": round(random.uniform(150.0, 500.0), 2),
                    "currency": "USD",
                    "merchant_id": random.choice(self.MERCHANTS),
                    "transaction_type": "WITHDRAWAL",
                    "status": "FAILED",
                    "event_timestamp": ts,
                    "is_fraud_ground_truth": True,
                })

        elif pattern_type == "duplicate_event":
            # Generate a base transaction and an exact duplicate with same ID
            base_tx = self.generate_normal_transaction(account_id=acc)
            base_tx["is_fraud_ground_truth"] = False
            dup_tx = dict(base_tx)
            dup_tx["is_fraud_ground_truth"] = True
            events.append(base_tx)
            events.append(dup_tx)

        return events

    def generate_malformed_event(self) -> Dict[str, Any]:
        """Generate an invalid or schema-violating payload for quarantine testing."""
        variant = random.choice(["missing_id", "negative_amount", "invalid_timestamp", "corrupt_type"])
        now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()

        if variant == "missing_id":
            return {
                "account_id": random.choice(self.accounts),
                "amount": 100.0,
                "currency": "USD",
                "merchant_id": "MERCH_CORRUPT",
                "status": "COMPLETED",
                "event_timestamp": now_iso,
            }
        elif variant == "negative_amount":
            return {
                "transaction_id": str(uuid.uuid4()),
                "account_id": random.choice(self.accounts),
                "amount": -50.0,
                "currency": "USD",
                "merchant_id": "MERCH_CORRUPT",
                "transaction_type": "PURCHASE",
                "status": "COMPLETED",
                "event_timestamp": now_iso,
            }
        elif variant == "invalid_timestamp":
            return {
                "transaction_id": str(uuid.uuid4()),
                "account_id": random.choice(self.accounts),
                "amount": 25.0,
                "currency": "USD",
                "merchant_id": "MERCH_CORRUPT",
                "transaction_type": "PURCHASE",
                "status": "COMPLETED",
                "event_timestamp": "NOT_A_TIMESTAMP_STRING",
            }
        else:
            return {
                "transaction_id": str(uuid.uuid4()),
                "account_id": random.choice(self.accounts),
                "amount": 80.0,
                "currency": "USD",
                "merchant_id": "MERCH_CORRUPT",
                "transaction_type": "UNKNOWN_ACTION_999",
                "status": "INVALID_STATUS",
                "event_timestamp": now_iso,
            }

    def emit_event(self, event: Dict[str, Any]) -> None:
        """Send event to Kafka topic."""
        key = event.get("account_id") or event.get("transaction_id") or "default"
        self.producer.send(self.topic, key=key, value=event)

    def run(self, max_count: Optional[int] = None) -> None:
        """Main simulation loop."""
        logger.info(
            "Starting transaction simulator at %.1f tx/s (fraud_ratio=%.2f, malformed_ratio=%.2f)...",
            self.rate, self.fraud_ratio, self.malformed_ratio
        )
        count = 0
        fraud_count = 0
        malformed_count = 0
        start_time = time.time()

        def stop_handler(signum, frame):
            logger.info("Signal received. Stopping simulation gracefully...")
            self.running = False

        signal.signal(signal.SIGINT, stop_handler)
        signal.signal(signal.SIGTERM, stop_handler)

        while self.running and (max_count is None or count < max_count):
            loop_start = time.time()

            # Check if malformed event should be injected
            if self.malformed_ratio > 0 and random.random() < self.malformed_ratio:
                bad_event = self.generate_malformed_event()
                self.emit_event(bad_event)
                count += 1
                malformed_count += 1
            # Check if fraud pattern should be injected
            elif self.fraud_ratio > 0 and random.random() < self.fraud_ratio:
                fraud_events = self.generate_fraud_pattern()
                for fe in fraud_events:
                    self.emit_event(fe)
                    count += 1
                    if fe.get("is_fraud_ground_truth", False):
                        fraud_count += 1
                    if max_count is not None and count >= max_count:
                        break
            else:
                event = self.generate_normal_transaction()
                self.emit_event(event)
                count += 1

            if count % 100 == 0:
                elapsed = time.time() - start_time
                actual_rate = count / elapsed if elapsed > 0 else 0
                logger.info(
                    "Emitted %d events (actual rate: %.1f tx/s | synthetic fraud: %d | malformed: %d)",
                    count, actual_rate, fraud_count, malformed_count
                )
                self.producer.flush()

            # Rate throttling
            elapsed_loop = time.time() - loop_start
            sleep_time = self.interval - elapsed_loop
            if sleep_time > 0:
                time.sleep(sleep_time)

        self.producer.flush()
        elapsed_total = time.time() - start_time
        logger.info(
            "Simulation finished. Total emitted: %d (rate: %.1f tx/s | fraud: %d | malformed: %d in %.1fs)",
            count, count / elapsed_total if elapsed_total > 0 else 0, fraud_count, malformed_count, elapsed_total
        )


def main():
    parser = argparse.ArgumentParser(description="Synthetic Bank Transaction Simulator")
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC_TRANSACTIONS", "financial_transactions"))
    parser.add_argument("--rate", type=float, default=float(os.getenv("TRANSACTION_RATE", "10.0")))
    parser.add_argument("--count", type=int, default=int(os.getenv("MAX_TRANSACTION_COUNT", "0")),
                        help="Max events to generate (0 for unlimited)")
    parser.add_argument("--seed", type=int, default=int(os.getenv("PRODUCER_SEED", "42")))
    parser.add_argument("--fraud-ratio", type=float, default=float(os.getenv("INJECT_FRAUD_RATIO", "0.15")))
    parser.add_argument("--malformed-ratio", type=float, default=float(os.getenv("INJECT_MALFORMED_RATIO", "0.02")))
    args = parser.parse_args()

    max_count = args.count if args.count > 0 else None
    sim = TransactionSimulator(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        rate=args.rate,
        seed=args.seed,
        fraud_ratio=args.fraud_ratio,
        malformed_ratio=args.malformed_ratio,
    )
    sim.run(max_count=max_count)


if __name__ == "__main__":
    main()
