import hashlib
import pytest
from spark.fraud_rules import FraudRulesEngine
from storage.postgres_writer import PostgresWriter


def test_deterministic_alert_id_generation():
    engine = FraudRulesEngine()
    tx_id = "test-tx-uuid-4455"
    rule_name = "HIGH_VALUE_TRANSACTION"

    id1 = engine.generate_alert_id(tx_id, rule_name)
    id2 = engine.generate_alert_id(tx_id, rule_name)
    assert id1 == id2
    assert len(id1) == 64  # SHA-256 hex string

    # Different rule must produce different alert_id
    id3 = engine.generate_alert_id(tx_id, "RAPID_REPEATED_TRANSFERS")
    assert id1 != id3


def test_duplicate_transaction_in_batch_detection():
    engine = FraudRulesEngine()
    tx_id = "tx-dup-999"

    tx1 = {
        "transaction_id": tx_id,
        "account_id": "ACC_1001",
        "amount": 100.0,
        "currency": "USD",
        "merchant_id": "MERCH_STORE",
        "transaction_type": "PURCHASE",
        "status": "COMPLETED",
        "event_timestamp": "2026-10-15T10:00:00Z",
    }

    # First occurrence: seen_tx_ids_in_batch is empty
    seen = set()
    alerts1 = engine.evaluate_transaction(tx1, seen_tx_ids_in_batch=seen)
    assert "DUPLICATE_TRANSACTION_EVENT" not in [a["rule_name"] for a in alerts1]

    # Add to seen set
    seen.add(tx_id)

    # Second occurrence (duplicate delivery)
    alerts2 = engine.evaluate_transaction(tx1, seen_tx_ids_in_batch=seen)
    rules2 = [a["rule_name"] for a in alerts2]
    assert "DUPLICATE_TRANSACTION_EVENT" in rules2


def test_postgres_writer_idempotent_sql_structure():
    """Verify that PostgresWriter SQL queries include idempotent conflict clauses."""
    import inspect

    src_tx = inspect.getsource(PostgresWriter.write_transactions)
    assert "ON CONFLICT (transaction_id) DO NOTHING" in src_tx

    src_alerts = inspect.getsource(PostgresWriter.write_fraud_alerts)
    assert "ON CONFLICT (transaction_id, rule_name) DO UPDATE" in src_alerts
