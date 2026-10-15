import datetime
import pytest
from spark.fraud_rules import FraudRulesEngine


@pytest.fixture
def engine():
    return FraudRulesEngine()


def test_high_value_transaction_trigger(engine):
    tx = {
        "transaction_id": "tx-hv-1",
        "account_id": "ACC_1001",
        "amount": 7500.00,
        "currency": "USD",
        "merchant_id": "MERCH_LUXURY",
        "transaction_type": "PURCHASE",
        "status": "COMPLETED",
        "event_timestamp": "2026-10-15T10:00:00Z",
    }
    alerts = engine.evaluate_transaction(tx)
    assert len(alerts) >= 1
    rule_names = [a["rule_name"] for a in alerts]
    assert "HIGH_VALUE_TRANSACTION" in rule_names

    # Assert non-trigger for normal amount
    tx_normal = dict(tx, transaction_id="tx-norm", amount=120.0)
    alerts_normal = engine.evaluate_transaction(tx_normal)
    assert "HIGH_VALUE_TRANSACTION" not in [a["rule_name"] for a in alerts_normal]


def test_rapid_repeated_transfers_trigger(engine):
    account = "ACC_BURST_01"
    now = datetime.datetime.now(datetime.timezone.utc)

    # First transfer - normal
    tx1 = {
        "transaction_id": "tx-burst-1",
        "account_id": account,
        "amount": 400.0,
        "currency": "USD",
        "merchant_id": "MERCH_TRANSFER",
        "transaction_type": "TRANSFER",
        "status": "COMPLETED",
        "event_timestamp": now.isoformat(),
    }
    alerts1 = engine.evaluate_transaction(tx1)
    assert "RAPID_REPEATED_TRANSFERS" not in [a["rule_name"] for a in alerts1]

    # Second transfer 5s later
    tx2 = dict(tx1, transaction_id="tx-burst-2", event_timestamp=(now + datetime.timedelta(seconds=5)).isoformat())
    alerts2 = engine.evaluate_transaction(tx2)
    assert "RAPID_REPEATED_TRANSFERS" not in [a["rule_name"] for a in alerts2]

    # Third transfer 10s later -> should trigger rapid transfers
    tx3 = dict(tx1, transaction_id="tx-burst-3", event_timestamp=(now + datetime.timedelta(seconds=10)).isoformat())
    alerts3 = engine.evaluate_transaction(tx3)
    assert "RAPID_REPEATED_TRANSFERS" in [a["rule_name"] for a in alerts3]


def test_unusual_spending_spike(engine):
    account = "ACC_SPIKE_01"
    now = datetime.datetime.now(datetime.timezone.utc)

    # Establish baseline of ~$50 across multiple purchases
    for i in range(3):
        t = {
            "transaction_id": f"tx-base-{i}",
            "account_id": account,
            "amount": 50.0,
            "currency": "USD",
            "merchant_id": "MERCH_GROCERY",
            "transaction_type": "PURCHASE",
            "status": "COMPLETED",
            "event_timestamp": (now + datetime.timedelta(minutes=i)).isoformat(),
        }
        engine.evaluate_transaction(t)

    # Sudden high-multiplier spike ($2,000 is 40x baseline of $50, well above 3.5x and $1500 threshold)
    spike_tx = {
        "transaction_id": "tx-spike-99",
        "account_id": account,
        "amount": 2000.0,
        "currency": "USD",
        "merchant_id": "MERCH_JEWELRY",
        "transaction_type": "PURCHASE",
        "status": "COMPLETED",
        "event_timestamp": (now + datetime.timedelta(minutes=5)).isoformat(),
    }
    alerts = engine.evaluate_transaction(spike_tx)
    assert "UNUSUAL_SPENDING_SPIKE" in [a["rule_name"] for a in alerts]


def test_repeated_failed_transactions(engine):
    account = "ACC_FAIL_01"
    now = datetime.datetime.now(datetime.timezone.utc)

    # 1st fail
    tx1 = {
        "transaction_id": "tx-f-1",
        "account_id": account,
        "amount": 100.0,
        "currency": "USD",
        "merchant_id": "MERCH_ATM",
        "transaction_type": "WITHDRAWAL",
        "status": "FAILED",
        "event_timestamp": now.isoformat(),
    }
    engine.evaluate_transaction(tx1)

    # 2nd fail
    tx2 = dict(tx1, transaction_id="tx-f-2", event_timestamp=(now + datetime.timedelta(seconds=20)).isoformat())
    engine.evaluate_transaction(tx2)

    # 3rd fail within 5 minutes -> triggers REPEATED_FAILED_TRANSACTIONS
    tx3 = dict(tx1, transaction_id="tx-f-3", event_timestamp=(now + datetime.timedelta(seconds=40)).isoformat())
    alerts3 = engine.evaluate_transaction(tx3)
    assert "REPEATED_FAILED_TRANSACTIONS" in [a["rule_name"] for a in alerts3]
    alert = [a for a in alerts3 if a["rule_name"] == "REPEATED_FAILED_TRANSACTIONS"][0]
    assert alert["severity"] == "CRITICAL"


def test_multiple_alerts_for_one_transaction(engine):
    """A transaction can trigger multiple rules simultaneously (e.g. HIGH_VALUE and RAPID_TRANSFERS)."""
    account = "ACC_MULTI_01"
    now = datetime.datetime.now(datetime.timezone.utc)

    # Emit 2 prior transfers
    for i in range(2):
        engine.evaluate_transaction({
            "transaction_id": f"tx-m-{i}",
            "account_id": account,
            "amount": 500.0,
            "currency": "USD",
            "merchant_id": "MERCH_WIRE",
            "transaction_type": "TRANSFER",
            "status": "COMPLETED",
            "event_timestamp": (now + datetime.timedelta(seconds=i * 5)).isoformat(),
        })

    # 3rd transfer is $8,000 (qualifies for both HIGH_VALUE and RAPID_REPEATED_TRANSFERS)
    multi_tx = {
        "transaction_id": "tx-m-multi",
        "account_id": account,
        "amount": 8000.0,
        "currency": "USD",
        "merchant_id": "MERCH_WIRE",
        "transaction_type": "TRANSFER",
        "status": "COMPLETED",
        "event_timestamp": (now + datetime.timedelta(seconds=15)).isoformat(),
    }
    alerts = engine.evaluate_transaction(multi_tx)
    rules_triggered = {a["rule_name"] for a in alerts}
    assert "HIGH_VALUE_TRANSACTION" in rules_triggered
    assert "RAPID_REPEATED_TRANSFERS" in rules_triggered
    assert len(rules_triggered) >= 2
