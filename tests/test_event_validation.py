import pytest
from spark.schemas import (
    ALLOWED_STATUSES,
    ALLOWED_TRANSACTION_TYPES,
    validate_record_dict,
)


def test_valid_transaction():
    valid_tx = {
        "transaction_id": "tx-12345",
        "account_id": "ACC_1001",
        "amount": 250.50,
        "currency": "USD",
        "merchant_id": "MERCH_AMAZON",
        "transaction_type": "PURCHASE",
        "status": "COMPLETED",
        "event_timestamp": "2026-10-15T10:30:00Z",
    }
    is_valid, reason = validate_record_dict(valid_tx)
    assert is_valid is True
    assert reason == ""


def test_missing_transaction_id():
    tx = {
        "account_id": "ACC_1001",
        "amount": 100.0,
        "currency": "USD",
        "merchant_id": "MERCH_AMAZON",
        "transaction_type": "PURCHASE",
        "status": "COMPLETED",
        "event_timestamp": "2026-10-15T10:30:00Z",
    }
    is_valid, reason = validate_record_dict(tx)
    assert is_valid is False
    assert "transaction_id" in reason


def test_missing_account_id():
    tx = {
        "transaction_id": "tx-100",
        "account_id": "",
        "amount": 100.0,
        "currency": "USD",
        "merchant_id": "MERCH_AMAZON",
        "transaction_type": "PURCHASE",
        "status": "COMPLETED",
        "event_timestamp": "2026-10-15T10:30:00Z",
    }
    is_valid, reason = validate_record_dict(tx)
    assert is_valid is False
    assert "account_id" in reason


def test_negative_or_zero_amount():
    for invalid_amt in [-50.0, 0.0, "abc"]:
        tx = {
            "transaction_id": "tx-100",
            "account_id": "ACC_1001",
            "amount": invalid_amt,
            "currency": "USD",
            "merchant_id": "MERCH_AMAZON",
            "transaction_type": "PURCHASE",
            "status": "COMPLETED",
            "event_timestamp": "2026-10-15T10:30:00Z",
        }
        is_valid, reason = validate_record_dict(tx)
        assert is_valid is False
        assert "amount" in reason.lower()


def test_unknown_transaction_type():
    tx = {
        "transaction_id": "tx-100",
        "account_id": "ACC_1001",
        "amount": 50.0,
        "currency": "USD",
        "merchant_id": "MERCH_AMAZON",
        "transaction_type": "CRYPTOCURRENCY_STAKE",
        "status": "COMPLETED",
        "event_timestamp": "2026-10-15T10:30:00Z",
    }
    is_valid, reason = validate_record_dict(tx)
    assert is_valid is False
    assert "transaction_type" in reason


def test_unknown_status():
    tx = {
        "transaction_id": "tx-100",
        "account_id": "ACC_1001",
        "amount": 50.0,
        "currency": "USD",
        "merchant_id": "MERCH_AMAZON",
        "transaction_type": "PURCHASE",
        "status": "CHARGEBACK_CONFIRMED",
        "event_timestamp": "2026-10-15T10:30:00Z",
    }
    is_valid, reason = validate_record_dict(tx)
    assert is_valid is False
    assert "status" in reason
