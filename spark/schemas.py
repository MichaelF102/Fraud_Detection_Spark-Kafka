import datetime
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Explicit PySpark schema for incoming transaction JSON messages
TRANSACTION_RAW_SCHEMA = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("account_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("merchant_id", StringType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("status", StringType(), True),
    StructField("event_timestamp", StringType(), True),
    StructField("is_fraud_ground_truth", BooleanType(), True),
])

ALLOWED_TRANSACTION_TYPES = {"PURCHASE", "PAYMENT", "TRANSFER", "WITHDRAWAL"}
ALLOWED_STATUSES = {"COMPLETED", "FAILED", "DECLINED", "PENDING"}


def validate_record_dict(rec: dict) -> tuple[bool, str]:
    """Pure-Python validation for a transaction dictionary.
    
    Returns:
        (is_valid, error_reason)
    """
    if not rec.get("transaction_id") or not str(rec.get("transaction_id")).strip():
        return False, "Missing or empty transaction_id"

    if not rec.get("account_id") or not str(rec.get("account_id")).strip():
        return False, "Missing or empty account_id"

    amount = rec.get("amount")
    if amount is None:
        return False, "Missing amount"
    try:
        amt_val = float(amount)
        if amt_val <= 0:
            return False, f"Non-positive amount: {amt_val}"
    except (ValueError, TypeError):
        return False, f"Invalid numeric amount: {amount}"

    if not rec.get("currency"):
        return False, "Missing currency"

    tx_type = rec.get("transaction_type")
    if not tx_type or tx_type not in ALLOWED_TRANSACTION_TYPES:
        return False, f"Invalid or unknown transaction_type: {tx_type}"

    status = rec.get("status")
    if not status or status not in ALLOWED_STATUSES:
        return False, f"Invalid or unknown status: {status}"

    ts = rec.get("event_timestamp")
    if not ts:
        return False, "Missing event_timestamp"
    try:
        datetime.datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
    except Exception:
        return False, f"Invalid ISO 8601 event_timestamp: {ts}"

    return True, ""
