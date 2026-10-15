import datetime
import hashlib
import logging
import os
from typing import Any, Dict, List, Optional
import yaml

logger = logging.getLogger("FraudRulesEngine")


class FraudRulesEngine:
    """Configurable rule engine for real-time fraud detection.
    
    Operates on parsed transaction dictionaries or micro-batch DataFrames.
    """

    def __init__(self, config_path: Optional[str] = None):
        self.config = self._load_config(config_path)
        self.rules = self.config.get("rules", {})
        # Account spending baseline cache: account_id -> {sum: float, count: int, avg: float}
        self.account_history: Dict[str, Dict[str, float]] = {}
        # Recent transactions buffer for sliding window checks: account_id -> list of tx dicts
        self.recent_tx_buffer: Dict[str, List[Dict[str, Any]]] = {}

    def _load_config(self, config_path: Optional[str]) -> dict:
        """Load YAML configuration or use production defaults."""
        default_path = os.getenv("FRAUD_RULES_CONFIG_PATH", "config/fraud_rules.yaml")
        path = config_path or default_path
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                return yaml.safe_load(f)
        logger.warning("Rules configuration file not found at %s. Using default rules.", path)
        return {
            "rules": {
                "high_value": {"enabled": True, "threshold_amount": 5000.0, "severity": "HIGH"},
                "rapid_transfers": {"enabled": True, "window_seconds": 60, "min_count": 3, "severity": "HIGH"},
                "unusual_spending": {"enabled": True, "spike_multiplier": 3.5, "min_trigger_amount": 1500.0, "severity": "MEDIUM"},
                "repeated_failed": {"enabled": True, "window_seconds": 300, "failure_threshold": 3, "severity": "CRITICAL"},
                "duplicate_transactions": {"enabled": True, "window_seconds": 30, "severity": "MEDIUM"},
            }
        }

    @staticmethod
    def generate_alert_id(transaction_id: str, rule_name: str) -> str:
        """Generate a deterministic deduplication key for an alert."""
        key = f"{transaction_id}::{rule_name}"
        return hashlib.sha256(key.encode("utf-8")).hexdigest()

    def evaluate_transaction(
        self,
        tx: Dict[str, Any],
        seen_tx_ids_in_batch: Optional[set] = None,
        db_existing_tx_ids: Optional[set] = None,
    ) -> List[Dict[str, Any]]:
        """Evaluate a single transaction against all active fraud rules.
        
        Returns a list of alert dictionaries if any rules trigger.
        """
        alerts: List[Dict[str, Any]] = []
        tx_id = str(tx.get("transaction_id", ""))
        acc_id = str(tx.get("account_id", ""))
        amount = float(tx.get("amount", 0.0))
        status = str(tx.get("status", "COMPLETED"))
        tx_type = str(tx.get("transaction_type", "PURCHASE"))
        event_ts_str = str(tx.get("event_timestamp", ""))
        processing_ts = datetime.datetime.now(datetime.timezone.utc).isoformat()

        # Parse event timestamp safely
        try:
            event_dt = datetime.datetime.fromisoformat(event_ts_str.replace("Z", "+00:00"))
        except Exception:
            event_dt = datetime.datetime.now(datetime.timezone.utc)

        # Rule 1: High-Value Transaction
        r1_cfg = self.rules.get("high_value", {})
        if r1_cfg.get("enabled", True):
            threshold = float(os.getenv("RULE_HIGH_VALUE_THRESHOLD", r1_cfg.get("threshold_amount", 5000.0)))
            if amount >= threshold:
                alerts.append({
                    "alert_id": self.generate_alert_id(tx_id, "HIGH_VALUE_TRANSACTION"),
                    "transaction_id": tx_id,
                    "account_id": acc_id,
                    "rule_name": "HIGH_VALUE_TRANSACTION",
                    "severity": r1_cfg.get("severity", "HIGH"),
                    "explanation": f"Transaction amount of ${amount:,.2f} exceeds high-value threshold of ${threshold:,.2f}",
                    "event_timestamp": event_ts_str,
                    "processing_timestamp": processing_ts,
                })

        # Rule 5: Duplicate Transaction Check
        r5_cfg = self.rules.get("duplicate_transactions", {})
        if r5_cfg.get("enabled", True):
            is_dup = False
            if seen_tx_ids_in_batch is not None and tx_id in seen_tx_ids_in_batch:
                is_dup = True
            elif db_existing_tx_ids is not None and tx_id in db_existing_tx_ids:
                is_dup = True

            if is_dup:
                alerts.append({
                    "alert_id": self.generate_alert_id(tx_id, "DUPLICATE_TRANSACTION_EVENT"),
                    "transaction_id": tx_id,
                    "account_id": acc_id,
                    "rule_name": "DUPLICATE_TRANSACTION_EVENT",
                    "severity": r5_cfg.get("severity", "MEDIUM"),
                    "explanation": f"Duplicate transaction event observed for ID {tx_id}",
                    "event_timestamp": event_ts_str,
                    "processing_timestamp": processing_ts,
                })

        # Maintain recent transaction buffer for account
        if acc_id not in self.recent_tx_buffer:
            self.recent_tx_buffer[acc_id] = []
        buf = self.recent_tx_buffer[acc_id]

        # Prune transactions older than 10 minutes to bound memory
        cutoff_dt = event_dt - datetime.timedelta(minutes=10)
        buf = [
            t for t in buf
            if t.get("parsed_dt") and t["parsed_dt"] >= cutoff_dt
        ]
        self.recent_tx_buffer[acc_id] = buf

        # Rule 2: Rapid Repeated Transfers (Velocity check)
        r2_cfg = self.rules.get("rapid_transfers", {})
        if r2_cfg.get("enabled", True):
            window_sec = int(r2_cfg.get("window_seconds", 60))
            min_count = int(r2_cfg.get("min_count", 3))
            allowed_types = set(r2_cfg.get("transaction_types", ["TRANSFER", "PAYMENT", "WITHDRAWAL"]))

            if tx_type in allowed_types:
                w_start = event_dt - datetime.timedelta(seconds=window_sec)
                recent_burst = [
                    t for t in buf
                    if t.get("transaction_type") in allowed_types and t.get("parsed_dt") and t["parsed_dt"] >= w_start
                ]
                # Include current transaction
                burst_count = len(recent_burst) + 1
                if burst_count >= min_count:
                    alerts.append({
                        "alert_id": self.generate_alert_id(tx_id, "RAPID_REPEATED_TRANSFERS"),
                        "transaction_id": tx_id,
                        "account_id": acc_id,
                        "rule_name": "RAPID_REPEATED_TRANSFERS",
                        "severity": r2_cfg.get("severity", "HIGH"),
                        "explanation": f"{burst_count} rapid transfers/payments detected within {window_sec}s for account {acc_id}",
                        "event_timestamp": event_ts_str,
                        "processing_timestamp": processing_ts,
                    })

        # Rule 3: Unusual Spending Relative to Baseline
        r3_cfg = self.rules.get("unusual_spending", {})
        if r3_cfg.get("enabled", True):
            multiplier = float(r3_cfg.get("spike_multiplier", 3.5))
            min_trigger = float(r3_cfg.get("min_trigger_amount", 1500.0))
            history = self.account_history.get(acc_id)
            if history and history.get("count", 0) >= 2:
                baseline_avg = history["sum"] / history["count"]
                if amount >= min_trigger and amount >= (baseline_avg * multiplier):
                    alerts.append({
                        "alert_id": self.generate_alert_id(tx_id, "UNUSUAL_SPENDING_SPIKE"),
                        "transaction_id": tx_id,
                        "account_id": acc_id,
                        "rule_name": "UNUSUAL_SPENDING_SPIKE",
                        "severity": r3_cfg.get("severity", "MEDIUM"),
                        "explanation": f"Transaction amount ${amount:,.2f} is {amount/baseline_avg:.1f}x higher than recent account baseline average (${baseline_avg:,.2f})",
                        "event_timestamp": event_ts_str,
                        "processing_timestamp": processing_ts,
                    })

        # Rule 4: Repeated Failed Transactions
        r4_cfg = self.rules.get("repeated_failed", {})
        if r4_cfg.get("enabled", True):
            window_sec = int(r4_cfg.get("window_seconds", 300))
            fail_threshold = int(r4_cfg.get("failure_threshold", 3))
            fail_statuses = set(r4_cfg.get("statuses", ["FAILED", "DECLINED"]))

            if status in fail_statuses:
                w_start = event_dt - datetime.timedelta(seconds=window_sec)
                recent_fails = [
                    t for t in buf
                    if t.get("status") in fail_statuses and t.get("parsed_dt") and t["parsed_dt"] >= w_start
                ]
                total_fails = len(recent_fails) + 1
                if total_fails >= fail_threshold:
                    alerts.append({
                        "alert_id": self.generate_alert_id(tx_id, "REPEATED_FAILED_TRANSACTIONS"),
                        "transaction_id": tx_id,
                        "account_id": acc_id,
                        "rule_name": "REPEATED_FAILED_TRANSACTIONS",
                        "severity": r4_cfg.get("severity", "CRITICAL"),
                        "explanation": f"{total_fails} failed or declined transactions detected for account {acc_id} within {window_sec}s",
                        "event_timestamp": event_ts_str,
                        "processing_timestamp": processing_ts,
                    })

        # Update account baseline & buffer for future evaluations (only for non-fraudulent normal flow)
        if status == "COMPLETED":
            if acc_id not in self.account_history:
                self.account_history[acc_id] = {"sum": amount, "count": 1}
            else:
                self.account_history[acc_id]["sum"] += amount
                self.account_history[acc_id]["count"] += 1

        # Add current transaction to buffer
        tx_buffered = dict(tx)
        tx_buffered["parsed_dt"] = event_dt
        self.recent_tx_buffer[acc_id].append(tx_buffered)

        return alerts
