-- =============================================================================
-- Real-Time Fraud Detection Pipeline - PostgreSQL Schema
-- =============================================================================

CREATE TABLE IF NOT EXISTS transactions (
    transaction_id VARCHAR(64) PRIMARY KEY,
    account_id VARCHAR(32) NOT NULL,
    amount NUMERIC(12, 2) NOT NULL,
    currency VARCHAR(8) NOT NULL,
    merchant_id VARCHAR(32) NOT NULL,
    transaction_type VARCHAR(32) NOT NULL,
    status VARCHAR(32) NOT NULL,
    event_timestamp TIMESTAMPTZ NOT NULL,
    is_fraud_ground_truth BOOLEAN DEFAULT FALSE,
    ingestion_timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_transactions_account_timestamp 
    ON transactions(account_id, event_timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_transactions_timestamp 
    ON transactions(event_timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_transactions_status 
    ON transactions(status);

CREATE INDEX IF NOT EXISTS idx_transactions_ground_truth 
    ON transactions(is_fraud_ground_truth);

-- Fraud alerts detected by the streaming engine
CREATE TABLE IF NOT EXISTS fraud_alerts (
    alert_id VARCHAR(128) PRIMARY KEY,
    transaction_id VARCHAR(64) NOT NULL REFERENCES transactions(transaction_id) ON DELETE CASCADE,
    account_id VARCHAR(32) NOT NULL,
    rule_name VARCHAR(64) NOT NULL,
    severity VARCHAR(16) NOT NULL,
    explanation TEXT NOT NULL,
    event_timestamp TIMESTAMPTZ NOT NULL,
    processing_timestamp TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_transaction_rule UNIQUE (transaction_id, rule_name)
);

CREATE INDEX IF NOT EXISTS idx_alerts_account 
    ON fraud_alerts(account_id);

CREATE INDEX IF NOT EXISTS idx_alerts_rule 
    ON fraud_alerts(rule_name);

CREATE INDEX IF NOT EXISTS idx_alerts_severity 
    ON fraud_alerts(severity);

CREATE INDEX IF NOT EXISTS idx_alerts_timestamp 
    ON fraud_alerts(event_timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_alerts_processing 
    ON fraud_alerts(processing_timestamp DESC);

-- Micro-batch and stream processing health metrics
CREATE TABLE IF NOT EXISTS processing_metrics (
    id SERIAL PRIMARY KEY,
    batch_id BIGINT NOT NULL,
    records_processed INTEGER NOT NULL DEFAULT 0,
    alerts_generated INTEGER NOT NULL DEFAULT 0,
    batch_duration_ms DOUBLE PRECISION NOT NULL DEFAULT 0,
    input_rate DOUBLE PRECISION DEFAULT 0,
    processing_rate DOUBLE PRECISION DEFAULT 0,
    quarantined_records INTEGER NOT NULL DEFAULT 0,
    recorded_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_metrics_recorded_at 
    ON processing_metrics(recorded_at DESC);

-- Quarantined / Malformed events storage (dead-letter sink)
CREATE TABLE IF NOT EXISTS quarantine_events (
    id SERIAL PRIMARY KEY,
    raw_payload TEXT NOT NULL,
    error_reason TEXT NOT NULL,
    received_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_quarantine_received 
    ON quarantine_events(received_at DESC);
