import streamlit as st
from common import apply_custom_css, render_sidebar_header, render_screenshots_gallery

st.set_page_config(
    page_title="Runbook & Technical Specs | SentinelFraud",
    page_icon="📖",
    layout="wide",
)

apply_custom_css()
render_sidebar_header()

st.title("📖 System Runbook, Architecture & Technical Specs")
st.caption("Comprehensive data engineering documentation, mathematical rule definitions, and CLI operational guide")

tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "🏛️ Architecture & Guarantees",
    "📐 Mathematical Formulations",
    "💻 CLI Runbook",
    "🗄️ Relational Schema",
    "📸 Screenshots",
])

with tab1:
    st.markdown("""
    ### End-to-End Pipeline Architecture
    The **SentinelFraud** pipeline executes real-time stream ingestion, validation, stateful aggregation, and multi-tier persistence entirely within local Docker Compose containers.
    
    #### Ingestion Layer: Apache Kafka 3.9.1 (KRaft Mode)
    - Runs in ZooKeeper-less KRaft mode, drastically reducing memory overhead and JVM metadata synchronization latency.
    - Configured with dual listeners:
      - Internal container network: `PLAINTEXT://kafka:9092`
      - Host accessibility: `PLAINTEXT_HOST://localhost:29092`
    
    #### Processing Layer: Apache Spark 3.5.8 (Structured Streaming)
    - Native Structured Streaming engine with explicit typed schema parsing.
    - Event-time processing with watermark support for velocity burst detection.
    - Micro-batch execution interval defaults to `5 seconds`, maintaining a healthy balance between latency and driver throughput.
    
    #### Storage & Lakehouse Layers:
    1. **PostgreSQL 16**: Relational operational store with idempotent batch upsert (`ON CONFLICT (transaction_id) DO NOTHING` and `ON CONFLICT (transaction_id, rule_name) DO UPDATE`).
    2. **Bronze Parquet Lake (`lake/bronze/`)**: Append-only raw ingested messages partitioned by `ingestion_date=YYYY-MM-DD`.
    3. **Silver Parquet Lake (`lake/silver/`)**: Validated, schema-normalized transactions partitioned by `event_date=YYYY-MM-DD`.
    """)

with tab2:
    st.markdown("""
    ### Mathematical Formulations of Configured Fraud Rules
    
    #### 1. High-Value Transaction Rule
    Triggers when transaction amount $A_i$ exceeds configured constant threshold $\\theta_{\\text{high}}$:
    $$\\text{Alert}_{\\text{high}} = \\begin{cases} 1 & \\text{if } A_i \\ge \\theta_{\\text{high}} \\\\ 0 & \\text{otherwise} \\end{cases}$$
    *(Default: $\\theta_{\\text{high}} = \\$5,000.00$, Severity: HIGH)*
    
    #### 2. Rapid Repeated Transfers (Velocity Burst)
    For account $k$, count transfers $N(t)$ within sliding event-time window $W$:
    $$N_k(t) = \\sum_{j \\in \\text{Transfers}_k} \\mathbb{I}(t_j \\in [t - W, t])$$
    $$\\text{Alert}_{\\text{rapid}} = \\begin{cases} 1 & \\text{if } N_k(t) \\ge C_{\\min} \\\\ 0 & \\text{otherwise} \\end{cases}$$
    *(Default: $W = 60\\text{s}, C_{\\min} = 3$, Severity: HIGH)*
    
    #### 3. Unusual Spending Spike Relative to Baseline
    Given account historical mean spend $\\bar{X}_k = \\frac{1}{M}\\sum_{m=1}^{M} A_{k,m}$:
    $$\\text{Alert}_{\\text{spike}} = \\begin{cases} 1 & \\text{if } A_i \\ge \\alpha \\cdot \\bar{X}_k \\quad \\land \\quad A_i \\ge \\theta_{\\min} \\\\ 0 & \\text{otherwise} \\end{cases}$$
    *(Default: $\\alpha = 3.5\\times, \\theta_{\\min} = \\$1,500.00$, Severity: MEDIUM)*
    
    #### 4. Cryptographic Alert Deduplication
    Deterministic alert identifier ensures strict database idempotency across micro-batch replays:
    $$\\text{Alert ID} = \\text{SHA-256}(T_{\\text{id}} \\parallel \\text{Rule Name})$$
    """)

with tab3:
    st.markdown("### Operational CLI Cheat Sheet")
    st.markdown("**Start all services:**")
    st.code("docker compose up -d kafka postgres spark-stream dashboard", language="bash")

    st.markdown("**Start the continuous transaction producer (10 tx/s):**")
    st.code("docker compose --profile producer up -d producer", language="bash")

    st.markdown("**Run the automated unit test suite:**")
    st.code("docker compose run --rm spark-stream pytest -v tests/", language="bash")

    st.markdown("**Run end-to-end multi-rate benchmark:**")
    st.code("docker compose run --rm -e POSTGRES_HOST=postgres producer python benchmark.py --rates 10,25,50 --duration 8 --bootstrap-servers kafka:9092", language="bash")

    st.markdown("**Query PostgreSQL directly from host CLI:**")
    st.code('docker exec fraud_postgres psql -U fraud_admin -d fraud_detection -c "SELECT rule_name, severity, count(*) FROM fraud_alerts GROUP BY rule_name, severity;"', language="bash")

with tab4:
    st.markdown("""
    ### Relational Database Schema Specification
    
    ```sql
    -- Transactions Master Ledger
    CREATE TABLE transactions (
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

    -- Detected Fraud Alerts (Idempotent Composite Key)
    CREATE TABLE fraud_alerts (
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

    -- Telemetry & Batch Health Metrics
    CREATE TABLE processing_metrics (
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

    -- Dead-Letter Quarantine Sink
    CREATE TABLE quarantine_events (
        id SERIAL PRIMARY KEY,
        raw_payload TEXT NOT NULL,
        error_reason TEXT NOT NULL,
        received_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
    );
    ```
    """)

with tab5:
    render_screenshots_gallery()
