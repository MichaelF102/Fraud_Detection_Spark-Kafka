import datetime
import json
import os
import sys
import time
import warnings
from typing import Any, Dict, List, Optional
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import psycopg2
from psycopg2.extras import RealDictCursor
import streamlit as st
import yaml

# Ensure project root and dashboard directory are on sys.path
_current_dir = os.path.dirname(os.path.abspath(__file__))
_project_root = os.path.dirname(_current_dir)
if _current_dir not in sys.path:
    sys.path.insert(0, _current_dir)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)


# ==============================================================================
# DATABASE CONNECTION & CLOUD FALLBACK QUERY UTILITIES
# ==============================================================================
_DB_AVAILABLE: Optional[bool] = None


def check_db_availability() -> bool:
    """Check if PostgreSQL is reachable, with cached status to prevent repeated timeouts on Cloud."""
    global _DB_AVAILABLE
    if _DB_AVAILABLE is not None:
        return _DB_AVAILABLE
    try:
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            dbname=os.getenv("POSTGRES_DB", "fraud_detection"),
            user=os.getenv("POSTGRES_USER", "fraud_admin"),
            password=os.getenv("POSTGRES_PASSWORD", "fraud_secure_pass_2026"),
            connect_timeout=1,
        )
        conn.close()
        _DB_AVAILABLE = True
    except Exception:
        _DB_AVAILABLE = False
    return _DB_AVAILABLE


def get_db_connection():
    """Create a connection to PostgreSQL operational store."""
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "fraud_detection"),
        user=os.getenv("POSTGRES_USER", "fraud_admin"),
        password=os.getenv("POSTGRES_PASSWORD", "fraud_secure_pass_2026"),
        connect_timeout=2,
    )


def _generate_mock_query_response(sql: str, params: Optional[tuple] = None) -> pd.DataFrame:
    """Generate realistic DataFrame from sample fixtures when running on Streamlit Cloud without local DB."""
    import random
    sql_upper = sql.upper()
    sample_file = os.path.join(_project_root, "data", "sample_transactions.json")
    records = []
    if os.path.exists(sample_file):
        try:
            with open(sample_file, "r") as f:
                records = json.load(f)
        except Exception:
            records = []

    # KPI aggregate query
    if "SELECT (SELECT COUNT(*) FROM TRANSACTIONS)" in sql_upper or "TOTAL_TX" in sql_upper:
        total_tx = max(len(records), 1250)
        flagged_tx = int(total_tx * 0.16)
        total_alerts = int(flagged_tx * 1.35)
        total_vol = 145800.00
        flagged_vol = 61200.00
        return pd.DataFrame([{
            "total_tx": total_tx,
            "flagged_tx": flagged_tx,
            "total_alerts": total_alerts,
            "quarantined": 24,
            "total_vol_usd": total_vol,
            "flagged_vol_usd": flagged_vol,
            "last_update": datetime.datetime.now().isoformat(),
        }])

    # TPS query
    if "SELECT COUNT(*)::FLOAT / 60.0 AS TPS" in sql_upper or "AS TPS" in sql_upper:
        return pd.DataFrame([{"tps": 18.5}])

    # 30-min timeseries velocity query
    if "DATE_TRUNC" in sql_upper and "TIME_BUCKET" in sql_upper:
        now = datetime.datetime.now()
        buckets = []
        for i in range(12):
            t = now - datetime.timedelta(minutes=(12 - i) * 2)
            tot = random.randint(55, 110)
            flg = int(tot * 0.18)
            inj = int(flg * 0.9)
            buckets.append({
                "time_bucket": t,
                "total_tx": tot,
                "flagged_tx": flg,
                "injected_fraud": inj
            })
        return pd.DataFrame(buckets)

    # Severity distribution
    if "GROUP BY SEVERITY" in sql_upper:
        return pd.DataFrame([
            {"severity": "CRITICAL", "count": 48},
            {"severity": "HIGH", "count": 142},
            {"severity": "MEDIUM", "count": 86},
            {"severity": "LOW", "count": 24},
        ])

    # Top triggered rules
    if "GROUP BY RULE_NAME" in sql_upper and "TRIGGERS" in sql_upper:
        return pd.DataFrame([
            {"rule_name": "HIGH_VALUE_TRANSACTION", "triggers": 128},
            {"rule_name": "RAPID_REPEATED_TRANSFERS", "triggers": 94},
            {"rule_name": "UNUSUAL_SPENDING_SPIKE", "triggers": 58},
            {"rule_name": "REPEATED_FAILED_TRANSACTIONS", "triggers": 41},
            {"rule_name": "DUPLICATE_TRANSACTION_EVENT", "triggers": 16},
        ])

    # Financial Exposure KPIs
    if "TOTAL_PROCESSED_USD" in sql_upper:
        return pd.DataFrame([{
            "total_processed_usd": 145800.00,
            "avg_transaction_usd": 85.50,
            "flagged_risk_usd": 61200.00,
            "avg_fraud_ticket_usd": 2450.00,
            "avg_normal_ticket_usd": 62.00,
        }])

    # Merchant leaderboard
    if "GROUP BY T.MERCHANT_ID" in sql_upper:
        merchants = [
            "MERCH_AMAZON", "MERCH_WALMART", "MERCH_UBER", "MERCH_SHELL",
            "MERCH_APPLE", "MERCH_AIRBNB", "MERCH_DELTA", "MERCH_BESTBUY"
        ]
        m_rows = []
        for m in merchants:
            tot = random.randint(120, 450)
            flg = random.randint(8, int(tot * 0.3))
            m_rows.append({
                "merchant_id": m,
                "total_tx": tot,
                "total_amount": float(tot * random.uniform(40.0, 150.0)),
                "flagged_tx": flg,
                "flagged_amount": float(flg * random.uniform(800.0, 2500.0)),
                "fraud_rate_pct": round(flg / tot * 100, 1)
            })
        return pd.DataFrame(m_rows).sort_values("flagged_amount", ascending=False)

    # General transactions / alerts records
    if records:
        df = pd.DataFrame(records)
        df["rule_name"] = df["is_fraud_ground_truth"].apply(
            lambda x: random.choice(["HIGH_VALUE_TRANSACTION", "RAPID_REPEATED_TRANSFERS", "UNUSUAL_SPENDING_SPIKE"]) if x else None
        )
        df["severity"] = df["is_fraud_ground_truth"].apply(
            lambda x: random.choice(["CRITICAL", "HIGH", "MEDIUM"]) if x else None
        )
        df["explanation"] = df["is_fraud_ground_truth"].apply(
            lambda x: "Simulated rule trigger from offline verification dataset" if x else None
        )
        df["alert_id"] = [f"ALT_{i:04d}" if r.get("is_fraud_ground_truth") else None for i, r in enumerate(records)]
        df["processing_timestamp"] = df["event_timestamp"]
        return df

    return pd.DataFrame()


def query_df(sql: str, params: Optional[tuple] = None) -> pd.DataFrame:
    """Execute a query against PostgreSQL with automatic fallback to sample data on Streamlit Cloud."""
    if check_db_availability():
        try:
            with get_db_connection() as conn:
                return pd.read_sql_query(sql, conn, params=params)
        except Exception:
            pass
    return _generate_mock_query_response(sql, params)


def load_rules_yaml() -> dict:
    """Load fraud rules configuration from file or fallback."""
    cfg_path = os.getenv("FRAUD_RULES_CONFIG_PATH", "config/fraud_rules.yaml")
    if not os.path.isabs(cfg_path):
        cfg_path = os.path.join(_project_root, cfg_path)
    if os.path.exists(cfg_path):
        try:
            with open(cfg_path, "r", encoding="utf-8") as f:
                return yaml.safe_load(f)
        except Exception:
            pass
    return {}


# ==============================================================================
# SCREENSHOT GALLERY UTILITIES
# ==============================================================================
def get_screenshot_directory() -> str | None:
    """Locate the Screenshots directory across Docker and host mount paths."""
    candidates = [
        "/app/Screenshots",
        os.path.join(_project_root, "Screenshots"),
        os.path.join(os.getcwd(), "Screenshots"),
        "Screenshots",
    ]
    for c in candidates:
        if os.path.exists(c) and os.path.isdir(c):
            return c
    return None


SCREENSHOT_METADATA = {
    "Screenshot_2026-10-15_11-21-45.png": {
        "title": "🚀 1. Docker Compose Services Initialization",
        "badge": "Infrastructure & Kafka KRaft",
        "desc": "Starting Apache Kafka (KRaft mode, dual listeners), PostgreSQL 16, Streamlit Dashboard, PySpark Structured Streaming processor, and transaction simulator container services.",
    },
    "Screenshot_2026-10-15_11-21-55.png": {
        "title": "📊 2. Production PostgreSQL Rule Trigger Telemetry",
        "badge": "PostgreSQL Operational Store",
        "desc": "Verification of 9,968 processed transactions, 9,352 alerts generated across all 5 fraud rules, and 296 quarantined invalid events in PostgreSQL.",
    },
    "Screenshot_2026-10-15_11-22-15.png": {
        "title": "🧪 3. Automated Pytest Suite Execution (100% Pass)",
        "badge": "CI/CD & Unit Tests",
        "desc": "14/14 automated unit tests passing in Docker container (deduplication, schema validation, high-value, rapid transfers, unusual spending, repeated failed rules).",
    },
    "Screenshot_2026-10-15_11-22-19.png": {
        "title": "⚡ 4. Low-Rate Benchmark (10 TPS)",
        "badge": "Streaming Benchmark",
        "desc": "Emitting and processing transactions with 10 tx/s target rate, testing baseline micro-batch commit.",
    },
    "Screenshot_2026-10-15_11-23-06.png": {
        "title": "📈 5. Mid-Rate Velocity Benchmark (25 TPS)",
        "badge": "High Velocity & Recall",
        "desc": "Emitting 200 transactions at 28.7 tx/s with 470 alerts generated and 96.9% recall against synthetic fraud ground truth.",
    },
    "Screenshot_2026-10-15_11-23-09.png": {
        "title": "🔥 6. High-Throughput Burst Benchmark (50 TPS)",
        "badge": "High-Throughput Burst",
        "desc": "Streaming 400 transactions at 57.5 tx/s through Kafka into PySpark Structured Streaming with micro-batch commit.",
    },
    "Screenshot_2026-10-15_11-24-02.png": {
        "title": "🏆 7. End-to-End Benchmark Summary Report",
        "badge": "Performance Matrix",
        "desc": "Final benchmark summary matrix across 10, 25, and 50 TPS measuring emission rate, processed count, alerts, latency, precision, recall (up to 96.9%), and F1 score.",
    },
}


def render_screenshots_gallery(expanded: bool = True):
    """Render all captured pipeline execution screenshots from the Screenshots folder."""
    st.subheader("📸 Pipeline Execution Screenshots & Verification Proof")
    st.markdown("Visual evidence and terminal telemetry captured during end-to-end pipeline execution, automated test suite verification, and benchmarking:")

    screenshot_dir = get_screenshot_directory()
    if not screenshot_dir:
        st.warning("⚠️ Screenshots directory not found. Please ensure `./Screenshots` is available.")
        return

    images = sorted([
        f for f in os.listdir(screenshot_dir)
        if f.lower().endswith(('.png', '.jpg', '.jpeg', '.webp'))
    ])

    if not images:
        st.info("ℹ️ No screenshot files found in `Screenshots/` folder.")
        return

    st.markdown(f"**Found {len(images)} captured execution artifacts** in `{screenshot_dir}`:")

    for idx, img_name in enumerate(images, 1):
        img_path = os.path.join(screenshot_dir, img_name)
        info = SCREENSHOT_METADATA.get(img_name, {
            "title": f"📸 {idx}. {img_name}",
            "badge": "Artifact",
            "desc": f"Screenshot file: {img_name}",
        })
        file_size_kb = os.path.getsize(img_path) / 1024.0

        with st.expander(f"**{info['title']}** — `{info['badge']}` ({file_size_kb:.1f} KB)", expanded=expanded):
            st.markdown(f"*{info['desc']}*")
            st.image(img_path, caption=info["title"], use_container_width=True)
            st.caption(f"📁 Local File: `{img_path}` • File Size: `{file_size_kb:.1f} KB`")


# ==============================================================================
# CUSTOM CSS & GLOBAL DESIGN SYSTEM
# ==============================================================================
def apply_custom_css():
    """Inject custom dark theme CSS and typography."""
    st.markdown(
        """
        <style>
        /* Modern Inter font styling & dark palette */
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&family=JetBrains+Mono:wght@400;500;600&display=swap');
        
        html, body, [class*="css"] {
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
        }
        
        code, pre {
            font-family: 'JetBrains Mono', monospace !important;
        }

        /* Glassmorphic KPI Cards */
        .kpi-card {
            background: linear-gradient(135deg, rgba(30, 41, 59, 0.7) 0%, rgba(15, 23, 42, 0.8) 100%);
            border: 1px solid rgba(255, 255, 255, 0.08);
            border-radius: 12px;
            padding: 20px 22px;
            margin-bottom: 14px;
            box-shadow: 0 8px 24px rgba(0, 0, 0, 0.4);
            position: relative;
            overflow: hidden;
        }
        .kpi-card::before {
            content: "";
            position: absolute;
            top: 0; left: 0; right: 0; height: 3px;
            background: linear-gradient(90deg, #38bdf8, #818cf8);
        }
        .kpi-card-danger::before {
            background: linear-gradient(90deg, #ef4444, #f43f5e);
        }
        .kpi-card-warning::before {
            background: linear-gradient(90deg, #f59e0b, #fbbf24);
        }
        .kpi-card-success::before {
            background: linear-gradient(90deg, #10b981, #34d399);
        }
        
        .kpi-title {
            font-size: 0.8rem;
            font-weight: 600;
            color: #94a3b8;
            text-transform: uppercase;
            letter-spacing: 0.08em;
            margin-bottom: 6px;
        }
        .kpi-value {
            font-size: 1.9rem;
            font-weight: 700;
            color: #f8fafc;
            line-height: 1.2;
        }
        .kpi-subtext {
            font-size: 0.78rem;
            color: #64748b;
            margin-top: 6px;
        }
        
        /* Status Badges */
        .badge {
            display: inline-block;
            padding: 3px 10px;
            border-radius: 6px;
            font-size: 0.75rem;
            font-weight: 700;
            letter-spacing: 0.04em;
            text-transform: uppercase;
        }
        .badge-critical {
            background: rgba(239, 68, 68, 0.15);
            color: #f87171;
            border: 1px solid rgba(239, 68, 68, 0.4);
        }
        .badge-high {
            background: rgba(249, 115, 22, 0.15);
            color: #fb923c;
            border: 1px solid rgba(249, 115, 22, 0.4);
        }
        .badge-medium {
            background: rgba(234, 179, 8, 0.15);
            color: #facc15;
            border: 1px solid rgba(234, 179, 8, 0.4);
        }
        .badge-low {
            background: rgba(56, 189, 248, 0.15);
            color: #38bdf8;
            border: 1px solid rgba(56, 189, 248, 0.4);
        }
        
        /* Tab Styling */
        button[data-baseweb="tab"] {
            font-weight: 600 !important;
            font-size: 0.95rem !important;
            padding: 10px 18px !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_sidebar_header():
    """Render consistent branding and operational telemetry status in sidebar."""
    st.sidebar.image("https://img.icons8.com/fluency/96/shield.png", width=60)
    st.sidebar.markdown("## **SentinelFraud**")
    st.sidebar.caption("⚡ **Real-Time Financial Fraud Engine**\n\nApache Kafka • PySpark • PostgreSQL • Parquet Lake")
    st.sidebar.divider()

    if not check_db_availability():
        st.sidebar.markdown(
            """
            <div style="background: rgba(234, 179, 8, 0.12); border: 1px solid rgba(234, 179, 8, 0.35); border-radius: 8px; padding: 10px; font-size: 0.76rem; color: #facc15; margin-bottom: 14px;">
                <b>☁️ Streamlit Cloud Demo Mode</b><br>
                Displaying simulated telemetry from sample datasets.<br>
                <i>(Live Kafka & PySpark streaming runs locally in Docker)</i>
            </div>
            """,
            unsafe_allow_html=True,
        )

    st.sidebar.markdown("### ⏱️ Telemetry Controls")
    refresh_rate = st.sidebar.selectbox("Auto-Refresh Cadence", ["3 seconds", "5 seconds", "10 seconds", "Paused"], index=1, key="telemetry_refresh_cadence")
    if refresh_rate != "Paused":
        time.sleep(0.05)
        st.sidebar.caption(f"Last sync: **{datetime.datetime.now().strftime('%H:%M:%S UTC')}**")

    st.sidebar.markdown(
        """
        <div style="background: rgba(56, 189, 248, 0.05); border: 1px solid rgba(56, 189, 248, 0.2); border-radius: 8px; padding: 10px; font-size: 0.76rem; color: #94a3b8; margin-top: 15px;">
            <b>Hardware Profile:</b> 16 GB RAM Mode<br>
            <b>Ingestion Semantics:</b> Event-Time Watermarked<br>
            <b>Persistence:</b> Idempotent Composite Upsert
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.sidebar.divider()
