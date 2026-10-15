import datetime
import json
import os
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

warnings.filterwarnings("ignore", category=UserWarning)

# ==============================================================================
# STREAMLIT APP CONFIGURATION & THEMING
# ==============================================================================
st.set_page_config(
    page_title="SentinelFraud | Real-Time Financial Intelligence",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom High-End Financial Dashboard CSS (Cyber-Fintech Dark Theme)
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600;700&family=Plus+Jakarta+Sans:wght@400;500;600;700&display=swap');
    
    html, body, [class*="css"] {
        font-family: 'Plus Jakarta Sans', -apple-system, BlinkMacSystemFont, sans-serif;
    }
    
    code, pre {
        font-family: 'JetBrains Mono', monospace !important;
    }

    .main {
        background-color: #0b0f19;
    }
    
    /* Top KPI Metric Cards */
    .kpi-card {
        background: linear-gradient(135deg, rgba(22, 27, 39, 0.95) 0%, rgba(15, 20, 31, 0.95) 100%);
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
    
    /* Code block stylings */
    .stCodeBlock {
        border-radius: 8px !important;
        border: 1px solid #1e293b !important;
    }
</style>
""", unsafe_allow_html=True)


# ==============================================================================
# DATABASE CONNECTION & CACHED QUERY HELPERS
# ==============================================================================
def get_db_connection():
    """Create a PostgreSQL connection with connect timeout."""
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "fraud_detection"),
        user=os.getenv("POSTGRES_USER", "fraud_admin"),
        password=os.getenv("POSTGRES_PASSWORD", "fraud_secure_pass_2026"),
        connect_timeout=6,
    )


@st.cache_data(ttl=2)
def query_df(sql: str, params: Optional[tuple] = None) -> pd.DataFrame:
    """Execute SQL query safely and return pandas DataFrame."""
    try:
        with get_db_connection() as conn:
            return pd.read_sql_query(sql, conn, params=params)
    except Exception as e:
        st.error(f"PostgreSQL Query Error: {e}")
        return pd.DataFrame()


def load_rules_yaml() -> dict:
    """Load fraud rules configuration from file or fallback."""
    cfg_path = os.getenv("FRAUD_RULES_CONFIG_PATH", "config/fraud_rules.yaml")
    if not os.path.isabs(cfg_path):
        cfg_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), cfg_path)
    if os.path.exists(cfg_path):
        try:
            with open(cfg_path, "r", encoding="utf-8") as f:
                return yaml.safe_load(f)
        except Exception:
            pass
    return {}


# ==============================================================================
# SIDEBAR NAVIGATION & TELEMETRY CONTROLS
# ==============================================================================
st.sidebar.image("https://img.icons8.com/fluency/96/shield.png", width=60)
st.sidebar.markdown("## **SentinelFraud**")
st.sidebar.caption("⚡ **Real-Time Financial Fraud Engine**\n\nApache Kafka • PySpark • PostgreSQL • Parquet Lake")

st.sidebar.divider()

page = st.sidebar.radio(
    "Navigation Console",
    [
        "🔴 1. Live Telemetry & Monitoring",
        "🔍 2. Fraud Forensic Investigation",
        "💰 3. Financial Risk & Merchant Analytics",
        "⚙️ 4. Detection Rules & Interactive Simulator",
        "⚡ 5. Pipeline Architecture & Cluster Health",
        "📖 6. System Runbook & Technical Specs",
    ],
    index=0,
)

st.sidebar.divider()
st.sidebar.markdown("### ⏱️ Telemetry Controls")
refresh_rate = st.sidebar.selectbox("Auto-Refresh Cadence", ["3 seconds", "5 seconds", "10 seconds", "Paused"], index=1)
if refresh_rate != "Paused":
    secs = int(refresh_rate.split()[0])
    time.sleep(0.05)
    st.sidebar.caption(f"Last updated: **{datetime.datetime.now().strftime('%H:%M:%S UTC')}**")

st.sidebar.markdown(
    """
    <div style="background: rgba(56, 189, 248, 0.05); border: 1px solid rgba(56, 189, 248, 0.2); border-radius: 8px; padding: 10px; font-size: 0.76rem; color: #94a3b8;">
        <b>Local Hardware Mode:</b> 16 GB RAM Profile<br>
        <b>Event Time Semantics:</b> Watermarked<br>
        <b>Storage Guarantees:</b> Idempotent Upsert
    </div>
    """,
    unsafe_allow_html=True,
)


# ==============================================================================
# PAGE 1: LIVE TELEMETRY & MONITORING
# ==============================================================================
if page == "🔴 1. Live Telemetry & Monitoring":
    st.title("🛡️ Real-Time Fraud Operations Center")
    st.caption("Live streaming event telemetry processed via Apache Spark Structured Streaming and PostgreSQL")

    # Fetch aggregate pipeline metrics
    kpi_df = query_df("""
        SELECT 
            (SELECT COUNT(*) FROM transactions) AS total_tx,
            (SELECT COUNT(DISTINCT transaction_id) FROM fraud_alerts) AS flagged_tx,
            (SELECT COUNT(*) FROM fraud_alerts) AS total_alerts,
            (SELECT COUNT(*) FROM quarantine_events) AS quarantined,
            (SELECT COALESCE(SUM(amount), 0) FROM transactions) AS total_vol_usd,
            (SELECT COALESCE(SUM(t.amount), 0) FROM transactions t JOIN fraud_alerts fa ON t.transaction_id = fa.transaction_id) AS flagged_vol_usd,
            (SELECT MAX(ingestion_timestamp) FROM transactions) AS last_update
    """)

    if kpi_df.empty or kpi_df["total_tx"].iloc[0] == 0:
        st.info("ℹ️ Pipeline is listening on Kafka topic `financial_transactions`. Waiting for incoming transaction events...")
        st.stop()

    kpi = kpi_df.iloc[0]
    total_tx = int(kpi["total_tx"] or 0)
    flagged_tx = int(kpi["flagged_tx"] or 0)
    total_alerts = int(kpi["total_alerts"] or 0)
    quarantined = int(kpi["quarantined"] or 0)
    total_vol = float(kpi["total_vol_usd"] or 0.0)
    flagged_vol = float(kpi["flagged_vol_usd"] or 0.0)
    alert_rate = (flagged_tx / total_tx * 100.0) if total_tx > 0 else 0.0

    # Calculate real-time TPS in last 60 seconds
    tps_df = query_df("""
        SELECT COUNT(*)::float / 60.0 AS tps
        FROM transactions
        WHERE ingestion_timestamp >= NOW() - INTERVAL '60 seconds'
    """)
    current_tps = float(tps_df["tps"].iloc[0]) if not tps_df.empty and tps_df["tps"].iloc[0] else 0.0

    # 6 Top Metric Cards
    m1, m2, m3, m4, m5, m6 = st.columns(6)
    with m1:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-title">Ingested Tx</div>
            <div class="kpi-value">{total_tx:,}</div>
            <div class="kpi-subtext">Kafka stream count</div>
        </div>""", unsafe_allow_html=True)
    with m2:
        st.markdown(f"""
        <div class="kpi-card kpi-card-danger">
            <div class="kpi-title">Flagged Fraud</div>
            <div class="kpi-value" style="color: #f87171;">{flagged_tx:,}</div>
            <div class="kpi-subtext">{alert_rate:.1f}% alert rate</div>
        </div>""", unsafe_allow_html=True)
    with m3:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-title">Alert Triggers</div>
            <div class="kpi-value" style="color: #fb923c;">{total_alerts:,}</div>
            <div class="kpi-subtext">Multi-rule triggers</div>
        </div>""", unsafe_allow_html=True)
    with m4:
        st.markdown(f"""
        <div class="kpi-card kpi-card-success">
            <div class="kpi-title">Throughput</div>
            <div class="kpi-value" style="color: #38bdf8;">{current_tps:.1f} <span style="font-size: 1rem;">tx/s</span></div>
            <div class="kpi-subtext">Live 60s ingestion</div>
        </div>""", unsafe_allow_html=True)
    with m5:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-title">Total Volume</div>
            <div class="kpi-value">${total_vol:,.0f}</div>
            <div class="kpi-subtext">Gross processed</div>
        </div>""", unsafe_allow_html=True)
    with m6:
        st.markdown(f"""
        <div class="kpi-card kpi-card-warning">
            <div class="kpi-title">Quarantined</div>
            <div class="kpi-value" style="color: #fbbf24;">{quarantined:,}</div>
            <div class="kpi-subtext">Malformed/Dead-letter</div>
        </div>""", unsafe_allow_html=True)

    # Row 1: Timeseries Volume & Severity Breakdown
    c_left, c_right = st.columns([2, 1])

    with c_left:
        st.subheader("📈 Ingestion Velocity & Fraud Detection Pulse")
        trend_df = query_df("""
            SELECT 
                DATE_TRUNC('minute', t.event_timestamp) AS time_bucket,
                COUNT(*) AS total_tx,
                COUNT(DISTINCT fa.transaction_id) AS flagged_tx,
                SUM(CASE WHEN t.is_fraud_ground_truth THEN 1 ELSE 0 END) AS injected_fraud
            FROM transactions t
            LEFT JOIN fraud_alerts fa ON t.transaction_id = fa.transaction_id
            WHERE t.event_timestamp >= NOW() - INTERVAL '30 minutes'
            GROUP BY 1
            ORDER BY 1 ASC
        """)
        if not trend_df.empty:
            fig_trend = go.Figure()
            fig_trend.add_trace(go.Scatter(
                x=trend_df["time_bucket"],
                y=trend_df["total_tx"],
                name="Total Transactions",
                mode="lines+markers",
                line=dict(color="#38bdf8", width=2.5),
                fill="tozeroy",
                fillcolor="rgba(56, 189, 248, 0.08)",
            ))
            fig_trend.add_trace(go.Bar(
                x=trend_df["time_bucket"],
                y=trend_df["flagged_tx"],
                name="Flagged by Rules",
                marker_color="#f97316",
                opacity=0.85,
            ))
            fig_trend.add_trace(go.Scatter(
                x=trend_df["time_bucket"],
                y=trend_df["injected_fraud"],
                name="Ground-Truth Fraud",
                mode="lines",
                line=dict(color="#ef4444", width=2, dash="dash"),
            ))
            fig_trend.update_layout(
                template="plotly_dark",
                height=320,
                margin=dict(l=10, r=10, t=10, b=10),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                plot_bgcolor="rgba(0,0,0,0)",
                paper_bgcolor="rgba(0,0,0,0)",
            )
            st.plotly_chart(fig_trend, use_container_width=True)
        else:
            st.caption("No volume in the last 30 minutes.")

    with c_right:
        st.subheader("⚠️ Alert Severity Matrix")
        sev_df = query_df("""
            SELECT severity, COUNT(*) AS count
            FROM fraud_alerts
            GROUP BY severity
        """)
        if not sev_df.empty:
            color_map = {
                "CRITICAL": "#ef4444",
                "HIGH": "#f97316",
                "MEDIUM": "#eab308",
                "LOW": "#38bdf8"
            }
            fig_sev = px.pie(
                sev_df,
                names="severity",
                values="count",
                hole=0.55,
                color="severity",
                color_discrete_map=color_map,
            )
            fig_sev.update_layout(
                template="plotly_dark",
                height=320,
                margin=dict(l=10, r=10, t=10, b=10),
                plot_bgcolor="rgba(0,0,0,0)",
                paper_bgcolor="rgba(0,0,0,0)",
            )
            st.plotly_chart(fig_sev, use_container_width=True)
        else:
            st.caption("No alerts generated yet.")

    # Row 2: Top Triggered Rules & Live Stream
    c_rules, c_stream = st.columns([1, 1.8])

    with c_rules:
        st.subheader("🎯 Most Frequently Triggered Rules")
        rules_df = query_df("""
            SELECT rule_name, COUNT(*) AS triggers
            FROM fraud_alerts
            GROUP BY rule_name
            ORDER BY triggers DESC
            LIMIT 6
        """)
        if not rules_df.empty:
            fig_rules = px.bar(
                rules_df,
                x="triggers",
                y="rule_name",
                orientation="h",
                color="triggers",
                color_continuous_scale="Viridis",
            )
            fig_rules.update_layout(
                template="plotly_dark",
                height=300,
                margin=dict(l=10, r=10, t=10, b=10),
                yaxis=dict(autorange="reversed"),
                plot_bgcolor="rgba(0,0,0,0)",
                paper_bgcolor="rgba(0,0,0,0)",
            )
            st.plotly_chart(fig_rules, use_container_width=True)
        else:
            st.caption("No rules triggered yet.")

    with c_stream:
        st.subheader("🕒 Live Transaction Ingestion Feed")
        live_stream_df = query_df("""
            SELECT 
                t.transaction_id,
                t.account_id,
                t.amount,
                t.transaction_type,
                t.status,
                t.event_timestamp,
                fa.rule_name,
                fa.severity
            FROM transactions t
            LEFT JOIN fraud_alerts fa ON t.transaction_id = fa.transaction_id
            ORDER BY t.event_timestamp DESC
            LIMIT 8
        """)
        st.dataframe(
            live_stream_df,
            use_container_width=True,
            height=300,
            column_config={
                "amount": st.column_config.NumberColumn("Amount ($)", format="$%.2f"),
                "event_timestamp": st.column_config.DatetimeColumn("Event Time", format="HH:mm:ss"),
            }
        )


# ==============================================================================
# PAGE 2: FRAUD FORENSIC INVESTIGATION
# ==============================================================================
elif page == "🔍 2. Fraud Forensic Investigation":
    st.title("🔍 Fraud Forensic Investigation & Case Management")
    st.caption("Forensic deep-dive by Transaction ID, Account ID, Rule patterns, and historical evidence")

    # Multi-dimensional search & filtering controls
    f1, f2, f3, f4 = st.columns(4)
    with f1:
        search_query = st.text_input("Account ID or Transaction UUID", placeholder="e.g. ACC_1005 or 4fa2...")
    with f2:
        selected_severity = st.selectbox("Severity Filter", ["ALL", "CRITICAL", "HIGH", "MEDIUM", "LOW"])
    with f3:
        selected_rule = st.selectbox(
            "Rule Filter",
            [
                "ALL",
                "HIGH_VALUE_TRANSACTION",
                "RAPID_REPEATED_TRANSFERS",
                "UNUSUAL_SPENDING_SPIKE",
                "REPEATED_FAILED_TRANSACTIONS",
                "DUPLICATE_TRANSACTION_EVENT",
            ]
        )
    with f4:
        min_amount = st.number_input("Minimum Transaction Amount ($)", min_value=0.0, value=0.0, step=100.0)

    base_sql = """
        SELECT 
            fa.alert_id,
            fa.transaction_id,
            fa.account_id,
            fa.rule_name,
            fa.severity,
            fa.explanation,
            t.amount,
            t.merchant_id,
            t.transaction_type,
            t.status,
            fa.event_timestamp,
            fa.processing_timestamp,
            ROUND(EXTRACT(EPOCH FROM (fa.processing_timestamp - fa.event_timestamp))::numeric, 3) AS latency_seconds
        FROM fraud_alerts fa
        JOIN transactions t ON fa.transaction_id = t.transaction_id
        WHERE 1=1
    """
    params = []
    if search_query:
        base_sql += " AND (fa.account_id ILIKE %s OR fa.transaction_id ILIKE %s)"
        params.extend([f"%{search_query}%", f"%{search_query}%"])
    if selected_severity != "ALL":
        base_sql += " AND fa.severity = %s"
        params.append(selected_severity)
    if selected_rule != "ALL":
        base_sql += " AND fa.rule_name = %s"
        params.append(selected_rule)
    if min_amount > 0:
        base_sql += " AND t.amount >= %s"
        params.append(min_amount)

    base_sql += " ORDER BY fa.event_timestamp DESC LIMIT 100"

    investigate_df = query_df(base_sql, tuple(params) if params else None)

    if investigate_df.empty:
        st.info("No fraud alerts matching the current search parameters.")
    else:
        st.markdown(f"**Found {len(investigate_df)} alerts matching investigation filters:**")
        st.dataframe(
            investigate_df[[
                "severity", "account_id", "rule_name", "amount", "merchant_id", "transaction_type", "event_timestamp", "explanation"
            ]],
            use_container_width=True,
            height=280,
            column_config={
                "amount": st.column_config.NumberColumn("Amount", format="$%.2f"),
                "event_timestamp": st.column_config.DatetimeColumn("Timestamp", format="YYYY-MM-DD HH:mm:ss"),
            }
        )

        st.divider()
        # Account 360 Drilldown
        available_accounts = investigate_df["account_id"].unique()
        selected_account = st.selectbox("🎯 Select Account for 360° Forensic Timeline:", available_accounts)

        if selected_account:
            st.subheader(f"📊 Forensic Timeline for Account: `{selected_account}`")

            acc_history = query_df("""
                SELECT 
                    t.transaction_id,
                    t.amount,
                    t.merchant_id,
                    t.transaction_type,
                    t.status,
                    t.event_timestamp,
                    t.is_fraud_ground_truth,
                    fa.rule_name,
                    fa.severity
                FROM transactions t
                LEFT JOIN fraud_alerts fa ON t.transaction_id = fa.transaction_id
                WHERE t.account_id = %s
                ORDER BY t.event_timestamp ASC
                LIMIT 100
            """, (selected_account,))

            if not acc_history.empty:
                # Timeline scatter plot
                fig_timeline = px.scatter(
                    acc_history,
                    x="event_timestamp",
                    y="amount",
                    color="severity",
                    color_discrete_map={
                        "CRITICAL": "#ef4444",
                        "HIGH": "#f97316",
                        "MEDIUM": "#eab308",
                        "LOW": "#38bdf8",
                        None: "#64748b"
                    },
                    size="amount",
                    hover_data=["transaction_type", "status", "merchant_id", "rule_name"],
                    title=f"Transaction Flow & Alert Triggers for {selected_account}",
                )
                fig_timeline.update_layout(
                    template="plotly_dark",
                    height=320,
                    margin=dict(l=10, r=10, t=30, b=10),
                    plot_bgcolor="rgba(0,0,0,0)",
                    paper_bgcolor="rgba(0,0,0,0)",
                )
                st.plotly_chart(fig_timeline, use_container_width=True)

                c_hist, c_alr = st.columns([1.5, 1])
                with c_hist:
                    st.markdown("##### Account Transaction Ledger")
                    st.dataframe(
                        acc_history[["event_timestamp", "amount", "transaction_type", "merchant_id", "status"]],
                        use_container_width=True,
                        height=240,
                        column_config={"amount": st.column_config.NumberColumn("Amount", format="$%.2f")}
                    )
                with c_alr:
                    st.markdown("##### Triggered Rules on this Account")
                    acc_alerts = query_df("""
                        SELECT rule_name, severity, explanation, event_timestamp
                        FROM fraud_alerts
                        WHERE account_id = %s
                        ORDER BY event_timestamp DESC
                    """, (selected_account,))
                    st.dataframe(acc_alerts, use_container_width=True, height=240)


# ==============================================================================
# PAGE 3: FINANCIAL RISK & MERCHANT ANALYTICS
# ==============================================================================
elif page == "💰 3. Financial Risk & Merchant Analytics":
    st.title("💰 Financial Risk & Merchant Exposure Analytics")
    st.caption("Value-at-risk analysis, merchant fraud concentration, and spending distribution modeling")

    # Financial Exposure KPIs
    fin_kpis = query_df("""
        SELECT 
            SUM(amount) AS total_processed_usd,
            AVG(amount) AS avg_transaction_usd,
            SUM(CASE WHEN fa.alert_id IS NOT NULL THEN t.amount ELSE 0 END) AS flagged_risk_usd,
            AVG(CASE WHEN fa.alert_id IS NOT NULL THEN t.amount ELSE NULL END) AS avg_fraud_ticket_usd,
            AVG(CASE WHEN fa.alert_id IS NULL THEN t.amount ELSE NULL END) AS avg_normal_ticket_usd
        FROM transactions t
        LEFT JOIN fraud_alerts fa ON t.transaction_id = fa.transaction_id
    """).iloc[0]

    tot_proc = float(fin_kpis["total_processed_usd"] or 0)
    flagged_risk = float(fin_kpis["flagged_risk_usd"] or 0)
    avg_ticket = float(fin_kpis["avg_transaction_usd"] or 0)
    avg_fraud = float(fin_kpis["avg_fraud_ticket_usd"] or 0)
    avg_normal = float(fin_kpis["avg_normal_ticket_usd"] or 0)
    loss_ratio = (flagged_risk / tot_proc * 100.0) if tot_proc > 0 else 0.0

    k1, k2, k3, k4 = st.columns(4)
    with k1:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-title">Gross Volume Ingested</div>
            <div class="kpi-value">${tot_proc:,.2f}</div>
            <div class="kpi-subtext">Avg Ticket: ${avg_ticket:.2f}</div>
        </div>""", unsafe_allow_html=True)
    with k2:
        st.markdown(f"""
        <div class="kpi-card kpi-card-danger">
            <div class="kpi-title">Value at Risk (Flagged)</div>
            <div class="kpi-value" style="color: #f87171;">${flagged_risk:,.2f}</div>
            <div class="kpi-subtext">Exposure: {loss_ratio:.2f}% of volume</div>
        </div>""", unsafe_allow_html=True)
    with k3:
        st.markdown(f"""
        <div class="kpi-card kpi-card-warning">
            <div class="kpi-title">Avg Flagged Ticket</div>
            <div class="kpi-value" style="color: #fbbf24;">${avg_fraud:.2f}</div>
            <div class="kpi-subtext">Normal avg: ${avg_normal:.2f}</div>
        </div>""", unsafe_allow_html=True)
    with k4:
        st.markdown(f"""
        <div class="kpi-card kpi-card-success">
            <div class="kpi-title">Fraud Ticket Multiple</div>
            <div class="kpi-value" style="color: #38bdf8;">{(avg_fraud/avg_normal):.1f}x</div>
            <div class="kpi-subtext">Anomaly skew ratio</div>
        </div>""", unsafe_allow_html=True)

    # Merchant Breakdown & Risk Ranking
    st.subheader("🏪 Merchant Risk & Exposure Leaderboard")
    merchant_df = query_df("""
        SELECT 
            t.merchant_id,
            COUNT(*) AS total_tx,
            SUM(t.amount) AS total_amount,
            COUNT(DISTINCT fa.transaction_id) AS flagged_tx,
            SUM(CASE WHEN fa.alert_id IS NOT NULL THEN t.amount ELSE 0 END) AS flagged_amount,
            ROUND((COUNT(DISTINCT fa.transaction_id)::float / COUNT(*)::float * 100)::numeric, 1) AS fraud_rate_pct
        FROM transactions t
        LEFT JOIN fraud_alerts fa ON t.transaction_id = fa.transaction_id
        GROUP BY t.merchant_id
        ORDER BY flagged_amount DESC
        LIMIT 15
    """)

    col_m_chart, col_m_table = st.columns([1.2, 1.8])
    with col_m_chart:
        if not merchant_df.empty:
            fig_merch = px.bar(
                merchant_df,
                x="merchant_id",
                y="flagged_amount",
                color="fraud_rate_pct",
                color_continuous_scale="Reds",
                title="Flagged Exposure by Merchant ($)",
                labels={"flagged_amount": "Flagged Volume ($)", "fraud_rate_pct": "Fraud Rate (%)"}
            )
            fig_merch.update_layout(
                template="plotly_dark",
                height=320,
                margin=dict(l=10, r=10, t=30, b=10),
                plot_bgcolor="rgba(0,0,0,0)",
                paper_bgcolor="rgba(0,0,0,0)",
            )
            st.plotly_chart(fig_merch, use_container_width=True)

    with col_m_table:
        st.dataframe(
            merchant_df,
            use_container_width=True,
            height=320,
            column_config={
                "total_amount": st.column_config.NumberColumn("Total ($)", format="$%.2f"),
                "flagged_amount": st.column_config.NumberColumn("Flagged ($)", format="$%.2f"),
                "fraud_rate_pct": st.column_config.ProgressColumn("Fraud %", min_value=0, max_value=100, format="%.1f%%"),
            }
        )

    # Transaction Type Distribution & Amount Distribution
    st.subheader("📊 Transaction Distribution & Modality Profile")
    col_dist1, col_dist2 = st.columns(2)

    with col_dist1:
        tx_types_df = query_df("""
            SELECT 
                transaction_type,
                COUNT(*) AS count,
                SUM(CASE WHEN fa.alert_id IS NOT NULL THEN 1 ELSE 0 END) AS flagged
            FROM transactions t
            LEFT JOIN fraud_alerts fa ON t.transaction_id = fa.transaction_id
            GROUP BY transaction_type
        """)
        if not tx_types_df.empty:
            fig_types = px.bar(
                tx_types_df,
                x="transaction_type",
                y=["count", "flagged"],
                barmode="group",
                title="Transaction Types: Total vs Flagged",
                color_discrete_map={"count": "#38bdf8", "flagged": "#ef4444"}
            )
            fig_types.update_layout(
                template="plotly_dark",
                height=280,
                margin=dict(l=10, r=10, t=30, b=10),
                plot_bgcolor="rgba(0,0,0,0)",
                paper_bgcolor="rgba(0,0,0,0)",
            )
            st.plotly_chart(fig_types, use_container_width=True)

    with col_dist2:
        amounts_df = query_df("""
            SELECT 
                amount,
                CASE WHEN fa.alert_id IS NOT NULL THEN 'Flagged Alert' ELSE 'Normal' END AS classification
            FROM transactions t
            LEFT JOIN fraud_alerts fa ON t.transaction_id = fa.transaction_id
            ORDER BY t.event_timestamp DESC
            LIMIT 500
        """)
        if not amounts_df.empty:
            fig_hist = px.histogram(
                amounts_df,
                x="amount",
                color="classification",
                nbins=40,
                log_y=True,
                title="Amount Distribution (Log Scale)",
                color_discrete_map={"Normal": "#38bdf8", "Flagged Alert": "#f97316"}
            )
            fig_hist.update_layout(
                template="plotly_dark",
                height=280,
                margin=dict(l=10, r=10, t=30, b=10),
                plot_bgcolor="rgba(0,0,0,0)",
                paper_bgcolor="rgba(0,0,0,0)",
            )
            st.plotly_chart(fig_hist, use_container_width=True)


# ==============================================================================
# PAGE 4: DETECTION RULES & INTERACTIVE SIMULATOR
# ==============================================================================
elif page == "⚙️ 4. Detection Rules & Interactive Simulator":
    st.title("⚙️ Detection Rules Engine & Interactive Simulator")
    st.caption("Inspect live rule configurations and test the detection engine against hypothetical transactions")

    # Load active configuration
    rules_cfg = load_rules_yaml()
    active_rules = rules_cfg.get("rules", {})

    st.subheader("📋 Active Fraud Rules Configuration")
    rule_stats_df = query_df("""
        SELECT 
            fa.rule_name,
            fa.severity,
            COUNT(*) AS total_triggers,
            ROUND(AVG(t.amount)::numeric, 2) AS avg_flagged_amount,
            MAX(t.amount) AS max_amount_flagged
        FROM fraud_alerts fa
        JOIN transactions t ON fa.transaction_id = t.transaction_id
        GROUP BY fa.rule_name, fa.severity
        ORDER BY total_triggers DESC
    """)
    st.dataframe(
        rule_stats_df,
        use_container_width=True,
        column_config={
            "avg_flagged_amount": st.column_config.NumberColumn("Avg Amount ($)", format="$%.2f"),
            "max_amount_flagged": st.column_config.NumberColumn("Max Amount ($)", format="$%.2f"),
        }
    )

    st.divider()

    # Interactive Sandbox Simulator
    st.subheader("🧪 Live Interactive Fraud Rule Simulator")
    st.markdown("Enter simulated transaction attributes below to evaluate how the rules engine would classify it in real-time:")

    sim_c1, sim_c2, sim_c3 = st.columns(3)
    with sim_c1:
        sim_acc = st.text_input("Account ID", value="ACC_1042")
        sim_amt = st.number_input("Transaction Amount ($)", value=6500.00, step=100.0)
    with sim_c2:
        sim_type = st.selectbox("Transaction Type", ["TRANSFER", "PURCHASE", "PAYMENT", "WITHDRAWAL"])
        sim_status = st.selectbox("Transaction Status", ["COMPLETED", "FAILED", "DECLINED", "PENDING"])
    with sim_c3:
        sim_burst_count = st.slider("Simulate Prior Transfers in Last 60s", min_value=0, max_value=10, value=2)
        sim_baseline_avg = st.number_input("Simulate Account Baseline Avg ($)", value=80.00, step=10.0)

    if st.button("⚡ Evaluate Transaction Against Rules Engine", type="primary"):
        # Evaluate rules locally
        triggered_rules = []

        # High-value
        hv_cfg = active_rules.get("high_value", {})
        if hv_cfg.get("enabled", True) and sim_amt >= hv_cfg.get("threshold_amount", 5000.0):
            triggered_rules.append({
                "rule": "HIGH_VALUE_TRANSACTION",
                "severity": hv_cfg.get("severity", "HIGH"),
                "explanation": f"Amount ${sim_amt:,.2f} exceeds high-value threshold (${hv_cfg.get('threshold_amount', 5000):,.2f})"
            })

        # Rapid transfers
        rt_cfg = active_rules.get("rapid_transfers", {})
        if rt_cfg.get("enabled", True) and sim_type in ["TRANSFER", "PAYMENT", "WITHDRAWAL"]:
            if (sim_burst_count + 1) >= rt_cfg.get("min_count", 3):
                triggered_rules.append({
                    "rule": "RAPID_REPEATED_TRANSFERS",
                    "severity": rt_cfg.get("severity", "HIGH"),
                    "explanation": f"{sim_burst_count + 1} rapid transfers/payments within {rt_cfg.get('window_seconds', 60)}s window"
                })

        # Unusual spending
        us_cfg = active_rules.get("unusual_spending", {})
        if us_cfg.get("enabled", True):
            mult = us_cfg.get("spike_multiplier", 3.5)
            min_trig = us_cfg.get("min_trigger_amount", 1500.0)
            if sim_amt >= min_trig and sim_amt >= (sim_baseline_avg * mult):
                triggered_rules.append({
                    "rule": "UNUSUAL_SPENDING_SPIKE",
                    "severity": us_cfg.get("severity", "MEDIUM"),
                    "explanation": f"Amount ${sim_amt:,.2f} is {sim_amt/sim_baseline_avg:.1f}x higher than baseline average (${sim_baseline_avg:.2f})"
                })

        # Repeated failed
        rf_cfg = active_rules.get("repeated_failed", {})
        if rf_cfg.get("enabled", True) and sim_status in ["FAILED", "DECLINED"]:
            triggered_rules.append({
                "rule": "REPEATED_FAILED_TRANSACTIONS",
                "severity": rf_cfg.get("severity", "CRITICAL"),
                "explanation": f"Transaction status '{sim_status}' indicates failure/decline on account"
            })

        if triggered_rules:
            st.error(f"🚨 **ALERT! {len(triggered_rules)} Fraud Rule(s) Triggered:**")
            for tr in triggered_rules:
                st.markdown(f"- **`{tr['rule']}`** ({tr['severity']}): {tr['explanation']}")
        else:
            st.success("✅ **CLEAN TRANSACTION**: No fraud rules triggered. Passes through to normal clearing.")

    st.divider()
    st.markdown("##### Active Configuration Source (`config/fraud_rules.yaml`)")
    st.code(yaml.dump(rules_cfg, default_flow_style=False), language="yaml")


# ==============================================================================
# PAGE 5: PIPELINE ARCHITECTURE & CLUSTER HEALTH
# ==============================================================================
elif page == "⚡ 5. Pipeline Architecture & Cluster Health":
    st.title("⚡ Streaming Pipeline Architecture & Cluster Health")
    st.caption("Low-level telemetry, micro-batch latency benchmarks, and dead-letter quarantine diagnostics")

    # Fetch micro-batch metrics
    metrics_df = query_df("""
        SELECT 
            batch_id,
            records_processed,
            alerts_generated,
            batch_duration_ms,
            input_rate,
            processing_rate,
            quarantined_records,
            recorded_at
        FROM processing_metrics
        ORDER BY recorded_at DESC
        LIMIT 40
    """)

    if not metrics_df.empty:
        latest = metrics_df.iloc[0]
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            st.markdown(f"""
            <div class="kpi-card">
                <div class="kpi-title">Latest Batch Duration</div>
                <div class="kpi-value">{latest['batch_duration_ms']:.1f} ms</div>
                <div class="kpi-subtext">Spark processing time</div>
            </div>""", unsafe_allow_html=True)
        with c2:
            st.markdown(f"""
            <div class="kpi-card kpi-card-success">
                <div class="kpi-title">Processing Rate</div>
                <div class="kpi-value">{latest['processing_rate']:.1f} tx/s</div>
                <div class="kpi-subtext">Throughput capacity</div>
            </div>""", unsafe_allow_html=True)
        with c3:
            st.markdown(f"""
            <div class="kpi-card">
                <div class="kpi-title">Total Batches</div>
                <div class="kpi-value">{len(metrics_df)}</div>
                <div class="kpi-subtext">Checkpointed executions</div>
            </div>""", unsafe_allow_html=True)
        with c4:
            st.markdown(f"""
            <div class="kpi-card kpi-card-warning">
                <div class="kpi-title">Quarantined Total</div>
                <div class="kpi-value">{int(metrics_df['quarantined_records'].sum())}</div>
                <div class="kpi-subtext">Dead-letter count</div>
            </div>""", unsafe_allow_html=True)

        # Micro-batch plots
        col_p1, col_p2 = st.columns(2)
        with col_p1:
            st.subheader("⏱️ Micro-batch Duration Over Time (ms)")
            fig_dur = px.line(
                metrics_df.sort_values("recorded_at"),
                x="recorded_at",
                y="batch_duration_ms",
                markers=True,
                title="Micro-Batch Execution Latency",
            )
            fig_dur.update_traces(line_color="#a855f7")
            fig_dur.update_layout(template="plotly_dark", height=280, margin=dict(l=10, r=10, t=30, b=10))
            st.plotly_chart(fig_dur, use_container_width=True)

        with col_p2:
            st.subheader("📊 Ingestion vs Processing Throughput")
            fig_tp = go.Figure()
            sorted_m = metrics_df.sort_values("recorded_at")
            fig_tp.add_trace(go.Scatter(x=sorted_m["recorded_at"], y=sorted_m["input_rate"], name="Input Rate", line=dict(color="#38bdf8")))
            fig_tp.add_trace(go.Scatter(x=sorted_m["recorded_at"], y=sorted_m["processing_rate"], name="Processing Rate", line=dict(color="#22c55e", dash="dot")))
            fig_tp.update_layout(template="plotly_dark", height=280, margin=dict(l=10, r=10, t=30, b=10))
            st.plotly_chart(fig_tp, use_container_width=True)

    # Dead-Letter Quarantine Viewer
    st.divider()
    st.subheader("🛑 Dead-Letter Quarantine Inspector (`quarantine_events`)")
    quarantine_df = query_df("""
        SELECT id, error_reason, raw_payload, received_at
        FROM quarantine_events
        ORDER BY received_at DESC
        LIMIT 10
    """)
    if not quarantine_df.empty:
        st.dataframe(quarantine_df, use_container_width=True, height=220)
    else:
        st.caption("No quarantined events recorded.")

    # Local Host Resource Utilization
    try:
        import psutil
        st.divider()
        st.subheader("💻 Local Host Resource Footprint (16 GB Profile)")
        cpu_usage = psutil.cpu_percent(interval=0.1)
        mem = psutil.virtual_memory()
        h1, h2, h3 = st.columns(3)
        with h1:
            st.metric("Host CPU Utilization", f"{cpu_usage}%")
        with h2:
            st.metric("Memory In-Use", f"{mem.percent}% ({mem.used / (1024**3):.1f} GB / {mem.total / (1024**3):.1f} GB)")
        with h3:
            st.metric("Available System RAM", f"{mem.available / (1024**3):.1f} GB")
    except Exception:
        pass


# ==============================================================================
# PAGE 6: SYSTEM RUNBOOK & TECHNICAL SPECS
# ==============================================================================
elif page == "📖 6. System Runbook & Technical Specs":
    st.title("📖 System Runbook, Architecture & Technical Specs")
    st.caption("Comprehensive data engineering documentation, mathematical rule definitions, and CLI operational guide")

    tab1, tab2, tab3, tab4 = st.tabs(["🏛️ Architecture & Guarantees", "📐 Mathematical Formulations", "💻 CLI Runbook", "🗄️ Relational Schema"])

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
        Triggers when transaction amount $A_i$ exceeds configured constant threshold $\theta_{\text{high}}$:
        $$\\text{Alert}_{\\text{high}} = \\begin{cases} 1 & \\text{if } A_i \\ge \\theta_{\text{high}} \\\\ 0 & \\text{otherwise} \\end{cases}$$
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
