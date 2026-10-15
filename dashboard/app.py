import datetime
import os
import time
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import psycopg2
import streamlit as st

# Configure Streamlit page layout and dark theme
st.set_page_config(
    page_title="SentinelFraud | Real-Time Financial Monitoring",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom CSS for dark theme aesthetic, glowing cards, and badge accents
st.markdown("""
<style>
    .metric-card {
        background-color: #1e222d;
        border: 1px solid #2d3343;
        border-radius: 8px;
        padding: 18px 20px;
        margin-bottom: 12px;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.3);
    }
    .metric-label {
        font-size: 0.85rem;
        color: #9aa0a6;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        margin-bottom: 4px;
    }
    .metric-value {
        font-size: 1.8rem;
        font-weight: 700;
        color: #e8eaed;
    }
    .badge-critical {
        background-color: rgba(239, 68, 68, 0.2);
        color: #f87171;
        border: 1px solid #ef4444;
        padding: 2px 8px;
        border-radius: 4px;
        font-weight: 600;
    }
    .badge-high {
        background-color: rgba(249, 115, 22, 0.2);
        color: #fb923c;
        border: 1px solid #f97316;
        padding: 2px 8px;
        border-radius: 4px;
        font-weight: 600;
    }
    .badge-medium {
        background-color: rgba(234, 179, 8, 0.2);
        color: #facc15;
        border: 1px solid #eab308;
        padding: 2px 8px;
        border-radius: 4px;
        font-weight: 600;
    }
</style>
""", unsafe_allow_html=True)


def get_db_connection():
    """Create a standalone PostgreSQL connection with timeout."""
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "fraud_detection"),
        user=os.getenv("POSTGRES_USER", "fraud_admin"),
        password=os.getenv("POSTGRES_PASSWORD", "fraud_secure_pass_2026"),
        connect_timeout=5,
    )


@st.cache_data(ttl=3)
def query_df(query: str, params: tuple = None) -> pd.DataFrame:
    """Execute SQL query safely and return pandas DataFrame."""
    try:
        with get_db_connection() as conn:
            return pd.read_sql_query(query, conn, params=params)
    except Exception as e:
        st.error(f"Database Query Error: {e}")
        return pd.DataFrame()


# Navigation sidebar
st.sidebar.image("https://img.icons8.com/fluency/96/shield.png", width=64)
st.sidebar.title("SentinelFraud")
st.sidebar.markdown("**Real-Time Stream Engine**")

page = st.sidebar.radio(
    "Navigation",
    ["🔴 Live Monitoring", "🔍 Fraud Investigation", "⚡ Pipeline Performance"],
    index=0,
)

auto_refresh = st.sidebar.checkbox("Auto-refresh (every 5s)", value=True)
if auto_refresh:
    time.sleep(0.1)  # brief yield
    st.sidebar.caption(f"Last polled: {datetime.datetime.now().strftime('%H:%M:%S UTC')}")

# ==============================================================================
# PAGE 1: LIVE MONITORING
# ==============================================================================
if page == "🔴 Live Monitoring":
    st.title("🛡️ Real-Time Fraud Operations Center")
    st.caption("Live telemetry stream powered by Apache Kafka, PySpark, and PostgreSQL")

    # Fetch high-level KPIs via optimized SQL
    kpi_df = query_df("""
        SELECT 
            (SELECT COUNT(*) FROM transactions) AS total_transactions,
            (SELECT COUNT(DISTINCT transaction_id) FROM fraud_alerts) AS flagged_transactions,
            (SELECT COUNT(*) FROM fraud_alerts) AS total_alerts,
            (SELECT COUNT(*) FROM quarantine_events) AS quarantined_events,
            (SELECT MAX(ingestion_timestamp) FROM transactions) AS last_update
    """)

    if kpi_df.empty or kpi_df["total_transactions"].iloc[0] == 0:
        st.info("ℹ️ No transactions processed yet. Waiting for streaming pipeline events...")
        st.stop()

    row = kpi_df.iloc[0]
    total_tx = int(row["total_transactions"] or 0)
    flagged_tx = int(row["flagged_transactions"] or 0)
    total_alerts = int(row["total_alerts"] or 0)
    quarantined = int(row["quarantined_events"] or 0)
    last_up = str(row["last_update"]) if row["last_update"] else "Never"
    alert_rate = (flagged_tx / total_tx * 100.0) if total_tx > 0 else 0.0

    # Calculate TPS from last 60 seconds
    tps_df = query_df("""
        SELECT COUNT(*)::float / 60.0 AS tps
        FROM transactions
        WHERE ingestion_timestamp >= NOW() - INTERVAL '60 seconds'
    """)
    current_tps = float(tps_df["tps"].iloc[0]) if not tps_df.empty and tps_df["tps"].iloc[0] else 0.0

    # KPI Metrics Row
    c1, c2, c3, c4, c5 = st.columns(5)
    with c1:
        st.markdown(f"""<div class="metric-card"><div class="metric-label">Processed Tx</div>
        <div class="metric-value">{total_tx:,}</div></div>""", unsafe_allow_html=True)
    with c2:
        st.markdown(f"""<div class="metric-card"><div class="metric-label">Flagged Tx</div>
        <div class="metric-value" style="color: #f87171;">{flagged_tx:,}</div></div>""", unsafe_allow_html=True)
    with c3:
        st.markdown(f"""<div class="metric-card"><div class="metric-label">Alert Rate</div>
        <div class="metric-value">{alert_rate:.2f}%</div></div>""", unsafe_allow_html=True)
    with c4:
        st.markdown(f"""<div class="metric-card"><div class="metric-label">Current TPS</div>
        <div class="metric-value" style="color: #38bdf8;">{current_tps:.1f}</div></div>""", unsafe_allow_html=True)
    with c5:
        st.markdown(f"""<div class="metric-card"><div class="metric-label">Quarantined</div>
        <div class="metric-value" style="color: #fbbf24;">{quarantined:,}</div></div>""", unsafe_allow_html=True)

    # Charts Row 1: Volume over time & Severity breakdown
    col_chart_left, col_chart_right = st.columns([2, 1])

    with col_chart_left:
        st.subheader("📈 Transaction Volume & Alert Trend (Past 15 Mins)")
        trend_df = query_df("""
            SELECT 
                DATE_TRUNC('minute', event_timestamp) AS minute_bucket,
                COUNT(*) AS total_tx,
                SUM(CASE WHEN is_fraud_ground_truth THEN 1 ELSE 0 END) AS ground_truth_fraud
            FROM transactions
            WHERE event_timestamp >= NOW() - INTERVAL '15 minutes'
            GROUP BY 1
            ORDER BY 1 ASC
        """)
        if not trend_df.empty:
            fig_trend = go.Figure()
            fig_trend.add_trace(go.Scatter(
                x=trend_df["minute_bucket"],
                y=trend_df["total_tx"],
                name="Total Transactions",
                mode="lines+markers",
                line=dict(color="#38bdf8", width=2),
                fill="tozeroy",
                fillcolor="rgba(56, 189, 248, 0.1)",
            ))
            fig_trend.add_trace(go.Bar(
                x=trend_df["minute_bucket"],
                y=trend_df["ground_truth_fraud"],
                name="Synthetic Fraud Injected",
                marker_color="#ef4444",
                opacity=0.7,
            ))
            fig_trend.update_layout(
                template="plotly_dark",
                height=320,
                margin=dict(l=20, r=20, t=20, b=20),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            )
            st.plotly_chart(fig_trend, use_container_width=True)
        else:
            st.caption("No volume in the last 15 minutes.")

    with col_chart_right:
        st.subheader("⚠️ Alert Severity Distribution")
        sev_df = query_df("""
            SELECT severity, COUNT(*) AS count
            FROM fraud_alerts
            GROUP BY severity
        """)
        if not sev_df.empty:
            color_map = {"CRITICAL": "#ef4444", "HIGH": "#f97316", "MEDIUM": "#eab308", "LOW": "#3b82f6"}
            fig_pie = px.pie(
                sev_df,
                names="severity",
                values="count",
                hole=0.45,
                color="severity",
                color_discrete_map=color_map,
            )
            fig_pie.update_layout(
                template="plotly_dark",
                height=320,
                margin=dict(l=20, r=20, t=20, b=20),
            )
            st.plotly_chart(fig_pie, use_container_width=True)
        else:
            st.caption("No alerts generated yet.")

    # Charts Row 2: Top Triggered Rules & Recent Transactions
    col_r1, col_r2 = st.columns([1, 2])
    with col_r1:
        st.subheader("🎯 Most Frequently Triggered Rules")
        rules_df = query_df("""
            SELECT rule_name, COUNT(*) AS triggers
            FROM fraud_alerts
            GROUP BY rule_name
            ORDER BY triggers DESC
            LIMIT 5
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
                height=280,
                margin=dict(l=20, r=20, t=20, b=20),
                yaxis=dict(autorange="reversed"),
            )
            st.plotly_chart(fig_rules, use_container_width=True)
        else:
            st.caption("No rules triggered yet.")

    with col_r2:
        st.subheader("🕒 Recent Transactions Stream")
        recent_tx_df = query_df("""
            SELECT 
                t.transaction_id,
                t.account_id,
                t.amount,
                t.currency,
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
        st.dataframe(recent_tx_df, use_container_width=True, height=280)


# ==============================================================================
# PAGE 2: FRAUD INVESTIGATION
# ==============================================================================
elif page == "🔍 Fraud Investigation":
    st.title("🔍 Fraud Investigation & Case Management")
    st.caption("Deep-dive forensic lookup by Account ID, Transaction ID, and Alert Patterns")

    col_filter1, col_filter2, col_filter3 = st.columns(3)
    with col_filter1:
        search_query = st.text_input("Search Transaction ID or Account ID", placeholder="e.g. ACC_1005 or UUID")
    with col_filter2:
        selected_severity = st.selectbox("Filter by Severity", ["ALL", "CRITICAL", "HIGH", "MEDIUM", "LOW"])
    with col_filter3:
        selected_rule = st.selectbox(
            "Filter by Rule",
            ["ALL", "HIGH_VALUE_TRANSACTION", "RAPID_REPEATED_TRANSFERS", "UNUSUAL_SPENDING_SPIKE", "REPEATED_FAILED_TRANSACTIONS", "DUPLICATE_TRANSACTION_EVENT"]
        )

    # Build dynamic query
    base_sql = """
        SELECT 
            fa.alert_id,
            fa.transaction_id,
            fa.account_id,
            fa.rule_name,
            fa.severity,
            fa.explanation,
            t.amount,
            t.currency,
            t.status,
            fa.event_timestamp,
            fa.processing_timestamp
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

    base_sql += " ORDER BY fa.event_timestamp DESC LIMIT 50"

    investigate_df = query_df(base_sql, tuple(params) if params else None)

    if investigate_df.empty:
        st.info("No alerts found matching search criteria.")
    else:
        st.markdown(f"**Found {len(investigate_df)} alerts:**")
        st.dataframe(investigate_df, use_container_width=True, height=300)

        # Account-level drill-down
        selected_acc = st.selectbox("Select Account for Detailed Forensic Timeline:", investigate_df["account_id"].unique())
        if selected_acc:
            st.divider()
            st.subheader(f"📊 Forensic History for Account: {selected_acc}")

            acc_history_df = query_df("""
                SELECT 
                    transaction_id,
                    amount,
                    currency,
                    merchant_id,
                    transaction_type,
                    status,
                    event_timestamp,
                    is_fraud_ground_truth
                FROM transactions
                WHERE account_id = %s
                ORDER BY event_timestamp DESC
                LIMIT 30
            """, (selected_acc,))

            col_acc1, col_acc2 = st.columns([2, 1])
            with col_acc1:
                st.markdown("##### Account Transaction Log")
                st.dataframe(acc_history_df, use_container_width=True, height=240)

            with col_acc2:
                st.markdown("##### Related Alerts for Account")
                related_alerts = query_df("""
                    SELECT rule_name, severity, explanation, event_timestamp
                    FROM fraud_alerts
                    WHERE account_id = %s
                    ORDER BY event_timestamp DESC
                    LIMIT 10
                """, (selected_acc,))
                st.dataframe(related_alerts, use_container_width=True, height=240)


# ==============================================================================
# PAGE 3: PIPELINE PERFORMANCE
# ==============================================================================
elif page == "⚡ Pipeline Performance":
    st.title("⚡ Pipeline Telemetry & Performance")
    st.caption("Real-time cluster metrics, latency benchmarks, and micro-batch statistics")

    # Fetch last 30 micro-batches
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
        LIMIT 30
    """)

    if metrics_df.empty:
        st.info("No processing metrics recorded yet.")
    else:
        latest = metrics_df.iloc[0]
        p1, p2, p3, p4 = st.columns(4)
        with p1:
            st.markdown(f"""<div class="metric-card"><div class="metric-label">Latest Batch Duration</div>
            <div class="metric-value">{latest['batch_duration_ms']:.1f} ms</div></div>""", unsafe_allow_html=True)
        with p2:
            st.markdown(f"""<div class="metric-card"><div class="metric-label">Input Rate</div>
            <div class="metric-value">{latest['input_rate']:.1f} tx/s</div></div>""", unsafe_allow_html=True)
        with p3:
            st.markdown(f"""<div class="metric-card"><div class="metric-label">Processing Rate</div>
            <div class="metric-value">{latest['processing_rate']:.1f} tx/s</div></div>""", unsafe_allow_html=True)
        with p4:
            st.markdown(f"""<div class="metric-card"><div class="metric-label">Total Batches</div>
            <div class="metric-value">{len(metrics_df)}</div></div>""", unsafe_allow_html=True)

        col_m1, col_m2 = st.columns(2)
        with col_m1:
            st.subheader("⏱️ Micro-batch Duration Over Time (ms)")
            fig_dur = px.line(
                metrics_df.sort_values("recorded_at"),
                x="recorded_at",
                y="batch_duration_ms",
                markers=True,
                labels={"batch_duration_ms": "Duration (ms)", "recorded_at": "Timestamp"},
            )
            fig_dur.update_traces(line_color="#a855f7")
            fig_dur.update_layout(template="plotly_dark", height=280, margin=dict(l=20, r=20, t=20, b=20))
            st.plotly_chart(fig_dur, use_container_width=True)

        with col_m2:
            st.subheader("📊 Ingestion Rate vs Processing Rate (tx/s)")
            fig_rates = go.Figure()
            fig_rates.add_trace(go.Scatter(
                x=metrics_df.sort_values("recorded_at")["recorded_at"],
                y=metrics_df.sort_values("recorded_at")["input_rate"],
                name="Input Rate",
                line=dict(color="#38bdf8")
            ))
            fig_rates.add_trace(go.Scatter(
                x=metrics_df.sort_values("recorded_at")["recorded_at"],
                y=metrics_df.sort_values("recorded_at")["processing_rate"],
                name="Processing Rate",
                line=dict(color="#22c55e", dash="dot")
            ))
            fig_rates.update_layout(template="plotly_dark", height=280, margin=dict(l=20, r=20, t=20, b=20))
            st.plotly_chart(fig_rates, use_container_width=True)

        st.subheader("📋 Micro-batch Execution Telemetry")
        st.dataframe(metrics_df, use_container_width=True)

        # Host system metrics if psutil is available
        try:
            import psutil
            st.divider()
            st.subheader("💻 Local Host Resource Footprint")
            cpu_usage = psutil.cpu_percent(interval=0.1)
            mem = psutil.virtual_memory()
            s1, s2, s3 = st.columns(3)
            with s1:
                st.metric("CPU Utilization", f"{cpu_usage}%")
            with s2:
                st.metric("Memory Usage", f"{mem.percent}% ({mem.used / (1024**3):.1f}GB / {mem.total / (1024**3):.1f}GB)")
            with s3:
                st.metric("Available RAM", f"{mem.available / (1024**3):.1f} GB")
        except Exception:
            pass
