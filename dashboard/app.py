import datetime
import os
import sys
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

# Ensure current and parent directory on sys.path
_current_dir = os.path.dirname(os.path.abspath(__file__))
if _current_dir not in sys.path:
    sys.path.insert(0, _current_dir)

from common import (
    apply_custom_css,
    render_sidebar_header,
    query_df,
    render_screenshots_gallery,
)

# Configure Streamlit page
st.set_page_config(
    page_title="SentinelFraud Operations Center",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded",
)

apply_custom_css()
render_sidebar_header()

st.title("🛡️ Real-Time Fraud Operations Center")
st.caption("Live streaming event telemetry processed via Apache Spark Structured Streaming and PostgreSQL")

tab_live, tab_screenshots = st.tabs(["📊 Live Operations Telemetry", "📸 Screenshots"])

with tab_screenshots:
    render_screenshots_gallery()

with tab_live:
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
    else:
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
