import streamlit as st
import pandas as pd
import plotly.express as px
from common import apply_custom_css, render_sidebar_header, query_df

st.set_page_config(
    page_title="Risk & Merchant Analytics | SentinelFraud",
    page_icon="💰",
    layout="wide",
)

apply_custom_css()
render_sidebar_header()

st.title("💰 Financial Risk & Merchant Exposure Analytics")
st.caption("Value-at-risk analysis, merchant fraud concentration, and spending distribution modeling")

# Financial Exposure KPIs
fin_kpis_df = query_df("""
    SELECT 
        SUM(amount) AS total_processed_usd,
        AVG(amount) AS avg_transaction_usd,
        SUM(CASE WHEN fa.alert_id IS NOT NULL THEN t.amount ELSE 0 END) AS flagged_risk_usd,
        AVG(CASE WHEN fa.alert_id IS NOT NULL THEN t.amount ELSE NULL END) AS avg_fraud_ticket_usd,
        AVG(CASE WHEN fa.alert_id IS NULL THEN t.amount ELSE NULL END) AS avg_normal_ticket_usd
    FROM transactions t
    LEFT JOIN fraud_alerts fa ON t.transaction_id = fa.transaction_id
""")

if not fin_kpis_df.empty:
    fin_kpis = fin_kpis_df.iloc[0]
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
        ratio = (avg_fraud / avg_normal) if avg_normal > 0 else 0.0
        st.markdown(f"""
        <div class="kpi-card kpi-card-success">
            <div class="kpi-title">Fraud Ticket Multiple</div>
            <div class="kpi-value" style="color: #38bdf8;">{ratio:.1f}x</div>
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
    if not merchant_df.empty:
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
