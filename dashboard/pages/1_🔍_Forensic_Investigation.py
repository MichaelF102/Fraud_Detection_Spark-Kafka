import streamlit as st
import pandas as pd
import plotly.express as px
from common import apply_custom_css, render_sidebar_header, query_df

st.set_page_config(
    page_title="Forensic Investigation | SentinelFraud",
    page_icon="🔍",
    layout="wide",
)

apply_custom_css()
render_sidebar_header()

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
