import streamlit as st
import pandas as pd
import yaml
from common import apply_custom_css, render_sidebar_header, query_df, load_rules_yaml

st.set_page_config(
    page_title="Rules & Simulator | SentinelFraud",
    page_icon="⚙️",
    layout="wide",
)

apply_custom_css()
render_sidebar_header()

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
if not rule_stats_df.empty:
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
