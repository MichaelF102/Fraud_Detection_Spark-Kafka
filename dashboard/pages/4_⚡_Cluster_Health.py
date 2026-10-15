import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from common import apply_custom_css, render_sidebar_header, query_df

st.set_page_config(
    page_title="Cluster Health | SentinelFraud",
    page_icon="⚡",
    layout="wide",
)

apply_custom_css()
render_sidebar_header()

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
