"""
Pipeline Leak Monitoring — Streamlit Dashboard
Uses Databricks SDK statement execution API (no connector hang).
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from databricks.sdk import WorkspaceClient
import os

st.set_page_config(
    page_title="Pipeline Leak Monitor",
    page_icon="🛢️",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Share+Tech+Mono&family=Barlow:wght@300;400;600;700&display=swap');
html, body, [class*="css"] { font-family: 'Barlow', sans-serif; background-color: #0a0e1a; color: #e0e6f0; }
section[data-testid="stSidebar"] { background: #0d1220; border-right: 1px solid #1e2d4a; }
div[data-testid="metric-container"] { background: #111827; border: 1px solid #1e2d4a; border-radius: 8px; padding: 16px; }
div[data-testid="metric-container"] label { font-family: 'Share Tech Mono', monospace; font-size: 0.7rem; letter-spacing: 0.12em; color: #4a7fa5; text-transform: uppercase; }
div[data-testid="metric-container"] div[data-testid="stMetricValue"] { font-family: 'Share Tech Mono', monospace; font-size: 2rem; color: #e0e6f0; }
h1 { font-family: 'Share Tech Mono', monospace; color: #4a9eff; }
h2 { font-family: 'Barlow', sans-serif; font-weight: 300; color: #8ab4d4; }
hr { border-color: #1e2d4a; }
.status-bar { font-family: 'Share Tech Mono', monospace; font-size: 0.7rem; color: #4a7fa5; letter-spacing: 0.08em; padding: 6px 0; border-bottom: 1px solid #1e2d4a; margin-bottom: 16px; }
</style>
""", unsafe_allow_html=True)

# ── Databricks SDK client ─────────────────────────────────────
# WorkspaceClient() picks up DATABRICKS_HOST, DATABRICKS_CLIENT_ID,
# DATABRICKS_CLIENT_SECRET automatically from Databricks Apps env.
@st.cache_resource(show_spinner=False)
def get_client():
    return WorkspaceClient()

@st.cache_data(ttl=60, show_spinner="Loading pipeline data...")
def load_gold():
    import requests
    import time

    host         = "https://dbc-a57c4392-7c2a.cloud.databricks.com"
    warehouse_id = "a9241e7cbe666b26"

    # Get OAuth token using the app's service principal credentials
    client_id     = os.environ.get("DATABRICKS_CLIENT_ID")
    client_secret = os.environ.get("DATABRICKS_CLIENT_SECRET")

    token_resp = requests.post(
        f"{host}/oidc/v1/token",
        data={
            "grant_type":    "client_credentials",
            "scope":         "all-apis",
            "client_id":     client_id,
            "client_secret": client_secret,
        }
    )
    access_token = token_resp.json()["access_token"]
    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
    
    # Show full response if something went wrong
    if "statement_id" not in resp.json():
        st.error(f"API response: {resp.json()}")
        st.stop()
        
    statement_id = resp.json()["statement_id"]

    # Poll until done
    for _ in range(30):
        time.sleep(2)
        poll = requests.get(
            f"{host}/api/2.0/sql/statements/{statement_id}",
            headers=headers,
        ).json()

        state = poll["status"]["state"]
        if state == "SUCCEEDED":
            cols = [c["name"] for c in poll["manifest"]["schema"]["columns"]]
            rows = poll.get("result", {}).get("data_array", [])
            return pd.DataFrame(rows, columns=cols)
        elif state in ("FAILED", "CANCELED", "CLOSED"):
            raise Exception(f"Query failed: {poll['status'].get('error')}")

    raise Exception("Timed out after 60 seconds.")

# ── Load ─────────────────────────────────────────────────────
try:
    df = load_gold()
except Exception as e:
    st.error(f"Failed to load Gold table: {e}")
    st.stop()

for col in ["pressure_psi","anomaly_score","flow_rate_lpm",
            "acoustic_g_rms","temperature_c","pressure_delta"]:
    df[col] = pd.to_numeric(df[col], errors="coerce")
df["event_timestamp"] = pd.to_datetime(df["event_timestamp"], errors="coerce")

# ── Sidebar ───────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### 🛢️ PIPELINE MONITOR")
    st.markdown("---")
    segments = sorted(df["pipe_segment_id"].dropna().unique())
    selected_segments = st.multiselect("Pipe Segments", options=segments, default=segments)
    selected_severities = st.multiselect("Severity Levels",
        options=["CRITICAL","WARNING","NORMAL"], default=["CRITICAL","WARNING","NORMAL"])
    st.markdown("---")
    min_date = df["event_timestamp"].min().date()
    max_date = df["event_timestamp"].max().date()
    date_range = st.date_input("Date Range", value=(min_date, max_date),
                               min_value=min_date, max_value=max_date)
    st.markdown("---")
    if st.button("🔄 Refresh Data"):
        st.cache_data.clear()
        st.rerun()
    st.markdown(f"""<div style='font-family:Share Tech Mono,monospace;font-size:0.65rem;color:#2d4a6a;margin-top:16px;'>
    SOURCE: main.pipeline_leak.gold_alerts<br>CACHE TTL: 60s<br>TOTAL RECORDS: {len(df):,}</div>""",
    unsafe_allow_html=True)

# ── Filter ────────────────────────────────────────────────────
filtered = df[df["pipe_segment_id"].isin(selected_segments) &
              df["severity"].isin(selected_severities)].copy()
if len(date_range) == 2:
    filtered = filtered[
        (filtered["event_timestamp"] >= pd.Timestamp(date_range[0])) &
        (filtered["event_timestamp"] <= pd.Timestamp(date_range[1]))]

color_map = {"CRITICAL":"#ef4444","WARNING":"#f59e0b","NORMAL":"#10b981"}

# ── Header ────────────────────────────────────────────────────
st.markdown("# 🛢️ Pipeline Leak Monitoring")
st.markdown(f"<div class='status-bar'>LIVE · KAFKA → BRONZE → SILVER → GOLD · ISOLATION FOREST · {len(filtered):,} EVENTS IN VIEW</div>",
            unsafe_allow_html=True)

# ── KPIs ──────────────────────────────────────────────────────
c1,c2,c3,c4,c5 = st.columns(5)
c1.metric("🔴 Critical",          len(filtered[filtered["severity"]=="CRITICAL"]))
c2.metric("🟡 Warning",           len(filtered[filtered["severity"]=="WARNING"]))
c3.metric("🟢 Normal",            len(filtered[filtered["severity"]=="NORMAL"]))
c4.metric("⚠️ Segments Critical", filtered[filtered["severity"]=="CRITICAL"]["pipe_segment_id"].nunique())
c5.metric("📉 Lowest Pressure",   f"{filtered['pressure_psi'].min():.1f} PSI" if len(filtered) else "N/A")
st.markdown("---")

# ── Bar + Donut ───────────────────────────────────────────────
col_l, col_r = st.columns([2,1])
with col_l:
    st.markdown("## Events by Pipe Segment")
    seg_sev = filtered.groupby(["pipe_segment_id","severity"]).size().reset_index(name="count")
    fig_bar = px.bar(seg_sev, x="count", y="pipe_segment_id", color="severity",
                     color_discrete_map=color_map, orientation="h", barmode="group", template="plotly_dark")
    fig_bar.update_layout(paper_bgcolor="#111827", plot_bgcolor="#111827", legend_title_text="",
                          margin=dict(l=0,r=0,t=10,b=0), font=dict(family="Barlow",color="#8ab4d4"),
                          yaxis_title="", xaxis_title="Event Count")
    st.plotly_chart(fig_bar, use_container_width=True)
with col_r:
    st.markdown("## Severity Split")
    sev_counts = filtered["severity"].value_counts().reset_index()
    sev_counts.columns = ["severity","count"]
    fig_donut = px.pie(sev_counts, names="severity", values="count", color="severity",
                       color_discrete_map=color_map, hole=0.6, template="plotly_dark")
    fig_donut.update_layout(paper_bgcolor="#111827", margin=dict(l=0,r=0,t=10,b=0),
                             font=dict(family="Barlow",color="#8ab4d4"), legend=dict(orientation="h",y=-0.1))
    fig_donut.update_traces(textinfo="percent+label")
    st.plotly_chart(fig_donut, use_container_width=True)
st.markdown("---")

# ── Dual charts ───────────────────────────────────────────────
st.markdown("## Anomaly Score & Pressure by Segment")
fig_dual = make_subplots(rows=1, cols=2,
    subplot_titles=("Worst Anomaly Score per Segment","Avg Pressure by Severity"))
score_by_seg = filtered.groupby("pipe_segment_id")["anomaly_score"].min().reset_index().sort_values("anomaly_score")
fig_dual.add_trace(go.Bar(x=score_by_seg["pipe_segment_id"], y=score_by_seg["anomaly_score"],
                          marker_color="#4a9eff", name="Worst Score"), row=1, col=1)
pressure_sev = filtered.groupby("severity")["pressure_psi"].mean().reset_index()
fig_dual.add_trace(go.Bar(x=pressure_sev["severity"], y=pressure_sev["pressure_psi"],
                          marker_color=[color_map.get(s,"#4a9eff") for s in pressure_sev["severity"]],
                          name="Avg Pressure"), row=1, col=2)
fig_dual.update_layout(paper_bgcolor="#111827", plot_bgcolor="#111827",
                       font=dict(family="Barlow",color="#8ab4d4"),
                       showlegend=False, margin=dict(l=0,r=0,t=40,b=0))
fig_dual.update_xaxes(showgrid=False)
fig_dual.update_yaxes(showgrid=True, gridcolor="#1e2d4a")
st.plotly_chart(fig_dual, use_container_width=True)
st.markdown("---")

# ── Cumulative impact ─────────────────────────────────────────
st.markdown("## Cumulative Anomaly Impact by Segment")
cumulative = filtered.groupby("pipe_segment_id")["anomaly_score"].sum().reset_index().sort_values("anomaly_score")
fig_cum = px.bar(cumulative, x="pipe_segment_id", y="anomaly_score", color="anomaly_score",
                 color_continuous_scale=["#ef4444","#f59e0b","#10b981"], template="plotly_dark")
fig_cum.update_layout(paper_bgcolor="#111827", plot_bgcolor="#111827",
                      font=dict(family="Barlow",color="#8ab4d4"), coloraxis_showscale=False,
                      margin=dict(l=0,r=0,t=10,b=0), xaxis_title="Pipe Segment", yaxis_title="Cumulative Score")
fig_cum.update_yaxes(showgrid=True, gridcolor="#1e2d4a")
st.plotly_chart(fig_cum, use_container_width=True)
st.markdown("---")

# ── Segment table ─────────────────────────────────────────────
st.markdown("## Pipeline Segment Details")
seg_summary = (
    filtered.groupby("pipe_segment_id")
    .agg(Events=("event_id","count"),
         Severity=("severity", lambda x: x.mode()[0] if len(x) else "N/A"),
         Avg_Pressure=("pressure_psi","mean"), Avg_Flow=("flow_rate_lpm","mean"),
         Avg_Acoustic=("acoustic_g_rms","mean"), Avg_Temp=("temperature_c","mean"),
         Worst_Score=("anomaly_score","min"))
    .reset_index().rename(columns={"pipe_segment_id":"Pipe Segment"})
)
for col in ["Avg_Pressure","Avg_Flow","Avg_Acoustic","Avg_Temp","Worst_Score"]:
    seg_summary[col] = seg_summary[col].round(2)
st.dataframe(seg_summary, use_container_width=True, hide_index=True)
st.markdown("---")

# ── Critical alerts ───────────────────────────────────────────
st.markdown("## 🔴 Critical Alert Details")
critical_df = (
    filtered[filtered["severity"]=="CRITICAL"]
    [["event_timestamp","pipe_segment_id","pressure_psi","pressure_delta","anomaly_score","alert_message"]]
    .sort_values("anomaly_score").head(50)
    .rename(columns={"event_timestamp":"Event Time","pipe_segment_id":"Pipe Segment",
                     "pressure_psi":"Pressure (PSI)","pressure_delta":"Pressure Delta",
                     "anomaly_score":"Anomaly Score","alert_message":"Alert Message"})
)
if len(critical_df):
    st.dataframe(critical_df, use_container_width=True, hide_index=True)
else:
    st.info("No critical alerts in current filter selection.")
st.markdown("---")

# ── Raw explorer ──────────────────────────────────────────────
with st.expander("🔍 Raw Data Explorer"):
    sort_col  = st.selectbox("Sort by", ["event_timestamp","anomaly_score","pressure_psi","pipe_segment_id"], index=1)
    ascending = st.checkbox("Ascending", value=True)
    st.dataframe(filtered.sort_values(sort_col, ascending=ascending).reset_index(drop=True),
                 use_container_width=True, height=400)
    st.download_button("⬇️ Download CSV", data=filtered.to_csv(index=False).encode("utf-8"),
                       file_name="pipeline_leak_filtered.csv", mime="text/csv")