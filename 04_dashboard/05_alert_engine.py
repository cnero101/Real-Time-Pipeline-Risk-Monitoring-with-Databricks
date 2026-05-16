# Databricks notebook source
# Cell 1: Query Gold table for current CRITICAL events
CATALOG    = 'main'
SCHEMA     = 'pipeline_leak'
GOLD_TABLE = f'{CATALOG}.{SCHEMA}.gold_alerts'

critical_df = spark.sql(f"""
    SELECT
        pipe_segment_id,
        COUNT(*)                        AS critical_count,
        ROUND(MIN(pressure_psi), 1)     AS lowest_pressure_psi,
        ROUND(AVG(pressure_psi), 1)     AS avg_pressure_psi,
        ROUND(MIN(anomaly_score), 4)    AS worst_anomaly_score,
        ROUND(AVG(acoustic_g_rms), 4)   AS avg_acoustic,
        ROUND(AVG(flow_rate_lpm), 1)    AS avg_flow_lpm,
        MAX(alert_message)              AS worst_alert_message,
        MAX(event_timestamp)            AS last_seen
    FROM {GOLD_TABLE}
    WHERE severity = 'CRITICAL'
    GROUP BY pipe_segment_id
    ORDER BY critical_count DESC
""")

critical_df.show(truncate=False)
critical_count = critical_df.count()
print(f'Total segments with CRITICAL events: {critical_count}')

# COMMAND ----------

# Cell 2: Build the detailed HTML email body
from datetime import datetime, timezone

def build_email_body(critical_pdf):
    timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
    
    # Build segment rows
    segment_rows = ""
    for _, row in critical_pdf.iterrows():
        # Determine row color by severity score
        color = "#FF4444" if row['worst_anomaly_score'] < -0.10 else "#FF8C00"
        segment_rows += f"""
        <tr style="background-color: #1a1a2e;">
            <td style="padding:10px; border:1px solid #333; color:#FF4444; font-weight:bold;">
                {row['pipe_segment_id']}
            </td>
            <td style="padding:10px; border:1px solid #333; color:{color}; font-weight:bold; text-align:center;">
                {int(row['critical_count'])}
            </td>
            <td style="padding:10px; border:1px solid #333; color:#FF6B6B; text-align:center;">
                {row['lowest_pressure_psi']} PSI
            </td>
            <td style="padding:10px; border:1px solid #333; color:#FFA500; text-align:center;">
                {row['avg_pressure_psi']} PSI
            </td>
            <td style="padding:10px; border:1px solid #333; color:#FFD700; text-align:center;">
                {row['avg_flow_lpm']} L/min
            </td>
            <td style="padding:10px; border:1px solid #333; color:#FF6B6B; text-align:center;">
                {row['avg_acoustic']}
            </td>
            <td style="padding:10px; border:1px solid #333; color:#FF4444;">
                {row['worst_alert_message']}
            </td>
            <td style="padding:10px; border:1px solid #333; color:#888; font-size:11px;">
                {str(row['last_seen'])[:19]}
            </td>
        </tr>
        """

    html = f"""
    <html>
    <body style="background-color:#0f0f1a; font-family:Arial,sans-serif; padding:20px;">

        <!-- Header Banner -->
        <div style="background-color:#8B0000; padding:20px; border-radius:8px; margin-bottom:20px;">
            <h1 style="color:white; margin:0; font-size:24px;">
                🚨 CRITICAL PIPELINE LEAK DETECTED
            </h1>
            <p style="color:#FFB3B3; margin:8px 0 0 0; font-size:14px;">
                Alberta Oil & Gas Pipeline Network — Immediate Action Required
            </p>
        </div>

        <!-- Alert Metadata -->
        <div style="background-color:#1a1a2e; border:1px solid #FF4444; 
                    border-radius:8px; padding:16px; margin-bottom:20px;">
            <table style="width:100%; border-collapse:collapse;">
                <tr>
                    <td style="color:#888; padding:4px; width:180px;">Alert Status</td>
                    <td style="color:#FF4444; font-weight:bold; padding:4px;">
                        ● CRITICAL — TRIGGERED
                    </td>
                </tr>
                <tr>
                    <td style="color:#888; padding:4px;">Detection Time</td>
                    <td style="color:white; padding:4px;">{timestamp}</td>
                </tr>
                <tr>
                    <td style="color:#888; padding:4px;">Segments Affected</td>
                    <td style="color:#FF4444; font-weight:bold; padding:4px;">
                        {len(critical_pdf)} of 5 pipeline segments
                    </td>
                </tr>
                <tr>
                    <td style="color:#888; padding:4px;">Total Critical Events</td>
                    <td style="color:#FF4444; font-weight:bold; padding:4px;">
                        {int(critical_pdf['critical_count'].sum())} events detected
                    </td>
                </tr>
                <tr>
                    <td style="color:#888; padding:4px;">Monitoring System</td>
                    <td style="color:white; padding:4px;">
                        Databricks Pipeline Leak Monitor — Medallion Architecture
                    </td>
                </tr>
            </table>
        </div>

        <!-- Critical Segments Table -->
        <div style="margin-bottom:20px;">
            <h2 style="color:#FF4444; font-size:16px; margin-bottom:10px;">
                ⚠️ Critical Segments — Ranked by Severity
            </h2>
            <table style="width:100%; border-collapse:collapse; font-size:13px;">
                <thead>
                    <tr style="background-color:#8B0000;">
                        <th style="padding:10px; border:1px solid #555; 
                                   color:white; text-align:left;">Segment</th>
                        <th style="padding:10px; border:1px solid #555; 
                                   color:white; text-align:center;">Critical Events</th>
                        <th style="padding:10px; border:1px solid #555; 
                                   color:white; text-align:center;">Min Pressure</th>
                        <th style="padding:10px; border:1px solid #555; 
                                   color:white; text-align:center;">Avg Pressure</th>
                        <th style="padding:10px; border:1px solid #555; 
                                   color:white; text-align:center;">Avg Flow</th>
                        <th style="padding:10px; border:1px solid #555; 
                                   color:white; text-align:center;">Avg Acoustic</th>
                        <th style="padding:10px; border:1px solid #555; 
                                   color:white; text-align:left;">Sensors Triggered</th>
                        <th style="padding:10px; border:1px solid #555; 
                                   color:white; text-align:left;">Last Seen</th>
                    </tr>
                </thead>
                <tbody>
                    {segment_rows}
                </tbody>
            </table>
        </div>

        <!-- What This Means -->
        <div style="background-color:#1a1a2e; border-left:4px solid #FF8C00;
                    padding:16px; border-radius:4px; margin-bottom:20px;">
            <h3 style="color:#FF8C00; margin:0 0 10px 0; font-size:14px;">
                📊 What This Means
            </h3>
            <p style="color:#CCC; margin:4px 0; font-size:13px;">
                • <b style="color:white;">Pressure drop</b> below 750 PSI 
                  (normal range: 800–1,200 PSI) indicates fluid escaping the pipeline
            </p>
            <p style="color:#CCC; margin:4px 0; font-size:13px;">
                • <b style="color:white;">Flow spike</b> above 1.25x rolling average 
                  indicates surge through breach point
            </p>
            <p style="color:#CCC; margin:4px 0; font-size:13px;">
                • <b style="color:white;">Acoustic z-score</b> above 2.0 indicates 
                  high-frequency vibration from escaping fluid
            </p>
            <p style="color:#CCC; margin:4px 0; font-size:13px;">
                • ML model: <b style="color:white;">Isolation Forest</b> — 
                  trained on 495 sensor readings, 100% precision on test set
            </p>
        </div>

        <!-- Immediate Actions -->
        <div style="background-color:#1a1a2e; border-left:4px solid #FF4444;
                    padding:16px; border-radius:4px; margin-bottom:20px;">
            <h3 style="color:#FF4444; margin:0 0 10px 0; font-size:14px;">
                🚒 Immediate Actions Required
            </h3>
            <p style="color:white; margin:6px 0; font-size:13px;">
                <b>1.</b> Dispatch inspection crew to 
                <b style="color:#FF4444;">
                    {critical_pdf.iloc[0]['pipe_segment_id']}
                </b> first 
                ({int(critical_pdf.iloc[0]['critical_count'])} critical events, 
                lowest pressure {critical_pdf.iloc[0]['lowest_pressure_psi']} PSI)
            </p>
            <p style="color:white; margin:6px 0; font-size:13px;">
                <b>2.</b> Cross-check all flagged segments against SCADA system readings
            </p>
            <p style="color:white; margin:6px 0; font-size:13px;">
                <b>3.</b> If confirmed, initiate emergency pipeline isolation protocol
            </p>
            <p style="color:white; margin:6px 0; font-size:13px;">
                <b>4.</b> Log incident in operations management system
            </p>
            <p style="color:white; margin:6px 0; font-size:13px;">
                <b>5.</b> Notify HSE team and document sensor readings for compliance
            </p>
        </div>

        <!-- Footer -->
        <div style="background-color:#111; padding:12px; border-radius:4px;
                    border-top:2px solid #333;">
            <p style="color:#666; font-size:11px; margin:0;">
                This alert was generated automatically by the Databricks Pipeline 
                Leak Monitoring System using Medallion Architecture 
                (Bronze → Silver → Gold) with MLflow Isolation Forest model v1.
                Do not reply to this email.
            </p>
            <p style="color:#666; font-size:11px; margin:4px 0 0 0;">
                View live dashboard → community.cloud.databricks.com
            </p>
        </div>

    </body>
    </html>
    """
    return html

# Convert to Pandas and build the email
critical_pdf = critical_df.toPandas()
email_body   = build_email_body(critical_pdf)

print('Email body built successfully')
print(f'Segments included: {len(critical_pdf)}')
print(f'Total critical events: {critical_pdf["critical_count"].sum()}')
print(f'Highest priority segment: {critical_pdf.iloc[0]["pipe_segment_id"]}')

# COMMAND ----------

# Cell 3: Send the detailed HTML alert email via Gmail SMTP
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime, timezone

# Safety check — rerun Cell 1 and 2 if variables are missing
try:
    critical_count
    critical_pdf
    email_body
except NameError:
    raise Exception("Variables missing — please run Cell 1 and Cell 2 first before running this cell.")

# ── Email configuration ──────────────────────────────────────────
SENDER_EMAIL    = "ifeanyinjoku2@gmail.com"   # Your Gmail address
SENDER_PASSWORD = "zfmt atit kshr vcul"       # Gmail App Password (see instructions below)
RECEIVER_EMAILS = [
    "ifeanyinjoku2@gmail.com",                # Your email
    "cneroklothnz@gmail.com",
    "toheebayuba82@gmail.com",
    # "operations@yourcompany.com",           # Add more recipients here
    # "security@yourcompany.com",
]

def send_alert_email(subject: str, html_body: str, receivers: list):
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From']    = SENDER_EMAIL
    msg['To']      = ', '.join(receivers)
    msg.attach(MIMEText(html_body, 'html'))
    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
        server.login(SENDER_EMAIL, SENDER_PASSWORD)
        server.sendmail(SENDER_EMAIL, receivers, msg.as_string())

if critical_count > 0:
    subject = (
        f"🚨 CRITICAL LEAK DETECTED — "
        f"{int(critical_pdf['critical_count'].sum())} Events Across "
        f"{len(critical_pdf)} Segments — "
        f"{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"
    )
    send_alert_email(subject, email_body, RECEIVER_EMAILS)
    print(f'Alert email sent to: {RECEIVER_EMAILS}')
    print(f'Subject: {subject}')
else:
    print('No CRITICAL events — no email sent.')