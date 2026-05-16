# Notebook Descriptions

Detailed descriptions of each notebook in the pipeline, including purpose, inputs, outputs, and key implementation decisions.

---

## 01_ingestion / 01_sensor_data_generator

**Purpose:** Simulates real-time sensor data for 5 pipeline segments and publishes to a Confluent Cloud Kafka topic.

**What it does:**
- Generates realistic sensor readings with configurable anomaly injection rate (8%)
- Publishes JSON records to Kafka every second
- Simulates physical behaviour: pressure drops, flow spikes, and acoustic anomalies occur together during a leak event

**Output schema:**
```
event_id          string    UUID for each reading
pipe_segment_id   string    PIPE-001 through PIPE-005
event_timestamp   string    ISO 8601 UTC timestamp
pressure_psi      double    Normal: 800–1,200 PSI; leak: 500–750 PSI
flow_rate_lpm     double    Normal: 900–1,100 LPM; leak: 1,100–1,600 LPM
acoustic_g_rms    double    Normal: 0.1–0.5 g RMS; leak: 1.5–3.5 g RMS
temperature_c     double    Normal: 35–55°C
is_anomaly        string    "leak" or "normal" (ground truth)
```

**Key decision:** `event_timestamp` is serialised as a string (ISO 8601) because Kafka serialises all values as bytes. The downstream Bronze notebook casts it to `TimestampType` using `to_timestamp()`.

---

## 02_streaming / 04_structured_streaming

**Purpose:** Real-time ingestion from Kafka to Bronze Delta table, and streaming transformation from Bronze to Silver.

**Cells:**
- **Cell 1–4:** Config, Kafka credentials, schema definition, Bronze table setup
- **Cell 5:** Batch read from Kafka (earliest to latest offsets) — used for historical backfill
- **Cell 6:** Streaming read from Kafka with `availableNow` trigger — processes new messages since last run
- **Cell 7:** Bronze verification (record count, schema check)
- **Cell 8:** Bronze → Silver streaming transformation using `foreachBatch`
- **Cell 9:** Gold table verification

**Key fixes implemented:**
- `DELTA_FAILED_TO_MERGE_FIELDS` — fixed by casting `event_timestamp` to `TimestampType` before Delta write
- `INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED` — fixed by switching from `processingTime` to `availableNow` trigger (Serverless limitation)
- `DBFS_DISABLED` — fixed by using Unity Catalog Volumes for checkpoint path instead of `/tmp/`
- `foreachBatch` write failures — resolved by using SQL `INSERT INTO` pattern inside the batch function

**Checkpoint path:** `/Volumes/main/pipeline_leak/checkpoints/`

---

## 02_streaming / 02_silver_transformation

**Purpose:** Reads Bronze Delta table as a static batch, applies feature engineering, and writes to Silver Delta table.

**Features engineered:**

| Feature | Formula | What it detects |
|---|---|---|
| `pressure_delta` | `pressure_psi - lag(pressure_psi, 1)` per segment | Sudden pressure drop |
| `flow_imbalance_ratio` | `(flow_rate_lpm - 1000) / 1000` | Flow spike above baseline |
| `acoustic_zscore` | `(acoustic_g_rms - mean) / stddev` per segment | Acoustic anomaly |
| `pressure_rolling_avg` | 10-reading rolling mean | Trend smoothing |
| `flow_rolling_avg` | 10-reading rolling mean | Trend smoothing |
| `acoustic_rolling_avg` | 10-reading rolling mean | Trend smoothing |
| `acoustic_rolling_std` | 10-reading rolling std | Volatility signal |

**Window spec:** `partitionBy("pipe_segment_id").orderBy("event_timestamp")`

**Output:** `main.pipeline_leak.silver_sensor_clean` — 10,185 records

---

## 03_ml_models / 03_model_training

**Purpose:** Trains an Isolation Forest anomaly detector on Silver feature data and logs everything to MLflow.

**Training process:**
1. Load Silver table, filter to labelled records
2. Select 7 features: `pressure_psi`, `flow_rate_lpm`, `acoustic_g_rms`, `temperature_c`, `pressure_delta`, `flow_imbalance_ratio`, `acoustic_zscore`
3. Fit `StandardScaler` on training features
4. Train `IsolationForest(contamination=0.08, random_state=42)`
5. Evaluate on test set: compute precision, recall, F1
6. Log model, scaler, metrics, and feature importance to MLflow experiment
7. Register both model and scaler to MLflow Model Registry

**MLflow artifacts logged:**
- `pipeline_leak_detector` — the Isolation Forest model
- `pipeline_scaler` — the StandardScaler
- Precision, recall, F1, confusion matrix
- Feature importance scores

**Model performance:**
- Precision: 100% on test set
- Anomaly score range: -0.117 (most anomalous) to +0.026 (most normal)
- Contamination parameter: 0.08 (8% expected anomaly rate)

---

## 02_streaming / 03_gold_scoring

**Purpose:** Loads the registered MLflow model and scaler, scores all Silver records, assigns severity levels, and writes to Gold Delta table.

**Scoring pipeline:**
1. Load `pipeline_leak_detector` and `pipeline_scaler` from MLflow Registry (`@production` alias, falls back to version 1)
2. Transform features with the scaler
3. `model.predict(X)` → `anomaly_prediction` (-1 = anomaly, 1 = normal)
4. `model.decision_function(X)` → `anomaly_score` (continuous severity)
5. Assign severity based on calibrated thresholds

**Severity thresholds (calibrated to actual score distribution):**
```python
if anomaly_prediction == 1:
    severity = "NORMAL"
elif anomaly_score <= -0.07:
    severity = "CRITICAL"
else:
    severity = "WARNING"
```

**Alert message generation:**
```python
# Example output:
"Pressure drop -459 PSI; Flow spike x1.27; Acoustic anomaly z=2.84"
```

Triggers: pressure_delta < -100, flow_imbalance_ratio > 1.25, abs(acoustic_zscore) > 2.0

**Output:** `main.pipeline_leak.gold_alerts` — 4,845 records partitioned by `pipe_segment_id` and `severity`

---

## 04_dashboard / 05_alert_engine

**Purpose:** Queries Gold table for critical events and sends formatted HTML email alerts to configured recipients.

**Alert logic:**
1. Query Gold for events where `severity = 'CRITICAL'` in the last run window
2. Group by `pipe_segment_id`, compute: event count, min/avg pressure, avg flow, avg acoustic
3. Rank segments by critical event count
4. Build HTML email with colour-coded severity table, sensor trigger details, and action steps
5. Send via SMTP to all configured recipients

**Email contents:**
- Alert header with total critical events and segments affected
- Per-segment table: critical count, min pressure, avg pressure, avg flow, acoustic, sensor triggers, last seen timestamp
- "What This Means" section explaining each sensor signal
- "Immediate Actions Required" ordered list
- Footer with monitoring system attribution and dashboard link

**Trigger:** Runs as task 4 in the automated Databricks Job, after `03_gold_scoring` completes.

---

## streamlit_app / app.py

**Purpose:** Interactive web dashboard deployed as a Databricks App, reading from the Gold Delta table.

**Features:**
- KPI row: Critical, Warning, Normal counts; Segments Critical; Lowest Pressure
- Events by pipe segment (grouped horizontal bar, Plotly)
- Severity split (donut chart, Plotly)
- Worst anomaly score per segment (bar chart)
- Avg pressure by severity (bar chart)
- Cumulative anomaly impact by segment (colour-coded bar)
- Pipeline segment details (summary table)
- Critical alert details (top 50 worst alerts)
- Raw data explorer with sort controls and CSV export

**Sidebar filters:** Pipe segment multi-select, severity multi-select, date range picker, manual refresh button

**Auth:** Uses `databricks.sdk.WorkspaceClient` with OAuth token exchange from injected `DATABRICKS_CLIENT_ID` and `DATABRICKS_CLIENT_SECRET` environment variables.

**Deployed at:** `https://pipeline-leak-monitor-7474655596240696.aws.databricksapps.com`
