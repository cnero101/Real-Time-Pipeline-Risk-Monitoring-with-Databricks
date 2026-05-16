# Real-Time Pipeline Risk Monitoring with Databricks

A production-grade, end-to-end real-time pipeline leak detection system built on Databricks, using a medallion architecture (Bronze → Silver → Gold), Apache Kafka for streaming ingestion, and an Isolation Forest ML model for anomaly detection. The system monitors 5 pipeline segments in real time, scores sensor readings every 30 minutes, and fires automated email alerts when critical leaks are detected.

---

## Architecture

```
Kafka (sensor data)
        │
        ▼
┌─────────────────────────────────┐
│  BRONZE — raw ingestion         │  5,350 records · Delta table
│  04_structured_streaming        │  Kafka → Bronze (batch + stream)
└─────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────┐
│  SILVER — feature engineering   │  10,185 records · Delta table
│  02_silver_transformation       │  pressure delta, flow imbalance,
│                                 │  acoustic z-score, rolling stats
└─────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────┐
│  ML MODEL — anomaly detection   │  Isolation Forest · MLflow registry
│  03_model_training              │  100% precision on test set
└─────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────┐
│  GOLD — scored alerts           │  4,845 alerts · Delta table
│  03_gold_scoring                │  CRITICAL / WARNING / NORMAL
└─────────────────────────────────┘
        │
   ┌────┴────┬────────────┐
   ▼         ▼            ▼
SQL       Streamlit    Email alerts
Dashboard   App       (every 30 min)
```

---

## Tech Stack

| Component | Technology |
|---|---|
| Data streaming | Apache Kafka (Confluent Cloud) |
| Data platform | Databricks (Serverless) |
| Storage format | Delta Lake (medallion architecture) |
| ML framework | Scikit-learn Isolation Forest |
| ML tracking | MLflow (Databricks managed) |
| Orchestration | Databricks Jobs (scheduled every 30 min) |
| Dashboard | Databricks SQL Dashboard |
| App | Streamlit (Databricks Apps) |
| Language | Python (PySpark, pandas) |

---

## Project Structure

```
pipeline-leak-monitoring/
│
├── 01_ingestion/
│   └── 01_sensor_data_generator.py     # Kafka producer — simulates 5 pipe segments
│
├── 02_streaming/
│   ├── 02_silver_transformation.py     # Bronze → Silver batch feature engineering
│   ├── 03_gold_scoring.py             # Silver → Gold MLflow model scoring
│   └── 04_structured_streaming.py     # Kafka → Bronze real-time stream
│
├── 03_ml_models/
│   ├── 03_model_training.py           # Isolation Forest training + MLflow logging
│   └── 04_lstm_model.py              # LSTM model (experimental)
│
├── 04_dashboard/
│   ├── 04_dashboard_queries.py        # SQL dashboard query definitions
│   └── 05_alert_engine.py            # Email alert logic + HTML formatting
│
└── streamlit_app/
    ├── app.py                         # Streamlit dashboard application
    ├── requirements.txt               # Python dependencies
    └── app.yaml                       # Databricks Apps config
```

---

## Notebooks

### 01_sensor_data_generator
Simulates real-time sensor data for 5 pipeline segments (PIPE-001 through PIPE-005) and publishes to a Kafka topic. Each record contains:
- `event_id`, `pipe_segment_id`, `event_timestamp`
- `pressure_psi` (normal range: 800–1,200 PSI)
- `flow_rate_lpm` (litres per minute)
- `acoustic_g_rms` (vibration — escaping fluid indicator)
- `temperature_c`
- `is_anomaly` (ground truth label for training)

### 02_silver_transformation
Reads Bronze Delta table and engineers features used by the ML model:
- `pressure_delta` — drop vs. previous reading (lag-1 window)
- `flow_imbalance_ratio` — deviation from 1,000 LPM baseline
- `acoustic_zscore` — standard deviations from segment mean
- Rolling averages for pressure, flow, and acoustic signal

### 03_gold_scoring
Loads the registered MLflow Isolation Forest model and scores Silver records:
- Predicts `anomaly_prediction` (-1 = anomaly, 1 = normal)
- Computes `anomaly_score` (decision function — more negative = more anomalous)
- Assigns severity: `CRITICAL` (score ≤ -0.07), `WARNING` (-0.07 to 0), `NORMAL` (> 0)
- Generates human-readable `alert_message` with triggered sensor details

### 04_structured_streaming
Implements both batch and streaming ingestion from Kafka to Bronze:
- Cell 5: batch read (historical backfill)
- Cell 6: streaming read with `availableNow` trigger
- Cell 8: Bronze → Silver streaming transformation via `foreachBatch`
- Fixes `DELTA_FAILED_TO_MERGE_FIELDS` timestamp error using `to_timestamp()` cast

### 03_model_training
Trains the Isolation Forest anomaly detector:
- StandardScaler for feature normalisation
- Isolation Forest with `contamination=0.08`
- Logs model, scaler, metrics, and feature importance to MLflow
- Registers model to MLflow Model Registry

### 05_alert_engine
Checks Gold table for critical events and sends email alerts:
- Queries top critical segments ranked by severity
- Builds HTML-formatted alert email with sensor trigger details
- Sends via SMTP to configured recipients
- Fires as task 4 in the automated pipeline job

---

## Automated Pipeline

The pipeline runs as a Databricks Job with 5 tasks scheduled every 30 minutes:

```
01_generate_sensor_data
        │
        ▼
02_silver_transformation
        │
        ▼
03_gold_scoring
        │
        ▼
04_alert_engine
        │
        ▼
05_model_retraining
```

All recent runs have succeeded with an average runtime of 1–2 minutes end-to-end.

---

## Medallion Tables

| Table | Records | Description |
|---|---|---|
| `main.pipeline_leak.bronze_sensor_raw` | 5,350 | Raw Kafka events with ingestion timestamp |
| `main.pipeline_leak.silver_sensor_clean` | 10,185 | Engineered features, cleaned, partitioned |
| `main.pipeline_leak.gold_alerts` | 4,845 | Scored alerts with severity and alert message |

---

## ML Model Performance

| Metric | Value |
|---|---|
| Model | Isolation Forest |
| Training records | 495 sensor readings |
| Precision on test set | 100% |
| Anomaly contamination | 8% |
| Score range | -0.117 (most anomalous) to +0.026 (most normal) |
| CRITICAL threshold | anomaly_score ≤ -0.07 |

---

## Setup & Prerequisites

### Requirements
- Databricks workspace (free tier or above)
- Confluent Cloud Kafka cluster (free tier works)
- Python 3.11+

### Environment variables / secrets
Set these as Databricks secrets or notebook variables:
```python
BOOTSTRAP_SERVERS = "your-kafka-bootstrap-server"
API_KEY           = "your-kafka-api-key"
API_SECRET        = "your-kafka-api-secret"
KAFKA_TOPIC       = "pipeline-sensor-data"
SMTP_HOST         = "smtp.gmail.com"
SMTP_USER         = "your-email@gmail.com"
SMTP_PASSWORD     = "your-app-password"
ALERT_RECIPIENTS  = ["recipient@email.com"]
```

### Unity Catalog
All tables use the `main.pipeline_leak` schema. Create it before running:
```sql
CREATE SCHEMA IF NOT EXISTS main.pipeline_leak;
```

### Running the pipeline
1. Run `01_sensor_data_generator` to populate Kafka with sensor data
2. Run `04_structured_streaming` to ingest Bronze and Silver
3. Run `03_model_training` to train and register the MLflow model
4. Run `03_gold_scoring` to score and populate Gold
5. Set up the Databricks Job to automate all steps on a schedule

---

## Dashboard

The SQL dashboard shows:
- Critical / Warning / Normal event counts (KPI cards)
- Events by pipe segment (grouped bar chart)
- Worst anomaly score per segment
- Cumulative anomaly impact
- Critical alert details table with triggered sensor readings
- Severity distribution (donut chart)

The Streamlit app (deployed on Databricks Apps) provides the same views with interactive filters for segment, severity, and date range, plus a CSV export.

---

## Alert Email Sample

When critical leaks are detected, the alert engine fires an HTML email with:
- Segments affected, ranked by severity
- Min/avg pressure, flow rate, acoustic readings per segment
- Specific sensor triggers (e.g. "Pressure drop -459 PSI; Flow spike x1.27; Acoustic anomaly z=2.84")
- Immediate action steps
- Link to live dashboard

---

## Key Engineering Decisions

**Why Isolation Forest?** Unsupervised anomaly detection is ideal here since true leak labels are rare in production. Isolation Forest handles high-dimensional sensor data well and is fast to retrain on new data.

**Why medallion architecture?** Separating raw ingestion (Bronze), feature engineering (Silver), and business logic (Gold) makes the pipeline modular, auditable, and easy to reprocess any layer independently.

**Why `foreachBatch` for streaming Silver?** Window functions (`lag`, `avg`, `stddev`) don't work directly on Spark streams. `foreachBatch` converts each micro-batch to a static DataFrame, applies window logic, and writes to Silver — same result as batch, but triggered by new Bronze records.

**Why `availableNow` trigger on Serverless?** Databricks Serverless clusters don't support continuous `processingTime` triggers. `availableNow` processes all available messages and stops cleanly, which works perfectly with the 30-minute scheduled job.
