# Portfolio Summary — Real-Time Pipeline Risk Monitoring with Databricks

## What I Built

A production-grade, end-to-end real-time pipeline leak detection system that monitors 5 oil and gas pipeline segments, detects anomalies using machine learning, and fires automated email alerts when critical leaks are identified.

The system ingests live sensor data from Apache Kafka, processes it through a three-layer medallion architecture on Databricks, scores each reading with an Isolation Forest model tracked in MLflow, and delivers alerts within 30 minutes of detection.

---

## The Problem

Pipeline leaks in oil and gas infrastructure are dangerous and expensive. Traditional monitoring relies on manual inspection or simple threshold rules that miss subtle early-stage anomalies. The goal was to build a system that:

- Ingests sensor data in real time (pressure, flow rate, acoustic vibration, temperature)
- Detects anomalies that don't trigger simple thresholds but represent genuine risk patterns
- Automatically notifies operators with enough detail to act immediately

---

## What Makes This Project Stand Out

### End-to-end ownership
Every layer was built from scratch — Kafka producer, Bronze ingestion, Silver feature engineering, ML training, Gold scoring, alert engine, SQL dashboard, and Streamlit app. No black boxes.

### Real streaming architecture
Not just batch processing dressed up as streaming. The pipeline uses Spark Structured Streaming to read from Kafka in real time, with `foreachBatch` for window function support on Databricks Serverless clusters.

### Production MLflow integration
The Isolation Forest model is trained, tracked, and registered in the MLflow Model Registry. The scoring pipeline loads the registered model by name and version — the same pattern used in production ML systems.

### Automated orchestration
A Databricks Job with 5 dependent tasks runs every 30 minutes. All runs have succeeded. Average runtime is under 2 minutes end-to-end.

### Real alert delivery
The alert engine sends HTML-formatted email alerts with specific sensor trigger details, segment rankings by severity, and immediate action steps — the kind of alert an on-call engineer can actually act on.

---

## Technical Highlights

| Area | Detail |
|---|---|
| Streaming | Kafka → Bronze via Spark Structured Streaming |
| Feature engineering | Window functions: lag, rolling avg, z-score per segment |
| ML model | Isolation Forest, unsupervised, 100% precision on test set |
| ML tracking | MLflow experiment tracking + Model Registry |
| Data format | Delta Lake with schema evolution |
| Orchestration | Databricks Jobs, 5-task DAG, 30-min schedule |
| Monitoring | SQL dashboard + Streamlit app (Databricks Apps) |
| Alerting | Automated HTML email with ranked segment details |

---

## Challenges Solved

**Timestamp schema conflict** — Kafka delivers all fields as strings. Delta Lake's schema enforcement threw `DELTA_FAILED_TO_MERGE_FIELDS` when writing `event_timestamp` as a string to a table expecting `TimestampType`. Fixed by casting with `to_timestamp()` before the write.

**Window functions in streaming** — `lag()`, `avg()`, and `stddev()` don't work directly on streaming DataFrames because Spark doesn't have the full partition in memory. Used `foreachBatch` to convert each micro-batch to a static DataFrame, apply window logic, then write to Silver.

**Serverless trigger limitation** — Databricks Serverless clusters don't support continuous `processingTime` streaming triggers. Switched to `availableNow` trigger which processes all available messages and stops cleanly — works perfectly with the scheduled job pattern.

**ML threshold calibration** — The model's decision function scores ranged from -0.117 to +0.026 (not the standard -0.5 to 0 range). Inspected the actual score distribution and calibrated severity thresholds to the real data: CRITICAL ≤ -0.07, WARNING between -0.07 and 0.

---

## Results

- 4,845 scored alerts across 5 pipeline segments
- 67 critical events identified (PIPE-001 worst at 45 critical events)
- Automated pipeline running every 30 minutes with 100% success rate
- Real email alerts delivered with full sensor context and action steps

---

## Skills Demonstrated

- Apache Kafka (producer, consumer, Confluent Cloud)
- Apache Spark (PySpark, Structured Streaming, foreachBatch, window functions)
- Delta Lake (medallion architecture, schema evolution, ACID transactions)
- Databricks (Serverless, Jobs, Apps, SQL Dashboard, Unity Catalog)
- MLflow (experiment tracking, model registry, pyfunc inference)
- Scikit-learn (Isolation Forest, StandardScaler, model serialisation)
- Python (pandas, requests, SMTP, data engineering patterns)
- Real-time data engineering (streaming pipelines, micro-batch processing)

---

## Links

- GitHub: [Real-Time-Pipeline-Risk-Monitoring-with-Databricks](https://github.com/your-username/Real-Time-Pipeline-Risk-Monitoring-with-Databricks)
- Live Dashboard: Databricks SQL Dashboard (available on request)
