# Databricks notebook source
# Cell 1: Load Silver table and production models
import mlflow.sklearn
import pandas as pd
import numpy as np

CATALOG = 'main'
SCHEMA  = 'pipeline_leak'
SILVER_TABLE = f'{CATALOG}.{SCHEMA}.silver_sensor_clean'
GOLD_TABLE   = f'{CATALOG}.{SCHEMA}.gold_alerts'

DETECTOR_MODEL = 'workspace.default.pipeline_leak_detector'
SCALER_MODEL   = 'workspace.default.pipeline_scaler'

FEATURE_COLS = [
    'pressure_psi', 'flow_rate_lpm', 'acoustic_g_rms', 'temperature_c',
    'pressure_delta', 'flow_imbalance_ratio', 'acoustic_zscore'
]

# Load production models by alias
model  = mlflow.sklearn.load_model(f'models:/{DETECTOR_MODEL}@production')
scaler = mlflow.sklearn.load_model(f'models:/{SCALER_MODEL}@production')

df_silver = spark.table(SILVER_TABLE)
print(f'Silver records : {df_silver.count()}')
print(f'Detector       : {type(model).__name__}')
print(f'Scaler         : {type(scaler).__name__}')

# COMMAND ----------

# Cell 2: Score all records and assign severity levels
pdf = df_silver.toPandas()
X   = scaler.transform(pdf[FEATURE_COLS].values)

# Isolation Forest: -1 = anomaly, 1 = normal
pdf['anomaly_prediction'] = model.predict(X)
pdf['anomaly_score']      = model.decision_function(X)  # More negative = more anomalous

def assign_severity(row):
    if row['anomaly_prediction'] == 1:
        return 'NORMAL'
    return 'CRITICAL' if row['anomaly_score'] < -0.15 else 'WARNING'

def build_alert_message(row):
    if row['severity'] == 'NORMAL':
        return ''
    triggers = []
    if pd.notna(row.get('pressure_delta')) and row['pressure_delta'] < -100:
        triggers.append(f"Pressure drop {row['pressure_delta']:.0f} PSI")
    if pd.notna(row.get('flow_imbalance_ratio')) and row['flow_imbalance_ratio'] > 1.25:
        triggers.append(f"Flow spike x{row['flow_imbalance_ratio']:.2f}")
    if pd.notna(row.get('acoustic_zscore')) and abs(row['acoustic_zscore']) > 2.0:
        triggers.append(f"Acoustic anomaly z={row['acoustic_zscore']:.2f}")
    return '; '.join(triggers) if triggers else 'Multi-sensor anomaly'

pdf['severity']      = pdf.apply(assign_severity, axis=1)
pdf['alert_message'] = pdf.apply(build_alert_message, axis=1)

print('=== SEVERITY DISTRIBUTION ===')
print(pdf['severity'].value_counts().to_string())
print()
print('=== SAMPLE CRITICAL ALERTS ===')
critical = pdf[pdf['severity'] == 'CRITICAL'][
    ['pipe_segment_id', 'pressure_psi', 'pressure_delta',
     'acoustic_zscore', 'anomaly_score', 'alert_message']
].head(5)
print(critical.to_string(index=False))

# COMMAND ----------

# Cell 2b: Inspect raw anomaly scores to calibrate threshold
pdf = df_silver.toPandas()
X   = scaler.transform(pdf[FEATURE_COLS].values)

pdf['anomaly_prediction'] = model.predict(X)
pdf['anomaly_score']      = model.decision_function(X)

# Show all anomalies and their scores
anomalies = pdf[pdf['anomaly_prediction'] == -1][['pipe_segment_id', 'pressure_psi', 'anomaly_score', 'is_anomaly']]

print(f'Total anomalies detected: {len(anomalies)}')
print(f'Score min : {anomalies["anomaly_score"].min():.6f}')
print(f'Score max : {anomalies["anomaly_score"].max():.6f}')
print(f'Score mean: {anomalies["anomaly_score"].mean():.6f}')
print()
print('All anomaly scores:')
print(anomalies.sort_values('anomaly_score').to_string(index=False))

# COMMAND ----------

# Cell 3: Score with correct threshold based on actual score range
# Scores range from -0.117 to -0.002
# Bottom half (more severe) = CRITICAL, top half = WARNING
# Using -0.07 as threshold (roughly the median anomaly score)

def assign_severity(row):
    if row['anomaly_prediction'] == 1:
        return 'NORMAL'
    return 'CRITICAL' if row['anomaly_score'] <= -0.07 else 'WARNING'

def build_alert_message(row):
    if row['severity'] == 'NORMAL':
        return ''
    triggers = []
    if pd.notna(row.get('pressure_delta')) and row['pressure_delta'] < -100:
        triggers.append(f"Pressure drop {row['pressure_delta']:.0f} PSI")
    if pd.notna(row.get('flow_imbalance_ratio')) and row['flow_imbalance_ratio'] > 1.25:
        triggers.append(f"Flow spike x{row['flow_imbalance_ratio']:.2f}")
    if pd.notna(row.get('acoustic_zscore')) and abs(row['acoustic_zscore']) > 2.0:
        triggers.append(f"Acoustic anomaly z={row['acoustic_zscore']:.2f}")
    return '; '.join(triggers) if triggers else 'Multi-sensor anomaly'

pdf['severity']      = pdf.apply(assign_severity, axis=1)
pdf['alert_message'] = pdf.apply(build_alert_message, axis=1)

print('=== SEVERITY DISTRIBUTION ===')
print(pdf['severity'].value_counts().to_string())
print()
print('=== SAMPLE CRITICAL ALERTS ===')
pdf[pdf['severity'] == 'CRITICAL'][
    ['pipe_segment_id', 'pressure_psi', 'pressure_delta',
     'anomaly_score', 'severity', 'alert_message']
].head(8).to_string(index=False)

# COMMAND ----------

# Cell 4: Write scored results to Gold Delta table
gold_cols = [
    'event_id', 'pipe_segment_id', 'event_timestamp',
    'pressure_psi', 'flow_rate_lpm', 'acoustic_g_rms', 'temperature_c',
    'pressure_delta', 'flow_imbalance_ratio', 'acoustic_zscore',
    'anomaly_score', 'severity', 'alert_message', 'is_anomaly'
]

df_gold = spark.createDataFrame(pdf[gold_cols])

(
    df_gold
    .write
    .format('delta')
    .mode('overwrite')
    .option('overwriteSchema', 'true')
    .partitionBy('pipe_segment_id', 'severity')
    .saveAsTable(GOLD_TABLE)
)

print('=== GOLD TABLE WRITTEN ===')
print()
spark.sql(f"""
    SELECT
        severity,
        pipe_segment_id,
        COUNT(*)                        AS events,
        ROUND(AVG(pressure_psi), 1)     AS avg_pressure,
        ROUND(MIN(anomaly_score), 4)    AS worst_score
    FROM {GOLD_TABLE}
    GROUP BY severity, pipe_segment_id
    ORDER BY severity, pipe_segment_id
""").show(30)

print('Gold layer complete.')
print('All three medallion layers are live:')
print(f'  Bronze : main.pipeline_leak.bronze_sensor_raw')
print(f'  Silver : main.pipeline_leak.silver_sensor_clean')
print(f'  Gold   : main.pipeline_leak.gold_alerts')

# COMMAND ----------

# MAGIC %md
# MAGIC Operational picture across all 5 pipeline segments:
# MAGIC
# MAGIC - CRITICAL events all have average pressures around 610–713 PSI (well below normal 1000 PSI) with worst scores down to -0.117
# MAGIC - NORMAL events sitting at 991–1006 PSI with positive scores
# MAGIC - WARNING events in between at 672–723 PSI

# COMMAND ----------

# Run in a new cell
spark.sql("DROP TABLE IF EXISTS main.pipeline_leak.gold_alerts_dual")
print("Dropped gold_alerts_dual")

# Check actual Gold schema
spark.sql("DESCRIBE main.pipeline_leak.gold_alerts").show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# Read existing Gold, add gold_ts, overwrite
df_gold = spark.table("main.pipeline_leak.gold_alerts")
df_gold = df_gold.withColumn("gold_ts", current_timestamp())

(
    df_gold
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("main.pipeline_leak.gold_alerts")
)

print("✅ gold_ts column added successfully")
print(f"Total records: {df_gold.count():,}")

# COMMAND ----------

CATALOG = "main"
SCHEMA  = "pipeline_leak"

print("=== MEDALLION LAYER AUDIT ===\n")

for table, ts_col in [
    ("bronze_sensor_raw",   "ingestion_ts"),
    ("silver_sensor_clean", "silver_ts"),
    ("gold_alerts",         "gold_ts"),
]:
    try:
        count  = spark.sql(f"SELECT COUNT(*) AS cnt FROM {CATALOG}.{SCHEMA}.{table}").collect()[0]["cnt"]
        latest = spark.sql(f"SELECT MAX({ts_col}) AS ts FROM {CATALOG}.{SCHEMA}.{table}").collect()[0]["ts"]
        print(f"✅ {table:30s} : {count:,} records | latest: {latest}")
    except Exception as e:
        print(f"❌ {table:30s} : ERROR -- {e}")

print("\n=== ALL TABLES ===")
spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA}").show()