# Databricks notebook source
# Cell 1: Read from Bronze Unity Catalog table
from pyspark.sql.functions import (
    col, when, avg, stddev, lag, round as spark_round
)
from pyspark.sql.window import Window

CATALOG = 'main'
SCHEMA  = 'pipeline_leak'
BRONZE_TABLE = f'{CATALOG}.{SCHEMA}.bronze_sensor_raw'
SILVER_TABLE = f'{CATALOG}.{SCHEMA}.silver_sensor_clean'

df_bronze = spark.table(BRONZE_TABLE)
print(f'Bronze records loaded: {df_bronze.count()}')
df_bronze.printSchema()

# COMMAND ----------

# Cell 2: Data quality — remove nulls and clip to physical sensor bounds
# Any reading outside these ranges is instrument noise, not real pipeline data

PRESSURE_MIN, PRESSURE_MAX  = 100.0, 3000.0   # PSI
FLOW_MIN,     FLOW_MAX      = 0.0,   600.0    # L/min
ACOUSTIC_MIN, ACOUSTIC_MAX  = 0.0,   10.0     # g RMS
TEMP_MIN,     TEMP_MAX      = -20.0, 200.0    # Celsius

df_silver = (
    df_bronze
    .dropna(subset=['pressure_psi', 'flow_rate_lpm', 'acoustic_g_rms', 'temperature_c'])
    .withColumn('pressure_psi',
        when(col('pressure_psi').between(PRESSURE_MIN, PRESSURE_MAX),   col('pressure_psi')))
    .withColumn('flow_rate_lpm',
        when(col('flow_rate_lpm').between(FLOW_MIN, FLOW_MAX),          col('flow_rate_lpm')))
    .withColumn('acoustic_g_rms',
        when(col('acoustic_g_rms').between(ACOUSTIC_MIN, ACOUSTIC_MAX), col('acoustic_g_rms')))
    .withColumn('temperature_c',
        when(col('temperature_c').between(TEMP_MIN, TEMP_MAX),          col('temperature_c')))
    .dropna()
)

before = df_bronze.count()
after  = df_silver.count()
print(f'Before quality filter : {before} records')
print(f'After quality filter  : {after} records')
print(f'Dropped               : {before - after} rows (out-of-range sensor readings)')

# COMMAND ----------

# Cell 3: Rolling window feature engineering
# These 3 features are what the ML model actually trains on

# 10-row rolling window per segment ordered by time
segment_window = (
    Window
    .partitionBy('pipe_segment_id')
    .orderBy('event_timestamp')
    .rowsBetween(-9, 0)
)
lag_window = Window.partitionBy('pipe_segment_id').orderBy('event_timestamp')

df_features = (
    df_silver
    # Rolling averages
    .withColumn('pressure_rolling_avg',
        spark_round(avg('pressure_psi').over(segment_window), 2))
    .withColumn('flow_rolling_avg',
        spark_round(avg('flow_rate_lpm').over(segment_window), 2))
    .withColumn('acoustic_rolling_avg',
        spark_round(avg('acoustic_g_rms').over(segment_window), 4))
    # Pressure delta — drop > 150 PSI between readings = leak signal
    .withColumn('pressure_delta',
        spark_round(col('pressure_psi') - lag('pressure_psi', 1).over(lag_window), 2))
    # Flow imbalance — spike relative to rolling average
    .withColumn('flow_imbalance_ratio',
        spark_round(col('flow_rate_lpm') / col('flow_rolling_avg'), 4))
    # Acoustic z-score — standard deviations above normal
    .withColumn('acoustic_rolling_std',
        spark_round(stddev('acoustic_g_rms').over(segment_window), 4))
    .withColumn('acoustic_zscore',
        spark_round(
            (col('acoustic_g_rms') - col('acoustic_rolling_avg'))
            / (col('acoustic_rolling_std') + 0.0001),
        4))
    .dropna()
)

print(f'Records with features: {df_features.count()}')
print()
df_features.select(
    'pipe_segment_id', 'pressure_psi', 'pressure_delta',
    'flow_imbalance_ratio', 'acoustic_zscore', 'is_anomaly'
).show(10)

# COMMAND ----------

# Cell 4: Write to Silver managed Delta table
(
    df_features
    .write
    .format('delta')
    .mode('overwrite')
    .option('overwriteSchema', 'true')
    .partitionBy('pipe_segment_id')
    .saveAsTable(SILVER_TABLE)
)

print('=== SILVER TABLE SUMMARY ===')
print()
spark.sql(f"""
    SELECT
        pipe_segment_id,
        COUNT(*)                              AS records,
        SUM(CASE WHEN is_anomaly = 'leak'
            THEN 1 ELSE 0 END)               AS leak_count,
        ROUND(AVG(pressure_delta), 2)         AS avg_pressure_delta,
        ROUND(AVG(flow_imbalance_ratio), 3)   AS avg_flow_imbalance,
        ROUND(AVG(acoustic_zscore), 3)        AS avg_acoustic_zscore
    FROM {SILVER_TABLE}
    GROUP BY pipe_segment_id
    ORDER BY pipe_segment_id
""").show()

print('Silver layer complete.')
print('Next: ML model training with MLflow.')

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