# Databricks notebook source
# Cell 1: Imports and pipeline configuration
import random
import uuid
from datetime import datetime, timezone
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)

PIPE_SEGMENTS = ['PIPE-001', 'PIPE-002', 'PIPE-003', 'PIPE-004', 'PIPE-005']

NORMAL = {
    'pressure_psi':   (1000, 50),
    'flow_rate_lpm':  (200,  20),
    'acoustic_g_rms': (0.3,  0.08),
    'temperature_c':  (45,   4),
}

LEAK = {
    'pressure_psi':   (680,  40),
    'flow_rate_lpm':  (280,  30),
    'acoustic_g_rms': (1.8,  0.3),
    'temperature_c':  (31,   3),
}

# Unity Catalog: catalog.schema.table format
CATALOG = 'main'
SCHEMA  = 'pipeline_leak'
BRONZE_TABLE = f'{CATALOG}.{SCHEMA}.bronze_sensor_raw'

print('Config loaded. Segments:', PIPE_SEGMENTS)
print(f'Bronze table: {BRONZE_TABLE}')

# COMMAND ----------

# Cell 2: Define the Bronze Delta table schema
bronze_schema = StructType([
    StructField('event_id',        StringType(),    False),
    StructField('pipe_segment_id', StringType(),    False),
    StructField('event_timestamp', TimestampType(), False),
    StructField('pressure_psi',    DoubleType(),    True),
    StructField('flow_rate_lpm',   DoubleType(),    True),
    StructField('acoustic_g_rms',  DoubleType(),    True),
    StructField('temperature_c',   DoubleType(),    True),
    StructField('is_anomaly',      StringType(),    False),
    StructField('ingestion_ts',    TimestampType(), False),
])

print('Bronze schema defined:')
for f in bronze_schema.fields:
    print(f'  {f.name:20s} {str(f.dataType):20s} nullable={f.nullable}')

# COMMAND ----------

# Cell 3: Sensor reading generator function
def generate_reading(pipe_segment_id: str, leak_probability: float = 0.08) -> dict:
    is_leak = random.random() < leak_probability
    ranges  = LEAK if is_leak else NORMAL

    return {
        'event_id':        str(uuid.uuid4()),
        'pipe_segment_id': pipe_segment_id,
        'event_timestamp': datetime.now(timezone.utc),
        'pressure_psi':    round(random.gauss(*ranges['pressure_psi']),    2),
        'flow_rate_lpm':   round(random.gauss(*ranges['flow_rate_lpm']),   2),
        'acoustic_g_rms':  round(abs(random.gauss(*ranges['acoustic_g_rms'])), 4),
        'temperature_c':   round(random.gauss(*ranges['temperature_c']),   2),
        'is_anomaly':      'leak' if is_leak else 'normal',
        'ingestion_ts':    datetime.now(timezone.utc),
    }

# Force a leak reading so you can see exactly what one looks like
sample_leak   = generate_reading('PIPE-001', leak_probability=1.0)
sample_normal = generate_reading('PIPE-001', leak_probability=0.0)

print('--- LEAK READING ---')
for k, v in sample_leak.items():
    print(f'  {k:20s} {v}')

print()
print('--- NORMAL READING ---')
for k, v in sample_normal.items():
    print(f'  {k:20s} {v}')

# COMMAND ----------

# Cell 4: Generate 500 records and write to Bronze Delta table
# Using Unity Catalog managed tables — for Databricks Free Edition

def generate_batch(records_per_segment: int = 100) -> list:
    records = []
    for pipe_id in PIPE_SEGMENTS:
        for _ in range(records_per_segment):
            records.append(generate_reading(pipe_id))
    return records

# Create the schema (database) if it doesn't exist
spark.sql(f'CREATE CATALOG IF NOT EXISTS {CATALOG}')
spark.sql(f'CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}')

print('Generating 500 records (100 per segment)...')
batch     = generate_batch(100)
df_bronze = spark.createDataFrame(batch, schema=bronze_schema)

# Write as managed Delta table in Unity Catalog
(
    df_bronze
    .write
    .format('delta')
    .mode('append')
    .partitionBy('pipe_segment_id')
    .saveAsTable(BRONZE_TABLE)
)

count = spark.sql(f'SELECT COUNT(*) AS cnt FROM {BRONZE_TABLE}').collect()[0]['cnt']
print(f'Bronze table: {count} records written')
print(f'Table: {BRONZE_TABLE}')

# COMMAND ----------

# Cell 5: Verify the Bronze table with a summary query
print('=== BRONZE TABLE SUMMARY ===')
print()

spark.sql(f"""
    SELECT 
        pipe_segment_id,
        COUNT(*)                                                 AS total_readings,
        SUM(CASE WHEN is_anomaly = 'leak' THEN 1 ELSE 0 END)   AS leak_count,
        SUM(CASE WHEN is_anomaly = 'normal' THEN 1 ELSE 0 END) AS normal_count,
        ROUND(AVG(pressure_psi), 1)                            AS avg_pressure,
        ROUND(MIN(pressure_psi), 1)                            AS min_pressure,
        ROUND(AVG(acoustic_g_rms), 4)                          AS avg_acoustic
    FROM {BRONZE_TABLE}
    GROUP BY pipe_segment_id
    ORDER BY pipe_segment_id
""").show()

print('Bronze layer complete.')
print('Next: Silver layer — cleaning and feature engineering.')