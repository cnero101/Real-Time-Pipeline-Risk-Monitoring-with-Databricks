# Databricks notebook source
# Cell 1: Install Kafka client Library
%pip install confluent-kafka --quiet

# COMMAND ----------

# Cell 2: Restart Kernel to activate confluent-kafka library
dbutils.library.restartPython()

# COMMAND ----------

# Cell 3: Verify confluent-kafka and configure connection
from confluent_kafka import Producer, Consumer, KafkaError
import json
import uuid
import random
from datetime import datetime, timezone

# ── Confluent Cloud connection config ─────────────────────────
# Replace with your actual values from Confluent Cloud
BOOTSTRAP_SERVERS = "your-kafka-bootstrap-server"
API_KEY           = "your-kafka-api-key"      # Type the API Key here
API_SECRET        = "your-kafka-api-secret"   # Paste from downloaded file
KAFKA_TOPIC       = "pipeline-sensor-data"

PRODUCER_CONFIG = {
    'bootstrap.servers':                     BOOTSTRAP_SERVERS,
    'security.protocol':                     'SASL_SSL',
    'sasl.mechanisms':                       'PLAIN',
    'sasl.username':                         API_KEY,
    'sasl.password':                         API_SECRET,
    'client.id':                             'databricks-pipeline-producer',
}

print('confluent-kafka imported successfully')
print(f'Bootstrap server : {BOOTSTRAP_SERVERS}')
print(f'API Key          : {API_KEY}')
print(f'Topic            : {KAFKA_TOPIC}')
print()
print('Ready to connect to Confluent Cloud')

# COMMAND ----------

# Cell 4: Kafka producer — sends sensor readings to Confluent Cloud
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

def generate_reading(pipe_id, leak_probability=0.08):
    is_leak = random.random() < leak_probability
    ranges  = LEAK if is_leak else NORMAL
    return {
        'event_id':        str(uuid.uuid4()),
        'pipe_segment_id': pipe_id,
        'event_timestamp': datetime.now(timezone.utc).isoformat(),
        'pressure_psi':    round(random.gauss(*ranges['pressure_psi']),    2),
        'flow_rate_lpm':   round(random.gauss(*ranges['flow_rate_lpm']),   2),
        'acoustic_g_rms':  round(abs(random.gauss(*ranges['acoustic_g_rms'])), 4),
        'temperature_c':   round(random.gauss(*ranges['temperature_c']),   2),
        'is_anomaly':      'leak' if is_leak else 'normal',
    }

def delivery_report(err, msg):
    if err:
        print(f'Delivery failed: {err}')

# Create producer
producer = Producer(PRODUCER_CONFIG)

# Send 50 readings — 10 per segment
print('Sending sensor readings to Confluent Cloud Kafka...')
print(f'Topic: {KAFKA_TOPIC}')
print()

sent_count = 0
for pipe_id in PIPE_SEGMENTS:
    for _ in range(10):
        reading = generate_reading(pipe_id)
        producer.produce(
            topic     = KAFKA_TOPIC,
            key       = pipe_id,
            value     = json.dumps(reading),
            callback  = delivery_report
        )
        producer.poll(0)
        sent_count += 1

# Wait for all messages to be delivered
producer.flush()
print(f'Successfully sent {sent_count} readings to Kafka')
print(f'Topic: {KAFKA_TOPIC}')
print()
print('Go to Confluent Cloud → Topics → pipeline_sensors → Messages')
print('You should see 50 messages arriving in real time')

# COMMAND ----------

# Cell 5: Kafka → Bronze (Batch Read with Timestamp Fix)
# -----------------------------------------------------------
# Reads all available messages from Kafka using a batch read,
# parses the JSON payload, casts event_timestamp to TimestampType
# (fixes DELTA_FAILED_TO_MERGE_FIELDS), and appends to Bronze.
# -----------------------------------------------------------

from pyspark.sql.functions import col, from_json, current_timestamp, to_timestamp
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType
)

# ── Config ──────────────────────────────────────────────────
CATALOG      = "main"
SCHEMA       = "pipeline_leak"
BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.bronze_sensor_raw"

# ── Schema ──────────────────────────────────────────────────
# event_timestamp is StringType here intentionally --
# Kafka delivers everything as bytes/string. We cast it below.
kafka_schema = StructType([
    StructField("event_id",        StringType(), True),
    StructField("pipe_segment_id", StringType(), True),
    StructField("event_timestamp", StringType(), True),   # cast happens below
    StructField("pressure_psi",    DoubleType(), True),
    StructField("flow_rate_lpm",   DoubleType(), True),
    StructField("acoustic_g_rms",  DoubleType(), True),
    StructField("temperature_c",   DoubleType(), True),
    StructField("is_anomaly",      StringType(), True),
])

# ── Read from Kafka (batch) ─────────────────────────────────
print("Reading messages from Kafka topic...")

kafka_batch = (
    spark.read
    .format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism",    "PLAIN")
    .option(
        "kafka.sasl.jaas.config",
        f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule '
        f'required username="{API_KEY}" password="{API_SECRET}";'
    )
    .option("subscribe",       KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .option("endingOffsets",   "latest")
    .load()
)

# ── Parse JSON + fix timestamp ──────────────────────────────
# THE FIX: to_timestamp() converts the string (e.g. "2025-01-15T10:23:44")
# into a proper TimestampType so it matches the Bronze table schema.
# Without this, Delta throws DELTA_FAILED_TO_MERGE_FIELDS on append.
df_kafka = (
    kafka_batch
    .select(from_json(col("value").cast("string"), kafka_schema).alias("data"))
    .select("data.*")
    .withColumn(
        "event_timestamp",
        to_timestamp(col("event_timestamp"))         # <-- the actual fix
    )
    .withColumn("ingestion_ts", current_timestamp()) # when we ingested it
    .dropna(subset=["event_id", "pipe_segment_id"])  # drop malformed rows
)

kafka_count = df_kafka.count()
print(f"Messages read from Kafka : {kafka_count}")
print()
print("Schema after parsing:")
df_kafka.printSchema()

# ── Append to Bronze ────────────────────────────────────────
(
    df_kafka
    .write
    .format("delta")
    .mode("append")
    .saveAsTable(BRONZE_TABLE)
)

# ── Verify ──────────────────────────────────────────────────
total = spark.sql(
    f"SELECT COUNT(*) AS cnt FROM {BRONZE_TABLE}"
).collect()[0]["cnt"]

print(f"\nKafka messages written   : {kafka_count}")
print(f"Bronze total records     : {total}")
print()
print("✅ Kafka → Bronze complete")
print("   Equivalent to: Kinesis→S3 (AWS) | Event Hubs→ADLS (Azure)")

# COMMAND ----------

# Create the volume if it doesn't exist
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.checkpoints")

# COMMAND ----------

# Cell 6: Kafka → Bronze (Continuous Structured Streaming)
# -----------------------------------------------------------
# Unlike Cell 5 (batch read), this runs a continuous stream.
# New Kafka messages are picked up automatically and written
# to Bronze every 30 seconds via trigger(processingTime).
# -----------------------------------------------------------

from pyspark.sql.functions import col, from_json, current_timestamp, to_timestamp
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType
)

# ── Config ──────────────────────────────────────────────────
CATALOG      = "main"
SCHEMA       = "pipeline_leak"
BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.bronze_sensor_raw"
CHECKPOINT_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints/kafka_to_bronze"

# ── Same schema as Cell 5 ───────────────────────────────────
kafka_schema = StructType([
    StructField("event_id",        StringType(), True),
    StructField("pipe_segment_id", StringType(), True),
    StructField("event_timestamp", StringType(), True),
    StructField("pressure_psi",    DoubleType(), True),
    StructField("flow_rate_lpm",   DoubleType(), True),
    StructField("acoustic_g_rms",  DoubleType(), True),
    StructField("temperature_c",   DoubleType(), True),
    StructField("is_anomaly",      StringType(), True),
])

# ── Streaming read from Kafka ───────────────────────────────
# Key difference vs Cell 5: spark.readStream instead of spark.read
# startingOffsets = "latest" so we only pick up NEW messages,
# not replay everything already written to Bronze in Cell 5.
print("Starting structured stream from Kafka...")

df_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism",    "PLAIN")
    .option(
        "kafka.sasl.jaas.config",
        f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule '
        f'required username="{API_KEY}" password="{API_SECRET}";'
    )
    .option("subscribe",       KAFKA_TOPIC)
    .option("startingOffsets", "latest")   # only new messages from here on
    .option("maxOffsetsPerTrigger", 1000)  # cap per micro-batch, prevents overload
    .load()
)

# ── Parse + cast (same fix as Cell 5) ──────────────────────
df_parsed = (
    df_stream
    .select(from_json(col("value").cast("string"), kafka_schema).alias("data"))
    .select("data.*")
    .withColumn("event_timestamp", to_timestamp(col("event_timestamp")))
    .withColumn("ingestion_ts",    current_timestamp())
    .dropna(subset=["event_id", "pipe_segment_id"])
)

# ── Write stream to Bronze ──────────────────────────────────
# checkpointLocation is mandatory for streaming writes to Delta.
# It tracks exactly which Kafka offsets have been committed
# so if the stream restarts, it picks up where it left off.
query = (
    df_parsed
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(availableNow=True)
    .toTable(BRONZE_TABLE)
)

print(f"Stream running. Writing to: {BRONZE_TABLE}")
print(f"Checkpoint at            : {CHECKPOINT_PATH}")
print(f"Trigger interval         : 30 seconds")
print()
print("Stream status:")
print(query.status)

# COMMAND ----------

# Cell 7: Verify streaming write to Bronze
total = spark.sql(f"SELECT COUNT(*) AS cnt FROM {BRONZE_TABLE}").collect()[0]["cnt"]
print(f"Bronze total records: {total}")

# Also check the most recent records came through
spark.sql(f"""
    SELECT pipe_segment_id, event_timestamp, ingestion_ts, pressure_psi
    FROM {BRONZE_TABLE}
    ORDER BY ingestion_ts DESC
    LIMIT 10
""").display()

# COMMAND ----------

# Cell 8: Bronze → Silver (Streaming Transformation)
# -----------------------------------------------------------
# Reads Bronze Delta table as a stream, applies the same
# feature engineering as 02_silver_transformation but in
# real-time. New Bronze rows are picked up automatically
# and written to Silver via availableNow trigger.
# -----------------------------------------------------------

from pyspark.sql.functions import (
    col, lag, abs, avg, stddev, when, lit,
    to_timestamp, current_timestamp
)
from pyspark.sql.window import Window

# ── Config ──────────────────────────────────────────────────
CATALOG       = "main"
SCHEMA        = "pipeline_leak"
BRONZE_TABLE  = f"{CATALOG}.{SCHEMA}.bronze_sensor_raw"
SILVER_TABLE  = f"{CATALOG}.{SCHEMA}.silver_sensor_clean"
CHECKPOINT_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints/bronze_to_silver"

print("Starting Bronze → Silver stream...")

# ── Read Bronze as a stream ─────────────────────────────────
# Delta tables support readStream natively -- Spark tracks
# which rows are new since the last micro-batch automatically.
df_bronze_stream = (
    spark.readStream
    .format("delta")
    .option("ignoreChanges", True)   # handles deletes/updates in Bronze gracefully
    .table(BRONZE_TABLE)
)

# ── Feature engineering ─────────────────────────────────────
# Same logic as 02_silver_transformation -- just running on
# the stream instead of a static DataFrame.

# Window per pipe segment, ordered by event time
window_spec = Window.partitionBy("pipe_segment_id").orderBy("event_timestamp")

def transform_to_silver(df):
    return (
        df
        # Pressure delta: drop vs previous reading
        .withColumn(
            "pressure_delta",
            col("pressure_psi") - lag("pressure_psi", 1).over(window_spec)
        )
        # Flow imbalance ratio: how far off from baseline (1000 lpm)
        .withColumn(
            "flow_imbalance",
            (col("flow_rate_lpm") - lit(1000.0)) / lit(1000.0)
        )
        # Acoustic z-score: how many std devs from the segment mean
        .withColumn(
            "acoustic_mean",
            avg("acoustic_g_rms").over(Window.partitionBy("pipe_segment_id"))
        )
        .withColumn(
            "acoustic_std",
            stddev("acoustic_g_rms").over(Window.partitionBy("pipe_segment_id"))
        )
        .withColumn(
            "acoustic_zscore",
            when(
                col("acoustic_std") > 0,
                (col("acoustic_g_rms") - col("acoustic_mean")) / col("acoustic_std")
            ).otherwise(lit(0.0))
        )
        # Drop intermediate columns
        .drop("acoustic_mean", "acoustic_std")
        # Add Silver processing timestamp
        .withColumn("silver_ts", current_timestamp())
        # Drop rows where feature engineering couldn't run (first row per segment)
        .dropna(subset=["pressure_delta"])
    )

# ── foreachBatch: apply transformations per micro-batch ─────
# Window functions don't work directly on streams, so we use
# foreachBatch to convert each micro-batch to a static DF,
# apply the window logic, then write to Silver.
def process_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        print(f"Batch {batch_id}: empty, skipping.")
        return

    silver_df = transform_to_silver(batch_df)
    row_count = silver_df.count()

    (
        silver_df
        .write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(SILVER_TABLE)
    )

    print(f"Batch {batch_id}: wrote {row_count} rows to Silver.")

# ── Write stream ─────────────────────────────────────────────
query = (
    df_bronze_stream
    .writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(availableNow=True)
    .start()
)

query.awaitTermination()

print()
print("Stream stopped cleanly.")

# ── Verify ───────────────────────────────────────────────────
total_silver = spark.sql(
    f"SELECT COUNT(*) AS cnt FROM {SILVER_TABLE}"
).collect()[0]["cnt"]

print(f"Silver total records: {total_silver}")
print()
print("✅ Bronze → Silver streaming complete")

# COMMAND ----------

# Cell 9: Verify Gold table (scored by 03_gold_scoring)
# -----------------------------------------------------------
# Gold layer already populated by 03_gold_scoring notebook.
# This cell confirms the streaming pipeline feeds into the
# same medallion architecture.
# -----------------------------------------------------------

CATALOG    = "main"
SCHEMA     = "pipeline_leak"
GOLD_TABLE = f"{CATALOG}.{SCHEMA}.gold_alerts"

print("=== GOLD TABLE SUMMARY ===\n")

spark.sql(f"""
    SELECT
        severity,
        COUNT(*)                  AS events,
        ROUND(AVG(pressure_psi), 1) AS avg_pressure,
        ROUND(MIN(anomaly_score), 4) AS worst_score
    FROM {GOLD_TABLE}
    GROUP BY severity
    ORDER BY severity
""").show()

total = spark.sql(
    f"SELECT COUNT(*) AS cnt FROM {GOLD_TABLE}"
).collect()[0]["cnt"]

print(f"Gold total records : {total}")
print()
print("✅ 04_structured_streaming complete")
print()
print("Full medallion architecture live:")
print(f"  Bronze : {CATALOG}.{SCHEMA}.bronze_sensor_raw")
print(f"  Silver : {CATALOG}.{SCHEMA}.silver_sensor_clean")
print(f"  Gold   : {CATALOG}.{SCHEMA}.gold_alerts")
