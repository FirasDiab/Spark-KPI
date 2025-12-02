# %% Imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, date_trunc, to_date, count, min as fmin, max as fmax, first
)
from pyspark.sql.types import *
from logging.handlers import RotatingFileHandler
import logging
import os

# %% Environment variables
MONGODB_URL = os.getenv("MONGO_URL") or "mongodb://mongodb:27017/"
DB = os.getenv("DB") or "TestDB"
INPUT_FOLDER = os.getenv("INPUT_FOLDER") or "/sparkdata/input/"
FILES_PER_TRIGGER = int(os.getenv("FILES_PER_TRIGGER") or 1000)
SPARK_EXECUTOR_MEMORY = os.getenv("SPARK_EXECUTOR_MEMORY") or "1g"
SPARK_MAX_CORES = os.getenv("SPARK_MAX_CORES") or "2"
SPARK_EXECUTOR_CORES = os.getenv("SPARK_EXECUTOR_CORES") or "1"
SPARK_PARTITIONS = os.getenv("SPARK_PARTITIONS") or "10"

HOURLY_MOC_DELTA_PATH = os.getenv("HOURLY_MOC_DELTA_PATH") or "/sparkdata/delta/hourly_moc_msisdn"
HOURLY_MOC_CHECKPOINT = os.getenv("HOURLY_MOC_CHECKPOINT") or "/sparkdata/checkpoints/hourly_moc_msisdn"
MSISDN_INFO_COLLECTION = os.getenv("MSISDN_INFO_COLLECTION") or "msc_msisdn_info"

# %% Logger
logger = logging.getLogger("spark_hourly_moc")
logger.setLevel(logging.DEBUG)
file_handler = RotatingFileHandler(
    "/app/logs/spark_hourly_moc.log", maxBytes=10_000_000, backupCount=3
)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
file_handler.setFormatter(formatter)
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
if not logger.handlers:
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

# %% SparkSession (same style as your existing builder)
spark = (
    SparkSession.builder
    .appName("sparkHourlyMOC")
    .config("spark.mongodb.write.connection.uri", MONGODB_URL)
    .config("spark.sql.shuffle.partitions", SPARK_PARTITIONS)
    .config("spark.executor.cores", SPARK_EXECUTOR_CORES)
    .config("spark.cores.max",SPARK_MAX_CORES)
    .config("spark.executor.memory", SPARK_EXECUTOR_MEMORY)
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
logger.info("Spark Initialized (Hourly MOC)")

# %% CDR schema (same as yours)
schema = StructType([
    StructField("xdr_start_datetime", TimestampType(), True),
    StructField("xdr_end_datetime", TimestampType(), True),
    StructField("xdr_type", StringType(), True),
    StructField("xdr_duration", IntegerType(), True),
    StructField("billing_start_datetime", TimestampType(), True),
    StructField("billing_end_datetime", TimestampType(), True),
    StructField("billing_duration", IntegerType(), True),
    StructField("call_destination", StringType(), True),
    StructField("served_phone_number", StringType(), True),
    StructField("served_country", StringType(), True),
    StructField("served_operator", StringType(), True),
    StructField("served_tos", StringType(), True),
    StructField("served_full_cellid", StringType(), True),
    StructField("served_imsi", StringType(), True),
    StructField("served_imei", StringType(), True),
    # StructField("served_type", StringType(), True),
    StructField("served_classmark2", StringType(), True),
    StructField("served_classmark3", StringType(), True),
    StructField("served_ported", BooleanType(), True),
    # StructField("served_ported_from", StringType(), True),
    StructField("served_inroamer", BooleanType(), True),
    StructField("other_phone_number", StringType(), True),
    StructField("other_country", StringType(), True),
    StructField("other_operator", StringType(), True),
    StructField("other_tos", StringType(), True),
    # StructField("other_full_cellid", StringType(), True),
    StructField("other_imsi", StringType(), True),
    StructField("other_imei", StringType(), True),
    # StructField("other_classmark2", StringType(), True),
    # StructField("other_classmark3", StringType(), True),
    # StructField("other_ported", BooleanType(), True),
    # StructField("other_ported_from", StringType(), True),
    StructField("called_phone_number", StringType(), True),
    StructField("called_imsi", StringType(), True),
    StructField("called_imei", StringType(), True),
    StructField("calling_phone_number", StringType(), True),
    StructField("calling_imsi", StringType(), True),
    StructField("calling_imei", StringType(), True),
    StructField("protocol_signature", StringType(), True),
    StructField("filename", StringType(), True),
    StructField("filename_aggregate", StringType(), True),
    StructField("record_insertion_datetime", TimestampType(), True),
])


# %% Streaming source (reads new parquet files as they land)
streaming_df = (
    spark.readStream
         .schema(schema)
         .option("maxFilesPerTrigger", FILES_PER_TRIGGER)
         .parquet(INPUT_FOLDER)
)

# %% foreachBatch function
def process_batch(batch_df, batch_id: int):
    """
    1) Filter outgoing (MOC) calls.
    2) Build hourly aggregation per msisdn.
    3) Write hourly aggregation to Delta (Step 3).
    4) Build & write msisdn info to Mongo (Step 4).
    """
    logger.info(f"Starting batch {batch_id}")

    # 1. Filter to outgoing voice calls only
    moc_df = (
        batch_df
        .filter(col("xdr_type") == lit("MOC"))
        .withColumn("event_ts", col("xdr_start_datetime"))
        .filter(col("event_ts").isNotNull())
        .withColumn("event_hour", date_trunc("hour", col("event_ts")))
        .withColumn("event_date", to_date(col("event_ts")))
        .withColumn("msisdn", col("served_phone_number"))
    )

    if moc_df.rdd.isEmpty():
        logger.info(f"No MOC rows in batch {batch_id}")
        return

    # ---- Step 3: Hourly aggregations to Delta ----
    hourly_agg = (
        moc_df.groupBy("event_date", "event_hour", "msisdn")
              .agg(count("*").alias("outgoing_calls_count"))
    )

    (
        hourly_agg.write
                  .format("delta")
                  .mode("append")
                  .partitionBy("event_date")
                  .save(HOURLY_MOC_DELTA_PATH)
    )
    logger.info(f"Batch {batch_id}: written hourly MOC to Delta")

    # ---- Step 4: MSISDN info to MongoDB ----
    # Example: for each msisdn, track first/last seen + one IMEI/IMSI sample
    msisdn_info_batch = (
        moc_df.groupBy("msisdn")
              .agg(
                  fmin("event_ts").alias("first_seen_on_network"),
                  fmax("event_ts").alias("last_seen_on_network"),
                  first("served_imsi", ignorenulls=True).alias("imsi"),
                  first("served_imei", ignorenulls=True).alias("imei")
              )
    )

    (
        msisdn_info_batch.write
                         .format("mongodb")
                         .mode("append")   # simple for now; in real life you would upsert
                         .option("database", DB)
                         .option("collection", MSISDN_INFO_COLLECTION)
                         .save()
    )
    logger.info(f"Batch {batch_id}: written msisdn info to Mongo")

# Start the streaming query
query = (
    streaming_df.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", HOURLY_MOC_CHECKPOINT)
        .outputMode("update")
        .start()
)

query.awaitTermination()
