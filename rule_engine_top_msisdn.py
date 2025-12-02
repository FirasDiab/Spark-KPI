from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as fsum, row_number
from pyspark.sql.window import Window
from logging.handlers import RotatingFileHandler
import logging
import os
import time  # <--- NEW

MONGODB_URL = os.getenv("MONGO_URL") or "mongodb://mongodb:27017/"
DB = os.getenv("DB") or "TestDB"
HOURLY_MOC_DELTA_PATH = os.getenv("HOURLY_MOC_DELTA_PATH") or "/sparkdata/delta/hourly_moc_msisdn"
MSISDN_INFO_COLLECTION = os.getenv("MSISDN_INFO_COLLECTION") or "msc_msisdn_info"
DETECTIONS_COLLECTION = os.getenv("DETECTIONS_COLLECTION") or "msc_detections"

# ---------- Logger ----------
logger = logging.getLogger("spark_rule_engine")
logger.setLevel(logging.DEBUG)
file_handler = RotatingFileHandler(
    "/app/logs/spark_rule_engine.log", maxBytes=10_000_000, backupCount=3
)
fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
file_handler.setFormatter(fmt)
console = logging.StreamHandler()
console.setFormatter(fmt)
if not logger.handlers:
    logger.addHandler(file_handler)
    logger.addHandler(console)

# ---------- SparkSession ----------
spark = (
    SparkSession.builder
    .appName("mscRuleEngineTopMsisdn")
    .config("spark.mongodb.read.connection.uri", MONGODB_URL)
    .config("spark.mongodb.write.connection.uri", MONGODB_URL)
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
logger.info("Spark initialized (Rule Engine)")


def run_rule_engine_top_msisdn():
    """
    One full run of the rule engine.
    """
    logger.info("Starting rule_engine_top_msisdn run")

    # ---- 1. Pull hourly aggregates from Delta ----
    hourly_df = spark.read.format("delta").load(HOURLY_MOC_DELTA_PATH)

    # Compute daily totals per (event_date, msisdn)
    daily_totals = (
        hourly_df.groupBy("event_date", "msisdn")
                 .agg(fsum("outgoing_calls_count").alias("daily_outgoing_calls"))
    )

    # ---- Optional: join with msisdn info from Mongo ----
    msisdn_info_df = (
        spark.read.format("mongodb")
             .option("database", DB)
             .option("collection", MSISDN_INFO_COLLECTION)
             .load()
    )

    # msisdn_info_df has: msisdn, first_seen_on_network, last_seen_on_network, imsi, imei
    daily_with_info = daily_totals.join(msisdn_info_df, on="msisdn", how="left")

    # ---- 2. For each day, find MSISDN with highest daily_outgoing_calls ----
    w = Window.partitionBy("event_date").orderBy(col("daily_outgoing_calls").desc())
    ranked = daily_with_info.withColumn("rn", row_number().over(w))
    top_per_day = ranked.filter(col("rn") == 1).drop("rn")

    # ---- 3. Turn into detections DF ----
    from pyspark.sql.functions import lit

    detections_df = (
        top_per_day
        .withColumn("detection_type", lit("TOP_CALLER_PER_DAY"))
        .withColumn("rule_name",     lit("rule_engine_top_msisdn"))
    )

    logger.info("Top MSISDN per day:")
    detections_df.show(truncate=False)

    # ---- 4. Store detections in MongoDB ----
    (
        detections_df.write
                     .format("mongodb")
                     .mode("overwrite")   # overwrite whole collection each run
                     .option("database", DB)
                     .option("collection", DETECTIONS_COLLECTION)
                     .save()
    )

    logger.info("Detections written to MongoDB")


if __name__ == "__main__":
    logger.info("Starting rule engine scheduler: run every 5 minutes")

    try:
        while True:
            start_ts = time.strftime("%Y-%m-%d %H:%M:%S")
            logger.info(f"=== Rule engine run at {start_ts} ===")
            try:
                run_rule_engine_top_msisdn()
            except Exception:
                logger.exception("rule_engine_top_msisdn run FAILED")
            logger.info("Sleeping 300 seconds (5 minutes) before next run...")
            time.sleep(300)
    finally:
        # This will only happen if the container/process is stopping
        logger.info("Stopping Spark session")
        spark.stop()
