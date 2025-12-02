# %% Module Imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, input_file_name, regexp_extract, min, regexp_replace, date_format, lit, when, udf, datediff, element_at, struct, size,to_timestamp
from pyspark.sql.types import *
from delta.tables import DeltaTable
from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array
from pyspark import StorageLevel
from pymongo import MongoClient, UpdateOne
import os
import logging
from logging.handlers import RotatingFileHandler
from utils.query import total_moc

# %% Environment Variables

MONGODB_URL = os.getenv("MONGO_URL") or "mongodb://mongodb:27017/"
CONFIG_DB = os.getenv("CONFIG_DB") or "Configs"
CONFIG_COL = os.getenv("CONFIG_COL") or "spark_msc_configs"
DB = os.getenv("DB") or "TestDB"
SPARK_EXECUTOR_MEMORY = os.getenv("SPARK_EXECUTOR_MEMORY") or "1g"
SPARK_MAX_CORES = os.getenv("SPARK_MAX_CORES") or "2"
SPARK_EXECUTOR_CORES = os.getenv("SPARK_EXECUTOR_CORES") or "1"
SPARK_PARTITIONS = os.getenv("SPARK_PARTITIONS") or "10"
INPUT_FOLDER = os.getenv("INPUT_FOLDER") or "/sparkdata/input/"
ARCHIVE_FOLDER = os.getenv("ARCHIVE_FOLDER") or "/sparkdata/archive/"
FILES_PER_TRIGGER = os.getenv("FILES_PER_TRIGGER") or 1000

# %% Set up logger
logger = logging.getLogger('spark_logger')
logger.setLevel(logging.DEBUG)
file_handler = RotatingFileHandler(
    # Rotate after 10,000,000 bytes ~ 10MB and keep 3 backups
    '/app/logs/spark_aggregation.log', maxBytes=10000000, backupCount=3
)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
# Add handlers only if not already added
if not logger.handlers:
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

# %% Initialize MongoDB
client = MongoClient(MONGODB_URL)
config_db = client[CONFIG_DB]
config_col = config_db[CONFIG_COL]

# %% Initialize SparkSession
builder = SparkSession.builder \
    .appName("sparkAggregation") \
    .config("spark.mongodb.write.connection.uri", MONGODB_URL)\
    .config("spark.sql.shuffle.partitions", SPARK_PARTITIONS) \
    .config("spark.executor.cores", SPARK_EXECUTOR_CORES) \
    .config("spark.cores.max",SPARK_MAX_CORES) \
    .config("spark.executor.memory", SPARK_EXECUTOR_MEMORY)\
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
    .config("spark.hadoop.fs.permissions.umask-mode","000") \
    .config("spark.databricks.delta.deletedFileRetentionDuration", "interval 1 hour") \
    .config("spark.history.fs.cleaner.enabled","true") \
    .config("spark.history.fs.cleaner.interval","1h") \
    .config("spark.history.fs.cleaner.maxAge","1d")
        

spark = builder.getOrCreate()
logger.info("Spark Initialized")

df = spark.read.parquet(INPUT_FOLDER)


# %% File schema
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
    StructField("served_type", StringType(), True),
    StructField("served_classmark2", StringType(), True),
    StructField("served_classmark3", StringType(), True),
    StructField("served_ported", BooleanType(), True),
    StructField("served_ported_from", StringType(), True),
    StructField("served_inroamer", BooleanType(), True),
    StructField("other_phone_number", StringType(), True),
    StructField("other_country", StringType(), True),
    StructField("other_operator", StringType(), True),
    StructField("other_tos", StringType(), True),
    StructField("other_full_cellid", StringType(), True),
    StructField("other_imsi", StringType(), True),
    StructField("other_imei", StringType(), True),
    StructField("other_classmark2", StringType(), True),
    StructField("other_classmark3", StringType(), True),
    StructField("other_ported", BooleanType(), True),
    StructField("other_ported_from", StringType(), True),
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

schema_imsi_info = StructType([

    StructField("first_seen_on_network", TimestampType(), nullable=True),
    StructField("msisdn", StringType(), nullable=True),
    StructField("imsi", StringType(), nullable=True),
    StructField("last_seen_on_network", TimestampType(), nullable=True),
])


# ==== STREAMING VERSION ====

streaming_df = (
    spark.readStream
         .schema(schema)
         .option("maxFilesPerTrigger", FILES_PER_TRIGGER)
         .parquet(INPUT_FOLDER)
)

streaming_df.withWatermark(
    "xdr_start_datetime", "24 hours"
).createOrReplaceTempView("xdr_data")

total_moc_count = spark.sql(total_moc)
# 2) MongoDB sink (another streaming query)
mongo_query = (
    total_moc_count
      .writeStream
      .outputMode("complete")
      .format("mongodb")
      .option("checkpointLocation", "/sparkdata/checkpoints/moc_counts")
      .option("uri", MONGODB_URL)
      .option("database", DB)
      .option("collection", "moc_counts")
      .start()
)

mongo_query.awaitTermination()

