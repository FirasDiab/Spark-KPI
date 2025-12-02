from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as F
from datetime import datetime

SPARK_MASTER_URL = "local[*]"
MONGO_URI = "mongodb://mongo_db:27017"
DB_NAME = "data"
COLLECTION = "firas_test"
SPARK_EXECUTOR_MEMORY = "1g"
SPARK_MAX_CORES = "2"
SPARK_EXECUTOR_CORES = "1"
SPARK_PARTITIONS = "10"
PARQUET_PATH = "/sparkdata/input/Parquet_Output"

spark = (
    SparkSession
    .builder
    .appName("Spark KPI")
    .master(SPARK_MASTER_URL)
    .config(
        "spark.jars.packages",
        "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0"
    )
    .config(
        "spark.mongodb.write.connection.uri",
        f"{MONGO_URI}/{DB_NAME}.{COLLECTION}"
    )
    .config("spark.sql.shuffle.partitions", SPARK_PARTITIONS) \
    .config("spark.executor.cores", SPARK_EXECUTOR_CORES) \
    .config("spark.cores.max", SPARK_MAX_CORES) \
    .config("spark.executor.memory", SPARK_EXECUTOR_MEMORY) \
    .getOrCreate()
)

df_parquet = spark.read.parquet(PARQUET_PATH)

start_dt = datetime(2025, 11, 26)
end_dt   = datetime(2025, 11, 27)

start_ms = int(start_dt.timestamp() * 1000)
end_ms   = int(end_dt.timestamp() * 1000)

df_f = (
    df_parquet
    .where(
        (F.col("CUST_LOCAL_ENTRY_DATE") >= start_ms) &
        (F.col("CUST_LOCAL_ENTRY_DATE") <  end_ms) &
        (F.col("ERROR_TYPE") == "0") &
        (F.col("TRANSFER_TYPE").isin("0", "1"))
    )
)

df_p = df_f.select(
    # date string (yyyy-MM-dd) from CUST_LOCAL_ENTRY_DATE (bigint ms -> sec -> string)
    F.from_unixtime(
        (F.col("CUST_LOCAL_ENTRY_DATE") / 1000).cast("long"),
        "yyyy-MM-dd"
    ).alias("date"),

    # PAY_TYPE -> prepaid / postpaid / hybrid
    F.when(F.col("PAY_TYPE") == "0", "prepaid") \
     .when(F.col("PAY_TYPE") == "1", "postpaid") \
     .when(F.col("PAY_TYPE") == "2", "hybrid") \
     .otherwise(F.lit(None)).alias("pay_type"),

    F.col("HANDLING_CHARGE"),

    # transfer_type: EXT_TRANS_ID null -> international, else local
    F.when(F.col("EXT_TRANS_ID").isNull(), "international")
     .otherwise("local").alias("transfer_type"),

    # transfer_flow from TRANSFER_TYPE
    F.when(F.col("TRANSFER_TYPE") == "0", "incoming") \
     .when(F.col("TRANSFER_TYPE") == "1", "outgoing") \
     .otherwise(F.lit(None)).alias("transfer_flow"),
)

df_p = df_p.withColumn(
    "fee_amount",
    F.col("HANDLING_CHARGE") / F.lit(10000.0)
)

tt = F.col("transfer_type")
tf = F.col("transfer_flow")
fa = F.col("fee_amount")

df_p = df_p.withColumn(
    "charge_status",
    F.when(
        (tt == "local") & (tf == "outgoing") & (fa == 0.5),
        "Correct applied fee"
    ).when(
        (tt == "local") & (tf == "outgoing") & (fa > 0.5),
        "Over charged fee"
    ).when(
        (tt == "local") & (tf == "outgoing") & (fa < 0.5),
        "Under charged fee"
    ).when(
        (tt == "international") & (fa == 0.0),
        "Correct applied fee"
    ).when(
        (tt == "international") & (fa > 0.0),
        "Over charged fee"
    ).when(
        (tt == "international") & (fa < 0.0),
        "Under charged fee"
    ).when(
        (tt == "local") & (tf == "incoming") & (fa == 0.0),
        "Correct applied fee"
    ).when(
        (tt == "local") & (tf == "incoming") & (fa > 0.0),
        "Over charged fee"
    ).when(
        (tt == "local") & (tf == "incoming") & (fa < 0.0),
        "Under charged fee"
    ).otherwise(F.lit(None))
)

df_agg = (
    df_p
    .groupBy(
        "date",
        "pay_type",
        "transfer_type",
        "transfer_flow",
        "charge_status"
    )
    .agg(
        F.count("*").alias("call_count"),
        F.sum("HANDLING_CHARGE").alias("total_fee_charge_raw")
    )
)

result = df_agg.select(
    "date",
    "pay_type",
    "transfer_type",
    "transfer_flow",
    "charge_status",
    "call_count",
    (F.col("total_fee_charge_raw") / F.lit(10000.0)).alias("total_fee_charge")
)

result.write \
    .format("mongodb") \
    .mode("append") \
    .option("collection", "firas_test") \
    .save()
