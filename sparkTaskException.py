from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime
MONGO_URI = "mongodb://mongo_db:27017"
DB_NAME = "data"
COLLECTION = "firas_test_exce"

spark = (
    SparkSession
    .builder
    .appName("Spark KPI Exce")
    .master("local[*]")
    .config(
        "spark.jars.packages",
        "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0"
    )
    .config(
        "spark.mongodb.write.connection.uri",
        f"{MONGO_URI}/{DB_NAME}.{COLLECTION}"
    )
    .getOrCreate()
)

PARQUET_PATH = "/sparkdata/input/Parquet_Output"

df_parquet = spark.read.parquet(PARQUET_PATH)

start_dt = datetime(2025, 11, 26)  # <-- change as needed
end_dt   = datetime(2025, 11, 27)  # <-- change as needed (exclusive)

start_ms = int(start_dt.timestamp() * 1000)
end_ms   = int(end_dt.timestamp() * 1000)

df_filtered = (
    df_parquet
    .filter(
        (F.col("CUST_LOCAL_ENTRY_DATE") >= start_ms) &
        (F.col("CUST_LOCAL_ENTRY_DATE") <  end_ms) &
        (F.col("ERROR_TYPE") == 0) &
        (F.col("TRANSFER_TYPE").isin(0, 1))  # [0,1]
    )
)

fee_divided = (F.col("HANDLING_CHARGE") / F.lit(10000.0))

df_proj = (
    df_filtered
    .select(
        # date
        F.from_unixtime(F.col("CUST_LOCAL_ENTRY_DATE") / 1000, "yyyy-MM-dd").alias("date"),

        # pay_type
        F.when(F.col("PAY_TYPE") == 0, "prepaid")
         .when(F.col("PAY_TYPE") == 1, "postpaid")
         .when(F.col("PAY_TYPE") == 2, "hybrid")
         .otherwise(F.lit(None)).alias("pay_type"),

        F.col("HANDLING_CHARGE"),

        # derived string transfer_type -> use different name to avoid clash
        F.when(F.col("EXT_TRANS_ID").isNull(), "international")
         .otherwise("local").alias("transfer_type_str"),

        # derived string transfer_flow -> different name
        F.when(F.col("TRANSFER_TYPE") == 0, "incoming")
         .when(F.col("TRANSFER_TYPE") == 1, "outgoing")
         .otherwise(F.lit(None)).alias("transfer_flow_str"),

        # original fields (keep as-is)
        "TRANSFER_LOG_ID",
        "ACCT_ID",
        "CUST_ID",
        "SUB_ID",
        "PRI_IDENTITY",
        "BATCH_NO",
        "CHANNEL_ID",
        "REASON_CODE",
        "RESULT_CODE",
        "ERROR_TYPE",
        "REQUEST_ACCT_BALANCE_ID",
        "DEST_ACCT_ID",
        "TRANSFER_TYPE",
        "TRANSFER_AMT",
        "TRANSFER_DATE",
        "CUST_LOCAL_TRANSFER_DATE",
        "TRANSFER_TRANS_ID",
        "EXT_TRANS_TYPE",
        "EXT_TRANS_ID",
        "ACCESS_METHOD",
        "DIAMETER_SESSIONID",
        "REVERSAL_TRANS_ID",
        "REVERSAL_REASON_CODE",
        "REVERSAL_DATE",
        "EXT_REF_ID",
        "STATUS",
        "ENTRY_DATE",
        "CUST_LOCAL_ENTRY_DATE",
        "REMARK",
        "OPER_ID",
        "DEPT_ID",
        "BE_ID",
        "BE_CODE",
        "REGION_ID",
        "REGION_CODE",
        "SRC_ACCT_ID",
        "CHG_SPENDING_LIMIT",
        "CHG_PAY_LIMIT",
        "CHG_BALANCE_LIMIT",
        "CHG_FU_LIMIT",
        "LOAN_INFO"
    )
)


df_with_status = (
    df_proj
    .withColumn(
        "status",
        F.when(
            (F.col("transfer_type_str") == "local") &
            (F.col("transfer_flow_str") == "outgoing") &
            (fee_divided == 0.5),
            "Correct charge"
        ).when(
            (F.col("transfer_type_str") == "local") &
            (F.col("transfer_flow_str") == "outgoing") &
            (fee_divided > 0.5),
            "Over charge"
        ).when(
            (F.col("transfer_type_str") == "local") &
            (F.col("transfer_flow_str") == "outgoing") &
            (fee_divided < 0.5),
            "Under charge"
        ).when(
            (F.col("transfer_type_str") == "international") &
            (fee_divided == 0.0),
            "Correct charge"
        ).when(
            (F.col("transfer_type_str") == "international") &
            (fee_divided > 0.0),
            "Over charge"
        ).when(
            (F.col("transfer_type_str") == "international") &
            (fee_divided < 0.0),
            "Under charge"
        ).when(
            (F.col("transfer_type_str") == "local") &
            (F.col("transfer_flow_str") == "incoming") &
            (fee_divided == 0.0),
            "Correct charge"
        ).when(
            (F.col("transfer_type_str") == "local") &
            (F.col("transfer_flow_str") == "incoming") &
            (fee_divided > 0.0),
            "Over charge"
        ).when(
            (F.col("transfer_type_str") == "local") &
            (F.col("transfer_flow_str") == "incoming") &
            (fee_divided < 0.0),
            "Under charge"
        )
        .otherwise(F.lit(None))
    )
)

df_exceptions = df_with_status.filter(
    F.col("status").isin("Under charge", "Over charge")
)

if not df_exceptions.isEmpty():
    (
        df_exceptions
        .write
        .format("mongodb")
        .mode("append")
        .option("collection", "firas_test_exception")
        .save()
    )
else:
    print("df_exceptions is empty – nothing to insert.")
