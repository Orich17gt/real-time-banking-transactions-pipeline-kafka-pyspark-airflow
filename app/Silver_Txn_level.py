from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

if __name__ == "__main__":

    # ---------------------------
    # Create Spark Session
    # ---------------------------
    spark = SparkSession.builder \
        .appName("SilverTxnBatch") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # ---------------------------
    # Read Bronze Transactions
    # ---------------------------
    bronze_df = spark.read.format("delta").load("/data/bronze/transactions")

    # ---------------------------
    # Get last processed timestamp from Silver
    # ---------------------------
    try:
        silver_df = spark.read.format("delta").load("/data/silver/transactions")

        last_ts = silver_df.agg(
            max("ingestion_time")
        ).collect()[0][0]

    except:
        last_ts = None

    # ---------------------------
    # Filter ONLY new data
    # ---------------------------
    if last_ts:
        df = bronze_df.filter(col("ingestion_time") > last_ts)
    else:
        df = bronze_df

    # ---------------------------
    # Data Quality Filters
    # ---------------------------
    df = df \
        .filter(col("transaction_id").isNotNull()) \
        .filter(col("account_id").isNotNull()) \
        .filter(col("customer_id").isNotNull()) \
        .filter(col("txn_amount").isNotNull()) \
        .filter(col("txn_amount") > 0) \
        .filter(col("txn_date").isNotNull())

    # ---------------------------
    # Convert Dates
    # ---------------------------
    df = df \
        .withColumn("txn_ts", to_timestamp(col("txn_date"))) \
        .withColumn("settlement_ts", to_timestamp(col("settlement_date"))) \
        .withColumn("ingestion_time", to_timestamp(col("ingestion_time")))

    # ---------------------------
    # Derived Columns
    # ---------------------------
    df = df \
        .withColumn("txn_date_only", to_date(col("txn_ts"))) \
        .withColumn("txn_hour", hour(col("txn_ts"))) \
        .withColumn("days_to_settle", datediff(col("settlement_ts"), col("txn_ts"))) \
        .withColumn(
            "is_settled",
            when(col("txn_status") == "settled", 1).otherwise(0)
        ) \
        .withColumn(
            "high_value_flag",
            when(col("txn_amount") > 10000, 1).otherwise(0)
        )

    # ---------------------------
    # Normalize / Clean Fields
    # ---------------------------
    df = df \
        .withColumn("txn_type", lower(trim(col("txn_type")))) \
        .withColumn("txn_status", lower(trim(col("txn_status")))) \
        .withColumn("channel", lower(trim(col("channel")))) \
        .withColumn("merchant_category", lower(trim(col("merchant_category")))) \
        .withColumn("currency", upper(trim(col("currency")))) \
        .withColumn("country_code", upper(trim(col("country_code")))) \
        .withColumn("credit_risk", lower(trim(col("credit_risk"))))

    # ---------------------------
    # Deduplicate (VERY IMPORTANT)
    # ---------------------------
    window_spec = Window.partitionBy("transaction_id").orderBy(col("ingestion_time").desc())

    df = df \
        .withColumn("row_num", row_number().over(window_spec)) \
        .filter(col("row_num") == 1) \
        .drop("row_num")

    # ---------------------------
    # Write Silver
    # ---------------------------
    df.write.format("delta") \
        .mode("append") \
        .save("/data/silver/transactions")

    print("✅ Silver Transactions batch complete")

    print("-"+str(df.count())+"-")

    df.show(5, False)



