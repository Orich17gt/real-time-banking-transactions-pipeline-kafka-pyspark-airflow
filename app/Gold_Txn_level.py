from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

if __name__ == "__main__":

    # ---------------------------
    # Spark Session
    # ---------------------------
    spark = SparkSession.builder \
        .appName("GoldTxnBatch") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # ---------------------------
    # Read Silver
    # ---------------------------
    df = spark.read.format("delta").load("/data/silver/transactions")

    # =========================================================
    # 🥇 FACT TABLE 1: TRANSACTION LIFECYCLE
    # =========================================================
    gold_transaction_lifecycle = df.select(
        "transaction_id",
        "account_id",
        "customer_id",
        col("txn_ts").alias("txn_date"),
        "txn_amount",
        "txn_status",
        col("settlement_ts").alias("settlement_date"),
        "days_to_settle",
        "is_settled",
        "high_value_flag"
    )

    # =========================================================
    # 🥈 FACT TABLE 2: FRAUD DETECTION
    # =========================================================
    gold_fraud_detection = df.withColumn(
        "fraud_flag",
        when(
            (col("txn_amount") > 10000) |
            (col("txn_hour").between(0, 4)) |
            (col("country_code") != "US"),
            1
        ).otherwise(0)
    ).select(
        "transaction_id",
        "account_id",
        "txn_amount",
        "txn_hour",
        "country_code",
        "channel",
        "fraud_flag"
    )

    # =========================================================
    # 🥉 FACT TABLE 3: CREDIT RISK
    # =========================================================
    gold_credit_risk = df.groupBy("account_id").agg(
        sum("txn_amount").alias("total_spend"),
        avg("txn_amount").alias("avg_txn"),
        count("*").alias("txn_count"),
        sum("high_value_flag").alias("high_value_txns")
    ).withColumn(
        "risk_score",
        col("high_value_txns") * 2 + col("txn_count") * 0.1
    )

    # =========================================================
    # 🟡 FACT TABLE 4: TRANSACTION SUMMARY
    # =========================================================
    gold_transaction_summary = df.groupBy("txn_date_only").agg(
        count("*").alias("total_txns"),
        sum("txn_amount").alias("total_amount"),
        avg("txn_amount").alias("avg_amount"),
        sum("is_settled").alias("settled_txns")
    )

    # =========================================================
    # 📦 DIM TABLES
    # =========================================================

    dim_date = df.select(
        col("txn_date_only").alias("date")
    ).distinct() \
     .withColumn("year", year("date")) \
     .withColumn("month", month("date")) \
     .withColumn("day", dayofmonth("date")) \
     .withColumn("week", weekofyear("date"))

    dim_account = df.select(
        "account_id"
    ).distinct()

    dim_customer = df.select(
        "customer_id"
    ).distinct()

    dim_transaction_type = df.select(
        "txn_type"
    ).distinct()

    dim_channel = df.select(
        "channel"
    ).distinct()

    dim_country = df.select(
        "country_code"
    ).distinct()

    dim_merchant = df.select(
        "merchant_category"
    ).distinct()

    # =========================================================
    # 💾 WRITE GOLD DELTA TABLES
    # =========================================================
    gold_transaction_lifecycle.write.format("delta").mode("overwrite").save("/data/gold/gold_transaction_lifecycle")
    gold_fraud_detection.write.format("delta").mode("overwrite").save("/data/gold/gold_fraud_detection")
    gold_credit_risk.write.format("delta").mode("overwrite").save("/data/gold/gold_credit_risk")
    gold_transaction_summary.write.format("delta").mode("overwrite").save("/data/gold/gold_transaction_summary")

    dim_date.write.format("delta").mode("overwrite").save("/data/gold/dim_date")
    dim_account.write.format("delta").mode("overwrite").save("/data/gold/dim_account")
    dim_customer.write.format("delta").mode("overwrite").save("/data/gold/dim_customer")
    dim_transaction_type.write.format("delta").mode("overwrite").save("/data/gold/dim_transaction_type")
    dim_channel.write.format("delta").mode("overwrite").save("/data/gold/dim_channel")
    dim_country.write.format("delta").mode("overwrite").save("/data/gold/dim_country")
    dim_merchant.write.format("delta").mode("overwrite").save("/data/gold/dim_merchant")

    # =========================================================
    # 📤 EXPORT CSV FOR POWER BI
    # =========================================================
    base_path = "/data/raw/"

    gold_transaction_lifecycle.coalesce(1).write.mode("overwrite").option("header", True).csv(base_path + "gold_transaction_lifecycle")
    gold_fraud_detection.coalesce(1).write.mode("overwrite").option("header", True).csv(base_path + "gold_fraud_detection")
    gold_credit_risk.coalesce(1).write.mode("overwrite").option("header", True).csv(base_path + "gold_credit_risk")
    gold_transaction_summary.coalesce(1).write.mode("overwrite").option("header", True).csv(base_path + "gold_transaction_summary")

    dim_date.coalesce(1).write.mode("overwrite").option("header", True).csv(base_path + "dim_date")
    dim_account.coalesce(1).write.mode("overwrite").option("header", True).csv(base_path + "dim_account")
    dim_customer.coalesce(1).write.mode("overwrite").option("header", True).csv(base_path + "dim_customer")
    dim_transaction_type.coalesce(1).write.mode("overwrite").option("header", True).csv(base_path + "dim_transaction_type")
    dim_channel.coalesce(1).write.mode("overwrite").option("header", True).csv(base_path + "dim_channel")
    dim_country.coalesce(1).write.mode("overwrite").option("header", True).csv(base_path + "dim_country")
    dim_merchant.coalesce(1).write.mode("overwrite").option("header", True).csv(base_path + "dim_merchant")

    gold_transaction_lifecycle.show(5, truncate=False)
    print("✅ Gold layer complete and CSV exported")




