from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, current_date, lit
from pyspark.sql.types import *
    
# ---------------------------
# TRANSACTION SCHEMA
# ---------------------------
txn_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("ingest_ts", StringType(), True),
    StructField("source_system", StringType(), True),

    StructField("transaction_id", StringType(), True),
    StructField("account_id", StringType(), True),
    StructField("customer_id", StringType(), True),

    StructField("txn_date", StringType(), True),
    StructField("txn_type", StringType(), True),
    StructField("txn_amount", DoubleType(), True),
    StructField("txn_status", StringType(), True),

    StructField("settlement_date", StringType(), True),
    StructField("account_balance", DoubleType(), True),

    StructField("fraud_flag", BooleanType(), True),
    StructField("credit_risk", StringType(), True),

    StructField("channel", StringType(), True),
    StructField("merchant_category", StringType(), True),
    StructField("currency", StringType(), True),
    StructField("country_code", StringType(), True)
])

# ---------------------------
# MAIN
# ---------------------------
if __name__ == "__main__":

    spark = SparkSession.builder \
        .appName("TxnKafkaConsumer") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # ---------------------------
    # READ FROM KAFKA
    # ---------------------------
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "txn") \
        .option("startingOffsets", "earliest") \
        .load()

    # ---------------------------
    # PARSE JSON
    # ---------------------------
    df = kafka_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), txn_schema).alias("data")) \
        .select("data.*")

    # ---------------------------
    # ADD METADATA
    # ---------------------------
    df = df \
        .withColumn("ingestion_date", current_date()) \
        .withColumn("ingestion_time", current_timestamp()) \
        .withColumn("source", lit("kafka_txn_stream"))

    # ---------------------------
    # WRITE TO BRONZE
    # ---------------------------
    query = df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("path", "/data/bronze/transactions") \
        .option("checkpointLocation", "/checkpoints/bronze/transactions") \
        .trigger(processingTime="30 seconds") \
        .start()

    print("🚀 Writing TRANSACTIONS to Bronze...")
    query.awaitTermination()