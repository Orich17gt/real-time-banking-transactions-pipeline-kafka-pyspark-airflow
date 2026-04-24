from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="silver_gold_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="*/3 * * * *",   # 🔥 EVERY 3 MINUTES
    catchup=False
) as dag:

    # =========================
    # RUN SILVER (INCREMENTAL)
    # =========================
    run_silver = BashOperator(
        task_id="run_silver_txn",
        bash_command="""
        docker exec spark spark-submit --packages io.delta:delta-spark_2.12:3.1.0 --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog /app/Silver_Txn_level.py
        """
    )

    # =========================
    # RUN GOLD BI (AFTER SILVER)
    # =========================
    run_gold_bi = BashOperator(
        task_id="run_gold_bi",
        bash_command="""
docker exec spark spark-submit --packages io.delta:delta-spark_2.12:3.1.0 --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog /app/Gold_Txn_level.py
        """
    )

    # =========================
    # FLOW
    # =========================
    run_silver >> run_gold_bi

