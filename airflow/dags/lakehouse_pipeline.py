import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# =====================================================
# PATHS
# =====================================================
BRONZE_JOBS_PATH = "/opt/airflow/jobs/bronze"
SILVER_JOBS_PATH = "/opt/airflow/jobs/silver"
GOLD_JOBS_PATH = "/opt/airflow/jobs/gold"

SPARK_BIN = "/opt/spark/bin/spark-submit"
SPARK_MASTER = "spark://spark-master:7077"
PY_FILES = "/opt/spark/spark.zip"

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="lakehouse_pipeline",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["bronze", "silver", "gold", "lakehouse"],
) as dag:

    start = BashOperator(
        task_id="start",
        bash_command="echo 'Iniciando Lakehouse Pipeline'"
    )

    end = BashOperator(
        task_id="end",
        bash_command="echo 'Lakehouse Pipeline finalizada'"
    )

    # =================================================
    # BRONZE (SERIAL)
    # =================================================
    ingest_bronze = BashOperator(
        task_id="ingest_all_to_bronze",
        bash_command="docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --py-files /opt/spark/spark/spark_session.py,/opt/spark/spark/schemas.py /opt/spark/jobs/bronze/ingest_all_to_bronze.py",
    )


    # =================================================
    # SILVER (PARALLEL)
    # =================================================
    silver_tasks = []

    for file in sorted(os.listdir(SILVER_JOBS_PATH)):
        if file.endswith(".py"):
            silver_tasks.append(
                BashOperator(
                    task_id=f"silver_{file.replace('.py','')}",
                    bash_command=f"docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --py-files /opt/spark/spark/spark_session.py,/opt/spark/spark/schemas.py /opt/spark/jobs/silver/{file}",
                )
            )

    silver_done = EmptyOperator(task_id="silver_done")

    # =================================================
    # GOLD (PARALLEL)
    # =================================================
    gold_dim_customers = BashOperator(
        task_id="gold_build_dim_customers",
        bash_command="docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --py-files /opt/spark/spark/spark_session.py,/opt/spark/spark/schemas.py /opt/spark/jobs/gold/build_dim_customers.py",
    )

    gold_dim_products = BashOperator(
        task_id="gold_build_dim_products",
        bash_command="docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --py-files /opt/spark/spark/spark_session.py,/opt/spark/spark/schemas.py /opt/spark/jobs/gold/build_dim_products.py",
    )

    gold_fact_order_items = BashOperator(
        task_id="gold_build_fact_order_items",
        bash_command="docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --py-files /opt/spark/spark/spark_session.py,/opt/spark/spark/schemas.py /opt/spark/jobs/gold/build_fact_order_items.py",
    )

    gold_fact_orders = BashOperator(
        task_id="gold_build_fact_orders",
        bash_command="docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --py-files /opt/spark/spark/spark_session.py,/opt/spark/spark/schemas.py /opt/spark/jobs/gold/build_fact_orders.py",
    )


    gold_done = EmptyOperator(task_id="gold_done")

    # =================================================
    # DEPENDENCIES
    # =================================================
    start >> ingest_bronze
    ingest_bronze >> silver_tasks
    silver_tasks >> silver_done
    silver_done >> [gold_dim_customers, gold_dim_products, gold_fact_order_items]
    gold_fact_order_items >> gold_fact_orders
    [gold_dim_customers, gold_dim_products, gold_fact_orders] >> gold_done >> end
