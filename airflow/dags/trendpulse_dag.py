import sys
sys.path.insert(0, "/mnt/d/social-media-analytics")

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "trendpulse",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def produce_posts():
    from ingestion.reddit_producer import run_producer
    run_producer(num_posts=50, delay=0.1)
    print("[DAG] Producer finished")

def consume_posts():
    from streaming.spark_consumer import run_consumer
    run_consumer()
    print("[DAG] Consumer finished")

def run_ml_predictor():
    import os
    os.environ["DB_HOST"] = "localhost"
    os.environ["DB_PORT"] = "5433"
    from models.virality_predictor import run_predictor
    run_predictor()
    print("[DAG] ML predictor finished")

def check_data_quality():
    import psycopg2
    conn = psycopg2.connect(
        host="localhost", port=5433,
        dbname="trendpulse",
        user="postgres", password="postgres"
    )
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM raw_posts")
    total = cur.fetchone()[0]
    assert total > 0, "No posts found!"
    print(f"[QC] Total posts: {total}")

    cur.execute("SELECT COUNT(*) FROM viral_predictions")
    preds = cur.fetchone()[0]
    assert preds > 0, "No predictions found!"
    print(f"[QC] Total predictions: {preds}")

    cur.execute("""
        SELECT ROUND(
            SUM(CASE WHEN predicted_viral THEN 1 ELSE 0 END)::NUMERIC
            / COUNT(*) * 100, 2
        ) FROM viral_predictions
    """)
    viral_rate = float(cur.fetchone()[0])
    print(f"[QC] Viral rate: {viral_rate}%")
    conn.close()

with DAG(
    dag_id="trendpulse_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="*/30 * * * *",
    catchup=False,
    tags=["trendpulse", "kafka", "ml", "dbt"],
) as dag:

    t1_produce = PythonOperator(
        task_id="produce_to_kafka",
        python_callable=produce_posts,
    )

    t2_consume = PythonOperator(
        task_id="consume_from_kafka",
        python_callable=consume_posts,
    )

    t3_dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="""
            source ~/airflow-venv/bin/activate &&
            cd /mnt/d/social-media-analytics/transform/trendpulse_dbt &&
            DB_HOST=localhost DB_PORT=5433 dbt run --profiles-dir ~/.dbt
        """,
    )

    t4_dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="""
            source ~/airflow-venv/bin/activate &&
            cd /mnt/d/social-media-analytics/transform/trendpulse_dbt &&
            DB_HOST=localhost DB_PORT=5433 dbt test --profiles-dir ~/.dbt
        """,
    )

    t5_ml = PythonOperator(
        task_id="run_virality_predictor",
        python_callable=run_ml_predictor,
    )

    t6_qc = PythonOperator(
        task_id="data_quality_check",
        python_callable=check_data_quality,
    )

    t1_produce >> t2_consume >> t3_dbt_run >> t4_dbt_test >> t5_ml >> t6_qc
