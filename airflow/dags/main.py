from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models.param import Param

default_args = {
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="main",
    default_args=default_args,
    description="ohmygod pipeline",
    schedule_interval="@daily",
    start_date=datetime(2019, 10, 1),
    catchup=False,
    tags=["example"],
    params={
        "snapshot_date": Param("2019-10-01", type="string", title="Snapshot Date", description="Date for snapshot")
    }
) as dag:

    transform_event_logs = SparkSubmitOperator(
        task_id="transform_event_logs",
        application="/opt/airflow/code/transform_event.py",
        conn_id="spark_default",
        application_args=["{{ params.snapshot_date }}"],
        dag=dag
    )

    transform_support_logs = SparkSubmitOperator(
        task_id="transform_support_logs",
        application="/opt/airflow/code/transform_support.py",
        conn_id="spark_default",
        application_args=["{{ params.snapshot_date }}"],
        dag=dag
    )

    daily_user_metrics = SparkSubmitOperator(
        task_id="daily_user_metrics",
        application="/opt/airflow/code/daily_metrics.py",
        conn_id="spark_default",
        application_args=["{{ params.snapshot_date }}"],
        packages="org.postgresql:postgresql:42.2.5",
        dag=dag
    )

    load_to_db = SparkSubmitOperator(
        task_id="load_to_db",
        application="/opt/airflow/code/load.py",
        conn_id="spark_default",
        application_args=["{{ params.snapshot_date }}"],
        packages="org.mongodb.spark:mongo-spark-connector_2.12:10.5.0",
        
        dag=dag
    )

    [transform_event_logs, transform_support_logs, daily_user_metrics] >> load_to_db
