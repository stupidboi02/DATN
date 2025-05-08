from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models.param import Param
default_args={
        "depends_on_past": False,
        "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        }
with DAG(
    "main",
    default_args = default_args,
    description ="ohmygod pipeline",
    schedule_interval='@daily',
    start_date=datetime(2019, 10, 1),
    catchup=False,
    tags=["example"],
    params={
        "snapshot_date": Param("2019-10-1", type="string", title="Snapshot Date", description="Date for snapshot")
    }
) as dag:
    process_profile = SparkSubmitOperator(
        task_id = "process_profile",
        conn_id = "spark_default",
        application="/opt/airflow/code/profile_user.py",
        conf={"snapshot_date": "{{ params.snapshot_date }}"}
    )
    load_to_mongo = SparkSubmitOperator(
        task_id = "load_to_mongo",
        conn_id = "spark_default",
        application="/opt/airflow/code/load.py",
        conf={"snapshot_date": "{{ params.snapshot_date }}"}
    )
    process_profile >> load_to_mongo
