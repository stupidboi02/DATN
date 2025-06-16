from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models.param import Param
from datetime import timedelta
default_args={
        "depends_on_past": False,
        "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        'execution_timeout': timedelta(hours=1),
        }
with DAG(
    "ingest_data",
    default_args = default_args,
    description ="ingest data to pipeline",
    catchup=False,
    tags=["kafka"],
    params={
        "snapshot_date": Param("2019-10-1", type="string", title="Snapshot Date", description="Date for snapshot")
    }
) as dag:
    kafka_task = BashOperator(
        task_id = "stream_to_kafka",
        bash_command = "python3 /var/lib/producer.py '{{ params.snapshot_date }}'"
    )
    write_stream = SparkSubmitOperator(
        task_id = "stream_to_hdfs",
        application="/opt/airflow/code/extract.py",
        conn_id="spark_default",
        application_args=["{{ params.snapshot_date }}"],
        packages = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
        conf={
            "spark.master": "spark://spark-master:7077"
        },
    )
