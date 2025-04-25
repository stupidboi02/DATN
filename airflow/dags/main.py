from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

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
    description ="Amazing pipeline",
    schedule_interval='@daily',
    start_date=datetime(2019, 10, 1),
    catchup=False,
    tags=["example"],
) as dag:
    produce_to_broker = BashOperator(
    task_id="produce_to_broker",
    bash_command="python3 /var/lib/producer.py {{ ds }}",
)
    extract_to_hdfs = SparkSubmitOperator(
        task_id = "extract_to_hdfs",
        conn_id = "spark_default",
        jars = "/opt/airflow/code/spark-sql-kafka-0-10_2.12-3.5.1.jar",
        application="/opt/airflow/code/extract.py"
    )
    
    produce_to_broker >> extract_to_hdfs