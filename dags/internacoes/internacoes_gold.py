from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from datetime import datetime
import pendulum


with DAG(
    dag_id="internacoes_gold_dag",
    schedule="11 08 * * *",  # roda depois da ingestão
    start_date=pendulum.datetime(2026, 4, 26, tz="America/Sao_Paulo"),
    catchup=True
) as dag:

    # espera a dag silver terminar
    wait_for_silver = ExternalTaskSensor(
        task_id="wait_for_silver",
        external_dag_id="internacoes_silver_dag",
        external_task_id="run_internacoes_silver",
        mode="poke",
        timeout=1800,
        poke_interval=30
    )

    # executa o Spark
    run_spark_job = BashOperator(
        task_id="run_internacoes_gold",
        bash_command="""
        docker exec spark-master spark-submit \
        --master spark://spark-master:7077 \
        --conf spark.driver.host=spark-master \
        --conf spark.driver.bindAddress=0.0.0.0 \
        --conf spark.hadoop.fs.s3a.access.key=admin \
        --conf spark.hadoop.fs.s3a.secret.key=admin123 \
        --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
        --conf spark.hadoop.fs.s3a.path.style.access=true \
        --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
        --conf spark.jars.ivy=/tmp/.ivy \
        --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
        --conf spark.hadoop.fs.s3a.endpoint.region=us-east-1 \
        --conf spark.hadoop.fs.s3a.connection.maximum=100 \
        --conf spark.hadoop.fs.s3a.attempts.maximum=1 \
        /opt/spark-jobs/internacoes/gold.py
        """
    )

    wait_for_silver >> run_spark_job