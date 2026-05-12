from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import pendulum
import boto3


def upload_to_minio():
    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",  # importante!
        aws_access_key_id="admin",
        aws_secret_access_key="admin123"
    )

    s3.upload_file(
        Filename="/opt/airflow/dados/internacoes/internacoes.csv",  # caminho dentro do container
        Bucket="database",
        Key="landing_zone/internacoes.csv"
    )


with DAG(
    dag_id="upload_internacoes",
    schedule="18 9 * * *",
    start_date=pendulum.datetime(2026, 5, 12, tz="America/Sao_Paulo"),
    catchup=False
) as dag:

    upload_task = PythonOperator(
        task_id="upload_csv_internacoes",
        python_callable=upload_to_minio
    )

upload_task 