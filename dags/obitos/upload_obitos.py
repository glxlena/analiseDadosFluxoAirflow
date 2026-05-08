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
        Filename="/opt/airflow/dados/obitos/obitos_2022_DF.csv",  # caminho dentro do container
        Bucket="datalake",
        Key="landing_zone/obitos_2022_DF.csv"
    )


with DAG(
    dag_id="upload_obitos",
    schedule="11 08 * * *",
    start_date=pendulum.datetime(2026, 4, 23, tz="America/Sao_Paulo"),
    catchup=False
) as dag:

    upload_task = PythonOperator(
        task_id="upload_csv_obitos",
        python_callable=upload_to_minio
    )

upload_task 