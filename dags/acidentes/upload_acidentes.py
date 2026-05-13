from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import pendulum
import boto3

# outra maneira de mandar os arquivos json de forma automatica sem precisar citar eles manualmente:
# import os
# pasta = "/opt/airflow/dados/acidentes"
# jsons = os.listdir(pasta)
######## ou ########
# jsons = [f for f in os.listdir(pasta) if f.endswith(".json")]

jsons = ["acidentes_2004-2020.json", "acidentes_2020-2025.json", "acidentes_2020-2026.json"]

def upload_to_minio():
    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="admin",
        aws_secret_access_key="admin123"
    )

    for json_file in jsons:
        path = f"/opt/airflow/dados/acidentes/{json_file}"

        s3.upload_file(
            Filename=path,
            Bucket="acidentesferroviarios",
            Key=f"lz/{json_file}"
        )

        print(f"=========== {json_file} enviado com sucesso ===========")

with DAG(
    dag_id="upload_acidentes",
    schedule="30 14 * * *",
    start_date=pendulum.datetime(2026, 5, 12, tz="America/Sao_Paulo"),
    catchup=False
) as dag:
    upload_task=PythonOperator(
        task_id="upload_jsons_acidentes",
        python_callable=upload_to_minio
    )

upload_task