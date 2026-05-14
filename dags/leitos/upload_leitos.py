from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import pendulum
import boto3
import requests
import json


def upload_to_minio():
    # faz requisição da api
    url = "https://apidadosabertos.saude.gov.br/assistencia-a-saude/hospitais-e-leitos?limit=1000&offset=0"

    response = requests.get(
        url,
        headers={"accept": "application/json;odata.metadata=minimal"}
    )

    # verifica se deu erro
    response.raise_for_status()

    dados = response.json()

    # salva o json localmente temporariamente
    caminho_arquivo = "/tmp/dados_leitos.json"

    with open(caminho_arquivo, "w", encoding="utf-8") as f:
        json.dump(dados, f, ensure_ascii=False, indent=4)

    # Conecta no MinIO
    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="admin",
        aws_secret_access_key="admin123"
    )

    s3.upload_file(
        Filename=caminho_arquivo,
        Bucket="leitos",
        Key="lz/dados_leitos.json"
    )

    print("Arquivo enviado com sucesso!")


with DAG(
    dag_id="upload_leitos",
    schedule="11 08 * * *",
    start_date=pendulum.datetime(2026, 5, 11, tz="America/Sao_Paulo"),
    catchup=False
) as dag:

    upload_task = PythonOperator(
        task_id="upload_apiLeitos",
        python_callable=upload_to_minio
    )

upload_task