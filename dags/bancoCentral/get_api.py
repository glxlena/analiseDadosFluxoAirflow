from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import pendulum
import boto3
import requests
import json


def upload_to_minio():
    # faz requisição da api
    url = "https://olinda.bcb.gov.br/olinda/servico/mecir_prog_anual_producao/versao/v1/odata/TodosDadosProducao?%24format=json"

    response = requests.get(
        url,
        headers={"accept": "application/json;odata.metadata=minimal"}
    )

    # verifica se deu erro
    response.raise_for_status()

    dados = response.json()

    # salva o json localmente temporariamente
    caminho_arquivo = "/tmp/dados_banco.json"

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
        Bucket="bancocentral",
        Key="lz/dados_banco.json"
    )

    print("Arquivo enviado com sucesso!")


with DAG(
    dag_id="upload_api",
    schedule="11 08 * * *",
    start_date=pendulum.datetime(2026, 5, 11, tz="America/Sao_Paulo"),
    catchup=False
) as dag:

    upload_task = PythonOperator(
        task_id="upload_apiBanco",
        python_callable=upload_to_minio
    )

upload_task