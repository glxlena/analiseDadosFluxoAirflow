from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import pendulum
import boto3


def upload_planos():
    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",  # importante!
        aws_access_key_id="admin",
        aws_secret_access_key="admin123"
    )

    s3.upload_file(
        Filename="/opt/airflow/dados/planos_saude/saude_precificacao.csv",  # caminho dentro do container
        Bucket="plano",
        Key="landing_zone/precificacao/saude_precificacao.csv"
    )

def upload_caracteristicas():
    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",  # importante!
        aws_access_key_id="admin",
        aws_secret_access_key="admin123"
    )

    s3.upload_file(
        Filename="/opt/airflow/dados/planos_saude/caracteristicas_plano.csv",  # caminho dentro do container
        Bucket="plano",
        Key="landing_zone/caracteristicas/caracteristicas_plano.csv"
    )

def upload_operadoras():
    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",  # importante!
        aws_access_key_id="admin",
        aws_secret_access_key="admin123"
    )

    s3.upload_file(
        Filename="/opt/airflow/dados/planos_saude/registro_operadoras.csv",  # caminho dentro do container
        Bucket="plano",
        Key="landing_zone/operadora/registro_operadoras.csv"
    )

with DAG(
    dag_id="upload_plano",
    schedule="56 9 * * *",
    start_date=pendulum.datetime(2026, 5, 4, tz="America/Sao_Paulo"),
    catchup=False
) as dag:

    upload_plano = PythonOperator(
        task_id="upload_csv_plano",
        python_callable=upload_planos # puxa a função a ser executada
    )
    upload_caracteristica = PythonOperator(
        task_id="upload_csv_caracteristica",
        python_callable=upload_caracteristicas
    )
    upload_operadora = PythonOperator(
        task_id="upload_csv_operadora",
        python_callable=upload_operadoras
    )

upload_plano >> upload_caracteristica >> upload_operadora