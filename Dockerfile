FROM apache/airflow:3.2.0

USER root

RUN apt-get update &&\
    apt-get install -y docker.io &&\
    apt-get clean

USER airflow
RUN pip install --no-cache-dir minio \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.2.0/constraints-3.13.txt"