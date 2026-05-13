from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os

spark = SparkSession.builder \
    .appName("Load Bronze - Acidentes Ferroviários") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "admin123") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .getOrCreate()

# caminhos no MinIO
jsons = ["acidentes_2004-2020.json", "acidentes_2020-2025.json", "acidentes_2020-2026.json"]

for json_file in jsons:
    lz = f"s3a://acidentesferroviarios/lz/{json_file}"
    print("==========================INICIANDO==========================")
    df = spark.read.option("multiline", "true").json(lz)
    print("==========================SCHEMA==========================")
    df.printSchema()
    print("==========================FIM==========================")
    coluna = df.columns[0]
    df = df.select(
        explode(col(f"`{coluna}`")).alias("acidente")
    )
    df = df.select("acidente.*")
    df = df.withColumnRenamed("Perímetro_Urbano", "perimetro_urbano") \
        .withColumnRenamed("Perímetro Urbano", "perimetro_urbano")
    nome_pasta = json_file.replace(".json", "")

    bronze_path = f"s3a://acidentesferroviarios/bronze/{nome_pasta}"


    df.write \
        .format("delta") \
        .mode("overwrite") \
        .save(bronze_path)
    
    print(f"========================== {json_file} SALVO ==========================")
