from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("Load Silver - Banco Central do Brasil") \
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
bronze_bancoCentral = "s3a://bancocentral/bronze/"
silver_bancoCentral = "s3a://bancocentral/silver/"

# leitura e tratamento de dados
df = spark.read.format("delta").load(bronze_bancoCentral)

"""
TRATAMENTO DE DADOS COMEÇA AQUI
* renomear o nome das colunas
* mudar o cast de ano, denominação e quantidade produzida
"""

# renomear o nome das colunas
df_silver = df.withColumnRenamed("DESC_ano", "ano") \
    .withColumnRenamed("DESC_deno", "denominacao") \
    .withColumnRenamed("DESC_especie", "especie") \
    .withColumnRenamed("DESC_quant_produzida", "quant_produzida")

# mudar o cast
df_silver = df_silver.withColumn("ano", expr("try_cast(ano as INTEGER)")) \
    .withColumn("denominacao", expr("try_cast(denominacao as DOUBLE)")) \
    .withColumn("quant_produzida", expr("try_cast(quant_produzida as INTEGER)"))

# salvar e particionar por ano
df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("ano") \
    .save(silver_bancoCentral)