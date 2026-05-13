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

# ESSE TRATAMENTO DE DADOS SURGIU POR CONTA DA NECESSIDADE DE UNIR OS 3 JSONS EM UMA ÚNICA TABELA, DANDO UM 
# DROP EM DADOS DUPLICADOS E SALVANDO TUDO EM UMA PASTA CONSOLIDADA PARA FACILITAR A CRIAÇÃO 
# DE DIMENSÕES E TABELA FATO NA CAMADA GOLD

df_2004_2020 = spark.read.format("delta").load(
    "s3a://acidentesferroviarios/silver/acidentes_2004-2020"
)

df_2020_2025 = spark.read.format("delta").load(
    "s3a://acidentesferroviarios/silver/acidentes_2020-2025"
)

df_2020_2026 = spark.read.format("delta").load(
    "s3a://acidentesferroviarios/silver/acidentes_2020-2026"
)

# unir tudo
df_consolidado = df_2004_2020.unionByName(df_2020_2025) \
    .unionByName(df_2020_2026)

# preencher linha nula
df_consolidado = df_consolidado.fillna({
    "linha": "nao_informada"
})

# remover duplicados
df_consolidado = df_consolidado.dropDuplicates([
    "data_hora_ocorrencia",
    "linha",
    "municipio",
    "concessionaria",
    "natureza"
])

# salvar silver consolidada
silver_consolidado = "s3a://acidentesferroviarios/silver/acidentes_consolidado"

df_consolidado.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("ano", "mes") \
    .save(silver_consolidado)

df_consolidado.show(50)