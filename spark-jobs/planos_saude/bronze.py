from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("Load Bronze - Planos de Saúde") \
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
lz_precificacao = "s3a://plano/landing_zone/precificacao/saude_precificacao.csv"
bronze_precificacao = "s3a://plano/bronze/precificacao"
lz_caracteristica = "s3a://plano/landing_zone/caracteristicas/caracteristicas_plano.csv"
bronze_caracteristica = "s3a://plano/bronze/caracteristicas"
lz_operadora = "s3a://plano/landing_zone/operadora/registro_operadoras.csv"
bronze_operadora = "s3a://plano/bronze/operadora"

# leitura dos arquivos
print("================================INICIANDO================================")
df_precificacao = spark.read.option("header", "true").option("inferSchema", "true").option("delimiter", ";").csv(lz_precificacao)
df_caracteristica = spark.read.option("header", "true").option("inferSchema", "true").option("delimiter", ";").csv(lz_caracteristica)
df_operadora = spark.read.option("header", "true").option("inferSchema", "true").option("delimiter", ";").csv(lz_operadora)
print("================================FINALIZADO================================")

# particionamento por mês e ano (PRECIFICAÇÃO)
df_precificacao = df_precificacao.withColumn("ano", year ("ANO_MES")) \
    .withColumn("mes", month ("ANO_MES"))

# OPERADORA
# substituir o espaço por _
# substituir o - por _
# tirar os acentos do nome da coluna
df_operadora = df_operadora.withColumnRenamed("reg ans", "reg_ans") \
    .withColumnRenamed("Operadora Medico-Hospitalar", "op_medico_hospitalar")

df_precificacao.printSchema()
df_caracteristica.printSchema()
df_operadora.printSchema()


# salvar (PRECIFICAÇÃO)
df_precificacao.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("ano", "mes") \
    .save(bronze_precificacao)

# salvar (CARACTERÍSTICAS)
df_caracteristica.write \
    .format("delta") \
    .mode("overwrite") \
    .save(bronze_caracteristica)

# salvar (OPERADORAS)
df_operadora.write \
    .format("delta") \
    .mode("overwrite") \
    .save(bronze_operadora)