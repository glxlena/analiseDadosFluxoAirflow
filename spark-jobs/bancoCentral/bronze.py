from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("Load Bronze - Banco Central do Brasil") \
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
lz_bancoCentral = "s3a://bancocentral/lz/dados_banco.json"
bronze_bancoCentral = "s3a://bancocentral/bronze/"

# iniciando a leitura
print("================================INICIANDO================================")
df_bronze = spark.read.option("multiline", "true").json(lz_bancoCentral)
# explode no array "value"
df_bronze = df_bronze.select(explode(col("value")).alias("dados")).select("dados.*")
print("================================FINALIZADO================================")

# evidências
print("================================SCHEMA================================")
df_bronze.printSchema()
print("================================SHOW================================")
df_bronze.show(5, truncate=False)

# salvar
df_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .save(bronze_bancoCentral)