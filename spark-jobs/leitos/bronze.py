from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("Load Bronze - Leitos de Hospitais") \
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

lz = "s3a://leitos/lz/dados_leitos.json"
bronze = "s3a://leitos/bronze"

# iniciando a leitura
print("================================INICIANDO================================")
df_bronze = spark.read.option("multiline", "true").json(lz)
# explode no array "hospitais_leitos"
df_bronze = df_bronze.select(explode(col("hospitais_leitos")).alias("leitos")).select("leitos.*")
print("================================FINALIZADO================================")

# evidências
print("================================SCHEMA================================")
df_bronze.printSchema()
print("================================SHOW================================")
df_bronze.show(5, truncate=False)

# tratar nome da coluna que tem , no meio
df_bronze = df_bronze.drop("motivo_da_desabilitacao_do_hospital,_caso_esteja_desabilitado")

# salvar
df_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .save(bronze)