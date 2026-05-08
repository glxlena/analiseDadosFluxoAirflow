from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("Load Bronze - Obitos DF") \
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
lz_path_in = "s3a://datalake/landing_zone/obitos_2022_DF.csv"
bronze_path = "s3a://datalake/bronze/"

# leitura
print("INICIANDO LEITURA...")
df = spark.read \
    .option("inferSchema", "true") \
    .csv(lz_path_in, header = True)
print("LEITURA FINALIZADA")
df.orderBy("i_idade_anos").select("i_idade_anos").distinct().show(truncate=False, n=df.count())

# tratamento dos dados
df = df.filter(col("i_idade_anos").isNotNull() & col("i_idade_anos").rlike(r"^\d+$"))
df.orderBy("i_idade_anos").select("i_idade_anos").distinct().show(truncate=False, n=df.count())

df_obitos = df \
    .withColumnRenamed("i_faixa_etaria", "faixa_etaria") \
    .withColumnRenamed("i_mes_obito", "mes_obito") \
    .withColumn("i_ano_obito", expr("try_cast(i_ano_obito as INTEGER)")) \
    .withColumn("i_idade_anos", expr("try_cast(i_idade_anos as INTEGER)")) \
    .withColumn("mes_obito", expr("try_cast (mes_obito as INTEGER)"))

df_obitos.write \
    .mode("overwrite") \
    .partitionBy("mes_obito") \
    .format("delta").save(bronze_path)

df_obitos.show()