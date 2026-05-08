from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("Load Silver - Internações") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "admin123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .getOrCreate()

# caminhos no MinIO
bronze_path = "s3a://database/bronze/"
silver_path = "s3a://database/silver/"

# tratamento de dados
df = spark.read.format("delta").load(bronze_path)
df.printSchema()

'''
TRATAMENTO DE DADOS COMEÇA AQUI
* organização da ordem dos dias e meses, tirando o horário
* padronização dos nomes das cidades, todas sem acentos e em caps lock
* particionamento por sexo do pacientes e mês
'''

df = df.withColumn(
    "data_internacao",
    to_date(col("data_internacao"), "dd/MM/yyyy HH:mm")
)
df = df \
    .withColumn("ano", year("data_internacao")) \
    .withColumn("mes", month("data_internacao")) \
    .withColumn("dia", dayofmonth("data_internacao"))

df = df \
    .withColumn("municipio", upper(translate(col("municipio"),
        "áàãâäéèêëíìîïóòõôöúùûüç", "aaaaaeeeeiiiiooooouuuuc")))

df = df.withColumn("idade", expr("try_cast(idade as INTEGER)"))

# salvar na silver
df_silver = df
df_silver.printSchema()

df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("sexo", "mes") \
    .save(silver_path)

df_silver.show()