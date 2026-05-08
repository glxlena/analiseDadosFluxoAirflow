from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("Load Silver - Obitos DF") \
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
bronze_path = "s3a://datalake/bronze/"
silver_path = "s3a://datalake/silver/"

# tratamento de dados
df_bronze = spark.read.format("delta").load(bronze_path)
df_bronze.printSchema()
df_silver = (df_bronze
    .withColumnRenamed("i_ano_obito", "ano_obito") \
    .withColumnRenamed("i_cid_capitulo", "cid_capitulo") \
    .withColumnRenamed("i_cid_obito", "cid_obito") \
    .withColumnRenamed("i_desc_emprego", "obito_emprego") \
    .withColumnRenamed("i_desc_cid_obito", "desc_cid_obito") \
    .withColumnRenamed("i_desc_local_obito", "local_obito") \
    .withColumnRenamed("i_desc_raca_cor", "cor_obito") \
    .withColumnRenamed("i_desc_radf_res", "reg_adm_moradia") \
    .withColumnRenamed("i_desc_regiao_saude_estab", "reg_atendimento") \
    .withColumnRenamed("i_desc_regiao_saude_res", "reg_moradia") \
    .withColumnRenamed("i_desc_sigla_estab_cnes", "sigla_estab_atend") \
    .withColumnRenamed("i_desc_tipo_obito", "tipo_obito") \
    .withColumnRenamed("i_desc_tipo_violencia", "tipo_violencia") \
    .withColumnRenamed("i_desc_uf_res", "uf_moradia") \
    .withColumnRenamed("i_idade_anos", "idade_obito") \
    .withColumnRenamed("i_sexo", "sexo_obito") \
    .withColumnRenamed("i_faixa_etaira", "faixa_etaria") \
    .withColumnRenamed("i_mes_obito", "mes_obito"))

df_silver.printSchema()

df_silver.write \
    .mode("overwrite") \
    .partitionBy("mes_obito", "uf_moradia") \
    .format("delta").save(silver_path)