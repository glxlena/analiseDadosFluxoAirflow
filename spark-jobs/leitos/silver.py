from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("Load Silver - Leitos de Hospitais") \
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

bronze = "s3a://leitos/bronze"
silver = "s3a://leitos/silver"

df = spark.read.format("delta").load(bronze)

df = df.withColumnRenamed("codigo_do_tipo_da_unidade", "cd_tipo_uni") \
    .withColumnRenamed("descricao_da_natureza_juridica_do_hosptial", "desc_juri_hosp") \
    .withColumnRenamed("descricao_do_tipo_da_unidade", "desc_tipo_uni") \
    .withColumnRenamed("nome_da_regiao_do_brasil_onde_fica_o_hospital", "regiao") \
    .withColumnRenamed("nome_do_municipio_onde_fica_o_hospital", "municipio") \
    .withColumnRenamed("quantidade_de_leitos_de_uti_adulto_do_hosptial", "qnt_uti_adulto") \
    .withColumnRenamed("quantidade_de_leitos_de_uti_coronariana_do_hosptial", "qnt_uti_coronariana") \
    .withColumnRenamed("quantidade_de_leitos_de_uti_do_hosptial", "qnt_leitos_uti") \
    .withColumnRenamed("quantidade_de_leitos_de_uti_neonatal_do_hosptial", "qnt_uti_neonatal") \
    .withColumnRenamed("quantidade_de_leitos_de_uti_pediatrico_do_hosptial", "qnt_uti_pediatrico") \
    .withColumnRenamed("quantidade_de_leitos_de_uti_queimado_do_hosptial", "qnt_uti_queimado") \
    .withColumnRenamed("quantidade_de_leitos_de_uti_sus_adulto_do_hosptial", "qnt_uti_sus_adulto") \
    .withColumnRenamed("quantidade_de_leitos_de_uti_sus_coronariana_do_hosptial", "qnt_uti_sus_coronariana") \
    .withColumnRenamed("quantidade_de_leitos_de_uti_sus_do_hosptial", "qnt_uti_sus") \
    .withColumnRenamed("quantidade_de_leitos_de_uti_sus_neonatal_do_hosptial", "qnt_uti_sus_neonatal") \
    .withColumnRenamed("quantidade_de_leitos_de_uti_sus_pediatrico_do_hosptial", "qnt_uti_sus_pediatrico") \
    .withColumnRenamed("quantidade_de_leitos_de_uti_sus_queimado_do_hosptial", "qnt_uti_sus_queimado") \
    .withColumnRenamed("quantidade_total_de_leitos_do_hosptial", "qnt_total_leitos") \
    .withColumnRenamed("quantidade_total_de_leitos_sus_do_hosptial", "qnt_total_leitos_sus") \
    .withColumnRenamed("tipo_da_gestao_do_hospital", "tipo_gestao") \
    .withColumnRenamed("unidade_da_federacao_onde_fica_o_hospital", "uf")

df = df.withColumn("qnt_uti_adulto", )

df = df.drop(
    "codigo_ibge_do_municipio",
    "complemento_do_endereco_do_hospital", 
    "enderco_do_hospital",
    "natureza_juridica_do_hospital",
    "nome_da_razao_social_do_hospital",
    "nome_do_bairro_do_endereco_do_hosptial",
    "nome_do_hospital",
    "numero_do_cep_do_hospital",
    "numero_do_endereco_do_hospital"
)

df.printSchema()
df.show(5)

df.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("regiao", "uf") \
    .save(silver)