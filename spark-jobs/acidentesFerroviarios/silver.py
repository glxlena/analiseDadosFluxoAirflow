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

pastas = ["acidentes_2020-2025", "acidentes_2020-2026"]
for pasta in pastas:
    bronze_path = f"s3a://acidentesferroviarios/bronze/{pasta}"
    df_um = spark.read.format("delta").load(bronze_path)

    # deixar tudo lower case
    df_um = df_um.toDF(*[c.lower() for c in df_um.columns])

    # tirar os acentos e afins
    df_um = df_um.withColumnRenamed("estação_anterior", "estacao_anterior") \
        .withColumnRenamed("estação_posterior", "estacao_posterior") \
        .withColumnRenamed("interrupção", "interrupcao") \
        .withColumnRenamed("quilômetro_inicial", "quilometro_inicial") \
        .withColumnRenamed("serviço_transporte", "servico_transporte")
    
    # mudar o type das colunas necessárias + criação de data hora + ano e mês para particionamento
    df_um = df_um.withColumn("data_ocorrencia", to_date(col("data_ocorrencia"), "dd/MM/yyyy")) \
        .withColumn("data_hora_ocorrencia",when(col("hora_ocorrencia").isNotNull(),to_timestamp(concat_ws(" ", col("data_ocorrencia"), col("hora_ocorrencia")),"yyyy-MM-dd HH:mm"))) \
        .withColumn("n_feridos", expr("try_cast(n_feridos as INTEGER)")) \
        .withColumn("n_obitos", expr("try_cast(n_obitos as INTEGER)")) \
        .withColumn("ano", year("data_ocorrencia")) \
        .withColumn("mes", month("data_ocorrencia"))
        
    # drop nas colunas extras que não tem na tabela 2004-2020
    df_um = df_um.drop(
        "quilômetro_final",
        "causa_contibutiva",
        "n_trem",
        "double_stack",
        "prejuízo_financeiro"
    )

    silver_path = f"s3a://acidentesferroviarios/silver/{pasta}"
    df_um.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("ano", "mes") \
        .save(silver_path)
    
dado_extra = "s3a://acidentesferroviarios/bronze/acidentes_2004-2020"
df = spark.read.format("delta").load(dado_extra)
df = df.toDF(*[c.lower() for c in df.columns])
df = df.withColumnRenamed("estação_anterior", "estacao_anterior") \
    .withColumnRenamed("estação_posterior", "estacao_posterior") \
    .withColumnRenamed("interrupção", "interrupcao") \
    .withColumnRenamed("quilômetro_inicial", "quilometro_inicial") \
    .withColumnRenamed("serviço_transporte", "servico_transporte")
    
df = df.withColumn("data_ocorrencia", to_date(col("data_ocorrencia"), "dd/MM/yyyy")) \
    .withColumn("data_hora_ocorrencia",when(col("hora_ocorrencia").isNotNull(),to_timestamp(concat_ws(" ", col("data_ocorrencia"), col("hora_ocorrencia")),"yyyy-MM-dd HH:mm"))) \
    .withColumn("n_feridos", expr("try_cast(n_feridos as INTEGER)")) \
    .withColumn("n_obitos", expr("try_cast(n_obitos as INTEGER)")) \
    .withColumn("ano", year("data_ocorrencia")) \
    .withColumn("mes", month("data_ocorrencia"))

silver = "s3a://acidentesferroviarios/silver/acidentes_2004-2020"

df.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("ano", "mes") \
    .save(silver)

df_um.printSchema()
df_um.show()
df.printSchema()
df.show()