from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("Load Gold - Internações") \
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
silver_path = "s3a://database/silver/"
gold_path = "s3a://database/gold/"

df_silver = spark.read.format("delta").load(silver_path)

# especialidade
dim_especialidade_df = df_silver.select("especialidade").dropDuplicates() \
    .withColumn("sk_especialidade", monotonically_increasing_id()+1)
dim_especialidade_df.write.format("delta").mode("overwrite").save(f"{gold_path}/dim_especialidade")

# municipio
dim_municipio_df = df_silver.select("municipio").dropDuplicates() \
    .withColumn("sk_municipio", monotonically_increasing_id()+1)
dim_municipio_df.write.format("delta").mode("overwrite").save(f"{gold_path}/dim_municipio")

# data
dim_data_df = df_silver.select("data_internacao", "ano", "mes", "dia").dropDuplicates() \
    .withColumn("sk_data", monotonically_increasing_id()+1)
dim_data_df.write.format("delta").mode("overwrite").save(f"{gold_path}/dim_data")

# sexo
dim_sx_df = df_silver.select("sexo").dropDuplicates() \
    .withColumn("sk_sexo", monotonically_increasing_id()+1)
dim_sx_df.write.format("delta").mode("overwrite").save(f"{gold_path}/dim_sx")

#fato 
fato_internacoes_df = df_silver.alias("s") \
    .join(broadcast(
        dim_especialidade_df.alias("e")
    ), (col("s.especialidade") == col("e.especialidade")), "inner") \
    .join(broadcast(
        dim_municipio_df.alias("m")
    ), (col("s.municipio") == col("m.municipio")), "inner") \
    .join(broadcast(
        dim_data_df.alias("d")
    ), col("s.data_internacao") == col("d.data_internacao"), "inner") \
    .join(broadcast(
        dim_sx_df.alias("sx")
    ), col("s.sexo") == col("sx.sexo"), "inner") \
    .select(
        col("e.sk_especialidade"),
        col("m.sk_municipio"),
        col("sx.sk_sexo"),
        col("d.sk_data"),
        col("d.mes"),
        col("s.idade")
    ).withColumn("qtd_internacoes", lit(1))

# salvar
fato_internacoes_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("maxRecordsPerFile", 1000000) \
    .partitionBy("mes") \
    .save(f"{gold_path}/fato_internacoes")

fato_internacoes_df.show()