from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("Load Gold - Banco Central do Brasil") \
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
silver_bancoCentral = "s3a://bancocentral/silver/"
gold_bancoCentral = "s3a://bancocentral/gold/"

df = spark.read.format("delta").load(silver_bancoCentral)

# dimensão tempo
dim_tempo_df = df.select("ano").dropDuplicates() \
    .withColumn("sk_tempo", monotonically_increasing_id()+1)
dim_tempo_df.write.format("delta").mode("overwrite").save(f"{gold_bancoCentral}/dim_tempo")

# dimensão denominação
dim_denominacao_df = df.select("denominacao").dropDuplicates() \
    .withColumn("sk_denominacao", monotonically_increasing_id()+1)
dim_denominacao_df.write.format("delta").mode("overwrite").save(f"{gold_bancoCentral}/dim_denominacao")

# dimensão espécie
dim_especie_df = df.select("especie").dropDuplicates() \
    .withColumn("sk_especie", monotonically_increasing_id()+1)
dim_especie_df.write.format("delta").mode("overwrite").save(f"{gold_bancoCentral}/dim_especie")

# tabela fato
fato_df = df.alias("s") \
    .join(
        broadcast(dim_tempo_df.alias("t")),
        col("s.ano") == col("t.ano"), "inner"
    ) \
    .join(
        broadcast(dim_denominacao_df.alias("d")),
        col("s.denominacao") == col("d.denominacao"), "inner"
    ) \
    .join(
        broadcast(dim_especie_df.alias("e")),
        col("s.especie") == col("e.especie"), "inner"
    ) \
    .select(
        col("t.sk_tempo"),
        col("d.sk_denominacao"),
        col("e.sk_especie"),
        col("s.quant_produzida").alias("quant_produzida"),
        col("t.ano").alias("ano")
    )

fato_df.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("ano") \
    .save(f"{gold_bancoCentral}/fato")

fato_df.show()