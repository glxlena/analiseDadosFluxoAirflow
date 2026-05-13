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

silver = "s3a://acidentesferroviarios/silver/acidentes_consolidado"
gold = "s3a://acidentesferroviarios/gold"

df = spark.read.format("delta").load(silver)

# preencher dados null
df = df.fillna({
    "estacao_anterior": "nao_informada",
    "estacao_posterior": "nao_informada",
    "outra_ferrovia": "nao_informada",
    "pn": "nao_informada"
})

# dimensão tempo
dim_tempo_df = df.select("data_ocorrencia", "data_hora_ocorrencia", "ano", "mes").dropDuplicates() \
    .withColumn("hora", hour("data_hora_ocorrencia")) \
    .withColumn("sk_tempo", monotonically_increasing_id()+1)
dim_tempo_df.write.format("delta").mode("overwrite").save(f"{gold}/dim_tempo")

# dimensão localização
dim_localizacao_df = df.select("uf", "municipio", "perimetro_urbano").dropDuplicates() \
    .withColumn("sk_localizacao", monotonically_increasing_id()+1)
dim_localizacao_df.write.format("delta").mode("overwrite").save(f"{gold}/dim_localizacao")

# dimensão acidente
dim_acidente_df = df.select("gravidade", "natureza", "causa_direta", "interrupcao").dropDuplicates() \
    .withColumn("sk_acidente", monotonically_increasing_id()+1)
dim_acidente_df.write.format("delta").mode("overwrite").save(f"{gold}/dim_acidente")

# dimensão ferrovia
dim_ferrovia_df = df.select("concessionaria", "linha", "estacao_anterior", "estacao_posterior", "outra_ferrovia", "pn").dropDuplicates() \
    .withColumn("sk_ferrovia", monotonically_increasing_id()+1)
dim_ferrovia_df.write.format("delta").mode("overwrite").save(f"{gold}/dim_ferrovia")

# dimensão transporte
dim_transporte_df = df.select("servico_transporte", "mercadoria", "equipagem", "prefixo").dropDuplicates() \
    .withColumn("sk_transporte", monotonically_increasing_id()+1)
dim_transporte_df.write.format("delta").mode("overwrite").save(f"{gold}/dim_transporte")

# TABELA FATO
fato_df = df.alias("s") \
    .join(broadcast(dim_tempo_df.alias("t")),
    ["data_ocorrencia", "ano", "mes"],
    "inner"
    ) \
    .join(broadcast(dim_localizacao_df.alias("l")),
    ["uf", "municipio", "perimetro_urbano"],
    "inner"
    ) \
    .join(broadcast(dim_acidente_df.alias("a")),
    ["gravidade", "natureza", "causa_direta", "interrupcao"],
    "inner"      
    ) \
    .join(broadcast(dim_ferrovia_df.alias("f")),
    ["concessionaria", "linha", "estacao_anterior", "estacao_posterior", "outra_ferrovia", "pn"],
    "inner"      
    ) \
    .join(broadcast(dim_transporte_df.alias("tr")),
    ["servico_transporte", "mercadoria", "equipagem", "prefixo"],
    "inner"
    ) \
    .select(
        col("t.sk_tempo"),
        col("l.sk_localizacao"),
        col("a.sk_acidente"),
        col("f.sk_ferrovia"),
        col("tr.sk_transporte"),
        col("s.n_feridos"),
        col("s.n_obitos"),
        lit(1).alias("quantidade_acidentes"),
        col("s.ano"),
        col("s.mes")
    )

fato_df.write.format("delta").mode("overwrite").partitionBy("ano", "mes").save(f"{gold}/fato_acidentes_ferroviarios")

fato_df.show()