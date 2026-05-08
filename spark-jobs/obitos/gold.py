from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("Load Gold - Obitos DF") \
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
silver_path = "s3a://datalake/silver/"
gold_path = "s3a://datalake/gold/"

df_silver = spark.read.format("delta").load(silver_path)

# dimensão tempo
tb_destino = "dim_tempo"
dim_tempo_df = df_silver.select("ano_obito", "mes_obito").dropDuplicates()
dim_tempo_df = dim_tempo_df.withColumn("sk_tempo", monotonically_increasing_id()+1)
dim_tempo_df.write.format("delta").mode("overwrite").save(f"{gold_path}/{tb_destino}")

# dimensão local moradia
tb_destino = "dim_local_moradia"
dim_local_moradia_df = df_silver.select(
    "uf_moradia", "reg_moradia", "reg_adm_moradia"
).dropDuplicates()
dim_local_moradia_df = dim_local_moradia_df.withColumn("sk_local_moradia", monotonically_increasing_id()+1)
dim_local_moradia_df.write.format("delta").mode("overwrite").save(f"{gold_path}/{tb_destino}")

# dimensão local atendimento 
tb_destino = "dim_local_atendimento"
dim_local_atendimento_df = df_silver.select(
    "reg_atendimento", "sigla_estab_atend"
).dropDuplicates()
dim_local_atendimento_df = dim_local_atendimento_df.withColumn("sk_local_atendimento", monotonically_increasing_id()+1)
dim_local_atendimento_df.write.format("delta").mode("overwrite").save(f"{gold_path}/{tb_destino}")

# dimensão pessoa
tb_destino = "dim_pessoa"
dim_pessoa_df = df_silver.select(
    "faixa_etaria", "idade_obito", "sexo_obito", "cor_obito"
).dropDuplicates()
dim_pessoa_df = dim_pessoa_df.withColumn("sk_pessoa", monotonically_increasing_id()+1)
dim_pessoa_df.write.format("delta").mode("overwrite").save(f"{gold_path}/{tb_destino}")

# dimensão causa
tb_destino = "dim_causa"
dim_causa_df = df_silver.select(
    "cid_obito", "desc_cid_obito", "cid_capitulo"
).dropDuplicates()
dim_causa_df = dim_causa_df.withColumn("sk_causa", monotonically_increasing_id()+1)
dim_causa_df.write.format("delta").mode("overwrite").save(f"{gold_path}/{tb_destino}")

# dimensão tipo óbito
tb_destino = "dim_tipo_obito"
dim_tipo_obito_df = df_silver.select("tipo_obito", "tipo_violencia").dropDuplicates()
dim_tipo_obito_df = dim_tipo_obito_df.withColumn("sk_tipo_obito", monotonically_increasing_id()+1)
dim_tipo_obito_df.write.format("delta").mode("overwrite").save(f"{gold_path}/{tb_destino}")

# tabela fato
tb_destino = "fato_obito"
fato_obito_df = df_silver.alias("s") \
    .join(broadcast(
        dim_tempo_df.select("ano_obito", "mes_obito", "sk_tempo").alias("t")), (col("s.ano_obito") == col("t.ano_obito")) &
        (col("s.mes_obito") == col("t.mes_obito")), "inner") \
    .join(broadcast(
        dim_local_moradia_df.select("uf_moradia", "reg_moradia", "reg_adm_moradia", "sk_local_moradia").alias("lm")),
        (col("s.uf_moradia") == col("lm.uf_moradia")) &
        (col("s.reg_moradia") == col("lm.reg_moradia")) &
        (col("s.reg_adm_moradia") == col ("lm.reg_adm_moradia")), "inner") \
    .join(broadcast(
        dim_local_atendimento_df.select("reg_atendimento", "sigla_estab_atend", "sk_local_atendimento").alias("la")),
        (col("s.reg_atendimento") == col("la.reg_atendimento")) &
        (col("s.sigla_estab_atend") == col("la.sigla_estab_atend")),
        "inner") \
    .join(broadcast(
        dim_pessoa_df.select("faixa_etaria", "idade_obito", "sexo_obito", "cor_obito", "sk_pessoa").alias("p")),
        (col("s.faixa_etaria") == col("p.faixa_etaria")) &
        (col("s.idade_obito") == col("p.idade_obito")) & 
        (col("s.sexo_obito") == col("p.sexo_obito")) &
        (col("s.cor_obito") == col("p.cor_obito")),
        "inner") \
    .join(broadcast(
        dim_causa_df.select("cid_obito", "desc_cid_obito", "cid_capitulo", "sk_causa"). alias("c")),
        (col("s.cid_obito") == col("c.cid_obito")) &
        (col("s.desc_cid_obito") == col("c.desc_cid_obito")) &
        (col("s.cid_capitulo") == col("c.cid_capitulo")),
        "inner") \
    .join(broadcast(
        dim_tipo_obito_df.select("tipo_obito", "tipo_violencia", "sk_tipo_obito").alias("to")),
        (col("s.tipo_obito") == col("to.tipo_obito")) &
        (col("s.tipo_violencia") == col("to.tipo_violencia")),
        "inner") \
    .select(
        col("t.sk_tempo"),
        col("lm.sk_local_moradia"),
        col("la.sk_local_atendimento"),
        col("p.sk_pessoa"),
        col("c.sk_causa"),
        col("to.sk_tipo_obito"),
        col("s.mes_obito")
    ).withColumn("quantidade_obitos", lit(1))

fato_obito_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("maxRecordsPerFile", 1000000) \
    .partitionBy("mes_obito") \
    .save(f"{gold_path}/{tb_destino}")