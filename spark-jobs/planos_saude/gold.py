from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("Load Gold - Planos de Saúde") \
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

silver_precificacao = "s3a://plano/silver/precificacao/"
silver_caracteristica = "s3a://plano/silver/caracteristicas/"
silver_operadora = "s3a://plano/silver/operadora/"
gold_planos = "s3a://plano/gold/"

df_precificacao = spark.read.format("delta").load(silver_precificacao)
df_caracteristica = spark.read.format("delta").load(silver_caracteristica)
df_operadora = spark.read.format("delta").load(silver_operadora)

# faixa etária
dim_faixa_df = df_precificacao.select("cd_faixa_etaria", "faixa_etaria_desc").dropDuplicates() \
    .withColumn("sk_faixa_etaria", monotonically_increasing_id()+1)
dim_faixa_df.write.format("delta").mode("overwrite").save(f"{gold_planos}/dim_faixa")

# tempo
dim_tempo_df = df_precificacao.select("ano_mes", "ano", "mes").dropDuplicates() \
    .withColumn("sk_tempo", monotonically_increasing_id()+1)
dim_tempo_df.write.format("delta").mode("overwrite").save(f"{gold_planos}/dim_tempo")

# operadora
dim_operadora_df = df_operadora.alias("o") \
    .join(df_caracteristica.select(col("registro_operadora"), col("razao_social")).alias("c"),
        col("o.reg_ans") == col("c.registro_operadora"), "left") \
    .groupBy(col("o.reg_ans")) \
    .agg(first("razao_social", ignorenulls=True).alias("razao_social")) \
    .withColumn("sk_operadora", monotonically_increasing_id() + 1)
dim_operadora_df.write.format("delta").mode("overwrite").save(f"{gold_planos}/dim_operadora")

# plano
dim_plano_df = df_caracteristica.alias("c") \
    .join(df_precificacao.select("id_plano", "nt_tipo").dropDuplicates().alias("p"),"id_plano","left") \
    .join(dim_operadora_df.alias("o"), col("c.registro_operadora") == col("o.reg_ans"), "left") \
    .select(
        col("c.id_plano"),
        col("c.nm_plano"),
        col("c.cd_plano"),
        col("c.obstetricia"),
        col("c.cobertura"),
        col("c.tipo_financiamento"),
        col("c.registro_operadora"),
        col("c.situacao_plano"),
        col("p.nt_tipo"),
        col("o.sk_operadora")
    ) \
    .dropDuplicates(["id_plano"]).withColumn("sk_plano", monotonically_increasing_id() + 1)
dim_plano_df.write.format("delta").mode("overwrite").save(f"{gold_planos}/dim_plano")

# nota técnica
dim_nota_df = df_precificacao.select("cd_nota").dropDuplicates() \
    .withColumn("sk_nota", monotonically_increasing_id()+1)
dim_nota_df.write.format("delta").mode("overwrite").save(f"{gold_planos}/dim_nota")


# TABELA FATO
fato_planos_df = df_precificacao.alias("s") \
    .join(broadcast(dim_faixa_df.alias("f")),
          col("s.cd_faixa_etaria") == col("f.cd_faixa_etaria"), "inner") \
    .join(broadcast(dim_tempo_df.alias("t")),
          col("s.ano_mes") == col("t.ano_mes"), "inner") \
    .join(broadcast(dim_plano_df.alias("p")),
          col("s.id_plano") == col("p.id_plano"), "inner") \
    .join(broadcast(dim_nota_df.alias("n")),
          col("s.cd_nota") == col("n.cd_nota"), "inner") \
    .select(
        col("p.sk_plano"),
        col("p.sk_operadora"),
        col("n.sk_nota"),
        col("f.sk_faixa_etaria"),
        col("t.sk_tempo"),
        col("t.mes"),
        col("t.ano"),
        col("s.vcm"),
        col("s.pct_desp_ass"),
        col("s.pct_carreg"),
        col("s.pct_carreg_admin"),
        col("s.pct_carreg_coml"),
        col("s.pct_carreg_lucro")
    )

# salvar
fato_planos_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("maxRecordsPerFile", 1000000) \
    .partitionBy("ano", "mes") \
    .save(f"{gold_planos}/fato_planos")

fato_planos_df.show()