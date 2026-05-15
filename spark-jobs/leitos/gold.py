from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("Load Gold - Leitos de Hospitais") \
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

silver = "s3a://leitos/silver"
gold = "s3a://leitos/gold"

df = spark.read.format("delta").load(silver)

# dimensão localização
dim_localizacao_df = df.select("regiao", "municipio", "uf").dropDuplicates() \
        .withColumn("sk_localizacao", monotonically_increasing_id()+1)
dim_localizacao_df = dim_localizacao_df.cache()
dim_localizacao_df.count()
dim_localizacao_df.write.format("delta").mode("overwrite").save(f"{gold}/dim_localizacao")

# dimensão gestão unidade
dim_gestao_df = df.select("tipo_gestao", "desc_tipo_gestao").dropDuplicates() \
        .withColumn("sk_gestao", monotonically_increasing_id()+1)
dim_gestao_df = dim_gestao_df.cache()
dim_gestao_df.count()
dim_gestao_df.write.format("delta").mode("overwrite").save(f"{gold}/dim_gestao")

# dimensão natureza jurídica
dim_natureza_juridica_df = df.select("desc_juri_hosp").dropDuplicates() \
        .withColumn("sk_natureza_juridica", monotonically_increasing_id()+1)
dim_natureza_juridica_df = dim_natureza_juridica_df.cache()
dim_natureza_juridica_df.count()
dim_natureza_juridica_df.write.format("delta").mode("overwrite").save(f"{gold}/dim_natureza_juridica")

# dimensão tipo unidade
dim_tipo_unidade_df = df.select("cd_tipo_uni", "desc_tipo_uni").dropDuplicates() \
        .withColumn("sk_tipo_unidade", monotonically_increasing_id()+1)
dim_tipo_unidade_df = dim_tipo_unidade_df.cache()
dim_tipo_unidade_df.count()
dim_tipo_unidade_df.write.format("delta").mode("overwrite").save(f"{gold}/dim_tipo_unidade")

# tabela fato
fato_leitos_df = df.alias("s") \
        .join(broadcast(dim_localizacao_df.alias("l")),
            [col("s.regiao") == col("l.regiao"),
             col("s.municipio") == col("l.municipio"),
             col("s.uf") == col("l.uf")
            ], "inner"
        ) \
        .join(broadcast(dim_gestao_df.alias("g")),
            [col("s.tipo_gestao") == col("g.tipo_gestao"),
             col("s.desc_tipo_gestao") == col("g.desc_tipo_gestao")
            ], "inner"
        ) \
        .join(broadcast(dim_natureza_juridica_df.alias("n")),
            col("s.desc_juri_hosp") == col("n.desc_juri_hosp"),
            "inner"
        ) \
        .join(broadcast(dim_tipo_unidade_df.alias("t")),
            [col("s.cd_tipo_uni") == col("t.cd_tipo_uni"),
             col("s.desc_tipo_uni") == col("t.desc_tipo_uni")
            ], "inner"
        ) \
        .select(
            col("l.sk_localizacao"),
            col("g.sk_gestao"),
            col("n.sk_natureza_juridica"),
            col("t.sk_tipo_unidade"),
            col("s.qnt_uti_adulto"),
            col("s.qnt_uti_coronariana"),
            col("s.qnt_leitos_uti"),
            col("s.qnt_uti_neonatal"),
            col("s.qnt_uti_pediatrico"),
            col("s.qnt_uti_queimado"),
            col("s.qnt_uti_sus_adulto"),
            col("s.qnt_uti_sus_coronariana"),
            col("s.qnt_uti_sus"),
            col("s.qnt_uti_sus_neonatal"),
            col("s.qnt_uti_sus_pediatrico"),
            col("s.qnt_uti_sus_queimado"),
            col("s.qnt_total_leitos"),
            col("s.qnt_total_leitos_sus"),
            col("s.regiao"),
            col("s.uf")
        )

fato_leitos_df.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("regiao", "uf") \
        .save(f"{gold}/fato_leitos")

fato_leitos_df.show(50)