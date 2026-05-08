from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import DataFrame

spark = SparkSession.builder \
    .appName("Load Silver - Planos de Saúde") \
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
bronze_precificacao = "s3a://plano/bronze/precificacao/"
silver_precificacao = "s3a://plano/silver/precificacao/"
bronze_caracteristicas = "s3a://plano/bronze/caracteristicas/"
silver_caracteristicas = "s3a://plano/silver/caracteristicas/"
bronze_operadora = "s3a://plano/bronze/operadora/"
silver_operadora = "s3a://plano/silver/operadora/"

# tratamento de dados (PRECIFICAÇÃO)
df_precificacao:DataFrame = spark.read.format("delta").load(bronze_precificacao)

"""
TRATAMENTO DE DADOS (PRECIFICAÇÃO):
* padronizar nomes das colunas (deixar tudo minúsculo)
* mudar a , dos valores para .
* Fazer o schema correto
* valores para double
* ver se é possível colocar a faixa etária correspondente de cada código (talvez fazer outra coluna ?)
"""
# padronizar nomes das colunas (deixar tudo minúsculo)
df_precificacao = df_precificacao.toDF(*[c.lower() for c in df_precificacao.columns])

# mudar a , dos valores para .
# Fazer o schema correto
# valores para double
df_precificacao = df_precificacao.withColumn("vcm", regexp_replace(col("vcm"), ",", ".")) \
    .withColumn("pct_desp_ass", regexp_replace(col("pct_desp_ass"), ",", ".")) \
    .withColumn("pct_carreg", regexp_replace(col("pct_carreg"), ",", ".")) \
    .withColumn("pct_carreg_admin", regexp_replace(col("pct_carreg_admin"), ",", ".")) \
    .withColumn("pct_carreg_coml", regexp_replace(col("pct_carreg_coml"), ",", ".")) \
    .withColumn("pct_carreg_lucro", regexp_replace(col("pct_carreg_lucro"), ",", ".")) \
    .withColumn("vcm", expr("try_cast(vcm as DOUBLE)")) \
    .withColumn("pct_desp_ass", expr("try_cast(pct_desp_ass as DOUBLE)")) \
    .withColumn("pct_carreg", expr("try_cast(pct_carreg as DOUBLE)")) \
    .withColumn("pct_carreg_admin", expr("try_cast(pct_carreg_admin as DOUBLE)")) \
    .withColumn("pct_carreg_coml", expr("try_cast(pct_carreg_coml as DOUBLE)")) \
    .withColumn("pct_carreg_lucro", expr("try_cast(pct_carreg_lucro as DOUBLE)"))

# coluna nova de faixa etaria
df_precificacao = df_precificacao.withColumn(
    "faixa_etaria_desc",
    when(col("cd_faixa_etaria") == 1, "0-18")
    .when(col("cd_faixa_etaria") == 2, "19-23")
    .when(col("cd_faixa_etaria") == 3, "24-28")
    .when(col("cd_faixa_etaria") == 4, "29-33")
    .when(col("cd_faixa_etaria") == 5, "34-38")
    .when(col("cd_faixa_etaria") == 6, "39-43")
    .when(col("cd_faixa_etaria") == 7, "44-48")
    .when(col("cd_faixa_etaria") == 8, "49-53")
    .when(col("cd_faixa_etaria") == 9, "54-58")
    .when(col("cd_faixa_etaria") == 10, "59+")
)

# evidências
df_precificacao.printSchema()
df_precificacao.show(20)

# tratamento de dados (CARACTERÍSTICAS)
df_caracteristica:DataFrame = spark.read.format("delta").load(bronze_caracteristicas)
"""
TRATAMENTO DE DADOS (CARACTERÍSTICAS):
* deixar todos os nomes das colunas com letra minúscula
* lg_odontologico = boolean
* padronizar dados da obstetricia
* remover possíveis dados duplicados por id_plano
"""
# deixar todos os nomes das colunas com letra minúscula
df_caracteristica = df_caracteristica.toDF(*[c.lower() for c in df_caracteristica.columns])

# lg_odontologico = boolean
df_caracteristica = df_caracteristica.withColumn("lg_odontologico", expr("try_cast(lg_odontologico as BOOLEAN)"))

# padronizar dados da obstetricia
df_caracteristica = df_caracteristica.withColumn(
    "obstetricia",
    when(col("obstetricia") == "COM OBSTETRICIA", "COM")
    .when(col("obstetricia") == "SEM OBSTETRICIA", "SEM")
    .when(col("obstetricia") == "NAO SE APLICA", "NAO_APLICA")
    .when(col("obstetricia") == "NAO IDENTIFICADO", "NAO_IDENTIFICADO")
    .otherwise("OUTROS")
)

# remover possíveis dados duplicados por id_plano
df_caracteristica = df_caracteristica.dropDuplicates(["id_plano"])

# tratamento de dados (OPERADORA)
df_operadora:DataFrame = spark.read.format("delta").load(bronze_operadora)
"""
TRATAMENTO DE DADOS (OPERADORA):
* deixar todos os nomes das colunas com letra minúscula
* colocar as colunas de número como int
* colocar as colunas de data como date
"""
# deixar todos os nomes das colunas com letra minúscula
df_operadora = df_operadora.toDF(*[c.lower() for c in df_operadora.columns])

# colocar as colunas de número como int
df_operadora = df_operadora.withColumn("reg_ans", expr("try_cast(reg_ans as INTEGER)")) \
    .withColumn("prazo_validade", expr("try_cast(prazo_validade as INTEGER)"))

# colocar as colunas de data como date
df_operadora = df_operadora.withColumn("data_da_primeira_acreditacao", to_date(col("data_da_primeira_acreditacao"), "dd/MM/yyyy")) \
    .withColumn("inicio_validade", to_date(col("inicio_validade"), "dd/MM/yyyy")) \
    .withColumn("fim_validade", to_date(col("fim_validade"), "dd/MM/yyyy")) 


df_precificacao.printSchema()
df_caracteristica.printSchema()
df_operadora.printSchema()

# salvar (PRECIFICAÇÃO)
df_precificacao.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("ano", "mes") \
    .save(silver_precificacao)
# salvar (CARACTERÍSTICAS)
df_caracteristica.write \
    .format("delta") \
    .mode("overwrite") \
    .save(silver_caracteristicas)
# salvar (OPERADORA)
df_operadora.write \
    .format("delta") \
    .mode("overwrite") \
    .save(silver_operadora)