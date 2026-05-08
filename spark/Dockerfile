FROM apache/spark:3.5.8-scala2.12-java17-python3-r-ubuntu

USER root

# Instala curl para baixar os jars
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# A pasta correta de jars na imagem oficial geralmente é /opt/spark/jars
# Garanta que ela existe
RUN mkdir -p /opt/spark/jars

# 1. Hadoop AWS (Conector S3A)
RUN curl -L -o /opt/spark/jars/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar

# 2. AWS Java SDK Bundle (OBRIGATÓRIO para o Hadoop-AWS 3.3.4)
# Note que o hadoop-aws 3.3.4 casa bem com o SDK 1.12.x
RUN curl -L -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# 3. Delta Lake
RUN curl -L -o /opt/spark/jars/delta-spark_2.12-3.3.2.jar \
    https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.3.2/delta-spark_2.12-3.3.2.jar && \
    curl -L -o /opt/spark/jars/delta-storage-3.3.2.jar \
    https://repo1.maven.org/maven2/io/delta/delta-storage/3.3.2/delta-storage-3.3.2.jar

# Instalação de pacotes python
RUN pip install --no-cache-dir minio delta-spark==3.3.2 boto3 botocore dotenv

# Ajuste de permissões para o usuário spark
RUN chmod 644 /opt/spark/jars/*.jar

# Voltamos para o usuário spark
USER spark

# Definimos o diretório de trabalho
WORKDIR /opt/spark