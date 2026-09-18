# bitnami/spark saiu do Docker Hub gratuito (Broadcom); a versão 3.5.1 congelada
# ainda existe no espelho "legacy", sem mais atualizações/patches
FROM bitnamilegacy/spark:3.5.1

USER root

# hadoop-aws: implementa o filesystem s3a:// que o Spark usa para ler/escrever no S3/MinIO
# aws-java-sdk-bundle: SDK da AWS que o hadoop-aws usa por baixo dos panos (autenticação, HTTP)
# Versões travadas para bater exatamente com o Hadoop 3.3.4 embutido no Spark 3.5.1.
ENV HADOOP_AWS_VERSION=3.3.4
ENV AWS_SDK_BUNDLE_VERSION=1.12.262

RUN install_packages curl && \
    curl -sS -L -o /opt/bitnami/spark/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
      "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" && \
    curl -sS -L -o /opt/bitnami/spark/jars/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar \
      "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_BUNDLE_VERSION}/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar"

USER 1001
