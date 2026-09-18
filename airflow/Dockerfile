FROM apache/airflow:2.10.3-python3.11

USER root

# Java é exigido pelo PySpark mesmo só para *enviar* um job a um cluster remoto (spark-submit)
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

USER airflow

# pyspark aqui precisa ser a MESMA versão do cluster Spark (ver spark/Dockerfile)
# apache-airflow-providers-apache-spark traz o SparkSubmitOperator usado na DAG.
# Versão travada em 4.1.5: é a última compatível com Airflow 2.x — a partir da
# série 4.10.0 o provider passou a exigir Airflow >=2.11, o que puxa uma árvore
# de dependências incompatível com nossa imagem base 2.10.3.
# trino: cliente Python usado pela DAG para criar a tabela e atualizar as partições
RUN pip install --no-cache-dir \
    pyspark==3.5.1 \
    apache-airflow-providers-apache-spark==4.1.5 \
    boto3==1.43.97 \
    redis==8.1.0 \
    trino==0.331.0
