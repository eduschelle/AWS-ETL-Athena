#!/bin/sh
# Substitui o entrypoint.sh embutido na imagem bitsondatadev/hive-metastore:latest,
# que está travado em MySQL/porta 3306 e ignora a variável METASTORE_TYPE
# (o script mais novo, com suporte a Postgres, existe no GitHub do projeto mas
# nunca foi publicado numa nova versão da imagem no Docker Hub).

export HADOOP_HOME=/opt/hadoop-3.2.0
export HADOOP_CLASSPATH=${HADOOP_HOME}/share/hadoop/tools/lib/aws-java-sdk-bundle-1.11.375.jar:${HADOOP_HOME}/share/hadoop/tools/lib/hadoop-aws-3.2.0.jar
export JAVA_HOME=/usr/local/openjdk-8
export METASTORE_DB_HOSTNAME=${METASTORE_DB_HOSTNAME:-localhost}
export METASTORE_DB_PORT=${METASTORE_DB_PORT:-5432}

echo "Waiting for database on ${METASTORE_DB_HOSTNAME}:${METASTORE_DB_PORT} ..."
while ! nc -z ${METASTORE_DB_HOSTNAME} ${METASTORE_DB_PORT}; do
  sleep 1
done

echo "Database on ${METASTORE_DB_HOSTNAME}:${METASTORE_DB_PORT} started"
echo "Init apache hive metastore (postgres) on ${METASTORE_DB_HOSTNAME}:${METASTORE_DB_PORT}"

/opt/apache-hive-metastore-3.0.0-bin/bin/schematool -initSchema -dbType postgres
/opt/apache-hive-metastore-3.0.0-bin/bin/start-metastore
