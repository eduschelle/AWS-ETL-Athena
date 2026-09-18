# bitsondatadev/hive-metastore vem pronta só para MySQL; adicionamos o driver
# JDBC do PostgreSQL, que é o backend que usamos (ver docker-compose.yml).
FROM bitsondatadev/hive-metastore:latest

RUN curl -sS -L -o /opt/apache-hive-metastore-3.0.0-bin/lib/postgresql-42.7.4.jar \
    "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar"
