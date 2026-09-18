---
name: aws-etl-athena
description: Contexto e gotchas operacionais do projeto AWS-ETL-Athena (pipeline local Redis->Spark->MinIO->Hive/Trino orquestrada por Airflow, simulando AWS). Use sempre que for subir, testar, depurar ou modificar este projeto.
---

# AWS-ETL-Athena — pipeline local simulando AWS

Pipeline de dados end-to-end 100% local via Docker Compose, simulando uma arquitetura AWS real: eventos
sintéticos de preços diários de ações da B3 chegam no Redis, são processados por PySpark, gravados como
Parquet particionado no MinIO (S3), catalogados no Hive Metastore e consultados via Trino. Tudo orquestrado
pelo Airflow. Ver `README.md` para instruções de uso end-to-end e a tabela de equivalência com serviços AWS.

## Ambiente de shell (Windows)

Este projeto foi desenvolvido numa máquina Windows com **dois shells que não compartilham estado**:
- **PowerShell**: onde o Docker Desktop está instalado e onde rodam `docker` / `docker compose`.
- **WSL (Ubuntu)**: onde vive a `.venv` Python usada pelo `producer/producer.py` (criada com o Python
  nativo do WSL — uma `.venv` criada pelo Python do Windows usa `Scripts/`, não `bin/`, e não funciona lá).

A integração Docker Desktop ↔ WSL **não está habilitada** nesta configuração, então `docker` não existe
dentro do WSL. Rode comandos Docker sempre via PowerShell; rode Python/producer sempre via WSL.

Cada chamada de PowerShell é um processo novo — `$env:Path` não persiste entre chamadas. Sempre inclua
isto no início de qualquer comando PowerShell que precise de `docker`/`aws`/`git`:
```powershell
$machinePath = [System.Environment]::GetEnvironmentVariable("Path","Machine")
$userPath = [System.Environment]::GetEnvironmentVariable("Path","User")
$env:Path = $machinePath + ";" + $userPath
```

Para rodar comandos do WSL a partir do PowerShell: `wsl -e bash -c "cd /mnt/c/Users/.../AWS-ETL-Athena && ..."`.

**Pegadinha de encoding:** `Get-Content arquivo.sql | docker exec -i trino trino ...` no PowerShell insere
um BOM UTF-8 que quebra o parser do Trino ("mismatched input '﻿'"). Use `cmd /c "docker exec -i trino
trino --catalog hive --schema default < sql\queries.sql"` em vez de pipe do PowerShell, ou rode via WSL
bash (onde `<` funciona normalmente).

## Gotchas de infraestrutura já resolvidos (não redescobrir)

Essas correções já estão aplicadas nos arquivos do projeto — documentado aqui só para explicar o *porquê*
caso algo precise ser tocado de novo:

- **`minio/minio` e `minio/mc` saíram do Docker Hub** (out/2025, mudança de licenciamento da MinIO Inc).
  Usamos `quay.io/minio/minio` e `quay.io/minio/mc` (espelho congelado, sem novas atualizações).
- **`bitnami/spark` saiu do Hub gratuito** (Broadcom). Usamos `bitnamilegacy/spark:3.5.1` — também
  congelado, mas ainda puxável.
- **`apache-airflow-providers-apache-spark` sem versão travada** puxa uma versão recente que exige
  `apache-airflow-core>=3.x`, incompatível com nossa imagem base `apache/airflow:2.10.3`, e faz o `pip`
  travar em backtracking (`ResolutionTooDeep`). Travado em `4.1.5` (última compatível com Airflow 2.x) no
  `airflow/Dockerfile`.
- **Trino `:latest` mudou o schema de config do S3** entre versões: propriedades antigas `hive.s3.*` foram
  substituídas por `fs.s3.enabled=true` + `s3.*` (ver `trino/catalog/hive.properties`). Por isso a imagem
  está travada em `trinodb/trino:483` no `docker-compose.yml` — não trocar para `:latest` sem revalidar as
  propriedades primeiro.
- **`bitsondatadev/hive-metastore:latest` tem um `entrypoint.sh` desatualizado**: o script publicado na
  imagem ignora a variável `METASTORE_TYPE` (só existe na versão mais nova do script, nunca republicada) e
  fica preso esperando MySQL na porta 3306. Substituímos por `hive-metastore/entrypoint.sh` (montado via
  volume) que espera Postgres na 5432 de verdade.
- **Essa mesma imagem não inclui o driver JDBC do Postgres.** `hive-metastore/Dockerfile` baixa
  `postgresql-42.7.4.jar` na build.
- **A conexão Spark do Airflow não funciona via `AIRFLOW_CONN_SPARK_DEFAULT` em formato de URI padrão.** O
  `SparkSubmitOperator` monta o `--master` como `f"{connection.host}:{connection.port}"` sem prefixar o
  esquema sozinho — então o campo `host` da Connection precisa conter `spark://spark-master` **já com o
  prefixo embutido**. Isso não dá pra expressar num env var de URI (o parser engoliria `spark://` como
  `conn_type`). Por isso a conexão é criada via CLI no `command` do serviço `airflow-init` no
  `docker-compose.yml`, não por variável de ambiente.
- **O driver do Spark roda dentro do container do Airflow**, não no cluster: o `SparkSubmitOperator` usa
  deploy-mode `client` por padrão, então o processo que chama `spark-submit` (dentro do `airflow-scheduler`)
  também precisa dos jars `hadoop-aws` + `aws-java-sdk-bundle` para falar com S3/MinIO — não bastam só no
  `spark/Dockerfile` do cluster. Ver a segunda etapa `RUN` do `airflow/Dockerfile`.
- **Duas DAG runs podem competir pela mesma fila do Redis.** Como `drenar_fila_redis()` faz `RPOP` até
  esvaziar, se duas execuções da DAG rodarem em paralelo (ex: uma manual + uma agendada disparadas juntas),
  uma delas drena tudo e a outra encontra fila vazia. Isso não é um bug do código em si, mas um
  comportamento a ter em mente ao testar/disparar a DAG múltiplas vezes seguidas — evite triggers
  concorrentes se quiser resultados previsíveis.
- **DAGs pausadas não processam nem runs disparadas manualmente** nesta versão do Airflow — rode
  `airflow dags unpause pipeline_b3_precos_acoes` antes de disparar, ou a run fica em `queued` para sempre.

## Fluxo de teste ponta a ponta

```powershell
# 1) subir tudo (primeira vez baixa/builda ~11 imagens, demora)
docker compose up -d --build

# 2) gerar eventos (via WSL)
wsl -e bash -c "cd /mnt/c/Users/edusc/OneDrive/Documentos/Projetos/AWS-ETL-Athena && source .venv/bin/activate && python producer/producer.py"

# 3) disparar e acompanhar a DAG
docker exec airflow-scheduler airflow dags trigger pipeline_b3_precos_acoes
docker exec airflow-scheduler airflow dags list-runs -d pipeline_b3_precos_acoes

# 4) validar dados via Trino
cmd /c "docker exec -i trino trino --catalog hive --schema default < sql\queries.sql"
```

Se algo falhar, os logs de task do Airflow ficam em
`/opt/airflow/logs/dag_id=.../run_id=.../task_id=.../attempt=N.log` dentro do container
(`docker exec airflow-scheduler cat <caminho>`) — a CLI desta versão do Airflow não tem `airflow tasks logs`.
