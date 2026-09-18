# AWS ETL Athena (local) — Pipeline de Preços Diários da B3

Pipeline de dados end-to-end, 100% local via Docker, simulando uma arquitetura real de dados na AWS:
eventos de preços de ações da B3 chegam em tempo real, são processados e ficam disponíveis para consulta SQL analítica.

## Arquitetura

```
producer.py --(LPUSH)--> Redis --(RPOP)--> Spark --(Parquet)--> MinIO (S3)
                                                                     |
                                                            Hive Metastore
                                                            (catálogo de tabelas)
                                                                     |
                                                                  Trino (SQL)
                                                                     ^
                                                          Airflow (orquestra tudo)
```

| Componente local | Equivalente na AWS |
|---|---|
| Redis | Kinesis Data Streams / SQS |
| PySpark (cluster master + worker) | AWS Glue Job / EMR |
| MinIO | S3 |
| Hive Metastore | AWS Glue Data Catalog |
| Trino | AWS Athena |
| Airflow | Step Functions / MWAA |

## Pré-requisitos

- Docker Desktop instalado e rodando (com pelo menos ~8 GB de RAM alocados — o ambiente sobe ~11 containers)
- Python 3.11+ com `venv` (usado só pelo `producer.py`, fora do Docker)
- (Opcional) conta AWS configurada via `aws configure`, caso queira apontar o pipeline pro S3 real em vez do MinIO

## 1. Configurar o ambiente

Copie/edite o `.env` na raiz do projeto se quiser trocar alguma credencial (usuário/senha do MinIO, Postgres, Airflow). Os valores padrão já funcionam out-of-the-box para uso local.

Crie e ative a virtualenv (usada apenas para rodar o `producer.py` a partir do seu host):

```bash
python3 -m venv .venv
source .venv/bin/activate      # Linux/WSL/Mac
# ou .venv\Scripts\activate    # PowerShell (Windows nativo)

pip install -r requirements.txt
```

## 2. Subir a infraestrutura

```bash
docker compose up -d --build
```

Na primeira vez isso baixa e builda várias imagens (Spark, Airflow, Trino, Hive Metastore) — pode levar alguns minutos.
Acompanhe com:

```bash
docker compose ps
```

Espere os serviços com `healthcheck` ficarem `healthy` antes de prosseguir (`redis`, `minio`, `postgres-hive`, `postgres-airflow`).

### Onde acessar cada serviço

| Serviço | URL / Endereço | Login |
|---|---|---|
| MinIO Console | http://localhost:9001 | `minioadmin` / `minioadmin123` |
| Trino UI | http://localhost:8080 | — |
| Spark Master UI | http://localhost:8081 | — |
| Airflow UI | http://localhost:8085 | `admin` / `admin` |
| Redis | `localhost:6379` | — |

## 3. Gerar eventos (simular o "tempo real")

Com a `.venv` ativada:

```bash
python producer/producer.py
```

Isso envia 100 eventos de preços (por padrão) para a fila `b3:precos_acoes` no Redis, um a cada meio segundo.

## 4. Rodar a pipeline (Airflow)

1. Acesse http://localhost:8085 (login `admin` / `admin`)
2. Localize a DAG `pipeline_b3_precos_acoes`
3. Ative o toggle (ela vem pausada por padrão) e clique em **Trigger DAG** (▶) para rodar manualmente, sem esperar o agendamento diário

A DAG tem duas tasks:
- `processar_precos_com_spark`: drena a fila do Redis, processa com Spark e grava Parquet no MinIO, particionado por `data_pregao`
- `atualizar_catalogo_trino`: cria a tabela (se não existir) e registra as novas partições no catálogo

Alternativa via linha de comando, sem usar a UI:

```bash
docker exec airflow-scheduler airflow dags trigger pipeline_b3_precos_acoes
```

## 5. Verificar os dados no MinIO

No console do MinIO (http://localhost:9001), navegue até o bucket `datalake` → `warehouse/precos_acoes/` — você deve ver pastas do tipo `data_pregao=2026-09-17/` com arquivos `.parquet` dentro.

## 6. Consultar via Trino (SQL analítico)

Abra um shell interativo do Trino:

```bash
docker exec -it trino trino --catalog hive --schema default
```

E rode, por exemplo:

```sql
SELECT * FROM hive.default.precos_acoes LIMIT 10;
```

Para rodar todas as consultas de exemplo de uma vez (`sql/queries.sql`):

```bash
docker exec -i trino trino --catalog hive --schema default < sql/queries.sql
```

## Testando o fluxo completo, do zero

```bash
docker compose up -d --build          # sobe tudo
source .venv/bin/activate
python producer/producer.py           # gera eventos no Redis
docker exec airflow-scheduler airflow dags trigger pipeline_b3_precos_acoes
# aguarde a DAG concluir (acompanhe em http://localhost:8085)
docker exec -i trino trino --catalog hive --schema default < sql/queries.sql
```

## Encerrando o ambiente

```bash
docker compose down          # para os containers, mantém os dados (volumes)
docker compose down -v       # para os containers E apaga todos os dados/volumes
```

## Extra: usando o S3 real da AWS em vez do MinIO

O `spark_job.py` já lê a URL de destino e as credenciais via variáveis de ambiente (`OUTPUT_PATH`, `MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`). Para gravar no S3 real:

1. Tenha um bucket S3 criado (ex: `aws-etl-athena-datalake-eduschelle`) e um usuário IAM com permissão de S3
2. Na `SparkSubmitOperator` da DAG (`dags/pipeline_dag.py`), passe `env_vars` com:
   - `OUTPUT_PATH=s3a://SEU-BUCKET/warehouse/precos_acoes/`
   - `MINIO_ENDPOINT=` (deixe em branco/remova — S3 real não usa endpoint customizado)
   - `MINIO_ACCESS_KEY` / `MINIO_SECRET_KEY` com as credenciais reais da AWS

Nenhuma linha de lógica de transformação muda — é só configuração de destino.
