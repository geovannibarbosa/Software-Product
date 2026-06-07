# Ecommerce Zeta Dados

Pipeline local com Airflow, Spark e Postgres usando Docker Compose.

## Pre-requisitos

- Docker Desktop
- Git
- Windows com WSL 2 habilitado para o Docker Desktop

As bibliotecas Python do projeto estao em `requirements.txt`. Para rodar pelo Docker Compose, elas sao instaladas/fornecidas dentro dos containers:

- `pyspark==3.5.1`
- `apache-airflow-providers-apache-spark==4.7.0`
- `apache-airflow-providers-postgres==5.10.0`
- `psycopg2-binary==2.9.9`
- `python-dateutil==2.9.0.post0`

## Como subir o pipeline

No diretorio do projeto:

```powershell
docker compose up --build -d
```

Acesse:

- Airflow: <http://localhost:8080>
- Spark Master: <http://localhost:8081>

Credenciais do Airflow:

- Usuario: `admin`
- Senha: `admin`

Na tela do Airflow, habilite e execute a DAG `lakehouse_pipeline`.

## Comandos uteis

Ver logs do scheduler:

```powershell
docker compose logs -f airflow-scheduler
```

Ver logs do webserver:

```powershell
docker compose logs -f airflow-webserver
```

Parar tudo:

```powershell
docker compose down
```

Recriar banco de metadados do Airflow:

```powershell
docker compose down -v
docker compose up --build -d
```
