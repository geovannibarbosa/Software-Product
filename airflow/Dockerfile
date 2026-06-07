FROM apache/airflow:2.8.4-python3.10

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends docker.io \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark==4.7.0 \
    apache-airflow-providers-postgres==5.10.0
