FROM apache/airflow:2.10.2-python3.10

USER root

RUN apt-get update && apt-get install -y \
    build-essential \
    default-jdk \
    libpq-dev \
    && apt-get clean

USER airflow

# 🔥 IMPORTANT: use constraints file so airflow does NOT break
ARG AIRFLOW_VERSION=2.10.2
ARG PYTHON_VERSION=3.10
ARG CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

RUN pip install --no-cache-dir \
    --constraint "${CONSTRAINT_URL}" \
    apache-airflow-providers-apache-spark \
    boto3 \
    psycopg2-binary
