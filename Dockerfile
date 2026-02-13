# Use official Airflow image which already contains Airflow
FROM apache/airflow:2.10.2-python3.10

# Switch to root to install system dependencies
USER root

RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    && apt-get clean

# Switch back to airflow user (required)
USER airflow

# Copy only the requirements
COPY requirements.txt /requirements.txt

# Install Python packages WITHOUT touching airflow internals
RUN pip install --no-cache-dir -r /requirements.txt
