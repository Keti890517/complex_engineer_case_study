FROM apache/airflow:2.8.1

# Install system deps you may need (unzip, curl, etc.)
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential curl unzip && \
    rm -rf /var/lib/apt/lists/*

USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Copy init script
COPY init_airflow.sh /opt/airflow/init_airflow.sh
RUN chmod +x /opt/airflow/init_airflow.sh