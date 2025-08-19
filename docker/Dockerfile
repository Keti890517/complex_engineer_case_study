FROM apache/airflow:2.9.0

# Install system dependencies
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential curl unzip && \
    rm -rf /var/lib/apt/lists/*

# Copy Python requirements and install as airflow user
COPY docker/requirements.txt /requirements.txt
USER airflow
RUN pip install --no-cache-dir -r /requirements.txt && \
    pip install --no-cache-dir --upgrade pytest

# Copy init script as root, set ownership and make executable
USER root
COPY docker/init_airflow.sh /opt/airflow/init_airflow.sh
RUN chown airflow: /opt/airflow/init_airflow.sh && chmod +x /opt/airflow/init_airflow.sh

# ✅ Fix permissions for pytest cache
RUN mkdir -p /opt/airflow/tests/.pytest_cache && chown -R airflow: /opt/airflow/tests

# Switch back to airflow user for runtime
USER airflow