FROM apache/airflow:3.3.2-python3.12

# Java for Spark (default-jre = Java 17 or 21, both supported by Spark 4)
# libgomp1 = OpenMP runtime that LightGBM needs at import time
USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends default-jre-headless procps libgomp1 \
 && apt-get clean && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java

USER airflow

# Separate virtualenvs so pipeline and dbt libraries never clash with Airflow's own
COPY --chown=airflow:root requirements-pipeline.txt requirements-dbt.txt /tmp/
RUN python -m venv /home/airflow/venvs/pipeline \
 && /home/airflow/venvs/pipeline/bin/pip install --no-cache-dir -r /tmp/requirements-pipeline.txt \
 && python -m venv /home/airflow/venvs/dbt \
 && /home/airflow/venvs/dbt/bin/pip install --no-cache-dir -r /tmp/requirements-dbt.txt

# Spark downloads its Kafka/Avro/S3 jars here; created now so the cache volume is writable
RUN mkdir -p /home/airflow/.ivy2 /home/airflow/.ivy2.5.2 /tmp/spark