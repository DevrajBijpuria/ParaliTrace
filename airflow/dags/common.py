from datetime import timedelta
import pendulum

HOME = "/opt/paralitrace"
PY = "/home/airflow/venvs/pipeline/bin/python"
DBT = "/home/airflow/venvs/dbt/bin/dbt"
START = pendulum.datetime(2026, 10, 14, tz="UTC")
SPARK_POOL = "spark"

DEFAULT_ARGS = {
    "owner": "paralitrace",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}