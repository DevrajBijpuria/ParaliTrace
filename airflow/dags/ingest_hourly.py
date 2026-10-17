from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from common import HOME, PY, START, DEFAULT_ARGS, SPARK_POOL

with DAG(
    dag_id="ingest_hourly",
    description="OpenAQ + Open-Meteo live data -> Kafka -> Spark -> S3 (Snowpipe loads automatically)",
    schedule="5 * * * *",
    start_date=START,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["paralitrace", "ingest"],
) as dag:

    produce_aq = BashOperator(
        task_id="produce_aq",
        bash_command=f"cd {HOME}/Kafka && {PY} aq_producer.py live",
    )
    consume_aq = BashOperator(
        task_id="consume_aq",
        pool=SPARK_POOL,
        bash_command=(
            f"mkdir -p /tmp/spark/aq && cd {HOME}/Kafka && "
            f"SPARK_TMP=/tmp/spark/aq {PY} aq_consumer.py; "
            "status=$?; rm -rf /tmp/spark/aq; exit $status"
        ),
    )
    produce_weather = BashOperator(
        task_id="produce_weather",
        bash_command=f"cd {HOME}/Kafka && {PY} weather_producer.py live",
    )
    consume_weather = BashOperator(
        task_id="consume_weather",
        pool=SPARK_POOL,
        bash_command=(
            f"mkdir -p /tmp/spark/weather && cd {HOME}/Kafka && "
            f"SPARK_TMP=/tmp/spark/weather {PY} weather_consumer.py; "
            "status=$?; rm -rf /tmp/spark/weather; exit $status"
        ),
    )

    produce_aq >> consume_aq
    produce_weather >> consume_weather