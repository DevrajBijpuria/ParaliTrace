from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from common import HOME, PY, START, DEFAULT_ARGS, SPARK_POOL

with DAG(
    dag_id="ingest_fires",
    description="NASA FIRMS live fire detections -> Kafka -> Spark -> S3",
    schedule="15 */3 * * *",
    start_date=START,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["paralitrace", "ingest"],
) as dag:

    produce_fires = BashOperator(
        task_id="produce_fires",
        bash_command=f"cd {HOME}/Kafka && {PY} producer.py live",
    )
    consume_fires = BashOperator(
        task_id="consume_fires",
        pool=SPARK_POOL,
        bash_command=(
            f"mkdir -p /tmp/spark/fires && cd {HOME}/Kafka && "
            f"SPARK_TMP=/tmp/spark/fires {PY} firm_consumer.py; "
            "status=$?; rm -rf /tmp/spark/fires; exit $status"
        ),
    )

    produce_fires >> consume_fires