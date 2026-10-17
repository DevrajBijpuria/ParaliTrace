from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from common import HOME, PY, DBT, START, DEFAULT_ARGS

with DAG(
    dag_id="transform_and_forecast",
    description="Check source freshness -> dbt build (models + tests) -> 24/48h PM2.5 forecast",
    schedule="45 */3 * * *",
    start_date=START,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["paralitrace", "transform", "ml"],
) as dag:

    check_freshness = BashOperator(
        task_id="check_source_freshness",
        bash_command=f"cd {HOME}/project && {DBT} source freshness",
        retries=0,
    )
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"cd {HOME}/project && {DBT} deps && {DBT} build",
        trigger_rule="all_done",
    )
    forecast = BashOperator(
        task_id="forecast_pm25",
        bash_command=f"cd {HOME} && {PY} ml/predict_live.py",
    )

    check_freshness >> dbt_build >> forecast