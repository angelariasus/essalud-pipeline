import sys
sys.path.append("/opt/airflow/bi")

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def _resolve_years(**context):
    """Retorna [year] si se inyectó vía dag_run.conf, o None si es ejecución mensual."""
    dag_run = context.get("dag_run")
    if dag_run and dag_run.conf and "year" in dag_run.conf:
        return [dag_run.conf["year"]]
    return None


def _run_silver(**context):
    """Ejecuta SilverPipeline con los años resueltos desde XCom."""
    ti = context["ti"]
    years = ti.xcom_pull(task_ids="resolve_years")
    from app.pipelines.silver_layer import SilverPipeline
    SilverPipeline().run(years=years)


with DAG(
    "ocds_silver_pipeline",
    default_args=default_args,
    description=(
        "Transforma datos Bronze en modelo dimensional y carga el Data Warehouse "
        "(Silver + Gold layers)."
    ),
    schedule_interval="@monthly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ocds", "silver", "gold", "dw"],
) as dag:

    resolve_years = PythonOperator(
        task_id="resolve_years",
        python_callable=_resolve_years,
    )

    run_silver = PythonOperator(
        task_id="run_silver_pipeline",
        python_callable=_run_silver,
    )

    resolve_years >> run_silver
