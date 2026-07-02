"""
DAG de las capas Silver + Gold (transformación y carga del DW).

Resuelve los años (de `dag_run.conf` o el default) y ejecuta el pipeline real:
Silver (aplana Bronze -> staging_flat), Synth (sintéticos 2024/2025 + adendas,
paso obligatorio antes de Gold: sin él 2024/2025 quedan casi vacíos) y Gold
(dimensiones -> destino). Al terminar dispara `ocds_alerting` (Fase 6).
Imports de `app.*` diferidos dentro de los callables (Airflow parsea seguido).
"""
import sys
from datetime import datetime, timedelta

sys.path.append("/opt/airflow/bi")

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

DEFAULT_YEARS = [2022, 2023, 2024, 2025]


def _resolve_years(**context):
    """Retorna los años desde dag_run.conf (year/years) o el default."""
    dag_run = context.get("dag_run")
    conf = (dag_run.conf if dag_run and dag_run.conf else {}) or {}
    if conf.get("years"):
        years = list(conf["years"])
    elif conf.get("year"):
        years = [conf["year"]]
    else:
        years = DEFAULT_YEARS
    context["ti"].xcom_push(key="years", value=years)
    return years


def _run_silver(**context):
    """Ejecuta SilverPipeline (Bronze -> staging_flat) para los años resueltos."""
    from app.pipelines.silver_layer import SilverPipeline

    years = context["ti"].xcom_pull(key="years", task_ids="resolve_years") or DEFAULT_YEARS
    SilverPipeline().run_silver(years=years)


def _run_synth(**context):
    """Genera los sintéticos (2025 CSV + boost 2024 + adendas) sobre staging_flat."""
    from app.services.synthetic_generator import run_synthetic

    dag_run = context.get("dag_run")
    conf = (dag_run.conf if dag_run and dag_run.conf else {}) or {}
    if conf.get("skip_synth"):
        print("skip_synth=true en dag_run.conf; se omite la generación sintética.")
        return
    run_synthetic(adicional_p=conf.get("adicional_rate"))


def _run_gold(**context):
    """Ejecuta GoldPipeline (staging_flat -> dimensiones -> destino)."""
    from app.pipelines.gold_layer import GoldPipeline

    dag_run = context.get("dag_run")
    conf = (dag_run.conf if dag_run and dag_run.conf else {}) or {}
    years = context["ti"].xcom_pull(key="years", task_ids="resolve_years") or DEFAULT_YEARS
    GoldPipeline().run(
        years=years,
        target=conf.get("target", "parquet"),
        profile=conf.get("profile", "local"),
    )


with DAG(
    "ocds_silver_pipeline",
    default_args=default_args,
    description=(
        "Transforma datos Bronze en modelo dimensional y carga el destino Gold "
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

    run_synth = PythonOperator(
        task_id="run_synth",
        python_callable=_run_synth,
    )

    run_gold = PythonOperator(
        task_id="run_gold_pipeline",
        python_callable=_run_gold,
    )

    trigger_alerting = TriggerDagRunOperator(
        task_id="trigger_alerting",
        trigger_dag_id="ocds_alerting",
    )

    resolve_years >> run_silver >> run_synth >> run_gold >> trigger_alerting
