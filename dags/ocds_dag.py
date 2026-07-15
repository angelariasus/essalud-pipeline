"""
DAGs de la capa Bronze (ingesta OCDS de EsSalud).

Los callables importan `app.*` de forma diferida (dentro de la función, no a nivel
de módulo): Airflow parsea los DAGs con frecuencia y los imports pesados
(pyspark, boto3, ...) no deben ejecutarse en el parseo. El proyecto se monta en
`/opt/airflow/project` (ver docker-compose), por eso se añade al `sys.path`.
"""
import sys
from datetime import datetime, timedelta

sys.path.append("/opt/airflow/project")

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


def _run_bronze_targeted(**context):
    """Ingesta Bronze dirigida por RUC/años (override vía dag_run.conf)."""
    from app.config.settings import settings
    from app.pipelines.bronze_layer import BronzePipeline

    dag_run = context.get("dag_run")
    conf = (dag_run.conf if dag_run and dag_run.conf else {}) or {}
    ruc = conf.get("ruc", settings.ESSALUD_RUC)
    limit = int(conf.get("limit", 0))
    if conf.get("years"):
        years = list(conf["years"])
    elif conf.get("year"):
        years = [conf["year"]]
    else:
        years = DEFAULT_YEARS

    pipeline = BronzePipeline()
    try:
        for year in years:
            pipeline.run_targeted_ingestion(ruc=ruc, limit=limit, year=year)
    finally:
        pipeline.close()


def _run_bronze_bulk(**context):
    """Descarga el archivo masivo mensual (override vía dag_run.conf)."""
    from app.pipelines.bronze_layer import BronzePipeline

    dag_run = context.get("dag_run")
    conf = (dag_run.conf if dag_run and dag_run.conf else {}) or {}
    now = datetime.utcnow()
    pipeline = BronzePipeline()
    try:
        pipeline.run_bulk_ingestion(
            source=conf.get("source", "SEACE"),
            file_type=conf.get("type", "JSON"),
            year=int(conf.get("year", now.year)),
            month=int(conf.get("month", now.month)),
        )
    finally:
        pipeline.close()


with DAG(
    "ocds_targeted_ingestion",
    default_args=default_args,
    description="Extrae datos OCDS para EsSalud en la Capa Bronce (Local y R2)",
    schedule_interval="@weekly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ocds", "bronze", "essalud"],
) as dag:

    run_targeted = PythonOperator(
        task_id="ingest_essalud_data",
        python_callable=_run_bronze_targeted,
    )

    trigger_silver = TriggerDagRunOperator(
        task_id="trigger_silver_pipeline",
        trigger_dag_id="ocds_silver_pipeline",
        wait_for_completion=False,
    )

    run_targeted >> trigger_silver


with DAG(
    "ocds_bulk_ingestion",
    default_args=default_args,
    description="Descarga el archivo masivo mensual de OCDS",
    schedule_interval="@monthly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ocds", "bronze", "bulk"],
) as dag_bulk:

    run_bulk = PythonOperator(
        task_id="ingest_monthly_bulk",
        python_callable=_run_bronze_bulk,
    )
