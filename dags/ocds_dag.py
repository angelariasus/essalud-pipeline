from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "ocds_targeted_ingestion",
    default_args=default_args,
    description="Extrae datos OCDS para EsSalud en la Capa Bronce (Local y R2)",
    schedule_interval="@weekly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ocds", "bronze", "essalud"],
) as dag:

    run_targeted = BashOperator(
        task_id="ingest_essalud_data",
        bash_command=(
            "echo 'Simulando extracción de datos OCDS...' && sleep 10 && echo 'Extracción completada con éxito.'"
        ),
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

    run_bulk = BashOperator(
        task_id="ingest_monthly_bulk",
        bash_command=(
            "echo 'Simulando ingesta masiva...' && sleep 10 && echo 'Ingesta completada con éxito.'"
        ),
    )
