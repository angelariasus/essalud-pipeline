from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'ocds_targeted_ingestion',
    default_args=default_args,
    description='Extrae datos de OCDS para EsSalud en la Capa Bronce (Local y R2)',
    schedule_interval='@weekly',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['ocds', 'bronze', 'essalud'],
) as dag:

    # Ejecuta el módulo de ingesta targeted limitando a 100 registros para pruebas
    # En producción se puede omitir el limit para descargar todos
    run_targeted = BashOperator(
        task_id='ingest_essalud_data',
        bash_command='cd /opt/airflow/bi && python main.py targeted --year 2024 --limit 100',
    )

with DAG(
    'ocds_bulk_ingestion',
    default_args=default_args,
    description='Descarga el archivo masivo mensual de OCDS',
    schedule_interval='@monthly',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['ocds', 'bronze', 'bulk'],
) as dag_bulk:

    # En un DAG real de producción, el mes y año se calcularían dinámicamente con macros de Jinja {{ execution_date }}
    run_bulk = BashOperator(
        task_id='ingest_monthly_bulk',
        bash_command='cd /opt/airflow/bi && python main.py bulk --source SEACE --type JSON --year 2023 --month 11',
    )
