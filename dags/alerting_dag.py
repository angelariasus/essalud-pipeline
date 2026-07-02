"""
DAG de la Fase 6 — alertas operativas de abastecimiento.

Corre tras Gold (lo dispara `ocds_silver_pipeline` vía TriggerDagRunOperator) o
manualmente. Construye `bi/Alertas.parquet` (HHI crítico + lead time anómalo) y
envía el correo formal al área de abastecimiento por SMTP (en el stack Docker el
default apunta a MailHog: UI en http://localhost:8025).

`dag_run.conf` soportado:
  - `to`      : destinatario (default SMTP_TO del entorno)
  - `source`  : hhi | leadtime | all (default all)
  - `dry_run` : true para construir el parquet sin enviar correo
"""
import sys
from datetime import datetime, timedelta

sys.path.append("/opt/airflow/bi")

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def _run_alerts(**context):
    from app.services.alerting import send_alerts

    dag_run = context.get("dag_run")
    conf = (dag_run.conf if dag_run and dag_run.conf else {}) or {}
    n = send_alerts(
        source=conf.get("source", "all"),
        to=conf.get("to"),
        dry_run=bool(conf.get("dry_run", False)),
    )
    print(f"Alertas notificadas: {n}")


with DAG(
    "ocds_alerting",
    default_args=default_args,
    description=(
        "Fase 6: consolida alertas (HHI critico + lead time anomalo) en "
        "bi/Alertas.parquet y notifica por correo al area de abastecimiento."
    ),
    schedule_interval=None,  # lo dispara ocds_silver_pipeline al terminar Gold
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ocds", "alerting", "fase6"],
) as dag:

    run_alerts = PythonOperator(
        task_id="run_alerts",
        python_callable=_run_alerts,
    )
