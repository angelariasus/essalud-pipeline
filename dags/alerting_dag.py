"""
DAG de alertas operativas de abastecimiento.

Corre tras Gold (lo dispara `ocds_silver_pipeline` vía TriggerDagRunOperator) o
manualmente. Construye `data/mart/Alertas.parquet` (HHI crítico + lead time anómalo) y
envía el correo formal al área de abastecimiento por SMTP (en el stack Docker el
default apunta a MailHog: UI en http://localhost:8025).

`dag_run.conf` soportado:
  - `to`      : destinatario (default SMTP_TO del entorno)
  - `source`  : hhi | leadtime | all (default all)
  - `dry_run` : true para construir el parquet sin enviar correo
"""
import sys
from datetime import datetime, timedelta

sys.path.append("/opt/airflow/project")

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def _build_alerts_parquet(**context):
    from app.services.alerting import build_alertas
    build_alertas(save=True)
    print("Alertas.parquet generado exitosamente.")

with DAG(
    "ocds_alerting",
    default_args=default_args,
    description="Predicciones ML, Consolidación de Alertas, Carga a SQL y Notificación Node.js.",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ocds", "alerting"],
) as dag:

    run_ml_predict = BashOperator(
        task_id="run_ml_predict",
        # Airflow monta la raiz del proyecto en /opt/airflow/project
        bash_command="cd /opt/airflow/project && jupyter nbconvert --to notebook --execute machine_learning/lead_time_predictor/LeadTime_Predictor.ipynb",
    )

    run_build_alerts = PythonOperator(
        task_id="run_build_alerts",
        python_callable=_build_alerts_parquet,
    )

    run_load_sql = BashOperator(
        task_id="run_load_sql",
        bash_command="cd /opt/airflow/project && python app/scripts/load_ml_to_sql.py",
    )

    run_notify_nodejs = BashOperator(
        task_id="run_notify_nodejs",
        bash_command="curl -X POST http://notifier-backend:3000/api/notify",
    )

    run_ml_predict >> run_build_alerts >> run_load_sql >> run_notify_nodejs
