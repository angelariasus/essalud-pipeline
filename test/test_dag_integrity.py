import os
import glob
from airflow.models import DagBag

def test_dag_integrity():
    """
    Importa todos los DAGs y verifica que no haya errores de importación
    (como sintaxis o dependencias cíclicas).
    """
    dag_bag = DagBag(dag_folder='dags/', include_examples=False)
    assert not dag_bag.import_errors, f"DAG import failures: {dag_bag.import_errors}"
