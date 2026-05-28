import pytest

airflow = pytest.importorskip("airflow", reason="apache-airflow not installed")

from airflow.models import DagBag


def test_dag_integrity():
    dag_bag = DagBag(dag_folder="dags/", include_examples=False)
    assert not dag_bag.import_errors, f"DAG import failures: {dag_bag.import_errors}"
    assert len(dag_bag.dag_ids) >= 1, "No DAGs found in dags/"
