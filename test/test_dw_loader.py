"""Tests unitarios del dw_loader (con mocks; sin SQL Server ni Spark)."""
from unittest.mock import MagicMock

import pytest

from app.loaders import dw_loader


def test_split_sql_batches_por_go():
    sql = "CREATE TABLE a (i int);\nGO\nCREATE TABLE b (j int);\nGO\n"
    batches = list(dw_loader._split_sql_batches(sql))
    assert len(batches) == 2
    assert "CREATE TABLE a" in batches[0]
    assert "CREATE TABLE b" in batches[1]


def test_staging_ddl_existe_y_define_procedure():
    assert dw_loader.STAGING_DDL_PATH.exists()
    sql = dw_loader.STAGING_DDL_PATH.read_text(encoding="utf-8")
    assert "CREATE SCHEMA" in sql
    assert "usp_Load_From_Staging" in sql
    # Debe parsear en varios lotes (CREATE SCHEMA y el procedure van separados por GO).
    assert len(list(dw_loader._split_sql_batches(sql))) >= 2


def test_jdbc_write_options(monkeypatch):
    monkeypatch.setattr(dw_loader.settings, "DW_JDBC_URL", "jdbc:sqlserver://h:1;databaseName=d")
    monkeypatch.setattr(dw_loader.settings, "DW_JDBC_USER", "sa")
    monkeypatch.setattr(dw_loader.settings, "DW_JDBC_PASSWORD", "x")
    monkeypatch.setattr(dw_loader.settings, "DW_JDBC_BATCHSIZE", 5000)
    opts = dw_loader._jdbc_write_options()
    assert opts["url"].startswith("jdbc:sqlserver://")
    assert opts["driver"] == dw_loader._JDBC_DRIVER
    assert opts["batchsize"] == "5000"
    assert opts["user"] == "sa"


def test_jdbc_se_deriva_de_conn_string(monkeypatch):
    monkeypatch.setattr(dw_loader.settings, "DW_JDBC_URL", "")
    monkeypatch.setattr(
        dw_loader.settings, "DW_CONN_STRING",
        "mssql+pyodbc://sa:p%40ss@localhost:11423/DW_EsSalud_Adquisiciones"
        "?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes",
    )
    url, user, pwd = dw_loader._jdbc_conn()
    assert url == ("jdbc:sqlserver://localhost:11423;databaseName=DW_EsSalud_Adquisiciones"
                   ";encrypt=true;trustServerCertificate=true")
    assert user == "sa"
    assert pwd == "p@ss"  # make_url decodifica el %40


def test_write_staging_jdbc_requiere_conexion(monkeypatch):
    monkeypatch.setattr(dw_loader.settings, "DW_JDBC_URL", "")
    monkeypatch.setattr(dw_loader.settings, "DW_CONN_STRING", "")
    with pytest.raises(ValueError, match="OCDS_DW_JDBC_URL|OCDS_DW_CONN_STRING"):
        dw_loader.write_staging_jdbc(MagicMock(), "stg.Dim_Proveedor")


def test_call_load_procedure_ejecuta_exec():
    engine = MagicMock()
    conn = engine.begin.return_value.__enter__.return_value
    dw_loader.call_load_procedure(engine)
    conn.exec_driver_sql.assert_called_once()
    assert "EXEC oro.usp_Load_From_Staging" in conn.exec_driver_sql.call_args[0][0]


def test_load_all_orquesta_en_orden(monkeypatch):
    calls = []
    monkeypatch.setattr(dw_loader, "execute_ddl", lambda e, p: calls.append(("ddl", str(p))))
    monkeypatch.setattr(dw_loader, "write_all_staging", lambda d: calls.append(("staging", None)))
    monkeypatch.setattr(dw_loader, "call_load_procedure", lambda e: calls.append(("proc", None)))

    engine = MagicMock()
    dw_loader.load_all({"fact": MagicMock()}, ddl_path="DDL.sql", engine=engine)

    steps = [c[0] for c in calls]
    # 2 DDL (producción + staging), luego staging JDBC, luego el procedure atómico.
    assert steps == ["ddl", "ddl", "staging", "proc"]
    engine.dispose.assert_not_called()  # engine inyectado: no se cierra aquí
