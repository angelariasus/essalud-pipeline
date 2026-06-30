"""Tests de la abstracción de destinos Gold (factory + targets; con mocks)."""
from unittest.mock import MagicMock

import pytest

from app.loaders import targets
from app.loaders.targets import get_target, available_targets, DEFAULT_TARGET
from app.loaders.targets.base import GoldTarget
from app.loaders.targets.parquet_target import ParquetGoldTarget
from app.loaders.targets.sqlserver_target import SqlServerTarget


def test_registry_y_default():
    assert DEFAULT_TARGET == "parquet"
    assert set(available_targets()) == {"parquet", "sqlserver"}


def test_get_target_parquet_y_sqlserver():
    assert isinstance(get_target("parquet"), ParquetGoldTarget)
    assert isinstance(get_target(None), ParquetGoldTarget)  # None -> default
    t = get_target("sqlserver", profile="docker")
    assert isinstance(t, SqlServerTarget)
    assert t.profile == "docker"


def test_get_target_desconocido():
    with pytest.raises(ValueError, match="desconocido"):
        get_target("duckdb")


def test_targets_implementan_contrato():
    assert issubclass(ParquetGoldTarget, GoldTarget)
    assert issubclass(SqlServerTarget, GoldTarget)
    assert ParquetGoldTarget.name == "parquet"
    assert SqlServerTarget.name == "sqlserver"


def test_parquet_target_escribe_cada_tabla(tmp_path):
    target = ParquetGoldTarget(base_path=tmp_path)
    # DataFrames de Spark simulados: solo se ejercita el contrato write/count.
    dims = {}
    writers = {}
    for key in ("dim_entidad", "fact"):
        df = MagicMock()
        df.count.return_value = 3
        writer = df.write.mode.return_value
        writers[key] = writer
        dims[key] = df

    target.load(dims)

    for key, df in dims.items():
        df.write.mode.assert_called_once_with("overwrite")
        # La ruta de salida incluye el nombre de la tabla.
        out = writers[key].parquet.call_args[0][0]
        assert key in out


def test_sqlserver_target_delega_en_load_all(monkeypatch):
    """SqlServerTarget no reimplementa la carga: delega en dw_loader.load_all."""
    captured = {}

    def fake_load_all(dims, ddl_path, *a, **kw):
        captured["dims"] = dims
        captured["ddl_path"] = ddl_path

    import app.loaders.dw_loader as dw_loader
    from app.loaders.targets import sqlserver_target as st
    monkeypatch.setattr(dw_loader, "load_all", fake_load_all)
    # Mockear el export a bi/ (no conectar a SQL Server real en el unit test).
    monkeypatch.setattr(st, "export_oro_to_bi", lambda *a, **k: captured.setdefault("bi", True))

    dims = {"fact": MagicMock()}
    SqlServerTarget().load(dims)
    assert captured["dims"] is dims
    assert "EsSalud_StarSchema_DDL.sql" in str(captured["ddl_path"])
    assert captured.get("bi") is True  # también exporta a bi/


def test_sqlserver_profile_docker_aplica_override(monkeypatch):
    """El perfil docker sustituye temporalmente las cadenas de conexión."""
    from app.loaders.targets import sqlserver_target as st

    monkeypatch.setattr(st.settings, "DW_CONN_STRING", "LOCAL_CONN", raising=False)
    monkeypatch.setattr(st.settings, "DW_CONN_STRING_DOCKER", "DOCKER_CONN", raising=False)

    seen = {}

    def fake_load_all(dims, ddl_path, *a, **kw):
        seen["conn_during"] = st.settings.DW_CONN_STRING

    import app.loaders.dw_loader as dw_loader
    monkeypatch.setattr(dw_loader, "load_all", fake_load_all)
    # Mockear el export a bi/ (no conectar a SQL Server real en el unit test).
    monkeypatch.setattr(st, "export_oro_to_bi", lambda *a, **k: None)

    SqlServerTarget(profile="docker").load({"fact": MagicMock()})
    assert seen["conn_during"] == "DOCKER_CONN"          # override activo en la carga
    assert st.settings.DW_CONN_STRING == "LOCAL_CONN"    # restaurado al terminar
