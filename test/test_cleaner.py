"""Tests del cleaner por capa (sin Spark ni SQL Server)."""
import pytest

from app.utils import cleaner


@pytest.fixture
def data_dir(tmp_path, monkeypatch):
    """Redirige DATA_DIR/BRONZE/SILVER/GOLD a un tmp y crea artefactos de prueba."""
    monkeypatch.setattr(cleaner.settings, "DATA_DIR", tmp_path)
    monkeypatch.setattr(cleaner.settings, "BRONZE_DIR", tmp_path / "bronze")
    monkeypatch.setattr(cleaner.settings, "SILVER_DIR", tmp_path / "silver")
    monkeypatch.setattr(cleaner.settings, "GOLD_DIR", tmp_path / "gold")
    (tmp_path / "bronze" / "records" / "x").mkdir(parents=True)
    (tmp_path / "bronze" / "records" / "x" / "a.json").write_text("{}")
    (tmp_path / "silver" / "staging_flat" / "anio_fiscal=2022").mkdir(parents=True)
    (tmp_path / "gold" / "fact").mkdir(parents=True)
    return tmp_path


def test_clean_silver_solo_borra_silver(data_dir):
    cleaner.clean_silver()
    assert not (data_dir / "silver" / "staging_flat").exists()
    # No tocó Bronze ni Gold.
    assert (data_dir / "bronze" / "records").exists()
    assert (data_dir / "gold").exists()


def test_clean_gold_parquet_solo_borra_gold(data_dir):
    cleaner.clean_gold(target="parquet")
    assert not (data_dir / "gold").exists()
    assert (data_dir / "bronze" / "records").exists()
    assert (data_dir / "silver" / "staging_flat").exists()


def test_clean_bronze_requiere_confirmacion(data_dir):
    with pytest.raises(ValueError, match="confirmación"):
        cleaner.clean_bronze(confirm=False)
    assert (data_dir / "bronze" / "records").exists()  # no borró


def test_clean_bronze_con_confirmacion(data_dir):
    cleaner.clean_bronze(confirm=True)
    assert not (data_dir / "bronze" / "records").exists()
    assert (data_dir / "silver" / "staging_flat").exists()  # no tocó Silver


def test_safe_rmtree_rechaza_fuera_de_data_dir(data_dir, tmp_path):
    outside = tmp_path.parent / "fuera"
    outside.mkdir(exist_ok=True)
    with pytest.raises(ValueError, match="fuera de DATA_DIR"):
        cleaner._safe_rmtree(outside)


def test_clean_layer_dispatch(data_dir):
    cleaner.clean_layer("silver")
    assert not (data_dir / "silver" / "staging_flat").exists()
    with pytest.raises(ValueError, match="desconocida"):
        cleaner.clean_layer("platinum")
