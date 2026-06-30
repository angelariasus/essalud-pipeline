"""Tests del generador sintético v2 (base CSV EsSalud; modelo escalado)."""
import datetime as dt

import pytest

from conftest import requires_spark
from app.services import synthetic_generator as sg


# ── Modelo de adicional desde el CSV (sin Spark; lee el CSV real, ~2MB) ───────
def test_load_adicional_model():
    ratios, p = sg.load_adicional_model()
    assert len(ratios) > 0
    # Ratios reales: fracciones (0, 1], la mayoría ≈ 0.25 (tope legal EsSalud).
    assert all(0 < r <= 1.0 for r in ratios)
    assert max(ratios) <= sg.ADICIONAL_CAP_RATIO + 1e-9
    # p escalado para señal BI/ML (mayor que el 0.15% real).
    assert p == sg.ESSALUD_ADICIONAL_P
    assert 0.005 < p < 0.20


# ── ocid realista (sin marca "synthetic-") ───────────────────────────────────
@requires_spark
def test_with_realistic_ocid(spark):
    df = spark.createDataFrame([("x-1",), ("x-2",)], ["ocid"])
    out = sg._with_realistic_ocid(df, 2024).collect()
    for r in out:
        assert r["ocid"].startswith("ocds-dgv273-seacev3-2024-2543-")
        assert "synthetic" not in r["ocid"]


# ── Modelo escalado: lógica pura del adicional (sin Spark) ───────────────────
def test_compute_adicional_cap_y_reglas():
    # ratio 0.9 sobre 1000 -> 900, pero el tope del 25% lo limita a 250.
    assert sg._compute_adicional(0.0, 0.0, 1000.0, [0.9], 1.0) == pytest.approx(250.0)
    # ratio normal por debajo del tope: 0.1 * 1000 = 100.
    assert sg._compute_adicional(0.0, 0.0, 1000.0, [0.1], 1.0) == pytest.approx(100.0)
    # sin ocurrencia (r_occ >= p) -> 0.
    assert sg._compute_adicional(0.9, 0.0, 1000.0, [0.25], 0.5) == 0.0
    # contratado nulo o <= 0 -> 0 (no se inventan adendas sin contrato).
    assert sg._compute_adicional(0.0, 0.0, None, [0.25], 1.0) == 0.0
    assert sg._compute_adicional(0.0, 0.0, 0.0, [0.25], 1.0) == 0.0
    # sin ratios -> 0.
    assert sg._compute_adicional(0.0, 0.0, 1000.0, [], 1.0) == 0.0


# ── 2024: boost por bootstrap del Silver real hasta el objetivo ──────────────
@requires_spark
def test_boost_year_alcanza_objetivo(spark, monkeypatch):
    cols = ["ocid", "monto_contratado", "monto_adicional", "tiene_adenda",
            "fecha_convocatoria", "fecha_buena_pro", "fecha_suscripcion", "anio_fiscal"]
    real_2024 = spark.createDataFrame(
        [("ocds-real-1", 500.0, 0.0, False,
          dt.date(2024, 2, 1), dt.date(2024, 3, 1), dt.date(2024, 4, 1), 2024)],
        cols,
    )
    pool = spark.createDataFrame(
        [(f"ocds-real-{i}", 1000.0 + i, 0.0, False,
          dt.date(2022, 5, 1), dt.date(2022, 6, 1), dt.date(2022, 7, 1), 2022)
         for i in range(20)],
        cols,
    )
    monkeypatch.setattr(sg, "read_staging", lambda s, years=None: real_2024)
    # El modelo adicional (UDF de Python) se prueba aparte en test_compute_adicional;
    # aquí se neutraliza para enfocar el test en conteos/ocids y evitar flakiness.
    monkeypatch.setattr(sg, "_apply_scaled_adicional", lambda df, *a, **k: df)
    written = {}
    monkeypatch.setattr(sg, "write_staging", lambda df, **kw: written.setdefault("df", df))

    out = sg.boost_year_with_synthetic(2024, target_total=10, pool=pool, spark=spark, seed=3)
    rows = out.collect()
    assert len(rows) == 10                                  # 1 real + 9 sintéticas
    assert all(r["anio_fiscal"] == 2024 for r in rows)
    assert all(r["fecha_convocatoria"] is None or r["fecha_convocatoria"].year == 2024
               for r in rows)
    # Las sintéticas llevan ocid realista; ninguna con marca.
    synth = [r for r in rows if r["ocid"].startswith("ocds-dgv273-seacev3-2024-2543-")]
    assert len(synth) == 9
    assert written["df"] is not None


# ── 2025: desde el CSV, conservando filas con adenda ─────────────────────────
@requires_spark
def test_build_2025_conserva_adendas(spark, monkeypatch):
    cols = ["ocid", "monto_contratado", "monto_adicional", "anio_fiscal"]
    fake_csv = spark.createDataFrame(
        [("ocds-2025-adenda", 1000.0, 250.0, 2025)]                       # 1 con adenda
        + [(f"ocds-2025-{i}", 500.0, 0.0, 2025) for i in range(50)],      # 50 sin adenda
        cols,
    )
    monkeypatch.setattr(sg, "load_essalud_csv", lambda s, schema: fake_csv)
    written = {}
    monkeypatch.setattr(sg, "write_staging", lambda df, **kw: written.setdefault("df", df))

    out = sg.build_2025_from_csv(spark, n=10, seed=1, save=True, schema=fake_csv.schema)
    rows = out.collect()
    assert len(rows) == 10
    # La fila con adenda real SIEMPRE se conserva (no se pierde en el muestreo).
    adendas = [r for r in rows if r["monto_adicional"] > 0]
    assert len(adendas) == 1 and adendas[0]["ocid"] == "ocds-2025-adenda"
    assert all(r["anio_fiscal"] == 2025 for r in rows)
