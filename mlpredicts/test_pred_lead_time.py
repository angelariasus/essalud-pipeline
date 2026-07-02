# -*- coding: utf-8 -*-
"""Verificacion end-to-end de la Fase 4 (Lead Time predictivo).

Comprueba el checklist del plan `fase4-lead-time-predictivo.md` sobre los
entregables reales generados por el notebook:
  - bi/Pred_Lead_Time.parquet
  - mlpredicts/models/best_model.joblib

Se ejecuta de forma aislada (no depende de Spark ni del conftest del proyecto):

    .venv/Scripts/python.exe mlpredicts/test_pred_lead_time.py

Tambien es compatible con pytest:  pytest mlpredicts/test_pred_lead_time.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.metrics import root_mean_squared_error

ROOT    = Path(__file__).resolve().parents[1]
BI_DIR  = ROOT / "bi"
MDL     = ROOT / "mlpredicts" / "models" / "best_model.joblib"
PRED    = BI_DIR / "Pred_Lead_Time.parquet"
FACT    = BI_DIR / "Fact_Ordenes_Y_Contratos.parquet"

EXPECTED_COLS = [
    "ID_Registro", "Codigo_Convocatoria", "N_Item", "Anio_Fiscal",
    "Red_Asistencial", "Categoria_Proceso",
    "Lead_Time_Actual", "Lead_Time_Predicho", "Residual",
]
CAT = ["Red_Asistencial", "Categoria_Proceso", "Especialidad_Autorizada",
       "Tipo_Red", "Metodo_Clasificacion"]
NUM = ["Monto_Adjudicado_Soles", "Anio_Fiscal", "Mes_Convocatoria", "retraso_historico_red"]
BIN = ["Flag_Contratacion_Directa", "Flag_Tiene_Adenda"]


# --------------------------------------------------------------------------- #
#  Reconstruccion de features identica al notebook (para la prueba modelo<->parquet)
# --------------------------------------------------------------------------- #
def _build_features() -> pd.DataFrame:
    fact = pd.read_parquet(FACT)
    ent  = pd.read_parquet(BI_DIR / "Dim_Entidad_Compradora.parquet")
    med  = pd.read_parquet(BI_DIR / "Dim_Medicamento.parquet")
    tipo = pd.read_parquet(BI_DIR / "Dim_Tipo_Proceso.parquet")
    df = (fact
        .merge(ent[["SK_Entidad", "Red_Asistencial", "Tipo_Red"]],
               left_on="FK_Entidad", right_on="SK_Entidad", how="left")
        .merge(med[["SK_Medicamento", "Especialidad_Autorizada", "Metodo_Clasificacion"]],
               left_on="FK_Medicamento", right_on="SK_Medicamento", how="left")
        .merge(tipo[["SK_Tipo_Proceso", "Categoria_Proceso"]],
               left_on="FK_Tipo_Proceso", right_on="SK_Tipo_Proceso", how="left"))
    to_dt = lambda c: pd.to_datetime(df[c], errors="coerce")
    df["Lead_Time_Comite"] = (to_dt("Fecha_Buena_Pro") - to_dt("Fecha_Convocatoria")).dt.days
    df.loc[df["Lead_Time_Comite"] < 0, "Lead_Time_Comite"] = np.nan
    df["Mes_Convocatoria"] = to_dt("Fecha_Convocatoria").dt.month.fillna(0).astype(int)
    df["Flag_Tiene_Adenda"] = (df["Monto_Adicional"].fillna(0) > 0).astype(int)
    hist = df.groupby("Red_Asistencial")["Lead_Time_Comite"].mean().rename("retraso_historico_red")
    df = df.merge(hist, on="Red_Asistencial", how="left")
    df["retraso_historico_red"] = df["retraso_historico_red"].fillna(df["Lead_Time_Comite"].mean())
    for c in CAT:
        df[c] = df[c].fillna("DESCONOCIDO")
    for c in BIN:
        df[c] = df[c].fillna(0)
    return df


# --------------------------------------------------------------------------- #
#  Fixtures perezosos (compartidos por script y pytest)
# --------------------------------------------------------------------------- #
_pred = None
_fact = None
def pred() -> pd.DataFrame:
    global _pred
    if _pred is None:
        _pred = pd.read_parquet(PRED)
    return _pred
def fact_len() -> int:
    global _fact
    if _fact is None:
        _fact = len(pd.read_parquet(FACT))
    return _fact


# --------------------------------------------------------------------------- #
#  Pruebas
# --------------------------------------------------------------------------- #
def test_entregables_existen():
    assert PRED.exists(), f"Falta {PRED}"
    assert MDL.exists(),  f"Falta {MDL}"


def test_filas_y_columnas():
    p = pred()
    assert len(p) == fact_len() == 9292, f"Filas {len(p)} != 9292"
    assert list(p.columns) == EXPECTED_COLS, f"Columnas inesperadas: {list(p.columns)}"


def test_predicho_no_nulo_y_no_negativo():
    p = pred()
    assert p["Lead_Time_Predicho"].notna().all(), "Hay nulos en Lead_Time_Predicho"
    assert (p["Lead_Time_Predicho"] >= 0).all(),  "Hay predicciones negativas"


def test_actual_nunca_negativo():
    # LA CORRECCION: los Lead Times negativos (fechas inconsistentes) son NaN, no -29.
    p = pred()
    v = p["Lead_Time_Actual"].notna()
    assert (p.loc[v, "Lead_Time_Actual"] >= 0).all(), "Lead_Time_Actual tiene negativos"
    assert v.sum() == 5938, f"Se esperaban 5938 actuals validos, hay {int(v.sum())}"


def test_residual_consistente():
    p = pred()
    v = p["Lead_Time_Actual"].notna()
    assert p.loc[~v, "Residual"].isna().all(), "Residual presente sin Actual"
    recomputed = p.loc[v, "Lead_Time_Actual"] - p.loc[v, "Lead_Time_Predicho"]
    assert np.allclose(p.loc[v, "Residual"], recomputed, atol=1e-9), "Residual != Actual - Predicho"


def test_id_registro_unico():
    p = pred()
    assert p["ID_Registro"].is_unique, "ID_Registro no es unico"
    assert p["ID_Registro"].min() == 0 and p["ID_Registro"].max() == len(p) - 1


def test_modelo_carga_y_predice():
    model = joblib.load(MDL)
    df = _build_features()
    sample = df[CAT + NUM + BIN].head(5)
    out = model.predict(sample)
    assert out.shape == (5,), f"Forma inesperada {out.shape}"
    assert np.isfinite(out).all(), "Predicciones no finitas"


def test_modelo_reproduce_el_parquet():
    # El parquet debe provenir del modelo guardado sobre las features documentadas.
    model = joblib.load(MDL)
    df = _build_features()
    recomputed = np.clip(model.predict(df[CAT + NUM + BIN]), 0, None).round(1)
    stored = pred().sort_values("ID_Registro")["Lead_Time_Predicho"].to_numpy()
    match = np.isclose(recomputed, stored, atol=0.05).mean()
    assert match == 1.0, f"El modelo solo reproduce {match:.4%} de las predicciones del parquet"


def test_sanidad_por_categoria():
    p = pred()
    m = p.groupby("Categoria_Proceso")["Lead_Time_Predicho"].mean()
    # Un proceso DIRECTO debe predecirse mucho mas rapido que uno COMPETITIVO.
    assert m["DIRECTO"] < m["CATALOGO"] < m["COMPETITIVO"], f"Orden por categoria ilogico:\n{m}"
    assert m["DIRECTO"] < 20, f"DIRECTO deberia ser ~pocos dias, es {m['DIRECTO']:.1f}"


def test_rmse_razonable():
    # Plan: RMSE esperado < 150 dias (std del target = ~98-124).
    p = pred()
    v = p["Lead_Time_Actual"].notna()
    rmse = root_mean_squared_error(p.loc[v, "Lead_Time_Actual"], p.loc[v, "Lead_Time_Predicho"])
    assert rmse < 150, f"RMSE de reajuste {rmse:.1f} >= 150"


# --------------------------------------------------------------------------- #
#  Runner de script (sin pytest)
# --------------------------------------------------------------------------- #
def _main() -> int:
    tests = [f for name, f in sorted(globals().items())
             if name.startswith("test_") and callable(f)]
    ok = 0
    print("=" * 74)
    print("VERIFICACION FASE 4 — Lead Time Predictivo")
    print("=" * 74)
    for t in tests:
        try:
            t()
            print(f"  [PASS] {t.__name__}")
            ok += 1
        except AssertionError as e:
            print(f"  [FAIL] {t.__name__}: {e}")
        except Exception as e:  # pragma: no cover
            print(f"  [ERROR] {t.__name__}: {type(e).__name__}: {e}")
    print("-" * 74)
    print(f"Resultado: {ok}/{len(tests)} pruebas OK")

    if ok == len(tests):
        p = pred()
        v = p["Lead_Time_Actual"].notna()
        print("\nResumen del entregable bi/Pred_Lead_Time.parquet:")
        print(f"  Filas ...................: {len(p)}")
        print(f"  Con Lead Time real ......: {int(v.sum())}")
        print(f"  Solo prediccion .........: {int((~v).sum())}")
        print(f"  MAE (dias) ..............: {p.loc[v,'Residual'].abs().mean():.1f}")
        print("\n  Prediccion media por categoria:")
        print(p.groupby("Categoria_Proceso")["Lead_Time_Predicho"].mean().round(1).to_string())
        print("\n  Muestra (fila 2 = la que antes tenia Actual=-29 / Residual=-95.6):")
        with pd.option_context("display.width", 200, "display.max_columns", 12):
            print(p.head(5).to_string(index=False))
    return 0 if ok == len(tests) else 1


if __name__ == "__main__":
    sys.exit(_main())
