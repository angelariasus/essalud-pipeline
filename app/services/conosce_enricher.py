"""
Enriquecimiento del staging Silver con datos CONOSCE (contratos SEACE).

El join es por (codigoconvocatoria, n_cod_contrato) a nivel de contrato.
CONOSCE tiene una fila por ítem dentro de cada contrato (num_item secuencial),
por lo que se agrega al nivel de contrato antes del join con el Silver.

Campos enriquecidos en el Silver:
  fecha_suscripcion:    CONOSCE como fallback cuando OCDS dateSigned es null.
  monto_adicional:      suma real por contrato (OCDS casi siempre 0 por amendments vacíos).
  num_contrato:         identificador textual del contrato (NUM_CONTRATO).
  tiene_resolucion:     indicador SI/NO de resolución aprobatoria.
  monto_reduccion:      reducción aprobada por adenda (no existe en OCDS).
  monto_prorroga:       prórroga por adenda (no existe en OCDS).
  monto_complementario: contratación complementaria (no existe en OCDS).
"""
from __future__ import annotations

import glob as _glob
from pathlib import Path
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from app.audit.logger import setup_logger
from app.config.settings import settings

logger = setup_logger("ocds_framework.services.conosce_enricher")

CONOSCE_DIR: Path = settings.EXTRA_DATA_DIR / "Contratos"

_AMOUNT_COLS = ["monto_adicional", "monto_reduccion", "monto_prorroga", "monto_complementario"]
_DROP_PREFIXED = [
    "_c_conv", "_c_ncod", "_c_num_contrato", "_c_tiene_resolucion",
    "_c_fecha_suscripcion", "_c_monto_adicional",
    "_c_monto_reduccion", "_c_monto_prorroga", "_c_monto_complementario",
]


def _load_conosce_pandas():
    """Carga y concatena todos los xlsx CONOSCE de `extra-data/Contratos/`."""
    import pandas as pd

    files = sorted(_glob.glob(str(CONOSCE_DIR / "*.xlsx")))
    if not files:
        raise FileNotFoundError(f"No se encontraron archivos xlsx en {CONOSCE_DIR}")

    dfs = []
    for f in files:
        df = pd.read_excel(
            f,
            dtype={"codigoconvocatoria": "Int64", "n_cod_contrato": "Int64", "num_item": "Int64"},
        )
        dfs.append(df)

    combined = pd.concat(dfs, ignore_index=True)
    logger.info(
        f"CONOSCE cargado: {len(combined)} filas de {len(files)} archivos "
        f"(codigoentidad únicos: {combined['codigoentidad'].nunique()})"
    )
    return combined


def _aggregate_conosce(df_pd):
    """
    Agrega CONOSCE al nivel (codigoconvocatoria, n_cod_contrato):
      - num_contrato, tiene_resolucion: primer valor del grupo.
      - fecha_suscripcion_contrato: primer valor no nulo.
      - monto_adicional, monto_reduccion, monto_prorroga, monto_complementario: suma.
    """
    import numpy as np
    import pandas as pd

    def _first_valid(s):
        non_null = s.dropna()
        return non_null.iloc[0] if len(non_null) > 0 else None

    def _sum_safe(s):
        return float(pd.to_numeric(s, errors="coerce").fillna(0).sum())

    def _resolucion(s):
        v = _first_valid(s)
        if v is None or (isinstance(v, float) and np.isnan(v)):
            return None
        if isinstance(v, (int, float)):
            return "SI" if v == 1 else "NO"
        return str(v).strip() or None

    agg = (
        df_pd
        .groupby(["codigoconvocatoria", "n_cod_contrato"], as_index=False)
        .agg(
            num_contrato=("num_contrato", _first_valid),
            tiene_resolucion_raw=("tieneresolucion", list),
            fecha_suscripcion_contrato=("fecha_suscripcion_contrato", _first_valid),
            monto_adicional_sum=("monto_adicional", _sum_safe),
            monto_reduccion_sum=("monto_reduccion", _sum_safe),
            monto_prorroga_sum=("monto_prorroga", _sum_safe),
            monto_complementario_sum=("monto_complementario", _sum_safe),
        )
    )

    agg["tiene_resolucion"] = agg["tiene_resolucion_raw"].apply(
        lambda vals: _resolucion(pd.Series(vals))
    )
    agg["fecha_suscripcion_contrato"] = pd.to_datetime(
        agg["fecha_suscripcion_contrato"], errors="coerce"
    ).dt.date

    return agg.drop(columns=["tiene_resolucion_raw"])


def load_conosce_spark(spark: SparkSession) -> Optional[DataFrame]:
    """Carga, agrega y devuelve el CONOSCE como Spark DataFrame listo para join."""
    try:
        pd_raw = _load_conosce_pandas()
        pd_agg = _aggregate_conosce(pd_raw)
    except FileNotFoundError as exc:
        logger.warning(f"Enriquecimiento CONOSCE omitido: {exc}")
        return None

    n = len(pd_agg)
    logger.info(f"CONOSCE agregado: {n} contratos únicos (codigoconvocatoria, n_cod_contrato)")

    return (
        spark.createDataFrame(pd_agg)
        .select(
            F.col("codigoconvocatoria").cast("long").alias("_c_conv"),
            F.col("n_cod_contrato").cast("long").alias("_c_ncod"),
            F.col("num_contrato").cast("string").alias("_c_num_contrato"),
            F.col("tiene_resolucion").cast("string").alias("_c_tiene_resolucion"),
            F.col("fecha_suscripcion_contrato").cast("date").alias("_c_fecha_suscripcion"),
            F.col("monto_adicional_sum").cast("double").alias("_c_monto_adicional"),
            F.col("monto_reduccion_sum").cast("double").alias("_c_monto_reduccion"),
            F.col("monto_prorroga_sum").cast("double").alias("_c_monto_prorroga"),
            F.col("monto_complementario_sum").cast("double").alias("_c_monto_complementario"),
        )
    )


def _add_null_conosce_cols(flat_df: DataFrame) -> DataFrame:
    """Añade las columnas CONOSCE como NULL/0 cuando no hay datos disponibles."""
    return (
        flat_df
        .withColumn("num_contrato", F.lit(None).cast("string"))
        .withColumn("tiene_resolucion", F.lit(None).cast("string"))
        .withColumn("monto_reduccion", F.lit(0.0))
        .withColumn("monto_prorroga", F.lit(0.0))
        .withColumn("monto_complementario", F.lit(0.0))
    )


def enrich_flat_with_conosce(flat_df: DataFrame, spark: SparkSession) -> DataFrame:
    """
    Enriquece el Silver staging con datos de contratos CONOSCE.

    JOIN: LEFT por (codigo_convocatoria, n_cod_contrato).

    Comportamiento:
    - fecha_suscripcion: usa CONOSCE como fallback cuando OCDS es null.
    - monto_adicional: reemplaza con valor CONOSCE cuando éste > 0
      (OCDS casi siempre 0 porque SEACE no publica amendments).
    - num_contrato, tiene_resolucion: nuevas columnas del Silver.
    - monto_reduccion, monto_prorroga, monto_complementario: nuevas columnas (0 si sin match).
    """
    conosce_df = load_conosce_spark(spark)
    if conosce_df is None:
        return _add_null_conosce_cols(flat_df)

    joined = flat_df.join(
        conosce_df,
        (flat_df["codigo_convocatoria"] == conosce_df["_c_conv"])
        & (flat_df["n_cod_contrato"] == conosce_df["_c_ncod"]),
        "left",
    )

    enriched = (
        joined
        .withColumn(
            "fecha_suscripcion",
            F.coalesce(F.col("fecha_suscripcion"), F.col("_c_fecha_suscripcion")),
        )
        .withColumn(
            "monto_adicional",
            F.when(
                F.col("_c_monto_adicional").isNotNull() & (F.col("_c_monto_adicional") > 0),
                F.col("_c_monto_adicional"),
            ).otherwise(F.coalesce(F.col("monto_adicional"), F.lit(0.0))),
        )
        .withColumn("num_contrato", F.col("_c_num_contrato"))
        .withColumn("tiene_resolucion", F.col("_c_tiene_resolucion"))
        .withColumn("monto_reduccion", F.coalesce(F.col("_c_monto_reduccion"), F.lit(0.0)))
        .withColumn("monto_prorroga", F.coalesce(F.col("_c_monto_prorroga"), F.lit(0.0)))
        .withColumn("monto_complementario", F.coalesce(F.col("_c_monto_complementario"), F.lit(0.0)))
        .drop(*_DROP_PREFIXED)
    )

    n_enriched = enriched.filter(F.col("num_contrato").isNotNull()).count()
    total = enriched.count()
    logger.info(
        f"CONOSCE enrichment: {n_enriched}/{total} filas con datos de contrato "
        f"({100 * n_enriched / total:.1f}% match rate)."
    )
    return enriched
