"""
Generación de datos sintéticos para la capa Silver (frontera Silver→Gold).

Motivación (verificado sobre los datos reales):
  - El Bronze real concentra la actividad en 2022-2023; **2025 queda con 0 filas**
    y **2024 con solo 58** en Silver (el `anio_fiscal` se deriva de la fecha de
    convocatoria, y los expedientes recientes describen procesos antiguos).
  - Los contratos OCDS reales traen `amendments: []`, por lo que `Monto_Adicional`
    es 0.0 en TODA la Fact → no hay señal de sobrecosto para BI/ML.

Base de referencia (v2): **`extra-data/CONOSCE_2025_essalud.csv`** — datos reales de
EsSalud 2025 (bienes) ya en el esquema Silver (26 columnas + extras `_`). Es la
fuente fiel del comportamiento EsSalud: adendas escasas (~0.15%) con ratio
adicional/contratado ≈ 0.25 (tope legal).

Los archivos xlsx `extra-data/Contratos/CONOSCE_CONTRATOS{anio}_0.xlsx` tienen
esquema de administración contractual (24 columnas), no el esquema Silver completo;
se usan en el enricher (capa Silver, paso 3) para JOIN por contrato, no como fuente
primaria de filas Silver.

Estrategia (NO aleatoria):
  - **2025**: se extrae del CSV (muestreo de `N_2025`, conservando las filas con
    adenda reales). `monto_adicional` se mantiene **intacto** (real del CSV).
  - **2024**: se conservan las filas reales (Bronze + CONOSCE) y se generan
    sintéticas por bootstrap del Silver real (reasignadas a 2024, ocids realistas,
    `monto_adicional` reseteado a 0 antes del modelo) hasta `N_2024`.
  - **2022-2024 monto_adicional**: el modelo *ratio-based* solo aplica a filas con
    `monto_adicional == 0`; las filas que CONOSCE ya enriqueció con valores reales
    los **conservan intactos**. El modelo usa ratios reales del CSV (≤ 25%) y
    ocurrencia escalada (`ESSALUD_ADICIONAL_P`) para dotar señal positiva al ML.

Los registros sintéticos imitan el formato real (mismo esquema, ocids con patrón
`ocds-dgv273-seacev3-{año}-2543-{seq}`) para parecer obtenidos por Bronce→Silver.
Es un **comando separado** (`synth`): Silver nunca genera sintético por sí solo.
"""
from __future__ import annotations

from functools import lru_cache
from typing import Optional, Sequence, Tuple

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StructType

from app.audit.logger import setup_logger
from app.config.settings import settings
from app.config.spark_session import get_spark_session
from app.services.ocds_flattener import read_staging, write_staging

logger = setup_logger("ocds_framework.services.synthetic_generator")

ESSALUD_CSV_FILENAME = "CONOSCE_2025_essalud.csv"

# Tope legal de adicionales en contratación pública peruana (25% del contrato).
ADICIONAL_CAP_RATIO = 0.25
# Ocurrencia ESCALADA de adendas para 2022-2024 (el real EsSalud es ~0.15%, muy
# escaso). Se sube bastante para distribuir los adicionales entre muchos registros
# y dar una clase positiva robusta al modelado ML, conservando la forma real
# (ratios ≤ 25%). Configurable por `--adicional-rate`. Nota: la tasa efectiva es
# algo menor, porque las filas sin `monto_contratado` no reciben adenda.
ESSALUD_ADICIONAL_P = 0.15
DATE_COLS = ["fecha_convocatoria", "fecha_buena_pro", "fecha_suscripcion"]

# Conteos objetivo por año (totales en staging tras la síntesis).
TARGET_COUNTS = {2024: 2370, 2025: 2176}

# Patrón de ocid realista (idéntico al de SEACE para EsSalud).
_OCID_PREFIX = "ocds-dgv273-seacev3"
_OCID_ENTITY = "2543"


def _csv_path():
    return settings.EXTRA_DATA_DIR / ESSALUD_CSV_FILENAME


@lru_cache(maxsize=1)
def load_adicional_model() -> Tuple[Tuple[float, ...], float]:
    """
    Deriva el modelo de `monto_adicional` del CSV de EsSalud (cacheado).

    Returns:
        (ratios_reales, p_scaled):
          - ratios_reales: `_ratio_adicional` (o adic/contratado) de las filas con
            adenda real (forma observada: casi todas ≈ 0.25, tope legal).
          - p_scaled: ocurrencia a aplicar en 2022-2024 (ESSALUD_ADICIONAL_P).
    """
    df = pd.read_csv(_csv_path())
    adic = pd.to_numeric(df["monto_adicional"], errors="coerce")
    contr = pd.to_numeric(df["monto_contratado"], errors="coerce")
    pos_mask = adic > 0

    if "_ratio_adicional" in df.columns:
        ratios = pd.to_numeric(df.loc[pos_mask, "_ratio_adicional"], errors="coerce")
    else:
        ratios = adic[pos_mask] / contr[pos_mask]
    ratios = ratios.replace([float("inf"), -float("inf")], pd.NA).dropna()
    ratios = ratios[(ratios > 0) & (ratios <= 1.0)]
    if ratios.empty:
        ratios = pd.Series([ADICIONAL_CAP_RATIO])

    logger.info(
        f"Modelo adicional EsSalud: {int(pos_mask.sum())} adendas reales de "
        f"{len(df)} filas; ratios={[round(r, 3) for r in ratios.tolist()]}; "
        f"p_escalado={ESSALUD_ADICIONAL_P}."
    )
    return tuple(float(r) for r in ratios.tolist()), float(ESSALUD_ADICIONAL_P)


def load_essalud_csv(spark: SparkSession, schema: StructType) -> DataFrame:
    """
    Lee el CSV de EsSalud 2025 y lo proyecta al **esquema exacto del staging**.

    Selecciona las 26 columnas Silver (ignora las extras `_`) y castea cada una al
    tipo del `schema` de referencia (fechas→date, montos→double, flags→bool,
    `anio_fiscal`→int), de modo que sea unionable con el staging real.
    """
    raw = (
        spark.read.option("header", True)
        .option("multiLine", True)
        .option("quote", '"')
        .option("escape", '"')
        .csv(str(_csv_path()).replace("\\", "/"))
    )
    raw_cols = set(raw.columns)
    return raw.select(
        *[
            (F.col(f.name) if f.name in raw_cols else F.lit(None)).cast(f.dataType).alias(f.name)
            for f in schema.fields
        ]
    )


# ── Modelo ratio-based (escalado): lógica pura + UDF que la envuelve ─────────
def _compute_adicional(r_occ, r_pick, contratado, ratios, p_occur: float) -> float:
    """
    Adicional de una fila: con prob `p_occur` toma un ratio real y aplica
    `ratio * contratado` con tope del 25%; en otro caso 0.0. Lógica pura (sin
    Spark) para poder testearla sin levantar workers de Python.
    """
    if r_occ is None or r_occ >= p_occur or not contratado or contratado <= 0:
        return 0.0
    if not ratios:
        return 0.0
    ratio = ratios[int(r_pick * len(ratios)) % len(ratios)]
    return float(min(ratio * contratado, ADICIONAL_CAP_RATIO * contratado))


def _make_ratio_udf(ratios_bc, p_occur: float):
    """UDF que envuelve `_compute_adicional` con los ratios en broadcast."""

    @F.udf(DoubleType())
    def _udf(r_occ, r_pick, contratado):
        return _compute_adicional(r_occ, r_pick, contratado, ratios_bc.value, p_occur)

    return _udf


def _apply_scaled_adicional(
    df: DataFrame, spark: SparkSession, seed: int, adicional_p: Optional[float] = None
) -> DataFrame:
    """
    Inyecta `monto_adicional`/`tiene_adenda` con el modelo escalado.

    Preserva los valores reales de CONOSCE: solo aplica el modelo a las filas
    cuyo `monto_adicional` sea 0 o nulo. Las filas con adicional real (ya
    enriquecidas por el CONOSCE enricher en Silver) no se modifican.
    """
    ratios, default_p = load_adicional_model()
    p = adicional_p if adicional_p is not None else default_p
    udf = _make_ratio_udf(spark.sparkContext.broadcast(list(ratios)), p)
    cols = df.columns
    return (
        df.withColumn("_r_occ", F.rand(seed))
        .withColumn("_r_pick", F.rand(seed + 1))
        .withColumn(
            "monto_adicional",
            F.when(
                F.col("monto_adicional") > 0,
                F.col("monto_adicional"),   # valor real de CONOSCE: se preserva
            ).otherwise(
                udf(F.col("_r_occ"), F.col("_r_pick"), F.col("monto_contratado"))
            ),
        )
        .withColumn("tiene_adenda", F.col("monto_adicional") > 0)
        .select(cols)
    )


# ── Helpers de transformación ────────────────────────────────────────────────
def _shift_year(df: DataFrame, year: int) -> DataFrame:
    """Reasigna fechas (y `anio_fiscal`) al año objetivo, preservando mes/día."""
    for col in DATE_COLS:
        df = df.withColumn(
            col,
            F.when(
                F.col(col).isNotNull(),
                F.add_months(F.col(col), (F.lit(year) - F.year(F.col(col))) * 12),
            ).otherwise(F.lit(None).cast("date")),
        )
    return df.withColumn("anio_fiscal", F.lit(year).cast("int"))


def _with_realistic_ocid(df: DataFrame, year: int) -> DataFrame:
    """
    Reemplaza `ocid` por uno con patrón real SEACE para EsSalud, en un rango de
    secuencia alto (9000000+) que no colisiona con los ocids reales (~1xxxxxx).
    """
    seq = (F.lit(9000000) + (F.monotonically_increasing_id() % F.lit(1000000))).cast("long")
    return df.withColumn(
        "ocid",
        F.concat(F.lit(f"{_OCID_PREFIX}-{year}-{_OCID_ENTITY}-"), seq.cast("string")),
    )


# ── 2025: desde el CSV (datos reales EsSalud) ────────────────────────────────
def build_2025_from_csv(
    spark: Optional[SparkSession] = None,
    n: int = TARGET_COUNTS[2025],
    seed: int = 42,
    save: bool = True,
    schema: Optional[StructType] = None,
) -> DataFrame:
    """
    Pobla `anio_fiscal=2025` con `n` filas del CSV de EsSalud 2025.

    Conserva siempre las filas con adenda real y muestrea el resto hasta `n`.
    `monto_adicional` se mantiene **intacto** (valores reales del CSV).
    """
    spark = spark or get_spark_session()
    schema = schema or read_staging(spark).schema
    csv = load_essalud_csv(spark, schema).withColumn("anio_fiscal", F.lit(2025).cast("int"))

    adendas = csv.filter(F.col("monto_adicional") > 0)
    n_adendas = adendas.count()
    rest = csv.filter(F.coalesce(F.col("monto_adicional"), F.lit(0.0)) <= 0)
    n_rest = max(0, n - n_adendas)
    sampled = rest.orderBy(F.rand(seed)).limit(n_rest)
    result = adendas.unionByName(sampled).select(schema.fieldNames())

    total = result.count()
    logger.info(
        f"2025 desde CSV: {total} filas ({n_adendas} con adenda real conservadas)."
    )
    if save:
        write_staging(result, dynamic_partitions=True)
        logger.info("2025 (CSV) escrito en staging_flat (partición 2025).")
    return result


# ── 2024 (y otros años): boost por bootstrap del Silver real ─────────────────
def boost_year_with_synthetic(
    year: int,
    target_total: int,
    pool: DataFrame,
    spark: SparkSession,
    seed: int = 42,
    save: bool = True,
    adicional_p: Optional[float] = None,
) -> DataFrame:
    """
    Lleva `year` a `target_total` filas: conserva las reales + bootstrap sintético.

    Args:
        pool: DataFrame del Silver real (población de bootstrap; debe estar
            cacheado/estable, capturado antes de cualquier escritura sintética).
    """
    real_year = read_staging(spark, years=[year])
    n_real = real_year.count()
    n_syn = max(0, target_total - n_real)
    logger.info(f"{year}: {n_real} reales + {n_syn} sintéticas -> objetivo {target_total}.")

    if n_syn > 0:
        pool_total = pool.count()
        if n_syn <= pool_total:
            sampled = pool.orderBy(F.rand(seed)).limit(n_syn)
        else:
            sampled = pool.sample(
                withReplacement=True, fraction=float(n_syn) / pool_total + 0.1, seed=seed
            ).limit(n_syn)
        # Resetear monto_adicional a 0 en las filas bootstrap: los adicionales que
        # traen son de años 2022/2023 (reales de CONOSCE) y no corresponden al año
        # destino. El modelo los asigna desde cero de forma consistente.
        syn = _with_realistic_ocid(_shift_year(sampled, year), year)
        syn = syn.withColumn("monto_adicional", F.lit(0.0)).withColumn(
            "tiene_adenda", F.lit(False)
        )
        combined = real_year.unionByName(syn)
    else:
        combined = real_year

    combined = _apply_scaled_adicional(
        combined, spark, seed, adicional_p
    ).select(real_year.columns)
    total = combined.count()
    logger.info(f"{year}: {total} filas tras boost + modelo adicional.")
    if save:
        write_staging(combined, dynamic_partitions=True)
    return combined


# ── Enriquecimiento de adicional en años reales (sin cambiar conteos) ────────
def enrich_monto_adicional(
    years: Sequence[int] = (2022, 2023),
    seed: int = 7,
    spark: Optional[SparkSession] = None,
    save: bool = True,
    adicional_p: Optional[float] = None,
) -> DataFrame:
    """
    Aplica el modelo de adicional escalado a las particiones reales de `years`,
    conservando sus conteos (solo cambia `monto_adicional`/`tiene_adenda`).
    """
    spark = spark or get_spark_session()
    years = list(years)
    df = read_staging(spark, years=years)
    if df.count() == 0:
        logger.warning(f"Sin datos en staging para {years}; nada que enriquecer.")
        return df

    enriched = _apply_scaled_adicional(df, spark, seed, adicional_p)
    n_adenda = enriched.filter(F.col("monto_adicional") > 0).count()
    logger.info(f"Enriquecido adicional en {years}: {n_adenda} filas con adenda.")
    if save:
        write_staging(enriched, dynamic_partitions=True)
    return enriched


# ── Orquestación (CLI `synth`) ───────────────────────────────────────────────
def run_synthetic(
    n_2025: int = TARGET_COUNTS[2025],
    n_2024: int = TARGET_COUNTS[2024],
    enrich_years: Optional[Sequence[int]] = (2022, 2023),
    seed: int = 42,
    adicional_p: Optional[float] = None,
) -> None:
    """
    Genera la capa sintética completa (lo que invoca la CLI `synth`).

    1. Captura el Silver real (pool de bootstrap) antes de cualquier escritura.
    2. 2024 -> `n_2024` (reales + bootstrap + modelo adicional).
    3. 2025 -> `n_2025` desde el CSV (adicional real intacto).
    4. Enriquece `monto_adicional` en `enrich_years` (2022, 2023).

    Args:
        adicional_p: ocurrencia de adendas para 2022-2024 (None => default
            `ESSALUD_ADICIONAL_P`). Súbela para más señal positiva en ML.
    """
    spark = get_spark_session()
    real_pool = read_staging(spark).cache()
    if real_pool.count() == 0:
        raise RuntimeError(
            "El staging Silver está vacío. Corre `silver` antes de `synth` "
            "(el sintético hace bootstrap de los datos reales)."
        )
    schema = real_pool.schema

    logger.info("=== Generación sintética v2 (base CSV EsSalud) ===")
    boost_year_with_synthetic(
        2024, n_2024, real_pool, spark, seed=seed, save=True, adicional_p=adicional_p
    )
    build_2025_from_csv(spark, n=n_2025, seed=seed, save=True, schema=schema)
    if enrich_years:
        enrich_monto_adicional(
            years=enrich_years, seed=seed + 100, spark=spark, save=True,
            adicional_p=adicional_p,
        )

    real_pool.unpersist()
    logger.info("=== Generación sintética completada ===")
