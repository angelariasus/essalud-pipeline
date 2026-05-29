"""
Aplanamiento robusto de la capa Bronze con Apache Spark (capa Silver staging).

Reemplaza la lectura iterativa con `pandas` por la API de DataFrames de Spark:

  1. **Lectura segura**: `spark.read.json` con `StructType` estricto, modo
     PERMISSIVE y captura de registros corruptos en `_corrupt_record`
     (no detiene el job ante JSON defectuoso).
  2. **Transformación**: `explode_outer` de los ítems de la licitación y
     resolución de award/contract por ítem mediante funciones de orden superior
     (`filter`/`exists`/`aggregate`), replicando la cascada de matching original.
  3. **Checkpoint**: escritura del resultado aplanado a Parquet particionado por
     `anio_fiscal` en `data/silver/staging_flat/`.

El grano de salida es una fila por ítem de licitación, idéntico al de la
implementación previa en pandas (ver pruebas de equivalencia).
"""
from __future__ import annotations

from pathlib import Path
from typing import List, Optional, Sequence, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    StringType,
    StructField,
    StructType,
)

from app.audit.logger import setup_logger
from app.config.settings import settings
from app.config.spark_session import get_spark_session
from app.utils.fuzzy_matcher import extract_red_asistencial

logger = setup_logger("ocds_framework.services.ocds_flattener")

CORRUPT_COL = "_corrupt_record"

# ── Esquema estricto del record OCDS proyectado (solo campos consumidos) ──────
_VALUE = StructType([StructField("amount", DoubleType())])
_UNIT = StructType([StructField("name", StringType())])
_ITEM = StructType([
    StructField("id", StringType()),
    StructField("description", StringType()),
    StructField("quantity", DoubleType()),
    StructField("unit", _UNIT),
])
_TENDER = StructType([
    StructField("title", StringType()),
    StructField("description", StringType()),
    StructField("procurementMethod", StringType()),
    StructField("procurementMethodDetails", StringType()),
    StructField("procurementMethodRationale", StringType()),
    StructField("value", _VALUE),
    StructField("tenderPeriod", StructType([StructField("startDate", StringType())])),
    StructField("items", ArrayType(_ITEM)),
])
_ID_ONLY = StructType([StructField("id", StringType())])
_SUPPLIER = StructType([StructField("id", StringType()), StructField("name", StringType())])
_AWARD = StructType([
    StructField("id", StringType()),
    StructField("date", StringType()),
    StructField("status", StringType()),
    StructField("value", _VALUE),
    StructField("suppliers", ArrayType(_SUPPLIER)),
    StructField("items", ArrayType(_ID_ONLY)),
])
_CONTRACT = StructType([
    StructField("id", StringType()),
    StructField("awardID", StringType()),
    StructField("dateSigned", StringType()),
    StructField("value", _VALUE),
    StructField("items", ArrayType(_ID_ONLY)),
    StructField("amendments", ArrayType(StructType([StructField("value", _VALUE)]))),
])
_PARTY = StructType([
    StructField("id", StringType()),
    StructField("name", StringType()),
    StructField("roles", ArrayType(StringType())),
    StructField("address", StructType([StructField("region", StringType())])),
    StructField("additionalIdentifiers", ArrayType(StructType([
        StructField("id", StringType()),
        StructField("scheme", StringType()),
    ]))),
])
_COMPILED = StructType([
    StructField("tender", _TENDER),
    StructField("awards", ArrayType(_AWARD)),
    StructField("contracts", ArrayType(_CONTRACT)),
    StructField("parties", ArrayType(_PARTY)),
])
BRONZE_SCHEMA = StructType([
    StructField("ocid", StringType()),
    StructField("compiledRelease", _COMPILED),
    StructField(CORRUPT_COL, StringType()),
])

# Columnas de fecha (string ISO) que se parsean a DATE tomando solo YYYY-MM-DD.
_DATE_COLUMNS = ["fecha_convocatoria", "fecha_buena_pro", "fecha_suscripcion"]

_RED_SCHEMA = StructType([
    StructField("candidato", StringType()),
    StructField("metodo", StringType()),
])


@F.udf(_RED_SCHEMA)
def _extract_red_udf(title, tender_desc, item_descs):
    """UDF: extrae la Red Asistencial (candidato/metodo) con degradación segura."""
    try:
        descs = [tender_desc] + (list(item_descs) if item_descs else [])
        res = extract_red_asistencial(title, descs)
        return (res.get("candidato"), res.get("metodo"))
    except Exception as exc:  # noqa: BLE001 - fail-safe, no detener el job
        return (None, f"ERROR: {exc}")


def _clean_ruc_col(col):
    """Extrae los dígitos finales de un id 'PE-RUC-20101102204' -> '20101102204'."""
    cleaned = F.regexp_extract(col, r"(\d+)$", 1)
    return (
        F.when(col.isNull(), F.lit(None))
        .when(cleaned == "", col)
        .otherwise(cleaned)
    )


def _to_date_col(col):
    """Parsea un ISO datetime tomando solo los primeros 10 chars (YYYY-MM-DD)."""
    return F.to_date(F.substring(col, 1, 10), "yyyy-MM-dd")


_VALID_SCHEMA = StructType([f for f in BRONZE_SCHEMA.fields if f.name != CORRUPT_COL])


def _empty_flat(spark: SparkSession) -> DataFrame:
    """DataFrame vacío con el esquema de salida del aplanamiento."""
    empty_valid = spark.createDataFrame([], _VALID_SCHEMA)
    flat = flatten_dataframe(empty_valid)
    return spark.createDataFrame([], flat.schema)


def read_bronze(spark: SparkSession, paths: Sequence[str]) -> DataFrame:
    """Lee records Bronze con esquema estricto, PERMISSIVE y captura de corruptos."""
    return (
        spark.read
        .schema(BRONZE_SCHEMA)
        .option("multiline", "true")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", CORRUPT_COL)
        # Se pasan directorios + pathGlobFilter en vez de un glob literal "*.json":
        # en Windows, un glob literal dispara FileNotFoundException al hacer
        # getFileStatus del patrón antes de expandirlo (ruido no fatal).
        .option("pathGlobFilter", "*.json")
        .json(list(paths))
    )


def split_corrupt(df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """Separa registros válidos (drop de _corrupt_record) de los corruptos."""
    valid = df.filter(F.col(CORRUPT_COL).isNull()).drop(CORRUPT_COL)
    corrupt = df.filter(F.col(CORRUPT_COL).isNotNull())
    return valid, corrupt


def flatten_dataframe(df_valid: DataFrame) -> DataFrame:
    """
    Aplana un DataFrame de records OCDS a una fila por ítem de licitación.

    Reproduce la semántica de la versión pandas: resolución de comprador (buyer
    con respaldo procuringEntity), Red Asistencial a nivel record, cascada de
    award/contract por ítem y suma de adendas.
    """
    tender = F.col("compiledRelease.tender")
    items = tender["items"]
    parties = F.col("compiledRelease.parties")

    # ── Comprador: primer party con rol buyer, respaldo procuringEntity ──
    buyer = F.coalesce(
        F.element_at(F.filter(parties, lambda p: F.array_contains(p["roles"], F.lit("buyer"))), 1),
        F.element_at(F.filter(parties, lambda p: F.array_contains(p["roles"], F.lit("procuringEntity"))), 1),
    )
    peruc = F.element_at(
        F.filter(buyer["additionalIdentifiers"], lambda x: x["scheme"] == F.lit("PE-RUC")), 1
    )["id"]

    rec = (
        df_valid
        .withColumn("_buyer", buyer)
        .withColumn("ocid", F.col("ocid"))
        .withColumn("title", tender["title"])
        .withColumn("metodo_contratacion", tender["procurementMethod"])
        .withColumn("detalles_metodo", tender["procurementMethodDetails"])
        .withColumn("causal_cd", tender["procurementMethodRationale"])
        .withColumn("monto_referencial", tender["value"]["amount"])
        .withColumn("fecha_convocatoria_raw", tender["tenderPeriod"]["startDate"])
        .withColumn("_items", items)
        .withColumn("_awards", F.col("compiledRelease.awards"))
        .withColumn("_contracts", F.col("compiledRelease.contracts"))
        .withColumn(
            "_red",
            _extract_red_udf(
                tender["title"],
                tender["description"],
                F.transform(items, lambda it: it["description"]),
            ),
        )
    )
    rec = (
        rec
        .withColumn("ruc_comprador", F.coalesce(peruc, _clean_ruc_col(F.col("_buyer")["id"])))
        .withColumn("nombre_comprador", F.col("_buyer")["name"])
        .withColumn("departamento_comprador", F.col("_buyer")["address"]["region"])
        .withColumn("red_candidato", F.col("_red")["candidato"])
        .withColumn("red_metodo", F.col("_red")["metodo"])
    )

    # ── Explode de ítems (explode_outer evita pérdida silenciosa); se descarta
    #    el relleno nulo de records sin ítems para conservar el grano por ítem. ──
    exp = rec.withColumn("item", F.explode_outer(F.col("_items"))).filter(F.col("item").isNotNull())
    exp = exp.withColumn("_item_id", F.col("item.id"))

    # ── Cascada de award por ítem (replica _find_award) ──
    awards_matching = F.filter(
        F.col("_awards"),
        lambda a: F.exists(a["items"], lambda i: i["id"] == F.col("_item_id")),
    )
    any_award_items = F.coalesce(
        F.exists(F.col("_awards"), lambda a: F.size(a["items"]) > 0), F.lit(False)
    )
    exp = exp.withColumn("_awards_matching", awards_matching).withColumn(
        "_any_award_items", any_award_items
    )
    exp = exp.withColumn(
        "_award",
        F.when(F.size("_awards_matching") > 0, F.element_at("_awards_matching", 1))
        .when((~F.col("_any_award_items")) & (F.size("_awards") > 0), F.element_at("_awards", 1))
        .otherwise(F.lit(None)),
    )

    # ── Cascada de contract por ítem (replica _find_contract) ──
    c_by_awardid = F.when(
        F.col("_award").isNotNull(),
        F.filter(
            F.col("_contracts"),
            lambda c: c["awardID"].isNotNull() & (c["awardID"] == F.col("_award")["id"]),
        ),
    )
    c_by_item = F.filter(
        F.col("_contracts"),
        lambda c: F.exists(c["items"], lambda i: i["id"] == F.col("_item_id")),
    )
    exp = exp.withColumn("_c_by_awardid", c_by_awardid).withColumn("_c_by_item", c_by_item)
    exp = exp.withColumn(
        "_contract",
        F.when(F.size("_contracts") <= 0, F.lit(None))
        .when(F.size("_c_by_awardid") > 0, F.element_at("_c_by_awardid", 1))
        .when(F.size("_c_by_item") > 0, F.element_at("_c_by_item", 1))
        .when(F.size("_contracts") == 1, F.element_at("_contracts", 1))
        .otherwise(F.lit(None)),
    )

    supplier = F.element_at(F.col("_award")["suppliers"], 1)
    monto_adicional = F.coalesce(
        F.aggregate(
            F.col("_contract")["amendments"],
            F.lit(0.0),
            lambda acc, am: acc + F.coalesce(am["value"]["amount"], F.lit(0.0)),
        ),
        F.lit(0.0),
    )

    flat = exp.select(
        F.col("ocid"),
        F.col("_item_id").alias("n_item"),
        F.col("item")["description"].alias("descripcion_item"),
        F.col("item")["quantity"].alias("cantidad"),
        F.col("item")["unit"]["name"].alias("unidad_medida"),
        _to_date_col(F.col("fecha_convocatoria_raw")).alias("fecha_convocatoria"),
        _to_date_col(F.col("_award")["date"]).alias("fecha_buena_pro"),
        _to_date_col(F.col("_contract")["dateSigned"]).alias("fecha_suscripcion"),
        F.col("monto_referencial"),
        F.col("_award")["value"]["amount"].alias("monto_adjudicado"),
        F.col("_contract")["value"]["amount"].alias("monto_contratado"),
        monto_adicional.alias("monto_adicional"),
        F.col("ruc_comprador"),
        F.col("nombre_comprador"),
        F.col("departamento_comprador"),
        F.col("red_candidato"),
        F.col("red_metodo"),
        _clean_ruc_col(supplier["id"]).alias("ruc_proveedor"),
        supplier["name"].alias("nombre_proveedor"),
        F.col("metodo_contratacion"),
        F.col("detalles_metodo"),
        F.col("causal_cd"),
        # pandas: `None == "direct"` -> False; Spark da NULL. Coalesce a False.
        F.coalesce(F.col("metodo_contratacion") == F.lit("direct"), F.lit(False))
        .alias("es_contratacion_directa"),
        F.col("_award")["status"].alias("estado_adjudicacion"),
        (F.size(F.col("_contract")["amendments"]) > 0).alias("tiene_adenda"),
    )

    flat = flat.withColumn(
        "anio_fiscal",
        F.coalesce(
            F.year(F.col("fecha_convocatoria")),
            F.year(F.col("fecha_buena_pro")),
            F.year(F.col("fecha_suscripcion")),
        ),
    )
    return flat


def _year_dir(year: int, ruc: str) -> Path:
    return settings.BRONZE_DIR / "records" / ruc / str(year)


def _as_dir(directory: Path) -> str:
    """Ruta del directorio en formato URI con forward slashes (se filtra *.json al leer)."""
    return str(directory).replace("\\", "/")


def flatten_paths(spark: SparkSession, paths: Sequence[str]) -> DataFrame:
    """Lee, separa corruptos (los registra) y aplana los records de las rutas dadas."""
    # Cache obligatorio: Spark prohíbe consultar solo `_corrupt_record` sobre la
    # lectura cruda (UNSUPPORTED_FEATURE.QUERY_ONLY_CORRUPT_RECORD_COLUMN). Cachear
    # además evita re-parsear el JSON al separar válidos/corruptos y aplanar.
    raw = read_bronze(spark, paths).cache()
    total = raw.count()
    valid, corrupt = split_corrupt(raw)
    n_corrupt = corrupt.count()
    if n_corrupt:
        logger.warning(f"Registros corruptos detectados (ignorados): {n_corrupt}")
    logger.info(f"Records leídos: {total} (válidos: {total - n_corrupt}, corruptos: {n_corrupt})")
    return flatten_dataframe(valid)


def flatten_year(
    year: int, ruc: str = settings.ESSALUD_RUC, spark: Optional[SparkSession] = None
) -> DataFrame:
    """Aplana un año de Bronze a un DataFrame de Spark por ítem."""
    spark = spark or get_spark_session()
    directory = _year_dir(year, ruc)
    if not directory.exists():
        logger.warning(f"Directorio Bronze inexistente: {directory}")
        return _empty_flat(spark)
    logger.info(f"Aplanando records Bronze desde: {directory}")
    return flatten_paths(spark, [_as_dir(directory)])


def write_staging(df: DataFrame, path: Optional[Path] = None) -> Path:
    """Escribe el aplanado a Parquet particionado por anio_fiscal (Silver staging)."""
    path = path or (settings.SILVER_DIR / "staging_flat")
    path.mkdir(parents=True, exist_ok=True)
    out = str(path).replace("\\", "/")
    logger.info(f"Escribiendo staging Parquet particionado -> {out}")
    df.write.mode("overwrite").partitionBy("anio_fiscal").parquet(out)
    return path


def flatten_all(
    years: List[int],
    ruc: str = settings.ESSALUD_RUC,
    spark: Optional[SparkSession] = None,
    save: bool = True,
) -> DataFrame:
    """
    Aplana varios años de Bronze en un único DataFrame de Spark.

    Lee en una sola operación los directorios existentes de los años solicitados
    y, opcionalmente, persiste el resultado como Parquet particionado (staging).
    """
    spark = spark or get_spark_session()
    dirs = [_year_dir(y, ruc) for y in years]
    paths = [_as_dir(d) for d in dirs if d.exists()]
    if not paths:
        logger.warning(f"Sin directorios Bronze para los años {years} (RUC {ruc}).")
        return _empty_flat(spark)

    flat = flatten_paths(spark, paths)
    n = flat.count()
    logger.info(f"Aplanado: {n} filas-ítem de {len(paths)} año(s).")

    if save and n:
        write_staging(flat)
    return flat
