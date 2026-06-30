"""
Construye las dimensiones de la capa Oro y resuelve las FK de la Fact con Spark.

Reescritura de la versión pandas a la API de DataFrames de Spark:
  - Las Surrogate Keys se generan con `row_number()` (forma distribuida y
    determinística, en lugar de un contador en memoria).
  - El Fuzzy Matching se ejecuta vía Pandas UDFs vectorizados con Broadcast de
    los maestros (Petitorio, Establecimientos).
  - Las FK de la Fact se resuelven por `join` contra los mapas de cada dimensión.

Las dimensiones precargadas por el DDL (Dim_Tiempo, Dim_Ubigeo departamentos,
Dim_Tipo_Proceso) NO se regeneran; sus SK se replican como constantes ancladas
al orden de inserción del DDL. La carga usa SET IDENTITY_INSERT (ver dw_loader).
"""
from __future__ import annotations

from typing import Dict, Tuple

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from pyspark.sql.window import Window

from app.audit.logger import setup_logger
from app.loaders.master_loader import normalize_text
from app.utils.fuzzy_matcher import make_match_medicamento_udf, make_match_red_udf

logger = setup_logger("ocds_framework.services.dim_resolver")

RUC_ESSALUD = "20131257750"

# ── SK anclados al DDL ───────────────────────────────────────────────────────
DEPARTAMENTO_SK: Dict[str, int] = {
    "AMAZONAS": 1, "ANCASH": 2, "APURIMAC": 3, "AREQUIPA": 4, "AYACUCHO": 5,
    "CAJAMARCA": 6, "CALLAO": 7, "CUSCO": 8, "HUANCAVELICA": 9, "HUANUCO": 10,
    "ICA": 11, "JUNIN": 12, "LA LIBERTAD": 13, "LAMBAYEQUE": 14, "LIMA": 15,
    "LORETO": 16, "MADRE DE DIOS": 17, "MOQUEGUA": 18, "PASCO": 19, "PIURA": 20,
    "PUNO": 21, "SAN MARTIN": 22, "TACNA": 23, "TUMBES": 24, "UCAYALI": 25,
}

TIPO_PROCESO_SK: Dict[str, int] = {
    "LICITACION PUBLICA": 1,
    "CONCURSO PUBLICO": 2,
    "ADJUDICACION SIMPLIFICADA": 3,
    "SUBASTA INVERSA ELECTRONICA": 5,
    "CONVENIO MARCO": 6,
    "COMPARACION DE PRECIOS": 7,
    "CONTRATACION DIRECTA": 8,
}
SK_TIPO_PROCESO_CD = 8
SK_TIPO_PROCESO_DESCONOCIDO = 12

# Sentinels (precargados por el DDL).
SK_SENTINEL = -1
SK_MED_NO_FARMA = -1
SK_MED_HISTORICO = -2
SK_TIEMPO_NULO = -1

PETITORIO_COLS = [
    "N_ITEM", "CODIGO_SAP", "DENOMINACION_DCI", "ESPECIFICACIONES_TECNICAS",
    "UNIDAD_MANEJO", "RESTRICCION_USO", "ESPECIALIDAD_AUTORIZADA", "INDICACIONES",
    "DENOMINACION_DCI_NORM",
]


@F.udf(StringType())
def _norm_udf(value):
    """normalize_text como UDF (mayúsculas, sin tildes, espacios colapsados)."""
    return normalize_text(value)


def _sk_tiempo(col_name: str):
    """Fecha DATE -> SK_Tiempo (YYYYMMDD int); NULL -> -1."""
    return F.coalesce(
        F.date_format(F.col(col_name), "yyyyMMdd").cast("int"), F.lit(SK_TIEMPO_NULO)
    )


# ── Dim_Proveedor ────────────────────────────────────────────────────────────
def build_dim_proveedor(flat_df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """Una fila por RUC de proveedor único. Devuelve (dimensión, mapa ruc->SK)."""
    w_pick = Window.partitionBy("ruc_proveedor").orderBy(
        F.col("nombre_proveedor").asc_nulls_last()
    )
    pares = (
        flat_df.where(F.col("ruc_proveedor").isNotNull())
        .withColumn("_rn", F.row_number().over(w_pick))
        .where(F.col("_rn") == 1)
        .select("ruc_proveedor", "nombre_proveedor")
    )

    nombre = F.when(
        (F.col("nombre_proveedor").isNull()) | (F.col("nombre_proveedor") == ""),
        F.lit("SIN NOMBRE"),
    ).otherwise(F.col("nombre_proveedor"))

    base = (
        pares
        .withColumn("_nombre", nombre)
        .withColumn("_es_consorcio", F.upper(F.col("_nombre")).contains("CONSORCIO"))
        .withColumn("SK_Proveedor", F.row_number().over(Window.orderBy("ruc_proveedor")))
    )
    tipo = (
        F.when(F.col("_es_consorcio"), F.lit("Consorcio"))
        .when(F.col("ruc_proveedor").startswith("20"), F.lit("Persona Juridica"))
        .when(F.col("ruc_proveedor").startswith("10"), F.lit("Persona Natural"))
        .otherwise(F.lit(None))
    )
    dim = base.select(
        "SK_Proveedor",
        F.substring("ruc_proveedor", 1, 11).alias("RUC_Proveedor"),
        F.substring(F.col("_nombre"), 1, 250).alias("Nombre_Proveedor"),
        tipo.alias("Tipo_Proveedor"),
        F.col("_es_consorcio").cast("int").alias("Es_Consorcio"),
    )
    prov_map = base.select("ruc_proveedor", "SK_Proveedor")
    logger.info(f"Dim_Proveedor: {dim.count()} proveedores.")
    return dim, prov_map


# ── Dim_Medicamento ──────────────────────────────────────────────────────────
def _petitorio_sdf(spark, petitorio_df: pd.DataFrame) -> DataFrame:
    """
    Convierte el Petitorio (pandas) a Spark DF de strings, deduplicado por DCI norm.

    Cuando varias filas comparten DENOMINACION_DCI_NORM (frecuente: distintas
    presentaciones), se conserva la de MAYOR índice original, replicando el
    "last-wins" del dict de la versión pandas de forma determinística.
    """
    pet = petitorio_df.reindex(columns=PETITORIO_COLS).copy()
    pet = pet.where(pd.notnull(pet), None)
    rows = [tuple(r) + (i,) for i, r in enumerate(pet.values.tolist())]
    schema = StructType(
        [StructField(c, StringType()) for c in PETITORIO_COLS]
        + [StructField("_ord", IntegerType())]
    )
    sdf = spark.createDataFrame(rows, schema).where(F.col("DENOMINACION_DCI_NORM").isNotNull())
    w = Window.partitionBy("DENOMINACION_DCI_NORM").orderBy(F.col("_ord").desc())
    return sdf.withColumn("_rn", F.row_number().over(w)).where(F.col("_rn") == 1).drop("_rn", "_ord")


def build_dim_medicamento(
    flat_df: DataFrame, petitorio_df: pd.DataFrame
) -> Tuple[DataFrame, DataFrame]:
    """
    Una fila por descripción SEACE normalizada clasificada como medicamento
    (EXACTO/FUZZY). Devuelve (dimensión, mapa norm->FK_Medicamento) donde el mapa
    también incluye HISTORICO (-2); DUDOSO queda fuera (se resuelve a -1 en la Fact).
    """
    spark = flat_df.sparkSession
    choices = petitorio_df["DENOMINACION_DCI_NORM"].dropna().tolist()
    med_udf = make_match_medicamento_udf(spark.sparkContext.broadcast(choices))

    descs = (
        flat_df.select("descripcion_item")
        .where(F.col("descripcion_item").isNotNull())
        .distinct()
        .withColumn("norm", _norm_udf(F.col("descripcion_item")))
        .where(F.col("norm").isNotNull())
    )
    matched = descs.withColumn("m", med_udf(F.col("descripcion_item"))).select(
        "descripcion_item", "norm", "m.dci", "m.score", "m.metodo"
    )
    # Grano: una fila por descripción normalizada (colapsa variantes crudas).
    matched1 = (
        matched.withColumn(
            "_rn",
            F.row_number().over(
                Window.partitionBy("norm").orderBy(F.col("descripcion_item").asc_nulls_last())
            ),
        )
        .where(F.col("_rn") == 1)
        .drop("_rn")
    )

    pet_sdf = _petitorio_sdf(spark, petitorio_df)
    em = matched1.where(F.col("metodo").isin("EXACTO", "FUZZY"))
    joined = (
        em.join(pet_sdf, em["dci"] == pet_sdf["DENOMINACION_DCI_NORM"], "left")
        .withColumn("SK_Medicamento", F.row_number().over(Window.orderBy("norm")))
    )

    dim = joined.select(
        "SK_Medicamento",
        F.col("N_ITEM").cast("int").alias("N_Item_Petitorio"),
        F.col("CODIGO_SAP").alias("Codigo_SAP"),
        F.substring(F.coalesce(F.col("DENOMINACION_DCI"), F.col("dci")), 1, 300).alias("Denominacion_DCI"),
        F.substring(F.col("ESPECIFICACIONES_TECNICAS"), 1, 500).alias("Especificaciones_Tecnicas"),
        F.substring(F.col("UNIDAD_MANEJO"), 1, 20).alias("Unidad_De_Manejo"),
        F.substring(F.col("RESTRICCION_USO"), 1, 50).alias("Restriccion_Uso"),
        F.substring(F.col("ESPECIALIDAD_AUTORIZADA"), 1, 300).alias("Especialidad_Autorizada"),
        F.substring(F.col("INDICACIONES"), 1, 1000).alias("Indicaciones_Observaciones"),
        F.lit(1).alias("En_Petitorio_2026"),
        F.substring(F.col("descripcion_item"), 1, 600).alias("Descripcion_SEACE_Original"),
        F.round(F.col("score"), 2).alias("Score_Fuzzy_Match"),
        F.col("metodo").alias("Metodo_Clasificacion"),
    )

    fk_real = joined.select("norm", F.col("SK_Medicamento").alias("fk_medicamento"))
    fk_hist = matched1.where(F.col("metodo") == "HISTORICO").select(
        "norm", F.lit(SK_MED_HISTORICO).alias("fk_medicamento")
    )
    desc_sk_df = fk_real.unionByName(fk_hist)

    logger.info(f"Dim_Medicamento: {dim.count()} clasificados (EXACTO/FUZZY).")
    return dim, desc_sk_df


# ── Dim_Entidad_Compradora (nivel Red) ───────────────────────────────────────
def build_dim_entidad(
    flat_df: DataFrame, establecimientos_df: pd.DataFrame
) -> Tuple[DataFrame, DataFrame]:
    """
    Una fila por Red Asistencial presente. FK_Ubigeo apunta al departamento
    dominante de la Red. Devuelve (dimensión, mapa red_candidato->red_canónica).
    """
    spark = flat_df.sparkSession
    red_choices = [
        r for r in establecimientos_df["RED_NORM"].dropna().unique()
        if r not in ("RED", "RED ASISTENCIAL")
    ]
    red_udf = make_match_red_udf(spark.sparkContext.broadcast(red_choices))

    cand_red = (
        flat_df.select("red_candidato")
        .where(F.col("red_candidato").isNotNull())
        .distinct()
        .withColumn("m", red_udf(F.col("red_candidato")))
        .select("red_candidato", F.col("m.red").alias("red"))
    )

    # Departamento dominante por Red (mode) -> FK_Ubigeo (pandas, datos pequeños).
    est = establecimientos_df.copy()
    est["DPTO_NORM"] = est["DPTO"].map(normalize_text)
    fk_rows = []
    for red, grp in est.groupby("RED_NORM"):
        modes = grp["DPTO_NORM"].mode()
        depto = modes.iloc[0] if len(modes) else None
        fk_rows.append((red, int(DEPARTAMENTO_SK.get(depto, SK_SENTINEL))))
    fk_df = spark.createDataFrame(fk_rows, ["red", "fk_ubigeo"]) if fk_rows else \
        spark.createDataFrame([], "red string, fk_ubigeo int")

    redes_sk = (
        cand_red.where(F.col("red").isNotNull())
        .select("red").distinct()
        .withColumn("SK_Entidad", F.row_number().over(Window.orderBy("red")))
    )
    ent = redes_sk.join(fk_df, "red", "left").withColumn(
        "FK_Ubigeo", F.coalesce(F.col("fk_ubigeo"), F.lit(SK_SENTINEL))
    )
    tipo_red = (
        F.when(F.col("red").contains("PRESTACIONAL"), F.lit("RP"))
        .when(F.col("red").contains("ASISTENCIAL"), F.lit("RA"))
        .otherwise(F.lit("INST"))
    )
    dim_ent = ent.select(
        "SK_Entidad",
        "FK_Ubigeo",
        F.format_string("RED%03d", F.col("SK_Entidad")).alias("Cod_EESS"),
        F.lit(RUC_ESSALUD).alias("RUC_Entidad"),
        F.substring(F.col("red"), 1, 100).alias("Red_Asistencial"),
        tipo_red.alias("Tipo_Red"),
        F.substring(F.col("red"), 1, 200).alias("Nombre_Centro"),
        F.lit("FUNCIONA").alias("Estado_Operativo"),
    )
    logger.info(f"Dim_Entidad: {dim_ent.count()} redes asistenciales.")
    return dim_ent, cand_red


# ── Resolución de la Fact ────────────────────────────────────────────────────
def resolve_fact(
    flat_df: DataFrame,
    prov_map: DataFrame,
    desc_sk_df: DataFrame,
    dim_ent: DataFrame,
    cand_red: DataFrame,
) -> DataFrame:
    """Resuelve todas las FK por join y arma la Fact. Descarta filas sin año fiscal."""
    f = flat_df.where(F.col("anio_fiscal").isNotNull())

    # Columnas CONOSCE opcionales: presentes cuando la capa Silver ya fue enriquecida;
    # ausentes cuando flat_df viene directo del flattener (p.ej. en tests unitarios).
    _conosce_defaults = {
        "num_contrato": F.lit(None).cast("string"),
        "monto_reduccion": F.lit(0.0).cast("double"),
        "monto_prorroga": F.lit(0.0).cast("double"),
        "monto_complementario": F.lit(0.0).cast("double"),
    }
    for col_name, default_expr in _conosce_defaults.items():
        if col_name not in f.columns:
            f = f.withColumn(col_name, default_expr)

    f = f.withColumn("norm", _norm_udf(F.col("descripcion_item")))

    # Medicamento (norm -> FK), Proveedor (ruc -> SK).
    f = f.join(desc_sk_df, "norm", "left").withColumn(
        "FK_Medicamento", F.coalesce(F.col("fk_medicamento"), F.lit(SK_MED_NO_FARMA))
    )
    f = f.join(prov_map, "ruc_proveedor", "left").withColumn(
        "FK_Proveedor", F.coalesce(F.col("SK_Proveedor"), F.lit(SK_SENTINEL))
    )

    # Entidad: red_candidato -> red canónica -> SK_Entidad + FK_Ubigeo_Item.
    ent_map = dim_ent.select(
        F.col("Red_Asistencial").alias("_red_canon"),
        F.col("SK_Entidad"),
        F.col("FK_Ubigeo").alias("_ent_fkubigeo"),
    )
    f = (
        f.join(cand_red.withColumnRenamed("red", "_red_canon"), "red_candidato", "left")
        .join(ent_map, "_red_canon", "left")
        .withColumn("FK_Entidad", F.coalesce(F.col("SK_Entidad"), F.lit(SK_SENTINEL)))
        .withColumn("FK_Ubigeo_Item", F.coalesce(F.col("_ent_fkubigeo"), F.lit(SK_SENTINEL)))
    )

    # Tipo de proceso.
    f = f.withColumn("_det_norm", _norm_udf(F.col("detalles_metodo")))
    tp = F.when(F.col("metodo_contratacion") == "direct", F.lit(SK_TIPO_PROCESO_CD))
    for clave, sk in TIPO_PROCESO_SK.items():
        tp = tp.when(F.col("_det_norm").contains(clave), F.lit(sk))
    tp = tp.otherwise(F.lit(SK_TIPO_PROCESO_DESCONOCIDO))
    f = f.withColumn("FK_Tipo_Proceso", tp)

    return f.select(
        _sk_tiempo("fecha_convocatoria").alias("FK_Tiempo_Convocatoria"),
        _sk_tiempo("fecha_buena_pro").alias("FK_Tiempo_Buena_Pro"),
        _sk_tiempo("fecha_suscripcion").alias("FK_Tiempo_Suscripcion"),
        "FK_Entidad", "FK_Medicamento", "FK_Proveedor", "FK_Tipo_Proceso", "FK_Ubigeo_Item",
        F.col("fecha_convocatoria").alias("Fecha_Convocatoria"),
        F.col("fecha_buena_pro").alias("Fecha_Buena_Pro"),
        F.col("fecha_suscripcion").alias("Fecha_Suscripcion"),
        F.col("n_item").cast("smallint").alias("N_Item"),
        F.col("codigo_convocatoria").cast("bigint").alias("Codigo_Convocatoria"),
        F.col("n_cod_contrato").cast("bigint").alias("N_Cod_Contrato"),
        F.col("num_contrato").cast("string").alias("Num_Contrato"),
        F.coalesce(F.col("cantidad"), F.lit(0.0)).alias("Cantidad_Adjudicada"),
        F.coalesce(F.col("monto_referencial"), F.lit(0.0)).alias("Monto_Referencial_Soles"),
        F.coalesce(F.col("monto_adjudicado"), F.lit(0.0)).alias("Monto_Adjudicado_Soles"),
        F.coalesce(F.col("monto_contratado"), F.lit(0.0)).alias("Monto_Contratado_Item"),
        F.coalesce(F.col("monto_adicional"), F.lit(0.0)).alias("Monto_Adicional"),
        F.coalesce(F.col("monto_reduccion"), F.lit(0.0)).alias("Monto_Reduccion"),
        F.coalesce(F.col("monto_prorroga"), F.lit(0.0)).alias("Monto_Prorroga"),
        F.coalesce(F.col("monto_complementario"), F.lit(0.0)).alias("Monto_Complementario"),
        F.col("es_contratacion_directa").cast("int").alias("Flag_Contratacion_Directa"),
        F.coalesce(F.col("norm").contains("FUERA DEL PETITORIO"), F.lit(False))
        .cast("int").alias("Flag_Fuera_Petitorio"),
        F.lit("PEN").alias("Moneda_Original"),
        F.when(F.col("es_contratacion_directa"), F.lit("CONTRATACION_DIRECTA"))
        .otherwise(F.lit("ADJUDICACION")).alias("Fuente_Dataset"),
        F.col("anio_fiscal").cast("int").alias("Anio_Fiscal"),
    )


def resolve_all(
    flat_df: DataFrame, petitorio_df: pd.DataFrame, establecimientos_df: pd.DataFrame
) -> Dict[str, DataFrame]:
    """Orquesta la construcción de dimensiones y la resolución de la Fact (Spark)."""
    flat_df = flat_df.cache()
    flat_df.count()  # materializa el cache (evita recomputar el flatten por dimensión)

    dim_prov, prov_map = build_dim_proveedor(flat_df)
    dim_med, desc_sk_df = build_dim_medicamento(flat_df, petitorio_df)
    dim_ent, cand_red = build_dim_entidad(flat_df, establecimientos_df)
    fact = resolve_fact(flat_df, prov_map, desc_sk_df, dim_ent, cand_red)
    return {
        "dim_entidad": dim_ent,
        "dim_medicamento": dim_med,
        "dim_proveedor": dim_prov,
        "fact": fact,
    }
