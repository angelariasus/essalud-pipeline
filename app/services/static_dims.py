"""
Genera Dim_Tiempo y Dim_Ubigeo como DataFrames Spark sin dependencia de SQL Server.
Expone también `export_dims_to_bi` para que cualquier destino Gold (Parquet o SQL
Server) pueda escribir los Parquet BI sin leer de vuelta desde el DW.

Los SKs están anclados al orden de inserción del DDL (EsSalud_StarSchema_DDL.sql)
para que los FK de la Fact resueltos por `dim_resolver` sean consistentes en
cualquier destino (Parquet o SQL Server).

  Dim_Tiempo : rango 2010-01-01 → 2030-12-31 + centinela SK=-1 (fechas nulas)
  Dim_Ubigeo : 25 departamentos + centinela SK=-1 (sin ubicación)
"""
from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from typing import Dict, List, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    BooleanType,
    ByteType,
    FloatType,
    IntegerType,
    DateType,
    ShortType,
    StringType,
    StructField,
    StructType,
)

# ── Dim_Tiempo ────────────────────────────────────────────────────────────────
_MESES = {
    1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril",
    5: "Mayo", 6: "Junio", 7: "Julio", 8: "Agosto",
    9: "Septiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre",
}
_DIAS = {
    0: "Lunes", 1: "Martes", 2: "Miércoles", 3: "Jueves",
    4: "Viernes", 5: "Sábado", 6: "Domingo",
}

_SCHEMA_TIEMPO = StructType([
    StructField("SK_Tiempo",        IntegerType(), False),
    StructField("Fecha_Completa",   DateType(),    False),
    StructField("Anio",             ShortType(),   False),
    StructField("Trimestre",        ByteType(),    False),
    StructField("Nombre_Trimestre", StringType(),  False),
    StructField("Mes",              ByteType(),    False),
    StructField("Nombre_Mes",       StringType(),  False),
    StructField("Semana_Anio",      ByteType(),    False),
    StructField("Dia",              ByteType(),    False),
    StructField("Dia_Semana",       ByteType(),    False),  # 1=Lun…7=Dom
    StructField("Nombre_Dia",       StringType(),  False),
    StructField("Es_Fin_Semana",    BooleanType(), False),
])


def build_dim_tiempo(
    spark: SparkSession,
    start: str = "2010-01-01",
    end: str = "2030-12-31",
) -> DataFrame:
    """Genera la dimensión calendario (spine diaria) de `start` a `end` inclusive."""
    rows: List[Tuple] = [
        (-1, date(1900, 1, 1), 1900, 1, "Q1", 1, "Enero", 1, 1, 1, "Lunes", False),
    ]
    d = date.fromisoformat(start)
    end_d = date.fromisoformat(end)
    while d <= end_d:
        wd = d.weekday()           # 0=Lun … 6=Dom
        trim = (d.month - 1) // 3 + 1
        rows.append((
            int(d.strftime("%Y%m%d")),
            d,
            d.year,
            trim,
            f"Q{trim}",
            d.month,
            _MESES[d.month],
            d.isocalendar()[1],    # semana ISO
            d.day,
            wd + 1,                # 1=Lun … 7=Dom  (igual que el CASE del DDL)
            _DIAS[wd],
            wd >= 5,               # sábado(5) o domingo(6)
        ))
        d += timedelta(days=1)
    return spark.createDataFrame(rows, _SCHEMA_TIEMPO)


# ── Dim_Ubigeo ────────────────────────────────────────────────────────────────
_SCHEMA_UBIGEO = StructType([
    StructField("SK_Ubigeo",      IntegerType(), True),
    StructField("Ubigeo",         StringType(),  True),
    StructField("Departamento",   StringType(),  False),
    StructField("Provincia",      StringType(),  True),
    StructField("Distrito",       StringType(),  True),
    StructField("Region_Natural", StringType(),  True),
    StructField("Macroregion",    StringType(),  True),
    StructField("Nivel_Geo",      StringType(),  False),
    StructField("Latitud",        FloatType(),   True),
    StructField("Longitud",       FloatType(),   True),
])

# SKs anclados al orden de inserción IDENTITY del DDL (centinela=-1, dptos 1-25).
# (Ubigeo, Departamento, Region_Natural, Macroregion, Latitud, Longitud)
_DPTOS = [
    ("010000", "AMAZONAS",      "SELVA",  "NORTE",    -6.229806,  -77.871810),
    ("020000", "ANCASH",        "SIERRA", "CENTRO",   -9.529782,  -77.527593),
    ("030000", "APURIMAC",      "SIERRA", "SUR",     -13.633919,  -72.881418),
    ("040000", "AREQUIPA",      "SIERRA", "SUR",     -16.409047,  -71.537451),
    ("050000", "AYACUCHO",      "SIERRA", "SUR",     -13.158850,  -74.223235),
    ("060000", "CAJAMARCA",     "SIERRA", "NORTE",    -7.163710,  -78.512796),
    ("070000", "CALLAO",        "COSTA",  "CENTRO",  -12.056503,  -77.118221),
    ("080000", "CUSCO",         "SIERRA", "SUR",     -13.531950,  -71.967463),
    ("090000", "HUANCAVELICA",  "SIERRA", "CENTRO",  -12.786773,  -74.976237),
    ("100000", "HUANUCO",       "SIERRA", "CENTRO",   -9.930640,  -76.242199),
    ("110000", "ICA",           "COSTA",  "SUR",     -14.067750,  -75.728623),
    ("120000", "JUNIN",         "SIERRA", "CENTRO",  -12.065286,  -75.204813),
    ("130000", "LA LIBERTAD",   "COSTA",  "NORTE",    -8.111599,  -79.028830),
    ("140000", "LAMBAYEQUE",    "COSTA",  "NORTE",    -6.771370,  -79.840880),
    ("150000", "LIMA",          "COSTA",  "CENTRO",  -12.046374,  -77.042793),
    ("160000", "LORETO",        "SELVA",  "ORIENTE",  -3.743700,  -73.251640),
    ("170000", "MADRE DE DIOS", "SELVA",  "ORIENTE", -12.593320,  -69.189215),
    ("180000", "MOQUEGUA",      "SIERRA", "SUR",     -17.193387,  -70.935645),
    ("190000", "PASCO",         "SIERRA", "CENTRO",  -10.682886,  -76.256145),
    ("200000", "PIURA",         "COSTA",  "NORTE",    -5.194490,  -80.632820),
    ("210000", "PUNO",          "SIERRA", "SUR",     -15.840221,  -70.021879),
    ("220000", "SAN MARTIN",    "SELVA",  "NORTE",    -6.034432,  -76.971700),
    ("230000", "TACNA",         "SIERRA", "SUR",     -18.006540,  -70.246360),
    ("240000", "TUMBES",        "COSTA",  "NORTE",    -3.566880,  -80.451530),
    ("250000", "UCAYALI",       "SELVA",  "ORIENTE",  -8.379150,  -74.553870),
]


def build_dim_ubigeo(spark: SparkSession) -> DataFrame:
    """Genera Dim_Ubigeo: 25 departamentos del Perú + centinela SK=-1."""
    rows: List[Tuple] = [
        (-1, None, "SIN UBICACION", None, None, "SIN DATO", "SIN DATO", "DEPARTAMENTO", None, None),
    ]
    for sk, (ubigeo, dpto, region, macro, lat, lon) in enumerate(_DPTOS, start=1):
        rows.append((sk, ubigeo, dpto, None, None, region, macro, "DEPARTAMENTO", lat, lon))
    return spark.createDataFrame(rows, _SCHEMA_UBIGEO)


# ── Dim_Tipo_Proceso ──────────────────────────────────────────────────────────
_SCHEMA_TIPO_PROCESO = StructType([
    StructField("SK_Tipo_Proceso",        IntegerType(), False),
    StructField("Tipo_Proceso_Seleccion", StringType(),  False),
    StructField("Objeto_Contractual",     StringType(),  False),
    StructField("Es_Contratacion_Directa", BooleanType(), False),
    StructField("Categoria_Proceso",      StringType(),  False),
])

# SKs anclados al orden de inserción IDENTITY del DDL (filas 1-12).
# SK_TIPO_PROCESO_DESCONOCIDO = 12 (usado por dim_resolver para el fallback).
_TIPOS_PROCESO = [
    (1,  "LICITACION PUBLICA",                    "BIEN",     False, "COMPETITIVO"),
    (2,  "CONCURSO PUBLICO",                      "SERVICIO", False, "COMPETITIVO"),
    (3,  "ADJUDICACION SIMPLIFICADA",             "BIEN",     False, "COMPETITIVO"),
    (4,  "ADJUDICACION SIMPLIFICADA",             "SERVICIO", False, "COMPETITIVO"),
    (5,  "SUBASTA INVERSA ELECTRONICA",           "BIEN",     False, "CATALOGO"),
    (6,  "CONVENIO MARCO",                        "BIEN",     False, "CATALOGO"),
    (7,  "COMPARACION DE PRECIOS",                "BIEN",     False, "COMPETITIVO"),
    (8,  "CONTRATACION DIRECTA",                  "BIEN",     True,  "DIRECTO"),
    (9,  "CONTRATACION DIRECTA",                  "SERVICIO", True,  "DIRECTO"),
    (10, "SELECCION DE CONSULTORES INDIVIDUALES", "SERVICIO", False, "COMPETITIVO"),
    (11, "REGIMEN ESPECIAL",                      "BIEN",     False, "REGIMEN_ESPECIAL"),
    (12, "DESCONOCIDO",                           "BIEN",     False, "REGIMEN_ESPECIAL"),
]


def build_dim_tipo_proceso(spark: SparkSession) -> DataFrame:
    """Genera Dim_Tipo_Proceso con los 12 tipos de proceso del DDL."""
    return spark.createDataFrame(_TIPOS_PROCESO, _SCHEMA_TIPO_PROCESO)


# ── Export BI ─────────────────────────────────────────────────────────────────
# Mapa clave interna del dims dict → nombre de tabla PascalCase para los Parquet BI.
BI_TABLE_MAP: Dict[str, str] = {
    "dim_tiempo":        "Dim_Tiempo",
    "dim_ubigeo":        "Dim_Ubigeo",
    "dim_entidad":       "Dim_Entidad_Compradora",
    "dim_medicamento":   "Dim_Medicamento",
    "dim_proveedor":     "Dim_Proveedor",
    "dim_tipo_proceso":  "Dim_Tipo_Proceso",
    "fact":              "Fact_Ordenes_Y_Contratos",
}


def export_dims_to_bi(dims: Dict[str, DataFrame], bi_dir) -> None:
    """
    Convierte los DataFrames del dict `dims` a archivos Parquet en `bi_dir`.

    Funciona con cualquier destino Gold (Parquet o SQL Server): no lee de vuelta
    desde el DW. Un archivo `<Tabla>.parquet` por entrada en `BI_TABLE_MAP`.
    """
    from app.audit.logger import setup_logger

    log = setup_logger("ocds_framework.services.static_dims")
    bi_path = Path(bi_dir)
    bi_path.mkdir(parents=True, exist_ok=True)
    log.info(f"Exportando modelo estrella -> Parquet BI en {bi_path}")
    for key, table in BI_TABLE_MAP.items():
        df = dims.get(key)
        if df is None:
            log.warning(f"  data/mart/: '{key}' no disponible en dims; se omite {table}.")
            continue
        pandas_df = df.toPandas()
        out = bi_path / f"{table}.parquet"
        pandas_df.to_parquet(out, index=False)
        log.info(f"  data/mart/: {table} ({len(pandas_df)} filas) -> {out.name}")
