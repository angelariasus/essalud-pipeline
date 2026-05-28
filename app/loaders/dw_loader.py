"""
Carga las dimensiones y la tabla de hechos en el Data Warehouse (SQL Server).

Orden de carga (respeta dependencias FK):
1. execute_ddl()                     → crea esquema, precarga Dim_Tiempo y 25 Dim_Ubigeo depts.
2. load_dim_ubigeo_distritos()       → INSERT nivel DISTRITO (depende de Dim_Ubigeo)
3. load_dim_entidad()                → depende de Dim_Ubigeo
4. load_dim_medicamento()            → independiente
5. load_dim_proveedor()              → independiente
6. load_fact()                       → depende de todas las Dims

Los SK se asignan DETERMINÍSTICAMENTE en Python (ver dim_resolver). Se usa
SET IDENTITY_INSERT ON para insertar con SK explícitos.
"""
import re
from pathlib import Path
from typing import Dict

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from app.audit.logger import setup_logger
from app.config.settings import settings

logger = setup_logger("ocds_framework.loaders.dw_loader")

# Columnas de la Fact que tienen DEFAULT 0 en el DDL y no vienen del DataFrame.
_FACT_ZERO_COLUMNS = [
    "Monto_Reduccion",
    "Monto_Prorroga",
    "Monto_Complementario",
]

# Columnas de la Fact que tienen DEFAULT NULL y no vienen del DataFrame.
_FACT_NULL_COLUMNS = [
    "Codigo_Convocatoria",
    "N_Cod_Contrato",
    "Num_Contrato",
    "Tiene_Resolucion",
    "Nro_Orden_Compra",
    "Tipo_Orden",
    "Estado_Contratacion_OC",
]

_SCHEMA = "oro"


def create_sqlalchemy_engine() -> Engine:
    """Crea el engine SQLAlchemy desde la cadena de conexión configurada."""
    conn_str = settings.DW_CONN_STRING
    if not conn_str:
        raise ValueError(
            "OCDS_DW_CONN_STRING no está configurada. "
            "Revisa tu archivo .env."
        )
    logger.info("Creando engine SQLAlchemy para DW.")
    return create_engine(conn_str)


def _split_sql_batches(sql: str):
    """Divide un script SQL por lotes separados por 'GO'."""
    for batch in re.split(r"^\s*GO\s*$", sql, flags=re.MULTILINE | re.IGNORECASE):
        batch = batch.strip()
        if batch:
            yield batch


def execute_ddl(engine: Engine, ddl_path: Path) -> None:
    """Ejecuta el script DDL completo (idempotente) con AUTOCOMMIT."""
    logger.info(f"Ejecutando DDL desde: {ddl_path}")
    if not ddl_path.exists():
        raise FileNotFoundError(f"Archivo DDL no encontrado: {ddl_path}")

    sql = ddl_path.read_text(encoding="utf-8")
    batches = list(_split_sql_batches(sql))
    logger.info(f"DDL dividido en {len(batches)} lotes.")

    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        for i, batch in enumerate(batches, start=1):
            logger.debug(f"Ejecutando lote DDL {i}/{len(batches)}...")
            conn.execute(text(batch))

    logger.info("DDL ejecutado correctamente.")


def _identity_insert_sql(table: str, on: bool) -> str:
    """Genera SET IDENTITY_INSERT schema.table ON|OFF."""
    estado = "ON" if on else "OFF"
    return f"SET IDENTITY_INSERT {_SCHEMA}.{table} {estado}"


def _load_dim(
    engine: Engine,
    table: str,
    df: pd.DataFrame,
    *,
    use_identity_insert: bool = True,
    chunksize: int = 100,
) -> None:
    """Carga un DataFrame en una dimensión con IDENTITY_INSERT si es necesario."""
    logger.info(f"Cargando {len(df)} filas en {_SCHEMA}.{table}...")
    # method='multi' con muchas columnas excede el límite de 2100 parámetros de SQL Server.
    # Se usa method=None (INSERT individuales por fila) con chunksize pequeño.
    with engine.begin() as conn:
        if use_identity_insert:
            conn.execute(text(_identity_insert_sql(table, True)))
        df.to_sql(
            table,
            conn,
            schema=_SCHEMA,
            if_exists="append",
            index=False,
            method=None,
            chunksize=chunksize,
        )
        if use_identity_insert:
            conn.execute(text(_identity_insert_sql(table, False)))
    logger.info(f"{_SCHEMA}.{table} cargado correctamente.")


def load_dim_entidad(engine: Engine, dim_df: pd.DataFrame) -> None:
    """Carga la dimensión de entidades compradoras."""
    if dim_df.empty:
        logger.warning("No hay entidades que cargar en Dim_Entidad_Compradora.")
        return
    _load_dim(engine, "Dim_Entidad_Compradora", dim_df)


def load_dim_medicamento(engine: Engine, dim_df: pd.DataFrame) -> None:
    """Carga la dimensión de medicamentos."""
    if dim_df.empty:
        logger.warning("No hay medicamentos que cargar en Dim_Medicamento.")
        return
    _load_dim(engine, "Dim_Medicamento", dim_df)


def load_dim_proveedor(engine: Engine, dim_df: pd.DataFrame) -> None:
    """Carga la dimensión de proveedores."""
    if dim_df.empty:
        logger.warning("No hay proveedores que cargar en Dim_Proveedor.")
        return
    _load_dim(engine, "Dim_Proveedor", dim_df)


def load_fact(engine: Engine, fact_df: pd.DataFrame, chunksize: int = 500) -> None:
    """Carga la tabla de hechos (sin IDENTITY_INSERT pues SK_Hecho es auto)."""
    if fact_df.empty:
        logger.warning("No hay filas que cargar en Fact_Ordenes_Y_Contratos.")
        return

    df = fact_df.copy()
    for col in _FACT_ZERO_COLUMNS:
        if col not in df.columns:
            df[col] = 0
    for col in _FACT_NULL_COLUMNS:
        if col not in df.columns:
            df[col] = None

    logger.info(f"Cargando {len(df)} filas en {_SCHEMA}.Fact_Ordenes_Y_Contratos...")
    with engine.begin() as conn:
        df.to_sql(
            "Fact_Ordenes_Y_Contratos",
            conn,
            schema=_SCHEMA,
            if_exists="append",
            index=False,
            method=None,
            chunksize=chunksize,
        )
    logger.info("Fact_Ordenes_Y_Contratos cargada correctamente.")


def load_all(
    engine: Engine,
    dims: Dict[str, pd.DataFrame],
    ddl_path: Path,
) -> None:
    """
    Orquesta la carga completa del DW respetando dependencias FK.

    Dim_Ubigeo no se carga aquí: a nivel Red se usan los 25 departamentos
    precargados por el DDL (FK_Ubigeo y FK_Ubigeo_Item apuntan a SK 1-25).
    """
    execute_ddl(engine, ddl_path)

    load_dim_entidad(engine, dims.get("dim_entidad", pd.DataFrame()))
    load_dim_medicamento(engine, dims.get("dim_medicamento", pd.DataFrame()))
    load_dim_proveedor(engine, dims.get("dim_proveedor", pd.DataFrame()))
    load_fact(engine, dims.get("fact", pd.DataFrame()))

    logger.info("=== Carga completa del Data Warehouse finalizada ===")
