"""
Carga las dimensiones y la Fact en el Data Warehouse (SQL Server) con Spark+JDBC.

Estrategia transaccional (fail-safe), conforme al plan de migración:
  1. `execute_ddl` reconstruye el esquema de producción (oro.*) y crea el esquema
     [stg] + el stored procedure de carga atómica.
  2. Spark escribe cada DataFrame por JDBC a tablas STAGING (`stg.*`), creadas/
     sobrescritas automáticamente. Si una escritura falla, producción no se toca.
  3. `call_load_procedure` ejecuta `oro.usp_Load_From_Staging`, que mueve los
     datos a producción dentro de UNA transacción (atómica, con ROLLBACK ante error).

Las SK llegan resueltas desde Spark (deterministas); el procedure usa
SET IDENTITY_INSERT para insertarlas explícitamente (ver dim_resolver).
"""
import re
from pathlib import Path
from typing import Dict, Optional

from pyspark.sql import DataFrame
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine, make_url

from app.audit.logger import setup_logger
from app.config.settings import settings

logger = setup_logger("ocds_framework.loaders.dw_loader")

_JDBC_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
_LOAD_PROCEDURE = "oro.usp_Load_From_Staging"

# Mapa clave-dimensión -> tabla staging destino (esquema stg).
_STAGING_TABLES = {
    "dim_entidad": "stg.Dim_Entidad_Compradora",
    "dim_medicamento": "stg.Dim_Medicamento",
    "dim_proveedor": "stg.Dim_Proveedor",
    "fact": "stg.Fact_Ordenes_Y_Contratos",
}

# DDL de staging + stored procedure (junto al DDL del star schema).
STAGING_DDL_PATH = settings.PROJECT_ROOT / "star-schema" / "EsSalud_Staging_DDL.sql"


def create_sqlalchemy_engine() -> Engine:
    """Crea el engine SQLAlchemy/pyodbc (para DDL y el stored procedure de carga)."""
    conn_str = settings.DW_CONN_STRING
    if not conn_str:
        raise ValueError("OCDS_DW_CONN_STRING no está configurada. Revisa tu archivo .env.")
    logger.info("Creando engine SQLAlchemy para DW.")
    return create_engine(conn_str)


def _split_sql_batches(sql: str):
    """Divide un script SQL por lotes separados por 'GO'."""
    for batch in re.split(r"^\s*GO\s*$", sql, flags=re.MULTILINE | re.IGNORECASE):
        batch = batch.strip()
        if batch:
            yield batch


def execute_ddl(engine: Engine, ddl_path: Path) -> None:
    """Ejecuta un script SQL completo (idempotente) con AUTOCOMMIT, lote por lote."""
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


def _derive_jdbc_from_conn(conn_str: str):
    """Deriva (url, user, password) JDBC desde la cadena SQLAlchemy/pyodbc del DW."""
    u = make_url(conn_str)
    host = u.host or "localhost"
    port = f":{u.port}" if u.port else ""
    db = u.database or ""
    url = (
        f"jdbc:sqlserver://{host}{port};databaseName={db}"
        ";encrypt=true;trustServerCertificate=true"
    )
    return url, (u.username or ""), (u.password or "")


def _jdbc_conn():
    """
    Conexión JDBC para Spark. Usa las variables OCDS_DW_JDBC_* si están definidas;
    si no, las deriva de OCDS_DW_CONN_STRING (el .env ya configurado del usuario),
    evitando duplicar credenciales.
    """
    if settings.DW_JDBC_URL:
        return settings.DW_JDBC_URL, settings.DW_JDBC_USER, settings.DW_JDBC_PASSWORD
    if settings.DW_CONN_STRING:
        return _derive_jdbc_from_conn(settings.DW_CONN_STRING)
    return "", "", ""


def _jdbc_write_options() -> Dict[str, str]:
    """Opciones comunes de escritura JDBC al DW."""
    url, user, password = _jdbc_conn()
    return {
        "url": url,
        "user": user,
        "password": password,
        "driver": _JDBC_DRIVER,
        "batchsize": str(settings.DW_JDBC_BATCHSIZE),
    }


def write_staging_jdbc(df: DataFrame, table: str, num_partitions: int = 4) -> None:
    """Escribe un DataFrame a una tabla staging por JDBC (overwrite, en lotes)."""
    url, _, _ = _jdbc_conn()
    if not url:
        raise ValueError(
            "Sin conexión al DW: define OCDS_DW_JDBC_URL u OCDS_DW_CONN_STRING en tu .env. "
            "Para la escritura Spark→DW define además OCDS_SPARK_JARS_PACKAGES (driver mssql-jdbc)."
        )
    n = df.rdd.getNumPartitions()
    writer = df.coalesce(min(n, num_partitions)) if n > num_partitions else df
    logger.info(f"Escribiendo staging por JDBC -> {table}")
    (
        writer.write.format("jdbc")
        .options(**_jdbc_write_options())
        .option("dbtable", table)
        .mode("overwrite")
        .save()
    )


def write_all_staging(dims: Dict[str, DataFrame]) -> None:
    """Escribe todas las dimensiones y la Fact a sus tablas staging."""
    for key, table in _STAGING_TABLES.items():
        df = dims.get(key)
        if df is None:
            logger.warning(f"Sin DataFrame para '{key}'; se omite staging {table}.")
            continue
        write_staging_jdbc(df, table)


def call_load_procedure(engine: Engine, procedure: str = _LOAD_PROCEDURE) -> None:
    """Ejecuta el stored procedure de carga atómica Staging -> Producción."""
    logger.info(f"Ejecutando carga atómica: EXEC {procedure}")
    with engine.begin() as conn:
        conn.exec_driver_sql(f"EXEC {procedure}")
    logger.info("Carga atómica (Staging -> Producción) completada.")


def load_all(
    dims: Dict[str, DataFrame],
    ddl_path: Path,
    staging_ddl_path: Optional[Path] = None,
    engine: Optional[Engine] = None,
) -> None:
    """
    Orquesta la carga completa del DW: DDL → staging (JDBC) → carga atómica (SP).

    Args:
        dims: dict con DataFrames de Spark (dim_entidad, dim_medicamento,
              dim_proveedor, fact).
        ddl_path: ruta al DDL del star schema (reconstruye producción).
        staging_ddl_path: ruta al DDL de staging + stored procedure
            (default: STAGING_DDL_PATH).
        engine: engine SQLAlchemy opcional (si no, se crea y se cierra aquí).
    """
    owns_engine = engine is None
    engine = engine or create_sqlalchemy_engine()
    try:
        execute_ddl(engine, ddl_path)
        execute_ddl(engine, staging_ddl_path or STAGING_DDL_PATH)
        write_all_staging(dims)
        call_load_procedure(engine)
        logger.info("=== Carga completa del Data Warehouse finalizada ===")
    finally:
        if owns_engine:
            engine.dispose()
