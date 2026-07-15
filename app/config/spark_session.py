"""
SparkSession singleton para las capas Silver/Gold del pipeline EsSalud.

Centraliza la configuración de Spark (Arrow para Pandas UDFs, Adaptive Query
Execution, número de particiones de shuffle, y los JARs del driver JDBC de SQL
Server) en un único punto, de modo que flattener, fuzzy matcher, dim_resolver y
dw_loader compartan la misma sesión.

El driver JDBC se inyecta vía variables de entorno (coordenadas Maven en
`OCDS_SPARK_JARS_PACKAGES` o rutas locales en `OCDS_SPARK_JARS`) en lugar de
hardcodearlo: así las pruebas/uso offline no dependen de la red y la carga al DW
sí dispone del driver. Como `spark.jars.packages` solo puede fijarse antes de
crear el SparkContext, la sesión se construye una sola vez con esa config.
"""
from __future__ import annotations

import os
import sys
from typing import Optional

from pyspark.sql import SparkSession

from app.audit.logger import setup_logger
from app.config.settings import settings

logger = setup_logger("ocds_framework.config.spark_session")

_spark: Optional[SparkSession] = None


def get_spark_session(app_name: Optional[str] = None) -> SparkSession:
    """
    Devuelve la SparkSession compartida (la crea en la primera llamada).

    Configuración aplicada:
      - Arrow habilitado (con fallback) para Pandas UDFs vectorizados.
      - Adaptive Query Execution (coalesce de particiones).
      - `spark.sql.shuffle.partitions` acotado para datasets locales pequeños.
      - JARs del driver JDBC desde `settings.SPARK_JARS_PACKAGES` / `SPARK_JARS`.
    """
    global _spark
    if _spark is not None:
        return _spark

    # Los workers de Python (UDFs / Pandas UDFs) deben usar el MISMO intérprete
    # que el driver. Sin esto, Spark intenta lanzar "python"/"python3" y en
    # Windows cae en el alias de Microsoft Store ("Python was not found"),
    # provocando SocketTimeoutException al conectar el worker.
    os.environ["PYSPARK_PYTHON"] = os.environ.get("PYSPARK_PYTHON") or sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = (
        os.environ.get("PYSPARK_DRIVER_PYTHON") or sys.executable
    )

    # En Windows, Java necesita encontrar hadoop.dll en el PATH
    if os.name == "nt":
        hadoop_home = os.environ.get("HADOOP_HOME", "")
        if hadoop_home:
            hadoop_bin = os.path.join(hadoop_home, "bin")
            if hadoop_bin not in os.environ.get("PATH", ""):
                os.environ["PATH"] = f"{hadoop_bin};{os.environ.get('PATH', '')}"

    builder = (
        SparkSession.builder
        .appName(app_name or settings.SPARK_APP_NAME)
        .master(settings.SPARK_MASTER)
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .config("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", settings.SPARK_SHUFFLE_PARTITIONS)
        # Evita timezones ambiguas al castear timestamps del OCDS.
        .config("spark.sql.session.timeZone", "America/Lima")
        # Fail-safe: datos sucios del OCDS deben coercer a NULL (como pandas
        # errors='coerce'), no lanzar excepción. Spark 4 trae ANSI ON por defecto.
        .config("spark.sql.ansi.enabled", "false")
        # Traceback de Python legible si un worker de UDF falla/crashea.
        .config("spark.sql.execution.pyspark.udf.faulthandler.enabled", "true")
    )

    if settings.SPARK_JARS_PACKAGES:
        logger.info(f"Spark jars.packages: {settings.SPARK_JARS_PACKAGES}")
        builder = builder.config("spark.jars.packages", settings.SPARK_JARS_PACKAGES)
    if settings.SPARK_JARS:
        logger.info(f"Spark jars: {settings.SPARK_JARS}")
        builder = builder.config("spark.jars", settings.SPARK_JARS)

    # Opciones JVM adicionales (p. ej. --add-opens para Arrow en Java 17/21).
    # Spark ya inyecta las necesarias; esta variable es un escape hatch opcional.
    extra_java_opts = os.getenv("OCDS_SPARK_DRIVER_JAVA_OPTS", "")
    if extra_java_opts:
        builder = builder.config("spark.driver.extraJavaOptions", extra_java_opts)

    logger.info(
        f"Creando SparkSession (master={settings.SPARK_MASTER}, "
        f"app={app_name or settings.SPARK_APP_NAME})."
    )
    _spark = builder.getOrCreate()
    _spark.sparkContext.setLogLevel(os.getenv("OCDS_SPARK_LOG_LEVEL", "WARN"))
    logger.info(f"SparkSession lista (Spark {_spark.version}).")
    return _spark


def stop_spark_session() -> None:
    """Detiene la SparkSession compartida (si existe) y limpia el singleton."""
    global _spark
    if _spark is not None:
        logger.info("Deteniendo SparkSession.")
        _spark.stop()
        _spark = None
