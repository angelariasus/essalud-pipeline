"""
Orquesta la transformación Silver/Gold con Apache Spark (Bronce → Plata → Oro).

Flujo completo:
1. Cargar maestros con pandas (Petitorio, Establecimientos) — se broadcastean.
2. Aplanar records Bronze con Spark (Silver staging en Parquet).
3. Construir dimensiones y resolver FK con Spark (Fuzzy matching vectorizado).
4. Cargar el Data Warehouse: staging por JDBC + carga atómica (stored procedure).
"""
from pathlib import Path

from app.audit.logger import setup_logger
from app.config.settings import settings
from app.config.spark_session import get_spark_session, stop_spark_session
from app.loaders.master_loader import (
    ESTABLECIMIENTOS_FILENAME,
    PETITORIO_FILENAME,
    load_establecimientos,
    load_petitorio,
)
from app.loaders.dw_loader import load_all
from app.services.dim_resolver import resolve_all
from app.services.ocds_flattener import flatten_all

logger = setup_logger("ocds_framework.pipelines.silver_layer")


class SilverPipeline:
    """
    Pipeline de la capa Silver/Gold: transforma datos Bronze en un modelo
    dimensional con Spark y los carga en el Data Warehouse (SQL Server).
    """

    def __init__(self):
        self.extra_data_dir: Path = settings.EXTRA_DATA_DIR
        self.ddl_path: Path = (
            settings.PROJECT_ROOT / "star-schema" / "EsSalud_StarSchema_DDL.sql"
        )

    def run(self, years=None, load_dw: bool = True):
        """
        Ejecuta el pipeline Silver/Gold completo.

        Args:
            years: Lista de años a procesar (default: [2022, 2023, 2024, 2025]).
            load_dw: Si False, omite la carga al DW (útil para validar el
                aplanamiento y las dimensiones sin una instancia de SQL Server).
        """
        years = years or [2022, 2023, 2024, 2025]
        logger.info(f"=== Iniciando Pipeline Silver/Gold (Spark) para años {years} ===")

        spark = get_spark_session()
        try:
            # 1. Cargar maestros (pandas).
            logger.info("Paso 1/4: Cargando maestros...")
            petitorio = load_petitorio(self.extra_data_dir / PETITORIO_FILENAME)
            establecimientos = load_establecimientos(
                self.extra_data_dir / ESTABLECIMIENTOS_FILENAME
            )

            # 2. Aplanar Bronze con Spark (+ staging Parquet).
            logger.info("Paso 2/4: Aplanando registros Bronze (Spark)...")
            flat_df = flatten_all(years, spark=spark)
            if not flat_df.take(1):
                logger.warning("No se encontraron datos Bronze para aplanar. Abortando.")
                return

            # 2.5 Limpieza con IA (Gemini API)
            logger.info("Paso 2.5/4: Limpiando descripciones con Inteligencia Artificial...")
            from app.services.ai_cleaner import GeminiCleaner
            import pyspark.sql.functions as F
            from pyspark.sql.types import StringType
            
            # Obtener descripciones únicas
            unique_rows = flat_df.select("descripcion_item").distinct().collect()
            raw_descs = [row.descripcion_item for row in unique_rows if row.descripcion_item]
            
            # Limpiar con Gemini
            ai_cleaner = GeminiCleaner()
            gemini_mapping = ai_cleaner.clean_descriptions(raw_descs)
            
            # Aplicar limpieza usando un UDF con Broadcast
            gemini_mapping_bc = spark.sparkContext.broadcast(gemini_mapping)
            
            def _apply_ai_clean(desc):
                return gemini_mapping_bc.value.get(desc, desc)
            
            ai_clean_udf = F.udf(_apply_ai_clean, StringType())
            
            flat_df = flat_df.withColumn(
                "descripcion_item",
                F.coalesce(ai_clean_udf(F.col("descripcion_item")), F.col("descripcion_item"))
            )

            # 3. Construir dimensiones y resolver FKs (Spark).
            logger.info("Paso 3/4: Construyendo dimensiones (Spark)...")
            dims = resolve_all(flat_df, petitorio, establecimientos)

            # 4. Cargar el DW (staging JDBC + carga atómica).
            if load_dw:
                logger.info("Paso 4/4: Cargando Data Warehouse (staging + carga atómica)...")
                load_all(dims, self.ddl_path)
            else:
                logger.info("Paso 4/4: OMITIDO (load_dw=False). Materializando dimensiones...")
                for name, df in dims.items():
                    logger.info(f"  {name}: {df.count()} filas.")

            logger.info("=== Pipeline Silver/Gold completado exitosamente ===")
        finally:
            stop_spark_session()
