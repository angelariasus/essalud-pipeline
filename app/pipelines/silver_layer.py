"""
Capa Silver: aplana los registros Bronze (OCDS) y los deja limpios en el staging
Parquet `data/silver/staging_flat/` (la frontera Silver→Gold).

Responsabilidad acotada (a diferencia de la versión previa, que además construía
dimensiones y cargaba el DW):
  1. Aplanar Bronze con Spark (una fila por ítem de licitación).
  2. Limpiar descripciones con IA (Gemini) **antes** de persistir, de modo que el
     Parquet de staging ya contenga `descripcion_item` normalizado.
  3. Escribir el staging particionado por `anio_fiscal` (overwrite dinámico: no
     borra años que no se están reprocesando).

La construcción de dimensiones + carga al destino vive ahora en `GoldPipeline`.
`SilverPipeline.run(..., load_dw=True)` se conserva como **façade** retro-
compatible (Silver + Gold a SQL Server) para no romper invocaciones existentes.
"""
from pathlib import Path
from typing import Optional, Sequence

from pyspark.sql import DataFrame

from app.audit.logger import setup_logger
from app.config.settings import settings
from app.config.spark_session import get_spark_session, stop_spark_session
from app.services.conosce_enricher import enrich_flat_with_conosce
from app.services.ocds_flattener import flatten_all, write_staging

logger = setup_logger("ocds_framework.pipelines.silver_layer")

DEFAULT_YEARS = [2022, 2023, 2024, 2025]


class SilverPipeline:
    """
    Pipeline de la capa Silver: Bronze (JSON) → staging Parquet limpio.
    """

    def __init__(self):
        self.extra_data_dir: Path = settings.EXTRA_DATA_DIR
        self.ddl_path: Path = (
            settings.PROJECT_ROOT / "star-schema" / "EsSalud_StarSchema_DDL.sql"
        )

    def run_silver(
        self, years: Optional[Sequence[int]] = None, rebuild: bool = False
    ) -> Optional[Path]:
        """
        Ejecuta solo la capa Silver (aplana + limpia IA + persiste staging).

        Args:
            years: años a procesar (default DEFAULT_YEARS).
            rebuild: si True, borra `staging_flat` antes de reescribir.

        Returns:
            Ruta del staging escrito, o None si no había datos Bronze.
        """
        years = list(years) if years else list(DEFAULT_YEARS)
        if rebuild:
            from app.utils.cleaner import clean_silver

            logger.info("--rebuild: limpiando staging Silver previo.")
            clean_silver()

        logger.info(f"=== Iniciando Pipeline Silver para años {years} ===")
        spark = get_spark_session()
        try:
            # 1. Aplanar Bronze (save=False: persistimos nosotros tras limpiar IA).
            logger.info("Paso 1/3: Aplanando registros Bronze (Spark)...")
            flat_df = flatten_all(years, spark=spark, save=False)
            if not flat_df.take(1):
                logger.warning("No se encontraron datos Bronze para aplanar. Abortando.")
                return None

            # 2. Limpieza con IA (Gemini) antes de persistir.
            logger.info("Paso 2/4: Limpiando descripciones con IA...")
            flat_df = self._apply_ai_cleaning(flat_df, spark)

            # 3. Enriquecimiento CONOSCE (contratos SEACE): rellena num_contrato,
            #    tiene_resolucion, montos de adenda y fecha_suscripcion cuando el
            #    OCDS no los trae. Join por (codigo_convocatoria, n_cod_contrato).
            logger.info("Paso 3/4: Enriqueciendo Silver con datos CONOSCE...")
            flat_df = enrich_flat_with_conosce(flat_df, spark)

            # 4. Persistir staging Parquet (overwrite dinámico por anio_fiscal).
            logger.info("Paso 4/4: Escribiendo staging Parquet...")
            out = write_staging(flat_df, dynamic_partitions=True)
            logger.info(f"=== Pipeline Silver completado: {out} ===")
            return out
        finally:
            stop_spark_session()

    def run(self, years: Optional[Sequence[int]] = None, load_dw: bool = True):
        """
        Façade retro-compatible: ejecuta Silver y, si `load_dw`, carga el DW.

        Mantiene la firma histórica `run(years=None, load_dw=True)`. Internamente
        delega en `run_silver` y, cuando `load_dw=True`, en `GoldPipeline` con
        destino SQL Server (comportamiento equivalente al original).
        """
        out = self.run_silver(years)
        if out is None:
            return
        if load_dw:
            logger.info("Façade: ejecutando capa Gold (SQL Server)...")
            from app.pipelines.gold_layer import GoldPipeline

            GoldPipeline().run(years, target="sqlserver")
        else:
            logger.info("load_dw=False: se omite la carga Gold (solo staging Silver).")

    def _apply_ai_cleaning(self, flat_df: DataFrame, spark) -> DataFrame:
        """
        Reemplaza `descripcion_item` por su versión limpia (Gemini), vía un mapa
        Broadcast aplicado con un UDF. Si el cleaner no está configurado
        (sin API key), devuelve un mapeo identidad y el DataFrame no cambia.
        """
        from pyspark.sql import functions as F
        from pyspark.sql.types import StringType

        from app.services.ai_cleaner import GeminiCleaner

        # Sin API key, el cleaner es identidad: NO añadir un UDF de Python al plan
        # (sería un no-op que solo agrega carga de workers y, en Windows, riesgo de
        # crash). Se devuelve el DataFrame intacto.
        cleaner = GeminiCleaner()
        if not cleaner.is_configured:
            logger.info("AI Cleaner desactivado (sin API key); se omite la limpieza.")
            return flat_df

        unique_rows = flat_df.select("descripcion_item").distinct().collect()
        raw_descs = [r.descripcion_item for r in unique_rows if r.descripcion_item]
        if not raw_descs:
            return flat_df

        gemini_mapping = cleaner.clean_descriptions(raw_descs)
        mapping_bc = spark.sparkContext.broadcast(gemini_mapping)

        def _clean(desc):
            return mapping_bc.value.get(desc, desc)

        clean_udf = F.udf(_clean, StringType())
        return flat_df.withColumn(
            "descripcion_item",
            F.coalesce(clean_udf(F.col("descripcion_item")), F.col("descripcion_item")),
        )
