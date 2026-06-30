"""
Capa Gold: construye el modelo dimensional desde el staging de Silver y lo carga
en el destino elegido (`GoldTarget`).

A diferencia de la versión previa (acoplada dentro de `SilverPipeline`), Gold es
ahora un paso independiente y reejecutable: lee el Parquet `staging_flat` que
Silver dejó (la frontera Silver→Gold), resuelve dimensiones + Fact con Spark
(`dim_resolver.resolve_all`) y delega la materialización en un `GoldTarget`
(Parquet por defecto, SQL Server opt-in). Así puede consumir indistintamente
datos reales o sintéticos (que viven en el mismo `staging_flat`).
"""
from __future__ import annotations

from pathlib import Path
from typing import Dict, Optional, Sequence

from pyspark.sql import DataFrame

from app.audit.logger import setup_logger
from app.config.settings import settings
from app.config.spark_session import get_spark_session, stop_spark_session
from app.loaders.master_loader import (
    ESTABLECIMIENTOS_FILENAME,
    PETITORIO_FILENAME,
    load_establecimientos,
    load_petitorio,
)
from app.loaders.targets import DEFAULT_TARGET, get_target
from app.loaders.targets.base import GoldTarget
from app.services.dim_resolver import resolve_all
from app.services.ocds_flattener import read_staging
from app.services.static_dims import build_dim_tiempo, build_dim_tipo_proceso, build_dim_ubigeo

logger = setup_logger("ocds_framework.pipelines.gold_layer")


class GoldPipeline:
    """
    Pipeline de la capa Gold: staging Silver → dimensiones/Fact → destino.
    """

    def __init__(self):
        self.extra_data_dir: Path = settings.EXTRA_DATA_DIR

    def run(
        self,
        years: Optional[Sequence[int]] = None,
        target: str | GoldTarget | None = None,
        profile: str = "local",
        rebuild: bool = False,
    ) -> Dict[str, DataFrame]:
        """
        Ejecuta la capa Gold.

        Args:
            years: años a cargar (filtra `anio_fiscal`); None = todo el staging.
            target: nombre de destino ('parquet'|'sqlserver') o instancia
                `GoldTarget` ya construida; None => destino por defecto (parquet).
            profile: perfil de conexión para SQL Server ('local'|'docker').
            rebuild: si True, limpia el destino antes de cargar (idempotencia).

        Returns:
            El dict de dimensiones/Fact resuelto (útil para inspección/tests).
        """
        years = list(years) if years else None
        target_obj = self._resolve_target(target, profile)

        if rebuild:
            from app.utils.cleaner import clean_gold

            logger.info(f"--rebuild: limpiando destino Gold '{target_obj.name}'.")
            clean_gold(target_obj)

        logger.info(
            f"=== Iniciando Pipeline Gold (target={target_obj.name}, años={years or 'todos'}) ==="
        )
        spark = get_spark_session()
        try:
            flat_df = read_staging(spark, years)
            if not flat_df.take(1):
                logger.warning(
                    "Staging Silver vacío (¿corriste Silver / synth?). Abortando Gold."
                )
                return {}

            logger.info("Cargando maestros (Petitorio, Establecimientos)...")
            petitorio = load_petitorio(self.extra_data_dir / PETITORIO_FILENAME)
            establecimientos = load_establecimientos(
                self.extra_data_dir / ESTABLECIMIENTOS_FILENAME
            )

            logger.info("Construyendo dimensiones y Fact (Spark)...")
            dims = resolve_all(flat_df, petitorio, establecimientos)

            # Dims estáticas: generadas aquí para que todos los destinos
            # (Parquet y SQL Server) produzcan las 7 tablas completas.
            dims["dim_tiempo"]       = build_dim_tiempo(spark)
            dims["dim_ubigeo"]       = build_dim_ubigeo(spark)
            dims["dim_tipo_proceso"] = build_dim_tipo_proceso(spark)

            # Materializar (cache) cada dim/Fact una sola vez: la lineage incluye
            # UDFs de fuzzy matching costosos y, en Windows, recomputarla varias
            # veces (count + write por tabla) sube el riesgo de crash de workers.
            for name, df in dims.items():
                dims[name] = df.cache()
                dims[name].count()

            logger.info(f"Cargando al destino '{target_obj.name}'...")
            target_obj.load(dims)

            logger.info("=== Pipeline Gold completado exitosamente ===")
            return dims
        finally:
            stop_spark_session()

    @staticmethod
    def _resolve_target(
        target: str | GoldTarget | None, profile: str
    ) -> GoldTarget:
        """Devuelve un GoldTarget a partir de un nombre, instancia o el default."""
        if isinstance(target, GoldTarget):
            return target
        name = target or DEFAULT_TARGET
        # `profile` solo aplica al destino SQL Server; los demás lo ignoran.
        kwargs = {"profile": profile} if name == "sqlserver" else {}
        return get_target(name, **kwargs)
