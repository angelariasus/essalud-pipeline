"""
Orquesta la transformación Silver (Bronce → Plata → Oro).

Flujo completo:
1. Cargar maestros (Petitorio, Establecimientos)
2. Aplanar registros Bronze por año
3. Construir dimensiones y resolver FKs
4. Ejecutar DDL y cargar Data Warehouse
"""
from pathlib import Path

from app.audit.logger import setup_logger
from app.config.settings import settings
from app.loaders.master_loader import (
    ESTABLECIMIENTOS_FILENAME,
    PETITORIO_FILENAME,
    load_establecimientos,
    load_petitorio,
)
from app.loaders.dw_loader import create_sqlalchemy_engine, load_all
from app.services.dim_resolver import resolve_all
from app.services.ocds_flattener import flatten_all

logger = setup_logger("ocds_framework.pipelines.silver_layer")


class SilverPipeline:
    """
    Pipeline de la capa Silver: transforma datos Bronze en un modelo dimensional
    y los carga en el Data Warehouse (SQL Server).
    """

    def __init__(self):
        self.extra_data_dir: Path = settings.EXTRA_DATA_DIR
        self.ddl_path: Path = (
            settings.PROJECT_ROOT / "star-schema" / "EsSalud_StarSchema_DDL.sql"
        )

    def run(self, years=None):
        """
        Ejecuta el pipeline Silver completo.

        Args:
            years: Lista de años a procesar (default: [2022, 2023, 2024, 2025]).
        """
        years = years or [2022, 2023, 2024, 2025]
        logger.info(f"=== Iniciando Pipeline Silver para años {years} ===")

        # 1. Cargar maestros
        logger.info("Paso 1/5: Cargando maestros...")
        petitorio_path = self.extra_data_dir / PETITORIO_FILENAME
        establecimientos_path = self.extra_data_dir / ESTABLECIMIENTOS_FILENAME
        petitorio = load_petitorio(petitorio_path)
        establecimientos = load_establecimientos(establecimientos_path)

        # 2. Aplanar Bronze
        logger.info("Paso 2/5: Aplanando registros Bronze...")
        flat_df = flatten_all(years)
        if flat_df.empty:
            logger.warning("No se encontraron datos Bronze para aplanar. Abortando.")
            return

        # 3. Construir dimensiones y resolver FKs (nivel Red: usa departamentos del DDL)
        logger.info("Paso 3/5: Construyendo dimensiones...")
        dims = resolve_all(flat_df, petitorio, establecimientos)

        # 4. Conectar al DW
        logger.info("Paso 4/5: Conectando al Data Warehouse...")
        engine = create_sqlalchemy_engine()

        # 5. Ejecutar DDL y cargar
        logger.info("Paso 5/5: Ejecutando DDL y cargando datos...")
        try:
            load_all(engine, dims, self.ddl_path)
        finally:
            engine.dispose()
        logger.info("=== Pipeline Silver completado exitosamente ===")
