"""
Destino Gold en Parquet local (`ParquetGoldTarget`).

Materializa cada dimensión y la Fact como un dataset Parquet bajo
`settings.GOLD_DIR` (default `data/gold/`). Es el destino **por defecto**: no
requiere SQL Server ni drivers JDBC, así que el pipeline completo Bronze→Silver→
Gold corre en cualquier máquina con solo Spark. Útil además como salida de
analítica directa (lectura desde pandas/DuckDB) y como checkpoint reproducible.
"""
from __future__ import annotations

from pathlib import Path
from typing import Dict, Optional

from pyspark.sql import DataFrame

from app.audit.logger import setup_logger
from app.config.settings import settings
from app.loaders.targets.base import GoldTarget
from app.services.static_dims import export_dims_to_bi

logger = setup_logger("ocds_framework.loaders.targets.parquet")


class ParquetGoldTarget(GoldTarget):
    """
    Escribe `dim_*` y `fact` como Parquet bajo `GOLD_DIR` (overwrite por tabla).

    Produce dos salidas:
      - `data/gold/<key>/` : Parquet multi-part por Spark (para lectura programática).
      - `data/bi/<Tabla>.parquet` : archivo único Pandas (para Power BI / DuckDB directo).
    """

    name = "parquet"

    def __init__(self, base_path: Optional[Path] = None):
        self.base_path = Path(base_path or settings.GOLD_DIR)

    def load(self, dims: Dict[str, DataFrame]) -> None:
        self.base_path.mkdir(parents=True, exist_ok=True)
        logger.info(f"Materializando capa Gold en Parquet -> {self.base_path}")
        for key, df in dims.items():
            if df is None:
                logger.warning(f"Sin DataFrame para '{key}'; se omite.")
                continue
            out = str(self.base_path / key).replace("\\", "/")
            n = df.count()
            df.write.mode("overwrite").parquet(out)
            logger.info(f"  {key}: {n} filas -> {out}")
        # Export bi/: archivos únicos PascalCase para Power BI / DuckDB.
        export_dims_to_bi(dims, settings.BI_DIR)
        logger.info("=== Capa Gold (Parquet) materializada ===")
