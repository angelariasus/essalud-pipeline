"""
Destino Gold en SQL Server (`SqlServerTarget`).

Envuelve la lógica transaccional ya existente y probada de
`app.loaders.dw_loader.load_all` (DDL idempotente → staging JDBC → stored
procedure atómico) **sin modificarla**, exponiéndola tras la interfaz
`GoldTarget`. Así el resto del pipeline depende de la abstracción y `dw_loader`
(cubierto por `test_dw_loader.py`) permanece intacto.

Perfiles:
  - `local` (default): usa `OCDS_DW_CONN_STRING` / `OCDS_DW_JDBC_*` del .env.
  - `docker`: usa las variantes `*_DOCKER` si están definidas; si no, cae a las
    estándar. La selección se hace sustituyendo temporalmente los atributos del
    `settings` compartido mientras corre la carga (y restaurándolos después),
    para no tocar `dw_loader`, que los lee de `settings`.
"""
from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Dict, Optional

from pyspark.sql import DataFrame

from app.audit.logger import setup_logger
from app.config.settings import settings
from app.loaders.targets.base import GoldTarget
from app.services.static_dims import export_dims_to_bi

logger = setup_logger("ocds_framework.loaders.targets.sqlserver")

# Atributos de settings que definen el perfil docker (override -> estándar).
_DOCKER_OVERRIDES = {
    "DW_CONN_STRING": "DW_CONN_STRING_DOCKER",
    "DW_JDBC_URL": "DW_JDBC_URL_DOCKER",
    "DW_JDBC_USER": "DW_JDBC_USER_DOCKER",
    "DW_JDBC_PASSWORD": "DW_JDBC_PASSWORD_DOCKER",
}


@contextmanager
def _profile(profile: str):
    """Aplica temporalmente las cadenas del perfil docker sobre `settings`."""
    if profile != "docker":
        yield
        return
    saved = {}
    try:
        for target_attr, docker_attr in _DOCKER_OVERRIDES.items():
            override = getattr(settings, docker_attr, "")
            if override:
                saved[target_attr] = getattr(settings, target_attr)
                setattr(settings, target_attr, override)
        if saved:
            logger.info(f"Perfil docker activo (override: {sorted(saved)}).")
        else:
            logger.warning(
                "Perfil docker sin variables *_DOCKER definidas; se usan las "
                "cadenas estándar del .env."
            )
        yield
    finally:
        for attr, value in saved.items():
            setattr(settings, attr, value)


class SqlServerTarget(GoldTarget):
    """Carga el modelo dimensional en SQL Server vía `dw_loader.load_all`."""

    name = "sqlserver"

    def __init__(self, ddl_path: Optional[Path] = None, profile: str = "local"):
        self.ddl_path = ddl_path or (
            settings.PROJECT_ROOT / "star-schema" / "EsSalud_StarSchema_DDL.sql"
        )
        self.profile = profile

    def load(self, dims: Dict[str, DataFrame]) -> None:
        # Import diferido: solo se requiere SQLAlchemy/pyodbc si se usa este destino.
        from app.loaders.dw_loader import load_all

        logger.info(f"Cargando capa Gold en SQL Server (perfil={self.profile}).")
        with _profile(self.profile):
            # load_all solo procesa las 4 tablas gestionadas por JDBC/SP
            # (dim_entidad, dim_medicamento, dim_proveedor, fact); dim_tiempo
            # y dim_ubigeo del dict son ignoradas por _STAGING_TABLES → no hay
            # conflicto con las tablas que el DDL ya crea y puebla en SQL Server.
            load_all(dims, self.ddl_path)
            # Export bi/: desde DataFrames en memoria (no requiere leer SQL Server).
            export_dims_to_bi(dims, settings.BI_DIR)
        logger.info("=== Capa Gold (SQL Server) cargada + export bi/ ===")
