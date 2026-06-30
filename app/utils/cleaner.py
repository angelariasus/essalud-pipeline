"""
Limpieza/reconstrucción por capa (Bronze · Silver · Gold).

Cada función borra **solo** los artefactos de su capa, nunca de otra, ni toca
Airflow ni Cloudflare R2. Se usan tanto desde la CLI (`clean <capa>`,
`--rebuild`) como desde los pipelines.

Garantías de seguridad:
  - Los `rmtree` se restringen a rutas dentro de `settings.DATA_DIR` (un guardia
    evita borrar fuera del data lake por una mala configuración de env).
  - `clean_bronze` exige confirmación explícita (`confirm=True`): re-descargar la
    capa Bronze desde la API es costoso, así que no se borra por accidente.
"""
from __future__ import annotations

import shutil
from pathlib import Path
from typing import Optional

from app.audit.logger import setup_logger
from app.config.settings import settings

logger = setup_logger("ocds_framework.utils.cleaner")


def _safe_rmtree(path: Path) -> bool:
    """Borra `path` recursivamente solo si está dentro de DATA_DIR. Devuelve si borró."""
    path = Path(path)
    data_dir = settings.DATA_DIR.resolve()
    try:
        path.resolve().relative_to(data_dir)
    except ValueError:
        raise ValueError(
            f"Negado por seguridad: {path} está fuera de DATA_DIR ({data_dir})."
        )
    if not path.exists():
        logger.info(f"Nada que limpiar (no existe): {path}")
        return False
    shutil.rmtree(path)
    logger.info(f"Eliminado: {path}")
    return True


def clean_bronze(confirm: bool = False) -> None:
    """
    Borra los registros Bronze (`data/bronze/records`).

    Requiere `confirm=True` (la CLI lo mapea a `--yes`): re-extraer Bronze desde
    la API OCDS es costoso, por eso no se borra sin confirmación explícita.
    """
    if not confirm:
        raise ValueError(
            "clean_bronze requiere confirmación explícita (confirm=True / --yes): "
            "borrar Bronze obliga a re-descargar desde la API."
        )
    _safe_rmtree(settings.BRONZE_DIR / "records")
    logger.info("Capa Bronze (records) limpiada.")


def clean_silver() -> None:
    """Borra el staging de Silver (`data/silver/staging_flat`)."""
    from app.services.ocds_flattener import staging_flat_path

    _safe_rmtree(staging_flat_path())
    logger.info("Capa Silver (staging_flat) limpiada.")


def clean_gold(target=None, profile: str = "local") -> None:
    """
    Limpia la capa Gold del destino indicado.

    - Parquet: borra `data/gold/`.
    - SQL Server: re-ejecuta el DDL idempotente (DROP+CREATE), dejando el esquema
      `oro.*`/`stg.*` reconstruido y vacío (carga atómica, sin datos previos).

    Args:
        target: nombre ('parquet'|'sqlserver'), instancia `GoldTarget`, o None
            (=> destino por defecto, parquet).
        profile: perfil de conexión SQL Server ('local'|'docker').
    """
    from app.loaders.targets import DEFAULT_TARGET, get_target
    from app.loaders.targets.base import GoldTarget

    if isinstance(target, GoldTarget):
        target_obj = target
    else:
        name = target or DEFAULT_TARGET
        kwargs = {"profile": profile} if name == "sqlserver" else {}
        target_obj = get_target(name, **kwargs)

    if target_obj.name == "parquet":
        base_path: Optional[Path] = getattr(target_obj, "base_path", settings.GOLD_DIR)
        _safe_rmtree(Path(base_path))
        logger.info("Capa Gold (Parquet) limpiada.")
    elif target_obj.name == "sqlserver":
        _rebuild_sqlserver_schema()
        logger.info("Capa Gold (SQL Server) reconstruida (DDL idempotente).")
    else:
        raise ValueError(
            f"clean_gold no sabe limpiar el destino '{target_obj.name}'."
        )


def _rebuild_sqlserver_schema() -> None:
    """Re-ejecuta el DDL del star schema + staging (drop+create idempotente)."""
    from app.loaders.dw_loader import (
        STAGING_DDL_PATH,
        create_sqlalchemy_engine,
        execute_ddl,
    )

    ddl_path = settings.PROJECT_ROOT / "star-schema" / "EsSalud_StarSchema_DDL.sql"
    engine = create_sqlalchemy_engine()
    try:
        execute_ddl(engine, ddl_path)
        execute_ddl(engine, STAGING_DDL_PATH)
    finally:
        engine.dispose()


def clean_layer(layer: str, *, confirm: bool = False, target=None, profile: str = "local") -> None:
    """Despacha la limpieza por nombre de capa (usado por la CLI `clean <capa>`)."""
    layer = layer.lower()
    if layer == "bronze":
        clean_bronze(confirm=confirm)
    elif layer == "silver":
        clean_silver()
    elif layer == "gold":
        clean_gold(target=target, profile=profile)
    else:
        raise ValueError(f"Capa desconocida: {layer!r} (usa bronze|silver|gold).")
