"""
Factory/registry de destinos de la capa Gold.

`get_target(name, **kwargs)` devuelve la implementación de `GoldTarget` pedida.
Registrar un nuevo destino (PostgreSQL, DuckDB, Data Lake, ...) es añadir una
entrada a `_REGISTRY` sin tocar `GoldPipeline` ni el resto del pipeline.
"""
from __future__ import annotations

from typing import Callable, Dict

from app.loaders.targets.base import GoldTarget
from app.loaders.targets.parquet_target import ParquetGoldTarget
from app.loaders.targets.sqlserver_target import SqlServerTarget

# Constructores perezosos por nombre (no se instancian hasta pedirlos).
_REGISTRY: Dict[str, Callable[..., GoldTarget]] = {
    "parquet": ParquetGoldTarget,
    "sqlserver": SqlServerTarget,
}

#: Destino por defecto cuando no se especifica `--target`.
DEFAULT_TARGET = "parquet"


def available_targets() -> list[str]:
    """Lista los nombres de destino registrados."""
    return sorted(_REGISTRY)


def get_target(name: str | None = None, **kwargs) -> GoldTarget:
    """
    Construye el `GoldTarget` por nombre.

    Args:
        name: 'parquet' (default) | 'sqlserver' | ... ; None => DEFAULT_TARGET.
        **kwargs: se pasan al constructor del destino (p. ej. `profile`,
            `ddl_path`, `base_path`).

    Raises:
        ValueError: si el nombre no está registrado.
    """
    key = (name or DEFAULT_TARGET).lower()
    if key not in _REGISTRY:
        raise ValueError(
            f"Destino Gold desconocido: {name!r}. "
            f"Disponibles: {available_targets()}."
        )
    return _REGISTRY[key](**kwargs)


__all__ = [
    "GoldTarget",
    "ParquetGoldTarget",
    "SqlServerTarget",
    "get_target",
    "available_targets",
    "DEFAULT_TARGET",
]
