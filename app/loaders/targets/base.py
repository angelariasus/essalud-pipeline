"""
Abstracción de destinos de la capa Gold (`GoldTarget`).

La capa Gold resuelve el modelo dimensional (`dim_*` + `fact`) como un dict de
DataFrames de Spark. Dónde se materializa ese modelo es una decisión de
infraestructura, no de la lógica de negocio: SQL Server (local o Docker),
Parquet local, y a futuro PostgreSQL / DuckDB / Data Lake.

`GoldTarget` define el contrato mínimo (`load`) que cualquier destino implementa,
de modo que `GoldPipeline` dependa de esta abstracción y no de un motor concreto
(inversión de dependencias). Nuevos destinos se registran en `get_target`
(ver `__init__.py`) sin tocar la lógica principal.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Dict

from pyspark.sql import DataFrame


class GoldTarget(ABC):
    """Contrato de un destino de carga de la capa Gold."""

    #: Identificador corto del destino (se usa en logs y en la factory).
    name: str = "base"

    @abstractmethod
    def load(self, dims: Dict[str, DataFrame]) -> None:
        """
        Materializa las dimensiones y la Fact en el destino.

        Args:
            dims: dict con los DataFrames de Spark resueltos por
                `dim_resolver.resolve_all` (claves: `dim_entidad`,
                `dim_medicamento`, `dim_proveedor`, `fact`).
        """
        raise NotImplementedError
