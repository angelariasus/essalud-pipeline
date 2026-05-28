from dataclasses import dataclass
from typing import Optional

@dataclass
class PaginationData:
    """Modelo para metadatos de paginación."""
    next_search_after: Optional[str] = None
    total: Optional[int] = None

@dataclass
class RecordSummary:
    """Modelo básico para resumen de un registro en listas."""
    ocid: str
    buyer_id: Optional[str] = None
    title: Optional[str] = None

@dataclass
class CatalogItem:
    """Modelo para item del catálogo de descargas."""
    source: str
    type: str
    year: int
    month: int
    url: Optional[str] = None
