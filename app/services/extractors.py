from typing import Generator, Dict, Any, List, Optional
import requests
from app.clients.ocds_client import OCDSClient
from app.audit.logger import setup_logger
from app.models.data_models import RecordSummary, CatalogItem

logger = setup_logger("ocds_framework.services.extractors")

class TargetedExtractor:
    """
    Extractor orientado a consultas específicas de la API OCDS.
    """
    
    def __init__(self, client: OCDSClient):
        self.client = client

    def _is_target_buyer(self, record: Dict[str, Any], target_ruc: str) -> bool:
        """Verifica si el record pertenece al RUC objetivo revisando sus parties."""
        compiled = record.get("compiledRelease", {})
        parties = compiled.get("parties", [])
        
        for party in parties:
            roles = party.get("roles", [])
            if "buyer" in roles or "procuringEntity" in roles:
                # Revisar ID principal
                if target_ruc in party.get("id", ""):
                    return True
                # Revisar identificadores adicionales
                for ident in party.get("additionalIdentifiers", []):
                    if target_ruc in ident.get("id", ""):
                        return True
        return False

    def paginate_records(self, buyer_ruc: str, target_year: int = None) -> Generator[RecordSummary, None, None]:
        """
        Itera sobre el endpoint /recordsAfter utilizando paginación asíncrona simulada.
        Implementa filtrado del lado del cliente (Client-Side) ya que la API no soporta filtros por RUC ni Fecha.
        
        Args:
            buyer_ruc (str): RUC del comprador para filtrar.
            target_year (int, opcional): Año a filtrar.
            
        Yields:
            RecordSummary: Información resumida de cada contrato.
        """
        from urllib.parse import urlparse, parse_qs
        
        msg = f"Iniciando extracción paginada (Client-Side Filter) para RUC: {buyer_ruc}"
        if target_year:
            msg += f" (Año: {target_year})"
        logger.info(msg)
        params = {"size": 100}

        pages_scanned = 0
        while True:
            response = self.client.get("recordsAfter", params=params)
            data = response.json()
            
            records_data = data.get("records", [])
            if not records_data:
                break
                
            pages_scanned += 1
            if pages_scanned % 50 == 0:
                logger.debug(f"Escaneadas {pages_scanned} páginas del portal buscando el RUC {buyer_ruc}...")
                
            for rec in records_data:
                # Filtrado estricto del lado del cliente (Buyer)
                if not self._is_target_buyer(rec, buyer_ruc):
                    continue
                    
                # Filtrado estricto temporal (Year)
                if target_year:
                    rec_date = rec.get("compiledRelease", {}).get("date", "")
                    if not rec_date.startswith(str(target_year)):
                        continue
                    
                yield RecordSummary(
                    ocid=rec.get("ocid", ""),
                    buyer_id=buyer_ruc,
                    title=rec.get("compiledRelease", {}).get("tender", {}).get("title")
                )

            next_url = data.get("links", {}).get("next")
            if not next_url:
                logger.info("Se alcanzó el final de la paginación global.")
                break
                
            parsed = urlparse(next_url)
            query_params = parse_qs(parsed.query)
            params = {k: v[0] for k, v in query_params.items()}

    def get_record_detail(self, ocid: str) -> Dict[str, Any]:
        """Obtiene el paquete completo de un Record por OCID."""
        response = self.client.get(f"record/{ocid}")
        return response.json()


class BulkExtractor:
    """
    Extractor orientado a la obtención de catálogos y descargas masivas mensuales.
    """
    
    def __init__(self, client: OCDSClient):
        self.client = client

    def fetch_catalog(self) -> List[CatalogItem]:
        """Obtiene la lista de archivos disponibles en el catálogo."""
        logger.info("Consultando catálogo masivo /files")
        response = self.client.get("files")
        data = response.json().get("data", [])
        
        items = []
        for item in data:
            items.append(CatalogItem(
                source=item.get("source", ""),
                type=item.get("type", ""),
                year=item.get("year", 0),
                month=item.get("month", 0),
                url=item.get("url")
            ))
        return items

    def get_file_stream(self, source: str, file_type: str, year: int, month: int) -> requests.Response:
        """
        Obtiene el stream de respuesta para un archivo masivo específico.
        """
        endpoint = f"file/{source}/{file_type}/{year}/{month}"
        logger.info(f"Solicitando stream binario para: {endpoint}")
        return self.client.get(endpoint, stream=True)
