from typing import Generator, Dict, Any, List
import requests
from app.clients.ocds_client import OCDSClient
from app.audit.logger import setup_logger
from app.models.data_models import CatalogItem

logger = setup_logger("ocds_framework.services.extractors")

class TargetedExtractor:
    """
    Extractor orientado a consultas específicas de la API OCDS.
    """

    # Categorías de adquisición a incluir. {"goods"} = solo bienes/medicamentos
    # (medicamentos, dispositivos, reactivos, insumos se publican todos bajo
    # "goods" en SEACE). Para incluir toda la contratación, usar None.
    ACCEPTED_CATEGORIES = {"goods"}

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

    def paginate_records(self, buyer_ruc: str, target_year: int = None) -> Generator[Dict[str, Any], None, None]:
        """
        Itera sobre el endpoint /recordsAfter aplicando filtrado del lado del cliente
        (la API no soporta filtros por RUC, fecha ni categoría).

        El listado de /recordsAfter ya entrega el `compiledRelease` completo (idéntico
        al de /record/{ocid}), por lo que se proyecta y emite directamente sin una
        segunda llamada HTTP de detalle.

        Args:
            buyer_ruc (str): RUC del comprador para filtrar.
            target_year (int, opcional): Año a filtrar.

        Yields:
            Dict[str, Any]: Record proyectado `{"ocid": ..., "compiledRelease": {...}}`.
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

                compiled = rec.get("compiledRelease", {})

                # Filtrado estricto temporal (Year)
                if target_year:
                    rec_date = compiled.get("date", "")
                    if not rec_date.startswith(str(target_year)):
                        continue

                # Filtrado por categoría de adquisición (None = todas).
                if self.ACCEPTED_CATEGORIES is not None:
                    category = compiled.get("tender", {}).get("mainProcurementCategory", "")
                    if category not in self.ACCEPTED_CATEGORIES:
                        continue

                ocid = rec.get("ocid", "")
                if not ocid:
                    continue

                yield {
                    "ocid": ocid,
                    "compiledRelease": self._project_release(compiled),
                }

            next_url = data.get("links", {}).get("next")
            if not next_url:
                logger.info("Se alcanzó el final de la paginación global.")
                break

            parsed = urlparse(next_url)
            query_params = parse_qs(parsed.query)
            params = {k: v[0] for k, v in query_params.items()}

    def _project_release(self, compiled: Dict[str, Any]) -> Dict[str, Any]:
        """
        Reduce el `compiledRelease` a los nodos que la capa Silver necesita.

        Descarta ruido pesado (documents, tenderers, historial de releases,
        clasificaciones adicionales) preservando lo esencial para el modelo estrella:
        fechas de Lead Time, montos, proveedor adjudicado, y el título/descripciones
        de donde se extrae la Red Asistencial.
        """
        tender = compiled.get("tender", {})

        projected_items = [
            {
                "id": it.get("id"),
                "description": it.get("description"),
                "quantity": it.get("quantity"),
                "unit": it.get("unit"),
                "classification": it.get("classification"),
                "totalValue": it.get("totalValue"),
                "deliveryLocation": it.get("deliveryLocation"),
            }
            for it in tender.get("items", [])
        ]

        # Solo buyer / procuringEntity / supplier; los tenderers (todos los postores)
        # son la mayor fuente de bloat y no se usan en la Fact.
        projected_parties = [
            {
                "id": p.get("id"),
                "name": p.get("name"),
                "roles": p.get("roles", []),
                "address": p.get("address"),
                "additionalIdentifiers": p.get("additionalIdentifiers"),
            }
            for p in compiled.get("parties", [])
            if any(r in p.get("roles", []) for r in ("buyer", "procuringEntity", "supplier"))
        ]

        projected_awards = [
            {
                "id": aw.get("id"),
                "date": aw.get("date"),
                "status": aw.get("status"),
                "value": aw.get("value"),
                "suppliers": aw.get("suppliers"),
                "items": [{"id": i.get("id")} for i in aw.get("items", [])],
            }
            for aw in compiled.get("awards", [])
        ]

        projected_contracts = [
            {
                "id": ct.get("id"),
                "awardID": ct.get("awardID"),
                "title": ct.get("title"),
                "dateSigned": ct.get("dateSigned"),
                "period": ct.get("period"),
                "value": ct.get("value"),
                "amendments": ct.get("amendments", []),
                "items": [{"id": i.get("id")} for i in ct.get("items", [])],
            }
            for ct in compiled.get("contracts", [])
        ]

        return {
            "date": compiled.get("date"),
            "buyer": compiled.get("buyer"),
            "tender": {
                "id": tender.get("id"),
                "title": tender.get("title"),
                "description": tender.get("description"),
                "status": tender.get("status"),
                "mainProcurementCategory": tender.get("mainProcurementCategory"),
                "procurementMethod": tender.get("procurementMethod"),
                "procurementMethodDetails": tender.get("procurementMethodDetails"),
                "procurementMethodRationale": tender.get("procurementMethodRationale"),
                "datePublished": tender.get("datePublished"),
                "tenderPeriod": tender.get("tenderPeriod"),
                "value": tender.get("value"),
                "items": projected_items,
            },
            "awards": projected_awards,
            "contracts": projected_contracts,
            "parties": projected_parties,
        }


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
