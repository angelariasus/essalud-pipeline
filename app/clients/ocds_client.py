import requests
from typing import Optional, Dict, Any
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from app.config.settings import settings
from app.audit.logger import setup_logger

logger = setup_logger("ocds_framework.clients.ocds_client")

class OCDSClient:
    """
    Cliente HTTP robusto para la API OCDS.
    """

    def __init__(self, base_url: str = settings.API_BASE_URL):
        self.base_url = base_url.rstrip('/')
        self.session = self._build_session()

    def _build_session(self) -> requests.Session:
        """
        Construye una sesión HTTP con reintentos exponenciales.
        
        Returns:
            requests.Session: Sesión configurada.
        """
        session = requests.Session()
        
        retry_strategy = Retry(
            total=settings.MAX_RETRIES,
            backoff_factor=settings.BACKOFF_FACTOR,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        session.headers.update({
            "User-Agent": "OCDS_DataEngineer_Bot/2.0",
            "Accept": "application/json"
        })
        
        return session

    def get(self, endpoint: str, params: Optional[Dict[str, Any]] = None, stream: bool = False) -> requests.Response:
        """
        Realiza una petición GET de manera resiliente.
        
        Args:
            endpoint (str): Ruta de la API.
            params (Optional[Dict[str, Any]]): Parámetros query.
            stream (bool): Si es True, permite iterar la respuesta binaria.
            
        Returns:
            requests.Response: Respuesta de la petición HTTP.
        """
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        logger.debug(f"GET Request: {url} | Params: {params}")
        
        response = self.session.get(url, params=params, stream=stream)
        
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP Error {url}: {e}")
            raise
            
        return response

    def close(self) -> None:
        """Cierra la sesión HTTP."""
        self.session.close()
