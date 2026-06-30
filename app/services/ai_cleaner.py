import json
import time
from typing import List, Dict

from google import genai
from google.genai import types

from app.audit.logger import setup_logger
from app.config.settings import settings

logger = setup_logger("ocds_framework.services.ai_cleaner")

_SAFETY_SETTINGS = [
    types.SafetySetting(
        category="HARM_CATEGORY_DANGEROUS_CONTENT", threshold="BLOCK_NONE"
    ),
    types.SafetySetting(
        category="HARM_CATEGORY_HATE_SPEECH", threshold="BLOCK_NONE"
    ),
    types.SafetySetting(
        category="HARM_CATEGORY_HARASSMENT", threshold="BLOCK_NONE"
    ),
    types.SafetySetting(
        category="HARM_CATEGORY_SEXUALLY_EXPLICIT", threshold="BLOCK_NONE"
    ),
]

_PROMPT_TEMPLATE = (
    "Eres un experto farmacéutico y analista de datos de EsSalud (Perú). "
    "Tu tarea es limpiar descripciones sucias, abreviadas o con errores "
    "ortográficos de medicamentos e insumos médicos y mapearlas a su nombre "
    "genérico estandarizado (Denominación Común Internacional - DCI) más "
    "cercano, o a un nombre comercial limpio si es un dispositivo médico.\n\n"
    "Devuelve un objeto JSON válido donde las claves (keys) sean EXACTAMENTE "
    "los textos crudos de entrada y los valores (values) sean la descripción "
    "limpia en MAYÚSCULAS. Si no sabes cómo limpiarlo, devuelve el mismo "
    "texto crudo.\n\nLimpia los siguientes textos:\n%s"
)


class GeminiCleaner:
    """Limpia descripciones de medicamentos e insumos usando la API de Gemini."""

    def __init__(self):
        self.model_name = settings.GEMINI_MODEL
        self.cache_file = settings.EXTRA_DATA_DIR / "gemini_cache.json"

        if settings.GEMINI_API_KEY:
            self.client = genai.Client(api_key=settings.GEMINI_API_KEY)
        else:
            logger.warning("GEMINI_API_KEY no configurado. AI Cleaner no funcionará.")
            self.client = None

        self.cache = self._load_cache()

    @property
    def is_configured(self) -> bool:
        """Indica si el cliente Gemini está disponible."""
        return self.client is not None

    def clean_descriptions(self, raw_descriptions: List[str]) -> Dict[str, str]:
        """
        Recibe una lista de descripciones crudas, filtra las ya cacheadas,
        y envía el resto a Gemini en lotes. Devuelve el diccionario completo.
        """
        if not self.is_configured:
            logger.warning("AI Cleaner desactivado por falta de API Key.")
            return {item: item for item in raw_descriptions if item}

        unique_items = list(
            {str(item).strip() for item in raw_descriptions if item}
        )
        items_to_clean = [item for item in unique_items if item not in self.cache]

        if not items_to_clean:
            logger.info("No hay ítems nuevos para limpiar con IA. Caché al 100%%.")
            return self.cache

        logger.info(
            "Llamando a Gemini para limpiar %d ítems nuevos.", len(items_to_clean)
        )

        batch_size = 50
        for i in range(0, len(items_to_clean), batch_size):
            batch = items_to_clean[i:i + batch_size]
            ok = self._process_batch(batch)
            if not ok:
                # La API no responde (sin cuota, credenciales, red...). En vez de
                # moler decenas de lotes que también fallarán, se aborta la limpieza
                # IA y los ítems restantes quedan como identidad (descripción cruda).
                logger.warning(
                    "Gemini no disponible; se aborta la limpieza IA "
                    "(los ítems restantes quedan sin limpiar)."
                )
                for item in items_to_clean:
                    self.cache.setdefault(item, item)
                break
            self._save_cache()
            if i + batch_size < len(items_to_clean):
                time.sleep(2)

        return self.cache

    def _load_cache(self) -> Dict[str, str]:
        if not self.cache_file.exists():
            return {}
        try:
            with open(self.cache_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except (OSError, ValueError) as e:
            logger.error("Error cargando caché de Gemini: %s", e)
            return {}

    def _save_cache(self):
        try:
            with open(self.cache_file, "w", encoding="utf-8") as f:
                json.dump(self.cache, f, ensure_ascii=False, indent=2)
        except OSError as e:
            logger.error("Error guardando caché de Gemini: %s", e)

    def _process_batch(self, batch: List[str]) -> bool:
        """Procesa un lote. Devuelve True si la API respondió, False si falló."""
        prompt = _PROMPT_TEMPLATE % json.dumps(batch, ensure_ascii=False)
        try:
            response = self.client.models.generate_content(
                model=self.model_name,
                contents=prompt,
                config=types.GenerateContentConfig(
                    temperature=0.0,
                    response_mime_type="application/json",
                    safety_settings=_SAFETY_SETTINGS,
                ),
            )
            cleaned_data = json.loads(response.text)
            for raw_item, clean_item in cleaned_data.items():
                if isinstance(clean_item, str):
                    self.cache[raw_item] = clean_item.strip().upper()
            return True
        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.error("Error en batch de Gemini: %s", e)
            for item in batch:
                self.cache.setdefault(item, item)
            return False
