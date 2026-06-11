import json
import time
from pathlib import Path
from typing import List, Dict

import google.generativeai as genai
from google.generativeai.types import HarmCategory, HarmBlockThreshold

from app.audit.logger import setup_logger
from app.config.settings import settings

logger = setup_logger("ocds_framework.services.ai_cleaner")

class GeminiCleaner:
    def __init__(self):
        self.api_key = settings.GEMINI_API_KEY
        self.model_name = settings.GEMINI_MODEL
        self.cache_file = settings.EXTRA_DATA_DIR / "gemini_cache.json"
        
        if self.api_key:
            genai.configure(api_key=self.api_key)
            self.model = genai.GenerativeModel(
                model_name=self.model_name,
                generation_config={
                    "temperature": 0.0,
                    "response_mime_type": "application/json",
                }
            )
        else:
            logger.warning("GEMINI_API_KEY no configurado. AI Cleaner no funcionará correctamente.")
            self.model = None

        self.cache = self._load_cache()

    def _load_cache(self) -> Dict[str, str]:
        if self.cache_file.exists():
            try:
                with open(self.cache_file, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Error cargando caché de Gemini: {e}")
        return {}

    def _save_cache(self):
        try:
            with open(self.cache_file, "w", encoding="utf-8") as f:
                json.dump(self.cache, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"Error guardando caché de Gemini: {e}")

    def clean_descriptions(self, raw_descriptions: List[str]) -> Dict[str, str]:
        """
        Recibe una lista de descripciones crudas, filtra las ya cacheadas,
        y envía el resto a Gemini en lotes. Devuelve el diccionario completo.
        """
        if not self.model:
            logger.warning("AI Cleaner desactivado por falta de API Key.")
            return {item: item for item in raw_descriptions if item}
            
        unique_items = list(set([str(item).strip() for item in raw_descriptions if item]))
        items_to_clean = [item for item in unique_items if item not in self.cache]
        
        if not items_to_clean:
            logger.info("No hay ítems nuevos para limpiar con IA. Usando caché al 100%.")
            return self.cache

        logger.info(f"Llamando a Gemini para limpiar {len(items_to_clean)} ítems nuevos.")
        
        batch_size = 50
        for i in range(0, len(items_to_clean), batch_size):
            batch = items_to_clean[i:i + batch_size]
            self._process_batch(batch)
            self._save_cache()
            if i + batch_size < len(items_to_clean):
                time.sleep(2) # Respetar rate limits básicos

        return self.cache

    def _process_batch(self, batch: List[str]):
        prompt = (
            "Eres un experto farmacéutico y analista de datos de EsSalud (Perú). "
            "Tu tarea es limpiar descripciones sucias, abreviadas o con errores ortográficos de medicamentos e insumos médicos "
            "y mapearlas a su nombre genérico estandarizado (Denominación Común Internacional - DCI) más cercano, "
            "o a un nombre comercial limpio si es un dispositivo médico.\n\n"
            "Ejemplo de entrada: ['PARACETAML 500', 'IBUPROFENO TABL 400MG', 'JERINGA DESC. 5ML C/AG.']\n"
            "Ejemplo de salida: {\"PARACETAML 500\": \"PARACETAMOL 500 MG\", \"IBUPROFENO TABL 400MG\": \"IBUPROFENO 400 MG\", \"JERINGA DESC. 5ML C/AG.\": \"JERINGA DESCARTABLE 5 ML CON AGUJA\"}\n\n"
            "Devuelve un objeto JSON válido donde las claves (keys) sean EXACTAMENTE los textos crudos de entrada "
            "y los valores (values) sean la descripción limpia en MAYÚSCULAS. Si no sabes cómo limpiarlo, devuelve el mismo texto crudo.\n\n"
            f"Limpia los siguientes textos:\n{json.dumps(batch, ensure_ascii=False)}"
        )
        
        try:
            response = self.model.generate_content(
                prompt,
                safety_settings={
                    HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_NONE,
                    HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_NONE,
                    HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_NONE,
                    HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_NONE,
                }
            )
            cleaned_data = json.loads(response.text)
            
            # Actualizar caché
            for raw_item, clean_item in cleaned_data.items():
                if isinstance(clean_item, str):
                    self.cache[raw_item] = clean_item.strip().upper()
                    
        except Exception as e:
            logger.error(f"Error en batch de Gemini: {e}")
            # Si falla, mapeamos a sí mismo para no bloquear el pipeline
            for item in batch:
                self.cache[item] = item
