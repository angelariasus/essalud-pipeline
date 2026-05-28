import json
from pathlib import Path
from typing import Dict, Any, Optional, Iterator
from app.audit.logger import setup_logger

logger = setup_logger("ocds_framework.storage.file_manager")

class FileManager:
    """
    Maneja la persistencia física en el Data Lake local.
    """

    def __init__(self, base_path: str):
        self.base_dir = Path(base_path)
        self._ensure_dir(self.base_dir)

    def _ensure_dir(self, path: Path) -> None:
        """Asegura la creación del directorio en disco."""
        if not path.exists():
            logger.debug(f"Creando directorio: {path}")
            path.mkdir(parents=True, exist_ok=True)

    def save_json(self, data: Dict[str, Any], path_suffix: str, filename: str) -> Path:
        """
        Guarda un diccionario como JSON en la ruta destino.
        
        Args:
            data (Dict[str, Any]): Información a guardar.
            path_suffix (str): Subdirectorios dentro de base_path (ej. 'records/essalud').
            filename (str): Nombre del archivo (ej. 'ocds-1234.json').
            
        Returns:
            Path: Ruta final del archivo guardado.
        """
        target_dir = self.base_dir / path_suffix
        self._ensure_dir(target_dir)
        
        if not filename.endswith(".json"):
            filename += ".json"
            
        file_path = target_dir / filename
        logger.debug(f"Escribiendo JSON en {file_path}")
        
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
            
        return file_path

    def save_binary_stream(self, content_stream: Iterator[bytes], path_suffix: str, filename: str) -> Path:
        """
        Guarda un flujo de bytes en el almacenamiento local.
        
        Args:
            content_stream (Iterator[bytes]): Generador de chunks de bytes.
            path_suffix (str): Subdirectorios.
            filename (str): Nombre del archivo (ej. 'data.zip').
            
        Returns:
            Path: Ruta final del archivo binario.
        """
        target_dir = self.base_dir / path_suffix
        self._ensure_dir(target_dir)
        
        file_path = target_dir / filename
        logger.debug(f"Escribiendo RAW/ZIP en {file_path}")
        
        with open(file_path, "wb") as f:
            for chunk in content_stream:
                if chunk:
                    f.write(chunk)
                    
        return file_path
