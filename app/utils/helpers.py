import zipfile
from pathlib import Path
from app.audit.logger import setup_logger

logger = setup_logger("ocds_framework.utils.helpers")

def extract_zip_in_place(zip_path: Path) -> Path:
    """
    Descomprime un archivo ZIP en el mismo directorio donde se ubica,
    usando una carpeta con el mismo nombre del archivo sin extensión.
    
    Args:
        zip_path (Path): Ruta del archivo ZIP.
        
    Returns:
        Path: Ruta del directorio resultante.
    """
    if not zipfile.is_zipfile(zip_path):
        logger.warning(f"El archivo {zip_path} no es un ZIP válido.")
        return zip_path.parent
        
    extract_dir = zip_path.parent / zip_path.stem
    logger.info(f"Descomprimiendo {zip_path.name} hacia {extract_dir}")
    
    extract_dir.mkdir(exist_ok=True)
    
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_dir)
        
    logger.debug(f"Descompresión completada para {zip_path.name}")
    return extract_dir
