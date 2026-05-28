import logging
import sys
from app.config.settings import settings

def setup_logger(name: str) -> logging.Logger:
    """
    Configura y retorna un logger específico.
    
    Args:
        name (str): Nombre del módulo para el logger.
        
    Returns:
        logging.Logger: Instancia del logger configurado.
    """
    logger = logging.getLogger(name)
    
    # Si ya tiene handlers, no duplicarlos
    if logger.handlers:
        return logger
        
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    # Console Handler (INFO default)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO))
    console_handler.setFormatter(formatter)

    # File Handler (DEBUG siempre) guardado en data/audit/executions/
    settings.EXECUTIONS_DIR.mkdir(parents=True, exist_ok=True)
    log_file_path = settings.EXECUTIONS_DIR / "ocds_extraction.log"
    file_handler = logging.FileHandler(log_file_path, mode="a", encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    logger.propagate = False
    
    return logger

# Logger por defecto para usos rápidos
default_logger = setup_logger("ocds_framework")
