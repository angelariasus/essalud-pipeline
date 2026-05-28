import os
from pathlib import Path
from dataclasses import dataclass
from dotenv import load_dotenv

# Cargar variables de entorno desde el archivo .env si existe
load_dotenv()

# Base del proyecto (sube 3 niveles: settings.py -> config/ -> app/ -> bi/)
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

@dataclass
class Settings:
    """
    Configuración centralizada para el framework OCDS.
    """
    API_BASE_URL: str = os.getenv("OCDS_API_BASE_URL", "https://contratacionesabiertas.oece.gob.pe/api/v1")
    ESSALUD_RUC: str = os.getenv("OCDS_ESSALUD_RUC", "20131257750")
    
    PROJECT_ROOT: Path = PROJECT_ROOT
    DATA_DIR: Path = PROJECT_ROOT / "data"
    BRONZE_DIR: Path = Path(os.getenv("OCDS_BRONZE_DIR", str(DATA_DIR / "bronze")))
    AUDIT_DIR: Path = DATA_DIR / "audit"
    EXECUTIONS_DIR: Path = AUDIT_DIR / "executions"
    QUALITY_CHECKS_DIR: Path = AUDIT_DIR / "quality_checks"
    
    # Configuraciones de Cloudflare R2
    USE_R2: bool = os.getenv("OCDS_USE_R2", "False").lower() in ("true", "1", "yes")
    R2_ACCOUNT_ID: str = os.getenv("OCDS_R2_ACCOUNT_ID", "")
    R2_ACCESS_KEY: str = os.getenv("OCDS_R2_ACCESS_KEY", "")
    R2_SECRET_KEY: str = os.getenv("OCDS_R2_SECRET_KEY", "")
    R2_BUCKET_NAME: str = os.getenv("OCDS_R2_BUCKET_NAME", "essalud-bronze-lake")
    
    LOG_LEVEL: str = os.getenv("OCDS_LOG_LEVEL", "INFO")
    MAX_RETRIES: int = int(os.getenv("OCDS_MAX_RETRIES", "5"))
    BACKOFF_FACTOR: float = float(os.getenv("OCDS_BACKOFF_FACTOR", "0.5"))

settings = Settings()
