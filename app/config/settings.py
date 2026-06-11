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
    SILVER_DIR: Path = Path(os.getenv("OCDS_SILVER_DIR", str(DATA_DIR / "silver")))
    EXTRA_DATA_DIR: Path = Path(os.getenv("OCDS_EXTRA_DATA_DIR", str(PROJECT_ROOT / "extra-data")))
    AUDIT_DIR: Path = DATA_DIR / "audit"
    EXECUTIONS_DIR: Path = AUDIT_DIR / "executions"
    QUALITY_CHECKS_DIR: Path = AUDIT_DIR / "quality_checks"
    
    # Configuraciones de Cloudflare R2
    USE_R2: bool = os.getenv("OCDS_USE_R2", "False").lower() in ("true", "1", "yes")
    R2_ACCOUNT_ID: str = os.getenv("OCDS_R2_ACCOUNT_ID", "")
    R2_ACCESS_KEY: str = os.getenv("OCDS_R2_ACCESS_KEY", "")
    R2_SECRET_KEY: str = os.getenv("OCDS_R2_SECRET_KEY", "")
    R2_BUCKET_NAME: str = os.getenv("OCDS_R2_BUCKET_NAME", "essalud-bronze-lake")

    # Data Warehouse (capa Gold - SQL Server). La cadena lleva credenciales: definir en .env.
    # DW_CONN_STRING (SQLAlchemy/pyodbc) se usa para DDL y el stored procedure de carga atómica.
    DW_CONN_STRING: str = os.getenv("OCDS_DW_CONN_STRING", "")

    # Conexión JDBC para la escritura distribuida con Spark a las tablas staging.
    # Formato URL: jdbc:sqlserver://host:port;databaseName=...;encrypt=true;trustServerCertificate=true
    DW_JDBC_URL: str = os.getenv("OCDS_DW_JDBC_URL", "")
    DW_JDBC_USER: str = os.getenv("OCDS_DW_JDBC_USER", "")
    DW_JDBC_PASSWORD: str = os.getenv("OCDS_DW_JDBC_PASSWORD", "")
    DW_JDBC_BATCHSIZE: int = int(os.getenv("OCDS_DW_JDBC_BATCHSIZE", "10000"))

    # Spark (capas Silver/Gold). El driver JDBC se inyecta por env para no
    # depender de la red en pruebas/uso offline (ver app/config/spark_session.py).
    # Default local[4]: en Windows, spawnear muchos workers de Python (UDFs)
    # concurrentemente crashea (local[*] => 1 worker por core). 4 es seguro y
    # paralelo; en Linux/Docker (sin ese bug) se puede subir a local[*] o clúster.
    SPARK_MASTER: str = os.getenv("OCDS_SPARK_MASTER", "local[4]")
    SPARK_APP_NAME: str = os.getenv("OCDS_SPARK_APP_NAME", "EsSalud_Pipeline")
    SPARK_SHUFFLE_PARTITIONS: str = os.getenv("OCDS_SPARK_SHUFFLE_PARTITIONS", "8")
    SPARK_JARS_PACKAGES: str = os.getenv("OCDS_SPARK_JARS_PACKAGES", "")
    SPARK_JARS: str = os.getenv("OCDS_SPARK_JARS", "")

    LOG_LEVEL: str = os.getenv("OCDS_LOG_LEVEL", "INFO")
    MAX_RETRIES: int = int(os.getenv("OCDS_MAX_RETRIES", "5"))
    BACKOFF_FACTOR: float = float(os.getenv("OCDS_BACKOFF_FACTOR", "0.5"))

    # Configuraciones de IA (Gemini API)
    GEMINI_API_KEY: str = os.getenv("GEMINI_API_KEY", "")
    GEMINI_MODEL: str = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")

settings = Settings()
