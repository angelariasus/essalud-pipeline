import os
from pathlib import Path
from dataclasses import dataclass
from dotenv import load_dotenv

# Cargar variables de entorno desde el archivo .env si existe.
# OCDS_ENV_FILE permite apuntar a un dotenv alternativo (p.ej. `.env.docker` en
# el contenedor de Airflow): el repo completo se monta en /opt/airflow/bi, y sin
# esto el `.env` de Windows (JAVA_HOME=C:\..., HADOOP_HOME=C:\hadoop) se cargaría
# dentro del contenedor Linux y rompería Spark.
load_dotenv(os.getenv("OCDS_ENV_FILE") or None)

# Base del proyecto (sube 3 niveles: settings.py -> config/ -> app/ -> data/mart/)
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
    # Capa Gold materializada en Parquet (destino por defecto, sin SQL Server).
    GOLD_DIR: Path = Path(os.getenv("OCDS_GOLD_DIR", str(DATA_DIR / "gold")))
    # Carpeta para Power BI: export de las tablas oro.* del DW a Parquet (incluye
    # Dim_Tiempo y Dim_Ubigeo, que las genera el DDL de SQL Server).
    BI_DIR: Path = Path(os.getenv("OCDS_BI_DIR", str(PROJECT_ROOT / "data" / "mart")))
    EXTRA_DATA_DIR: Path = Path(os.getenv("OCDS_EXTRA_DATA_DIR", str(PROJECT_ROOT / "reference")))
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

    # Perfil "docker": cadenas alternas para apuntar a una instancia SQL Server en
    # contenedor (se eligen con `gold --target sqlserver --profile docker`). Si no
    # se definen, el perfil docker cae a las cadenas estándar de arriba.
    DW_CONN_STRING_DOCKER: str = os.getenv("OCDS_DW_CONN_STRING_DOCKER", "")
    DW_JDBC_URL_DOCKER: str = os.getenv("OCDS_DW_JDBC_URL_DOCKER", "")
    DW_JDBC_USER_DOCKER: str = os.getenv("OCDS_DW_JDBC_USER_DOCKER", "")
    DW_JDBC_PASSWORD_DOCKER: str = os.getenv("OCDS_DW_JDBC_PASSWORD_DOCKER", "")

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

    # Fase 6 — Alertas por correo (SMTP). Default pensado para Gmail App Password
    # (smtp.gmail.com:587 + STARTTLS) o MailHog local (localhost:1025, sin TLS).
    SMTP_HOST: str = os.getenv("SMTP_HOST", "")
    SMTP_PORT: int = int(os.getenv("SMTP_PORT", "587"))
    SMTP_USER: str = os.getenv("SMTP_USER", "")
    SMTP_PASSWORD: str = os.getenv("SMTP_PASSWORD", "")
    SMTP_STARTTLS: bool = os.getenv("SMTP_STARTTLS", "True").lower() in ("true", "1", "yes")
    SMTP_FROM: str = os.getenv("SMTP_FROM", "") or os.getenv("SMTP_USER", "")
    SMTP_TO: str = os.getenv("SMTP_TO", "")

settings = Settings()
