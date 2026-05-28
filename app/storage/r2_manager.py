import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
from pathlib import Path

from app.config.settings import settings
from app.audit.logger import setup_logger

logger = setup_logger("ocds_framework.storage.r2_manager")

class R2Manager:
    """
    Maneja la persistencia de la Capa Bronce directamente en Cloudflare R2.
    """

    def __init__(self):
        self.bucket_name = settings.R2_BUCKET_NAME
        
        if not settings.R2_ACCOUNT_ID:
            logger.warning("R2_ACCOUNT_ID no configurado. La conexión podría fallar.")

        endpoint_url = f"https://{settings.R2_ACCOUNT_ID}.r2.cloudflarestorage.com"
        
        self.s3_client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=settings.R2_ACCESS_KEY,
            aws_secret_access_key=settings.R2_SECRET_KEY,
            config=Config(signature_version="s3v4")
        )
        
        self._ensure_bucket()

    def _ensure_bucket(self):
        """Verifica si el bucket existe; si no, intenta crearlo."""
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            logger.debug(f"Bucket '{self.bucket_name}' localizado en R2.")
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code")
            if error_code == "404":
                logger.info(f"El bucket '{self.bucket_name}' no existe. Intentando crearlo...")
                try:
                    self.s3_client.create_bucket(Bucket=self.bucket_name)
                    logger.info("Bucket creado exitosamente.")
                except Exception as creation_error:
                    logger.error(f"Error al crear el bucket: {creation_error}")
            else:
                logger.error(f"No se pudo acceder al bucket: {e}")

    def upload_file(self, local_path: Path, object_key: str) -> str:
        """Sube un archivo local existente a R2."""
        logger.debug(f"Subiendo archivo a R2: s3://{self.bucket_name}/{object_key}")
        try:
            content_type = "application/json" if str(local_path).endswith(".json") else "application/zip"
            
            with open(local_path, "rb") as f:
                self.s3_client.put_object(
                    Bucket=self.bucket_name,
                    Key=object_key,
                    Body=f,
                    ContentType=content_type
                )
            return f"r2://{self.bucket_name}/{object_key}"
        except Exception as e:
            logger.error(f"Error subiendo el archivo {local_path} a R2: {e}")
            raise
