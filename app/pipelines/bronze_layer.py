from app.audit.logger import setup_logger
from app.clients.ocds_client import OCDSClient
from app.config.settings import settings
from app.storage.file_manager import FileManager
from app.services.extractors import TargetedExtractor, BulkExtractor
from app.utils.helpers import extract_zip_in_place

logger = setup_logger("ocds_framework.pipelines.bronze_layer")

class BronzePipeline:
    """
    Orquesta la ingesta de datos en la Capa Bronce del Data Lake.
    """
    
    def __init__(self):
        self.client = OCDSClient()
        self.local_storage = FileManager(settings.BRONZE_DIR)
        
        self.cloud_storage = None
        if settings.USE_R2:
            from app.storage.r2_manager import R2Manager
            self.cloud_storage = R2Manager()
            logger.info("Storage Backend: Híbrido (Data Lake Local + Cloudflare R2)")
        else:
            logger.info("Storage Backend: Data Lake Local (Disco)")
            
        self.targeted_extractor = TargetedExtractor(self.client)
        self.bulk_extractor = BulkExtractor(self.client)

    def run_targeted_ingestion(self, ruc: str, limit: int = 0, year: int = None) -> None:
        msg = f"== Iniciando Ingesta Targeted (Bronze) para RUC {ruc}"
        if year:
            msg += f" (Año {year})"
        msg += " =="
        logger.info(msg)
        
        count = 0
        for summary in self.targeted_extractor.paginate_records(ruc, target_year=year):
            if limit > 0 and count >= limit:
                logger.warning(f"Límite {limit} alcanzado. Terminando ingesta.")
                break
                
            if not summary.ocid:
                continue
                
            try:
                detail = self.targeted_extractor.get_record_detail(summary.ocid)
                # Guardar en local siempre
                path_suffix = f"records/{ruc}/{year}" if year else f"records/{ruc}"
                local_path = self.local_storage.save_json(
                    data=detail, 
                    path_suffix=path_suffix, 
                    filename=summary.ocid
                )
                
                # Subir a R2 si está configurado
                if self.cloud_storage:
                    object_key = f"{path_suffix}/{local_path.name}"
                    self.cloud_storage.upload_file(local_path, object_key)
                    
                count += 1
            except Exception as e:
                logger.error(f"Falló ingesta del OCID {summary.ocid}: {e}")
                
        logger.info(f"== Ingesta Targeted Finalizada. Registros procesados: {count} ==")

    def run_bulk_ingestion(self, source: str, file_type: str, year: int, month: int) -> None:
        logger.info(f"== Iniciando Ingesta Bulk (Bronze) para {source} {year}-{month:02d} ==")
        
        try:
            response = self.bulk_extractor.get_file_stream(source, file_type, year, month)
            filename = f"{source}_{file_type}_{year}_{month:02d}.zip"
            
            # Guardar el ZIP localmente
            zip_path = self.local_storage.save_binary_stream(
                content_stream=response.iter_content(chunk_size=8192),
                path_suffix="bulk_files",
                filename=filename
            )
            
            # Subir a R2 si está configurado
            if self.cloud_storage:
                object_key = f"bulk_files/{filename}"
                self.cloud_storage.upload_file(zip_path, object_key)
            
            # Descomprimir in-place localmente
            extract_zip_in_place(zip_path)
            
            logger.info("== Ingesta Bulk Finalizada Exitosamente ==")
            
        except Exception as e:
            logger.error(f"Error crítico en la ingesta Bulk: {e}")

    def close(self):
        self.client.close()
