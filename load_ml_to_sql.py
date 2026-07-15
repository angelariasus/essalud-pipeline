import sys
from pathlib import Path
import pandas as pd
from dotenv import load_dotenv

# Ensure the app module can be imported
sys.path.insert(0, str(Path(__file__).parent.resolve()))

from app.loaders.dw_loader import create_sqlalchemy_engine
from app.audit.logger import setup_logger

logger = setup_logger("ml_to_sql")

def load_parquet_to_sql():
    # 1. Setup connection
    load_dotenv()
    engine = create_sqlalchemy_engine()
    
    bi_dir = Path("bi")
    
    with engine.begin() as conn:
        from sqlalchemy import text
        
        # 1. Alertas
        alertas_path = bi_dir / "Alertas.parquet"
        if alertas_path.exists():
            logger.info(f"Reading {alertas_path}...")
            df_alertas = pd.read_parquet(alertas_path)
            conn.execute(text("DROP TABLE IF EXISTS oro.Alertas"))
            conn.execute(text("""
                CREATE TABLE oro.Alertas (
                    Tipo_Alerta VARCHAR(50),
                    Anio INT,
                    Red_Asistencial VARCHAR(200),
                    Medicamento VARCHAR(500),
                    RUC_Proveedor VARCHAR(15),
                    Nombre_Proveedor VARCHAR(300),
                    Metrica VARCHAR(50),
                    Valor FLOAT,
                    Umbral FLOAT,
                    Detalle VARCHAR(MAX)
                )
            """))
            logger.info(f"Uploading {len(df_alertas)} rows to oro.Alertas...")
            # Reemplazar NaNs por None
            df_alertas = df_alertas.astype(object).where(pd.notnull(df_alertas), None)
            records = df_alertas.to_dict('records')
            if records:
                # Insert in batches
                batch_size = 1000
                for i in range(0, len(records), batch_size):
                    batch = records[i:i+batch_size]
                    conn.execute(
                        text("""INSERT INTO oro.Alertas (Tipo_Alerta, Anio, Red_Asistencial, Medicamento, RUC_Proveedor, Nombre_Proveedor, Metrica, Valor, Umbral, Detalle) 
                                VALUES (:Tipo_Alerta, :Anio, :Red_Asistencial, :Medicamento, :RUC_Proveedor, :Nombre_Proveedor, :Metrica, :Valor, :Umbral, :Detalle)"""),
                        batch
                    )
            logger.info("Successfully uploaded Alertas.")
            
        # 2. Pred_Lead_Time
        pred_path = bi_dir / "Pred_Lead_Time.parquet"
        if pred_path.exists():
            logger.info(f"Reading {pred_path}...")
            df_pred = pd.read_parquet(pred_path)
            conn.execute(text("DROP TABLE IF EXISTS oro.Pred_Lead_Time"))
            conn.execute(text("""
                CREATE TABLE oro.Pred_Lead_Time (
                    ID_Registro INT,
                    Codigo_Convocatoria BIGINT,
                    N_Item INT,
                    Anio_Fiscal INT,
                    Red_Asistencial VARCHAR(200),
                    Categoria_Proceso VARCHAR(50),
                    Lead_Time_Actual FLOAT,
                    Lead_Time_Predicho FLOAT,
                    Residual FLOAT
                )
            """))
            logger.info(f"Uploading {len(df_pred)} rows to oro.Pred_Lead_Time...")
            df_pred = df_pred.astype(object).where(pd.notnull(df_pred), None)
            records = df_pred.to_dict('records')
            if records:
                batch_size = 1000
                for i in range(0, len(records), batch_size):
                    batch = records[i:i+batch_size]
                    conn.execute(
                        text("""INSERT INTO oro.Pred_Lead_Time (ID_Registro, Codigo_Convocatoria, N_Item, Anio_Fiscal, Red_Asistencial, Categoria_Proceso, Lead_Time_Actual, Lead_Time_Predicho, Residual) 
                                VALUES (:ID_Registro, :Codigo_Convocatoria, :N_Item, :Anio_Fiscal, :Red_Asistencial, :Categoria_Proceso, :Lead_Time_Actual, :Lead_Time_Predicho, :Residual)"""),
                        batch
                    )
            logger.info("Successfully uploaded Pred_Lead_Time.")

if __name__ == "__main__":
    try:
        load_parquet_to_sql()
        logger.info("Done syncing ML results to SQL Server.")
    except Exception as e:
        logger.error(f"Error syncing ML results to SQL Server: {e}")
        sys.exit(1)
