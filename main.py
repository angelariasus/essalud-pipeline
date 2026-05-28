import argparse
import sys
from app.config.settings import settings
from app.audit.logger import setup_logger
from app.pipelines.bronze_layer import BronzePipeline

logger = setup_logger("ocds_framework.main")

def main():
    parser = argparse.ArgumentParser(
        description="Framework ELT OCDS - Capa Bronce (Arquitectura Modular)"
    )
    
    subparsers = parser.add_subparsers(dest="command", help="Estrategias de Ingesta")
    
    # Subparser para extracción dirigida
    targeted_parser = subparsers.add_parser("targeted", help="Ingesta específica por RUC")
    targeted_parser.add_argument(
        "--ruc", 
        type=str, 
        default=settings.ESSALUD_RUC, 
        help=f"RUC de la entidad (default: {settings.ESSALUD_RUC})"
    )
    targeted_parser.add_argument(
        "--limit", 
        type=int, 
        default=0, 
        help="Límite de registros para pruebas (0 = todos)"
    )
    targeted_parser.add_argument(
        "--year", 
        type=int, 
        default=None, 
        help="Filtra los contratos por un año específico (ej. 2023)"
    )
    
    # Subparser para extracción masiva
    bulk_parser = subparsers.add_parser("bulk", help="Descarga masiva de catálogos")
    bulk_parser.add_argument("--source", type=str, required=True, help="Fuente (ej. SEACE)")
    bulk_parser.add_argument("--type", type=str, default="JSON", help="Formato (ej. JSON)")
    bulk_parser.add_argument("--year", type=int, required=True, help="Año (ej. 2023)")
    bulk_parser.add_argument("--month", type=int, required=True, help="Mes (1-12)")

    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(1)

    pipeline = BronzePipeline()
    
    try:
        if args.command == "targeted":
            pipeline.run_targeted_ingestion(ruc=args.ruc, limit=args.limit, year=args.year)
            
        elif args.command == "bulk":
            pipeline.run_bulk_ingestion(
                source=args.source, 
                file_type=args.type, 
                year=args.year, 
                month=args.month
            )
    finally:
        pipeline.close()

if __name__ == "__main__":
    main()
