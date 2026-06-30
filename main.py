"""
CLI del framework ELT OCDS — orquestación de la arquitectura Medallion.

Subcomandos:
  - targeted / bulk : ingesta Bronze (compatibilidad histórica, sin cambios).
  - bronze          : ingesta Bronze de varios años (atajo de `targeted`).
  - silver          : aplana Bronze + limpieza IA -> staging_flat.
  - gold            : staging_flat -> dimensiones/Fact -> destino (parquet|sqlserver).
  - run-all/pipeline: Bronze -> Silver -> Gold en secuencia.
  - synth           : datos sintéticos (2025 + monto_adicional 2022-2024).
  - clean           : limpia artefactos de una capa (bronze|silver|gold).

Cada subcomando instancia solo lo que necesita (dispatch por comando), de modo
que correr Silver/Gold no levante el cliente HTTP de Bronze.
"""
import argparse
import sys

from app.audit.logger import setup_logger
from app.config.settings import settings

logger = setup_logger("ocds_framework.main")

DEFAULT_YEARS = [2022, 2023, 2024, 2025]


# ── Handlers ─────────────────────────────────────────────────────────────────
def _handle_targeted(args):
    from app.pipelines.bronze_layer import BronzePipeline

    pipeline = BronzePipeline()
    try:
        pipeline.run_targeted_ingestion(ruc=args.ruc, limit=args.limit, year=args.year)
    finally:
        pipeline.close()


def _handle_bulk(args):
    from app.pipelines.bronze_layer import BronzePipeline

    pipeline = BronzePipeline()
    try:
        pipeline.run_bulk_ingestion(
            source=args.source, file_type=args.type, year=args.year, month=args.month
        )
    finally:
        pipeline.close()


def _handle_bronze(args):
    """Ingesta Bronze dirigida para uno o varios años (atajo de `targeted`)."""
    from app.pipelines.bronze_layer import BronzePipeline

    years = args.years or DEFAULT_YEARS
    pipeline = BronzePipeline()
    try:
        for year in years:
            logger.info(f"Bronze: ingesta año {year} (RUC {args.ruc}).")
            pipeline.run_targeted_ingestion(ruc=args.ruc, limit=args.limit, year=year)
    finally:
        pipeline.close()


def _handle_silver(args):
    from app.pipelines.silver_layer import SilverPipeline

    SilverPipeline().run_silver(years=args.years or DEFAULT_YEARS, rebuild=args.rebuild)


def _handle_gold(args):
    from app.pipelines.gold_layer import GoldPipeline

    GoldPipeline().run(
        years=args.years or DEFAULT_YEARS,
        target=args.target,
        profile=args.profile,
        rebuild=args.rebuild,
    )


def _handle_run_all(args):
    """Bronze -> Silver -> Gold en secuencia para los años indicados."""
    from app.pipelines.bronze_layer import BronzePipeline
    from app.pipelines.gold_layer import GoldPipeline
    from app.pipelines.silver_layer import SilverPipeline

    years = args.years or DEFAULT_YEARS

    logger.info(f"=== RUN-ALL: Bronze -> Silver -> Gold para {years} ===")
    bronze = BronzePipeline()
    try:
        for year in years:
            bronze.run_targeted_ingestion(ruc=args.ruc, limit=args.limit, year=year)
    finally:
        bronze.close()

    SilverPipeline().run_silver(years=years)
    GoldPipeline().run(years=years, target=args.target, profile=args.profile)
    logger.info("=== RUN-ALL completado ===")


def _handle_synth(args):
    from app.services.synthetic_generator import run_synthetic

    enrich_years = None if args.no_enrich else (2022, 2023)
    run_synthetic(
        n_2025=args.n_2025, n_2024=args.n_2024, enrich_years=enrich_years,
        adicional_p=args.adicional_rate,
    )


def _handle_clean(args):
    from app.utils.cleaner import clean_layer

    clean_layer(
        args.layer, confirm=args.yes, target=args.target, profile=args.profile
    )


# ── Parser ───────────────────────────────────────────────────────────────────
def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Framework ELT OCDS - Arquitectura Medallion (Bronze/Silver/Gold)"
    )
    sub = parser.add_subparsers(dest="command", help="Comando a ejecutar")

    # -- targeted (compat) --
    p = sub.add_parser("targeted", help="Ingesta Bronze específica por RUC")
    p.add_argument("--ruc", type=str, default=settings.ESSALUD_RUC)
    p.add_argument("--limit", type=int, default=0, help="0 = todos")
    p.add_argument("--year", type=int, default=None)
    p.set_defaults(func=_handle_targeted)

    # -- bulk (compat) --
    p = sub.add_parser("bulk", help="Descarga masiva de catálogos")
    p.add_argument("--source", type=str, required=True)
    p.add_argument("--type", type=str, default="JSON")
    p.add_argument("--year", type=int, required=True)
    p.add_argument("--month", type=int, required=True)
    p.set_defaults(func=_handle_bulk)

    # -- bronze --
    p = sub.add_parser("bronze", help="Ingesta Bronze de varios años")
    p.add_argument("--ruc", type=str, default=settings.ESSALUD_RUC)
    p.add_argument("--limit", type=int, default=0, help="0 = todos")
    p.add_argument("--years", type=int, nargs="+", default=None,
                   help=f"Años (default: {DEFAULT_YEARS})")
    p.set_defaults(func=_handle_bronze)

    # -- silver --
    p = sub.add_parser("silver", help="Aplana Bronze + limpieza IA -> staging_flat")
    p.add_argument("--years", type=int, nargs="+", default=None)
    p.add_argument("--rebuild", action="store_true", help="Limpia staging antes")
    p.set_defaults(func=_handle_silver)

    # -- gold --
    p = sub.add_parser("gold", help="staging_flat -> dimensiones -> destino")
    p.add_argument("--years", type=int, nargs="+", default=None)
    p.add_argument("--target", type=str, default="parquet",
                   choices=["parquet", "sqlserver"], help="Destino (default parquet)")
    p.add_argument("--profile", type=str, default="local",
                   choices=["local", "docker"], help="Perfil SQL Server")
    p.add_argument("--rebuild", action="store_true", help="Limpia/reconstruye el destino antes")
    p.set_defaults(func=_handle_gold)

    # -- run-all / pipeline --
    for name in ("run-all", "pipeline"):
        p = sub.add_parser(name, help="Bronze -> Silver -> Gold en secuencia")
        p.add_argument("--ruc", type=str, default=settings.ESSALUD_RUC)
        p.add_argument("--limit", type=int, default=0)
        p.add_argument("--years", type=int, nargs="+", default=None)
        p.add_argument("--target", type=str, default="parquet",
                       choices=["parquet", "sqlserver"])
        p.add_argument("--profile", type=str, default="local",
                       choices=["local", "docker"])
        p.set_defaults(func=_handle_run_all)

    # -- synth / synthetic --
    for name in ("synth", "synthetic"):
        p = sub.add_parser(name, help="Genera datos sintéticos (2025 desde CSV + 2024 boost + adicional 22-23)")
        p.add_argument("--n-2025", type=int, default=2176,
                       help="Filas de 2025 desde el CSV de EsSalud (default 2176)")
        p.add_argument("--n-2024", type=int, default=2370,
                       help="Total de filas de 2024 tras el boost sintético (default 2370)")
        p.add_argument("--adicional-rate", type=float, default=None,
                       help="Ocurrencia de adendas en 2022-2024 (default 0.15; súbela para más señal ML)")
        p.add_argument("--no-enrich", action="store_true",
                       help="No enriquecer monto_adicional en 2022-2023")
        p.set_defaults(func=_handle_synth)

    # -- clean --
    p = sub.add_parser("clean", help="Limpia artefactos de una capa")
    p.add_argument("layer", choices=["bronze", "silver", "gold"])
    p.add_argument("--target", type=str, default="parquet",
                   choices=["parquet", "sqlserver"], help="Destino Gold a limpiar")
    p.add_argument("--profile", type=str, default="local", choices=["local", "docker"])
    p.add_argument("--yes", action="store_true", help="Confirma borrado de Bronze")
    p.set_defaults(func=_handle_clean)

    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()
    if not getattr(args, "func", None):
        parser.print_help()
        sys.exit(1)
    args.func(args)


if __name__ == "__main__":
    main()
