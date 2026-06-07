"""
Re-extracción Bronze de una sola pasada (temporal) — SIN tope por año y
ahora TODAS las categorías (goods/services/works), porque el extractor ya no
filtra por categoría (ACCEPTED_CATEGORIES = None).

Recorre el portal OCDS UNA vez (más reciente primero) y guarda todos los
records de EsSalud cuyo `compiledRelease.date` caiga en 2022-2026, clasificados
por año. Corte automático cuando ya pasamos el rango (racha de records con
compiledRelease.date < 2022).

Uso: python _reextract_binned.py
"""
from app.config.settings import settings
from app.clients.ocds_client import OCDSClient
from app.services.extractors import TargetedExtractor
from app.storage.file_manager import FileManager
from app.audit.logger import setup_logger

logger = setup_logger("reextract_binned")

RUC = settings.ESSALUD_RUC
YEARS = {2022, 2023, 2024, 2025}
MIN_YEAR = min(YEARS)
STOP_STREAK = 400   # records EsSalud seguidos con compiledRelease.date < 2022 => cortar


def main() -> None:
    client = OCDSClient()
    extractor = TargetedExtractor(client)
    fm = FileManager(settings.BRONZE_DIR)

    counts = {y: 0 for y in YEARS}
    scanned = 0
    saved = 0
    below_streak = 0
    try:
        logger.info(f"Inicio re-extracción COMPLETA (todas las categorías) RUC {RUC} | años {sorted(YEARS)}")
        for rec in extractor.paginate_records(RUC, target_year=None):
            scanned += 1
            date = (rec.get("compiledRelease") or {}).get("date") or ""
            try:
                year = int(str(date)[:4])
            except (ValueError, TypeError):
                year = None

            if year is not None and year < MIN_YEAR:
                below_streak += 1
                if below_streak >= STOP_STREAK:
                    logger.info(f"{below_streak} records EsSalud seguidos con fecha < {MIN_YEAR}; pasamos el rango. Cortando.")
                    break
                continue
            below_streak = 0

            if year not in YEARS:
                continue
            ocid = rec.get("ocid")
            if not ocid:
                continue
            fm.save_json(data=rec, path_suffix=f"records/{RUC}/{year}", filename=ocid)
            counts[year] += 1
            saved += 1
            if saved % 250 == 0:
                logger.info(f"Guardados {saved} (EsSalud vistos {scanned}) | {dict(sorted(counts.items()))}")
        logger.info(f"FIN. EsSalud vistos={scanned} Guardados={saved} | {dict(sorted(counts.items()))}")
    finally:
        client.close()


if __name__ == "__main__":
    main()
