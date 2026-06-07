"""Diagnóstico: escanea el portal (más reciente primero) SIN filtro de categoría
y tabula registros de EsSalud por (año de datePublished, mainProcurementCategory).
Objetivo: ver si hay compras EsSalud 2024-2026 que el filtro `goods` descarta."""
from collections import Counter
from urllib.parse import urlparse, parse_qs

from app.config.settings import settings
from app.clients.ocds_client import OCDSClient
from app.services.extractors import TargetedExtractor

RUC = settings.ESSALUD_RUC
client = OCDSClient()
ext = TargetedExtractor(client)

by_year_cat = Counter()     # (anio_datePublished, categoria) -> n   (solo EsSalud)
recent_low_streak = 0       # cuántos EsSalud seguidos con año < 2024 (para cortar)
essalud_seen = 0
portal_scanned = 0
params = {"size": 100}
STOP_PORTAL = 120000        # tope duro de seguridad

try:
    while True:
        resp = client.get("recordsAfter", params=params)
        data = resp.json()
        recs = data.get("records", [])
        if not recs:
            break
        for rec in recs:
            portal_scanned += 1
            if not ext._is_target_buyer(rec, RUC):
                continue
            essalud_seen += 1
            cr = rec.get("compiledRelease", {}) or {}
            t = cr.get("tender", {}) or {}
            dp = t.get("datePublished") or ""
            yr = str(dp)[:4] if dp else "NULL"
            cat = t.get("mainProcurementCategory") or "NULL"
            by_year_cat[(yr, cat)] += 1
            if yr.isdigit() and int(yr) < 2024:
                recent_low_streak += 1
            else:
                recent_low_streak = 0
        # ¿ya pasamos claramente el rango reciente? (300 EsSalud seguidos < 2024)
        if recent_low_streak >= 300:
            print(f"[stop] {recent_low_streak} EsSalud seguidos con datePublished<2024; cortando.")
            break
        if portal_scanned >= STOP_PORTAL:
            print(f"[stop] tope portal {STOP_PORTAL} alcanzado.")
            break
        nxt = data.get("links", {}).get("next")
        if not nxt:
            print("[stop] fin de paginación.")
            break
        q = parse_qs(urlparse(nxt).query)
        params = {k: v[0] for k, v in q.items()}
        if essalud_seen and essalud_seen % 500 == 0:
            print(f"... portal={portal_scanned} EsSalud={essalud_seen}")
finally:
    client.close()

print(f"\nPortal escaneado: {portal_scanned} | EsSalud hallados: {essalud_seen}")
print("\n(año_datePublished, categoria) -> n  [EsSalud, todas las categorías]:")
for (yr, cat), n in sorted(by_year_cat.items(), key=lambda x: (x[0][0], -x[1])):
    print(f"  {yr:6} {str(cat):14} {n}")
