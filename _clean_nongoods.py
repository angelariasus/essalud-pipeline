"""Limpieza (temporal): elimina del Bronze los records cuya
tender.mainProcurementCategory != 'goods' (deja solo medicamentos/bienes)."""
import json
from pathlib import Path

BASE = Path("data/bronze/records/20131257750")
kept = 0
deleted = 0
for d in sorted(p for p in BASE.iterdir() if p.is_dir()):
    dk = dd = 0
    for f in d.glob("*.json"):
        try:
            j = json.load(open(f, encoding="utf-8"))
            cat = (j.get("compiledRelease", {}).get("tender", {}) or {}).get("mainProcurementCategory")
        except Exception:
            cat = None
        if cat == "goods":
            dk += 1
        else:
            f.unlink()
            dd += 1
    kept += dk
    deleted += dd
    print(f"  {d.name}: goods={dk} eliminados(no-goods)={dd}")
print(f"TOTAL goods conservados={kept} | no-goods eliminados={deleted}")
