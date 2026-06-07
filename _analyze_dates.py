"""Diagnóstico: compara campos de fecha de convocatoria en el Bronze."""
import json
from collections import Counter
from pathlib import Path

BASE = Path("data/bronze/records/20131257750")
YEARS = ["2022", "2023", "2024", "2025", "2026"]


def yr(v):
    if not v:
        return None
    s = str(v)[:4]
    return s if s.isdigit() else None


tp_dist = Counter()      # año de tenderPeriod.startDate
dp_dist = Counter()      # año de tender.datePublished
status_dist = Counter()
tp_null = dp_null = total = 0
sample_tender_keys = Counter()

for y in YEARS:
    d = BASE / y
    if not d.exists():
        continue
    for f in d.glob("*.json"):
        try:
            j = json.load(open(f, encoding="utf-8"))
        except Exception:
            continue
        cr = j.get("compiledRelease", {}) or {}
        t = cr.get("tender", {}) or {}
        tp = (t.get("tenderPeriod") or {})
        total += 1
        for k in t.keys():
            sample_tender_keys[k] += 1
        a = yr(tp.get("startDate"))
        b = yr(t.get("datePublished"))
        if a is None:
            tp_null += 1
        else:
            tp_dist[a] += 1
        if b is None:
            dp_null += 1
        else:
            dp_dist[b] += 1
        status_dist[t.get("status")] += 1

print(f"TOTAL records: {total}")
print(f"\ntenderPeriod.startDate -> NULL: {tp_null} ({100*tp_null/total:.1f}%)")
print("  por año:", dict(sorted(tp_dist.items())))
print(f"\ntender.datePublished   -> NULL: {dp_null} ({100*dp_null/total:.1f}%)")
print("  por año:", dict(sorted(dp_dist.items())))
print("\ntender.status:", dict(status_dist))
print("\nclaves tender presentes (conteo):", dict(sorted(sample_tender_keys.items(), key=lambda x: -x[1])))
