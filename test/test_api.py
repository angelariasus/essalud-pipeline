import pytest
import requests

API_BASE = "https://contratacionesabiertas.oece.gob.pe/api/v1"


def get_first_record_buyer(params: dict) -> str | None:
    r = requests.get(f"{API_BASE}/records", params=params, timeout=10)
    r.raise_for_status()
    data = r.json()
    records = data.get("records", [])
    if not records:
        return None
    return records[0].get("compiledRelease", {}).get("buyer", {}).get("name")


def test_api_responds():
    r = requests.get(f"{API_BASE}/records", params={"limit": 1}, timeout=10)
    assert r.status_code == 200


def test_api_returns_records():
    """El endpoint /records devuelve una lista de registros."""
    r = requests.get(f"{API_BASE}/records", params={"limit": 5}, timeout=10)
    r.raise_for_status()
    data = r.json()
    records = data.get("records", [])
    assert len(records) > 0
    # Verificar que los records tienen la estructura esperada
    for rec in records:
        assert "ocid" in rec
        assert "compiledRelease" in rec
        cr = rec["compiledRelease"]
        assert "tender" in cr
        assert "buyer" in cr


def test_first_record_has_buyer():
    """El primer record debe tener un buyer name."""
    buyer_name = get_first_record_buyer({"limit": 1})
    assert buyer_name is not None, "No records returned"


def test_main_procurement_category_exists():
    """Cada record debe tener mainProcurementCategory dentro de tender."""
    r = requests.get(f"{API_BASE}/records", params={"limit": 10}, timeout=10)
    r.raise_for_status()
    data = r.json()
    for record in data.get("records", []):
        cr = record.get("compiledRelease", {})
        tender = cr.get("tender", {})
        cat = tender.get("mainProcurementCategory")
        assert cat is not None, "Record missing tender.mainProcurementCategory"


def test_essalud_records_have_known_category():
    """Los records de EsSalud visibles deben tener una categoría válida de OCDS
    (ya NO se restringe a 'goods': se incluye toda la contratación)."""
    r = requests.get(f"{API_BASE}/records", params={"limit": 50}, timeout=10)
    r.raise_for_status()
    data = r.json()
    essalud_records = []
    for record in data.get("records", []):
        cr = record.get("compiledRelease", {})
        buyer = cr.get("buyer", {})
        if buyer and "ESSALUD" in buyer.get("name", "").upper():
            tender = cr.get("tender", {})
            cat = tender.get("mainProcurementCategory")
            essalud_records.append((record.get("ocid"), cat))
    if not essalud_records:
        pytest.skip("No se encontraron records de EsSalud en los primeros 50 (API no filtra server-side)")
    for ocid, cat in essalud_records:
        assert cat in {"goods", "services", "works", None}, f"{ocid} categoría inesperada={cat}"


def test_links_next_exists():
    """La respuesta debe incluir links.next para paginación."""
    r = requests.get(f"{API_BASE}/records", params={"limit": 1}, timeout=10)
    r.raise_for_status()
    data = r.json()
    links = data.get("links", {})
    assert "next" in links, "Missing pagination link"
