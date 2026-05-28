import json
import re
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd

from app.config.settings import settings
from app.audit.logger import setup_logger
from app.utils.fuzzy_matcher import extract_red_asistencial

logger = setup_logger("ocds_framework.services.ocds_flattener")

# Columnas de fecha que se parsean a datetime tras construir el DataFrame.
_DATE_COLUMNS = ["fecha_convocatoria", "fecha_buena_pro", "fecha_suscripcion"]


def _get_buyer(parties: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Devuelve el party con rol buyer (o procuringEntity como respaldo)."""
    for p in parties:
        if "buyer" in p.get("roles", []):
            return p
    for p in parties:
        if "procuringEntity" in p.get("roles", []):
            return p
    return None


def _clean_ruc(raw: Optional[str]) -> Optional[str]:
    """
    Extrae los dígitos finales de un id tipo 'PE-RUC-20101102204' -> '20101102204'.
    Conserva los dígitos aunque no formen un RUC válido de 11 (datos sucios del origen);
    la validación de longitud queda para la construcción de Dim_Proveedor.
    """
    if not raw:
        return None
    m = re.search(r"(\d+)$", raw)
    return m.group(1) if m else raw


def _ruc_from_party(party: Optional[Dict[str, Any]]) -> Optional[str]:
    """RUC del comprador: preferir additionalIdentifiers[PE-RUC], luego el id."""
    if not party:
        return None
    for ident in party.get("additionalIdentifiers") or []:
        if ident.get("scheme") == "PE-RUC":
            return ident.get("id")
    return _clean_ruc(party.get("id"))


def _find_award(item_id: str, awards: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Award que adjudica el ítem (por item.id). Si ningún award detalla ítems, usa el primero."""
    for a in awards:
        if item_id in [i.get("id") for i in a.get("items", [])]:
            return a
    if awards and all(not a.get("items") for a in awards):
        return awards[0]
    return None


def _find_contract(
    award: Optional[Dict[str, Any]], item_id: str, contracts: List[Dict[str, Any]]
) -> Optional[Dict[str, Any]]:
    """
    Contrato del ítem, en cascada: por awardID, luego por item.id en sus ítems,
    luego 1:1 si hay un único contrato (el awardID del OCDS a veces no concuerda).
    """
    if not contracts:
        return None
    if award:
        for c in contracts:
            if c.get("awardID") and c.get("awardID") == award.get("id"):
                return c
    for c in contracts:
        if item_id in [i.get("id") for i in c.get("items", [])]:
            return c
    if len(contracts) == 1:
        return contracts[0]
    return None


def _sum_amendments(contract: Optional[Dict[str, Any]]) -> float:
    """Suma de montos de adendas del contrato (0.0 si no hay)."""
    if not contract:
        return 0.0
    total = 0.0
    for am in contract.get("amendments") or []:
        amount = (am.get("value") or {}).get("amount")
        if isinstance(amount, (int, float)):
            total += amount
    return total


def flatten_record(record: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Aplana un record OCDS proyectado a una fila por ítem de licitación."""
    cr = record.get("compiledRelease", {})
    tender = cr.get("tender", {})
    items = tender.get("items", [])
    awards = cr.get("awards", [])
    contracts = cr.get("contracts", [])
    buyer = _get_buyer(cr.get("parties", []))

    title = tender.get("title")
    metodo = tender.get("procurementMethod")
    item_descs = [it.get("description") for it in items]
    red = extract_red_asistencial(title, [tender.get("description")] + item_descs)

    ruc_comprador = _ruc_from_party(buyer)
    nombre_comprador = buyer.get("name") if buyer else None
    region_comprador = (buyer.get("address") or {}).get("region") if buyer else None

    rows = []
    for item in items:
        item_id = item.get("id")
        award = _find_award(item_id, awards)
        contract = _find_contract(award, item_id, contracts)
        supplier = (award.get("suppliers") or [{}])[0] if award else {}

        rows.append(
            {
                "ocid": record.get("ocid"),
                "n_item": item_id,
                "descripcion_item": item.get("description"),
                "cantidad": item.get("quantity"),
                "unidad_medida": (item.get("unit") or {}).get("name"),
                "fecha_convocatoria": (tender.get("tenderPeriod") or {}).get("startDate"),
                "fecha_buena_pro": award.get("date") if award else None,
                "fecha_suscripcion": contract.get("dateSigned") if contract else None,
                "monto_referencial": (tender.get("value") or {}).get("amount"),
                "monto_adjudicado": (award.get("value") or {}).get("amount") if award else None,
                "monto_contratado": (contract.get("value") or {}).get("amount") if contract else None,
                "monto_adicional": _sum_amendments(contract),
                "ruc_comprador": ruc_comprador,
                "nombre_comprador": nombre_comprador,
                "departamento_comprador": region_comprador,
                "red_candidato": red.get("candidato"),
                "red_metodo": red.get("metodo"),
                "ruc_proveedor": _clean_ruc(supplier.get("id")),
                "nombre_proveedor": supplier.get("name"),
                "metodo_contratacion": metodo,
                "detalles_metodo": tender.get("procurementMethodDetails"),
                "causal_cd": tender.get("procurementMethodRationale"),
                "es_contratacion_directa": metodo == "direct",
                "estado_adjudicacion": award.get("status") if award else None,
                "tiene_adenda": bool(contract and contract.get("amendments")),
            }
        )
    return rows


def _read_bronze_records(directory: Path) -> Iterable[Dict[str, Any]]:
    """Itera los records proyectados (*.json) de un directorio Bronze."""
    if not directory.exists():
        logger.warning(f"Directorio Bronze inexistente: {directory}")
        return
    for path in sorted(directory.glob("*.json")):
        try:
            with open(path, encoding="utf-8") as fh:
                yield json.load(fh)
        except (json.JSONDecodeError, OSError) as e:
            logger.error(f"No se pudo leer {path.name}: {e}")


def _build_dataframe(records: Iterable[Dict[str, Any]]) -> pd.DataFrame:
    """Construye el DataFrame plano y parsea fechas / año fiscal."""
    rows: List[Dict[str, Any]] = []
    for rec in records:
        rows.extend(flatten_record(rec))

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    for col in _DATE_COLUMNS:
        # Tomar solo YYYY-MM-DD del ISO con timezone para evitar corrimientos de día.
        df[col] = pd.to_datetime(
            df[col].astype("string").str.slice(0, 10), format="%Y-%m-%d", errors="coerce"
        )

    df["anio_fiscal"] = (
        df["fecha_convocatoria"].dt.year
        .fillna(df["fecha_buena_pro"].dt.year)
        .fillna(df["fecha_suscripcion"].dt.year)
        .astype("Int64")
    )
    return df


def flatten_bronze_dir(directory: Path) -> pd.DataFrame:
    """Aplana todos los records de un directorio Bronze a un DataFrame por ítem."""
    logger.info(f"Aplanando records Bronze desde: {directory}")
    df = _build_dataframe(_read_bronze_records(directory))
    logger.info(f"Aplanado: {len(df)} filas-ítem.")
    return df


def flatten_year(year: int, ruc: str = settings.ESSALUD_RUC, save: bool = True) -> pd.DataFrame:
    """Aplana un año de Bronze y, opcionalmente, lo persiste como Parquet en staging."""
    directory = settings.BRONZE_DIR / "records" / ruc / str(year)
    df = flatten_bronze_dir(directory)

    if save and not df.empty:
        staging = settings.SILVER_DIR / "staging"
        staging.mkdir(parents=True, exist_ok=True)
        out = staging / f"ocds_flat_{year}.parquet"
        df.to_parquet(out, engine="pyarrow", index=False)
        logger.info(f"Staging escrito: {out} ({len(df)} filas).")
    return df


def flatten_all(years: List[int], ruc: str = settings.ESSALUD_RUC) -> pd.DataFrame:
    """Aplana y concatena varios años."""
    frames = [flatten_year(y, ruc=ruc) for y in years]
    frames = [f for f in frames if not f.empty]
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)
