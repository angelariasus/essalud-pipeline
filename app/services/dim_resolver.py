"""
Construye las dimensiones de la capa Oro y resuelve las claves foráneas de la Fact.

Decisión arquitectónica: los SK se asignan de forma DETERMINÍSTICA en Python
(no por IDENTITY de SQL Server). Esto permite resolver todas las FK sin una
conexión a la BD y validar el pipeline completo offline. El dw_loader (Paso 8)
inserta con SET IDENTITY_INSERT ON usando estos SK.

Las dimensiones precargadas por el DDL (Dim_Tiempo, Dim_Ubigeo departamentos,
Dim_Tipo_Proceso) NO se regeneran aquí; sus SK se replican como constantes
ancladas al orden de inserción del DDL.
"""
from typing import Dict, List, Optional, Tuple

import pandas as pd

from app.audit.logger import setup_logger
from app.loaders.master_loader import normalize_text
from app.utils.fuzzy_matcher import (
    match_medicamento,
    match_red_asistencial,
)

logger = setup_logger("ocds_framework.services.dim_resolver")

RUC_ESSALUD = "20131257750"

# ── SK anclados al DDL ───────────────────────────────────────────────────────
# Dim_Ubigeo: sentinel -1 + 25 departamentos con IDENTITY(1,1) en el orden del DDL.
DEPARTAMENTO_SK: Dict[str, int] = {
    "AMAZONAS": 1, "ANCASH": 2, "APURIMAC": 3, "AREQUIPA": 4, "AYACUCHO": 5,
    "CAJAMARCA": 6, "CALLAO": 7, "CUSCO": 8, "HUANCAVELICA": 9, "HUANUCO": 10,
    "ICA": 11, "JUNIN": 12, "LA LIBERTAD": 13, "LAMBAYEQUE": 14, "LIMA": 15,
    "LORETO": 16, "MADRE DE DIOS": 17, "MOQUEGUA": 18, "PASCO": 19, "PIURA": 20,
    "PUNO": 21, "SAN MARTIN": 22, "TACNA": 23, "TUMBES": 24, "UCAYALI": 25,
}

# Dim_Tipo_Proceso: SK por procurementMethodDetails normalizado (orden del DDL).
TIPO_PROCESO_SK: Dict[str, int] = {
    "LICITACION PUBLICA": 1,
    "CONCURSO PUBLICO": 2,
    "ADJUDICACION SIMPLIFICADA": 3,
    "SUBASTA INVERSA ELECTRONICA": 5,
    "CONVENIO MARCO": 6,
    "COMPARACION DE PRECIOS": 7,
    "CONTRATACION DIRECTA": 8,
}
SK_TIPO_PROCESO_CD = 8           # CONTRATACION DIRECTA (BIEN)
SK_TIPO_PROCESO_DESCONOCIDO = 12

# Sentinels (precargados por el DDL).
SK_SENTINEL = -1
SK_MED_NO_FARMA = -1
SK_MED_HISTORICO = -2
SK_MED_PAQUETE = -3
SK_MED_GENERICO = -4
SK_TIEMPO_NULO = -1


def _to_sk_tiempo(value) -> int:
    """Convierte una fecha (Timestamp/NaT) al SK YYYYMMDD; NaT -> -1."""
    if value is None or pd.isna(value):
        return SK_TIEMPO_NULO
    return int(value.strftime("%Y%m%d"))


def _to_date(value):
    """Timestamp -> date (para columnas DATE del DW); NaT -> None."""
    if value is None or pd.isna(value):
        return None
    return value.date()


# ── Dim_Proveedor ────────────────────────────────────────────────────────────
def build_dim_proveedor(flat_df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, int]]:
    """
    Una fila por RUC de proveedor único (no nulo). Devuelve el DataFrame de la
    dimensión y el mapa ruc -> SK_Proveedor.
    """
    pares = (
        flat_df[["ruc_proveedor", "nombre_proveedor"]]
        .dropna(subset=["ruc_proveedor"])
        .drop_duplicates(subset=["ruc_proveedor"])
    )

    rows, ruc_to_sk = [], {}
    for sk, (_, r) in enumerate(pares.iterrows(), start=1):
        ruc = r["ruc_proveedor"]
        nombre = r["nombre_proveedor"] or "SIN NOMBRE"
        es_consorcio = "CONSORCIO" in (normalize_text(nombre) or "")
        if es_consorcio:
            tipo = "Consorcio"
        elif ruc.startswith("20"):
            tipo = "Persona Juridica"
        elif ruc.startswith("10"):
            tipo = "Persona Natural"
        else:
            tipo = None
        ruc_to_sk[ruc] = sk
        rows.append(
            {
                "SK_Proveedor": sk,
                "RUC_Proveedor": ruc[:11],
                "Nombre_Proveedor": nombre[:250],
                "Tipo_Proveedor": tipo,
                "Es_Consorcio": int(es_consorcio),
            }
        )

    logger.info(f"Dim_Proveedor: {len(rows)} proveedores.")
    return pd.DataFrame(rows), ruc_to_sk


# ── Dim_Medicamento ──────────────────────────────────────────────────────────
def build_dim_medicamento(
    flat_df: pd.DataFrame, petitorio_df: pd.DataFrame
) -> Tuple[pd.DataFrame, Dict[str, int]]:
    """
    Una fila por descripción SEACE única clasificada como medicamento (EXACTO o
    FUZZY). Las DUDOSO/HISTORICO no generan fila: se mapean a sentinels.

    Devuelve el DataFrame de la dimensión y el mapa descripcion_norm -> SK.
    """
    choices = petitorio_df["DENOMINACION_DCI_NORM"].dropna().tolist()
    # Índice DCI_NORM -> fila del petitorio para copiar atributos del match.
    pet_by_dci = {
        row["DENOMINACION_DCI_NORM"]: row
        for _, row in petitorio_df.iterrows()
        if row["DENOMINACION_DCI_NORM"]
    }

    descripciones = flat_df["descripcion_item"].dropna().unique()
    rows, desc_to_sk = [], {}
    sk = 1
    for desc in descripciones:
        norm = normalize_text(desc)
        if not norm:
            continue
        res = match_medicamento(desc, choices)
        metodo = res["metodo"]

        if metodo in ("EXACTO", "FUZZY"):
            pet = pet_by_dci.get(res["dci"], {})
            n_item = pet.get("N_ITEM")
            try:
                n_item = int(n_item) if n_item not in (None, "") else None
            except (ValueError, TypeError):
                n_item = None
            rows.append(
                {
                    "SK_Medicamento": sk,
                    "N_Item_Petitorio": n_item,
                    "Codigo_SAP": (pet.get("CODIGO_SAP") or None),
                    "Denominacion_DCI": (pet.get("DENOMINACION_DCI") or res["dci"])[:300],
                    "Especificaciones_Tecnicas": _trunc(pet.get("ESPECIFICACIONES_TECNICAS"), 500),
                    "Unidad_De_Manejo": _trunc(pet.get("UNIDAD_MANEJO"), 20),
                    "Restriccion_Uso": _trunc(pet.get("RESTRICCION_USO"), 50),
                    "Especialidad_Autorizada": _trunc(pet.get("ESPECIALIDAD_AUTORIZADA"), 300),
                    "Indicaciones_Observaciones": _trunc(pet.get("INDICACIONES"), 1000),
                    "En_Petitorio_2026": 1,
                    "Descripcion_SEACE_Original": desc[:600],
                    "Score_Fuzzy_Match": round(res["score"], 2),
                    "Metodo_Clasificacion": metodo,
                }
            )
            desc_to_sk[norm] = sk
            sk += 1
        elif metodo == "HISTORICO":
            desc_to_sk[norm] = SK_MED_HISTORICO
        else:  # DUDOSO
            desc_to_sk[norm] = SK_MED_NO_FARMA

    clasificados = len(rows)
    logger.info(
        f"Dim_Medicamento: {clasificados} clasificados (EXACTO/FUZZY) de "
        f"{len(descripciones)} descripciones únicas."
    )
    return pd.DataFrame(rows), desc_to_sk


# ── Dim_Entidad_Compradora (nivel Red) ───────────────────────────────────────
def _resolve_red_map(flat_df: pd.DataFrame, red_choices: List[str]) -> Dict[str, Optional[str]]:
    """Mapa red_candidato -> Red canónica del Excel (cacheado por candidato único)."""
    cand_to_red: Dict[str, Optional[str]] = {}
    for cand in flat_df["red_candidato"].dropna().unique():
        cand_to_red[cand] = match_red_asistencial(cand, red_choices)["red"]
    return cand_to_red


def build_dim_entidad(
    flat_df: pd.DataFrame, establecimientos_df: pd.DataFrame
) -> Tuple[pd.DataFrame, Dict[str, int], Dict[str, Optional[str]]]:
    """
    Una fila por Red Asistencial presente en los datos. FK_Ubigeo apunta al
    departamento dominante de la Red (SK del departamento precargado).

    Devuelve: (DataFrame dimensión, mapa red_canónica -> SK_Entidad,
    mapa red_candidato -> red_canónica).
    """
    red_choices = [
        r for r in establecimientos_df["RED_NORM"].dropna().unique()
        if r not in ("RED", "RED ASISTENCIAL")
    ]
    cand_to_red = _resolve_red_map(flat_df, red_choices)

    # Departamento dominante por Red (mode de DPTO de sus establecimientos).
    est = establecimientos_df.copy()
    est["DPTO_NORM"] = est["DPTO"].map(normalize_text)
    red_to_depto: Dict[str, str] = {}
    for red, grp in est.groupby("RED_NORM"):
        modes = grp["DPTO_NORM"].mode()
        if len(modes):
            red_to_depto[red] = modes.iloc[0]

    redes_presentes = sorted({r for r in cand_to_red.values() if r})

    rows, red_to_sk = [], {}
    for sk, red in enumerate(redes_presentes, start=1):
        depto = red_to_depto.get(red)
        fk_ubigeo = DEPARTAMENTO_SK.get(depto, SK_SENTINEL)
        if "PRESTACIONAL" in red:
            tipo_red = "RP"
        elif "ASISTENCIAL" in red:
            tipo_red = "RA"
        else:
            tipo_red = "INST"
        red_to_sk[red] = sk
        rows.append(
            {
                "SK_Entidad": sk,
                "FK_Ubigeo": fk_ubigeo,
                "Cod_EESS": f"RED{sk:03d}",
                "RUC_Entidad": RUC_ESSALUD,
                "Red_Asistencial": red[:100],
                "Tipo_Red": tipo_red,
                "Nombre_Centro": red[:200],
                "Estado_Operativo": "FUNCIONA",
            }
        )

    logger.info(f"Dim_Entidad: {len(rows)} redes asistenciales.")
    return pd.DataFrame(rows), red_to_sk, cand_to_red


# ── Resolución de la Fact ────────────────────────────────────────────────────
def _tipo_proceso_sk(metodo_contratacion: str, detalles: Optional[str]) -> int:
    """SK_Tipo_Proceso desde el método/detalle OCDS (default DESCONOCIDO)."""
    if metodo_contratacion == "direct":
        return SK_TIPO_PROCESO_CD
    det = normalize_text(detalles) or ""
    for clave, sk in TIPO_PROCESO_SK.items():
        if clave in det:
            return sk
    return SK_TIPO_PROCESO_DESCONOCIDO


def resolve_fact(
    flat_df: pd.DataFrame,
    desc_to_sk: Dict[str, int],
    ruc_to_sk: Dict[str, int],
    red_to_sk: Dict[str, int],
    cand_to_red: Dict[str, Optional[str]],
    entidad_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Resuelve todas las FK y arma el DataFrame de la Fact. Descarta filas sin año
    fiscal identificable (Anio_Fiscal es NOT NULL en el DW).
    """
    red_to_ubigeo = dict(zip(entidad_df["Red_Asistencial"], entidad_df["FK_Ubigeo"]))

    rows = []
    descartadas = 0
    for _, r in flat_df.iterrows():
        anio = r.get("anio_fiscal")
        if pd.isna(anio):
            descartadas += 1
            continue

        # Red -> FK_Entidad y FK_Ubigeo_Item (Gap 3: el ítem hereda el dpto. de la Red).
        red_canonica = cand_to_red.get(r.get("red_candidato"))
        fk_entidad = red_to_sk.get(red_canonica, SK_SENTINEL) if red_canonica else SK_SENTINEL
        fk_ubigeo_item = red_to_ubigeo.get(red_canonica, SK_SENTINEL) if red_canonica else SK_SENTINEL

        # Medicamento.
        norm = normalize_text(r.get("descripcion_item"))
        fk_med = desc_to_sk.get(norm, SK_MED_NO_FARMA) if norm else SK_MED_NO_FARMA

        # Proveedor.
        fk_prov = ruc_to_sk.get(r.get("ruc_proveedor"), SK_SENTINEL)

        es_cd = bool(r.get("es_contratacion_directa"))
        fuera_petitorio = "FUERA DEL PETITORIO" in (norm or "")

        rows.append(
            {
                "FK_Tiempo_Convocatoria": _to_sk_tiempo(r.get("fecha_convocatoria")),
                "FK_Tiempo_Buena_Pro": _to_sk_tiempo(r.get("fecha_buena_pro")),
                "FK_Tiempo_Suscripcion": _to_sk_tiempo(r.get("fecha_suscripcion")),
                "FK_Tiempo_Emision_OC": SK_TIEMPO_NULO,  # Gap 1: OCDS no expone OC
                "FK_Entidad": fk_entidad,
                "FK_Medicamento": fk_med,
                "FK_Proveedor": fk_prov,
                "FK_Tipo_Proceso": _tipo_proceso_sk(r.get("metodo_contratacion"), r.get("detalles_metodo")),
                "FK_Ubigeo_Item": fk_ubigeo_item,
                "Fecha_Convocatoria": _to_date(r.get("fecha_convocatoria")),
                "Fecha_Buena_Pro": _to_date(r.get("fecha_buena_pro")),
                "Fecha_Suscripcion": _to_date(r.get("fecha_suscripcion")),
                "Fecha_Emision_OC": None,  # Gap 1
                "N_Item": None,            # id OCDS excede SMALLINT; sin nº de ítem secuencial
                "Cantidad_Adjudicada": _num(r.get("cantidad")),
                "Monto_Referencial_Soles": _num(r.get("monto_referencial")),
                "Monto_Adjudicado_Soles": _num(r.get("monto_adjudicado")),
                "Monto_Contratado_Item": _num(r.get("monto_contratado")),
                "Monto_Adicional": _num(r.get("monto_adicional")),
                "Monto_Total_OC": 0,       # Gap 1
                "Flag_Contratacion_Directa": int(es_cd),
                "Flag_Fuera_Petitorio": int(fuera_petitorio),
                "Estado_Item": _trunc(r.get("estado_adjudicacion"), 30),
                "Moneda_Original": "PEN",
                "Fuente_Dataset": "CONTRATACION_DIRECTA" if es_cd else "ADJUDICACION",
                "Anio_Fiscal": int(anio),
            }
        )

    if descartadas:
        logger.warning(f"Fact: {descartadas} filas descartadas sin año fiscal.")
    logger.info(f"Fact: {len(rows)} filas resueltas.")
    return pd.DataFrame(rows)


def _num(value) -> float:
    """NaN/None -> 0.0; numérico -> float."""
    if value is None or pd.isna(value):
        return 0.0
    return float(value)


def _trunc(value, length: int) -> Optional[str]:
    """Recorta a `length` chars; None/NaN -> None."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    return str(value)[:length]


def resolve_all(
    flat_df: pd.DataFrame, petitorio_df: pd.DataFrame, establecimientos_df: pd.DataFrame
) -> Dict[str, pd.DataFrame]:
    """Orquesta la construcción de dimensiones y la resolución de la Fact."""
    dim_prov, ruc_to_sk = build_dim_proveedor(flat_df)
    dim_med, desc_to_sk = build_dim_medicamento(flat_df, petitorio_df)
    dim_ent, red_to_sk, cand_to_red = build_dim_entidad(flat_df, establecimientos_df)
    fact = resolve_fact(flat_df, desc_to_sk, ruc_to_sk, red_to_sk, cand_to_red, dim_ent)
    return {
        "dim_entidad": dim_ent,
        "dim_medicamento": dim_med,
        "dim_proveedor": dim_prov,
        "fact": fact,
    }
