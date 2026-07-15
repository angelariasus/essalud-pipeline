import re
from typing import List, Optional

import pandas as pd
from rapidfuzz import fuzz, process
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StructField, StructType, StringType, DoubleType

from app.loaders.master_loader import normalize_text
from app.audit.logger import setup_logger

logger = setup_logger("ocds_framework.utils.fuzzy_matcher")

# Umbrales de clasificación de medicamentos (scorer token_set_ratio, validado:
# medicamentos reales caen en 75-100, no-medicamentos en 47-52).
SCORE_EXACTO = 90
SCORE_FUZZY = 70
SCORE_RED_MIN = 80

# Mapeo de códigos SEACE de EsSalud -> nombre canónico de Red (idéntico al maestro
# de Establecimientos). Resuelve el Gap 2: los códigos del título son abreviaturas
# que no matchean por similitud de caracteres contra los nombres completos.
# Códigos ambiguos o no listados quedan sin mapear (Red = NULL, se loguean).
CODIGO_RED_MAP = {
    # Redes Asistenciales (departamentales)
    "RAAMA": "RED ASISTENCIAL AMAZONAS",
    "RAAMAZ": "RED ASISTENCIAL AMAZONAS",
    "RAANC": "RED ASISTENCIAL ANCASH",
    "RAAPU": "RED ASISTENCIAL APURIMAC",
    "RAAR": "RED ASISTENCIAL AREQUIPA",
    "RAARQ": "RED ASISTENCIAL AREQUIPA",
    "RAAYA": "RED ASISTENCIAL AYACUCHO",
    "RAAYAC": "RED ASISTENCIAL AYACUCHO",
    "RACAJ": "RED ASISTENCIAL CAJAMARCA",
    "RACUS": "RED ASISTENCIAL CUSCO",
    "RACUSCO": "RED ASISTENCIAL CUSCO",
    "RAHV": "RED ASISTENCIAL HUANCAVELICA",
    "RAHVCA": "RED ASISTENCIAL HUANCAVELICA",
    "RAHCO": "RED ASISTENCIAL HUANUCO",
    "RAHUA": "RED ASISTENCIAL HUANUCO",
    "RAHZ": "RED ASISTENCIAL HUARAZ",
    "RAICA": "RED ASISTENCIAL ICA",
    "RAJAE": "RED ASISTENCIAL JAEN",
    "RAJAEN": "RED ASISTENCIAL JAEN",
    "RAJUL": "RED ASISTENCIAL JULIACA",
    "RAJUN": "RED ASISTENCIAL JUNIN",
    "RALL": "RED ASISTENCIAL LA LIBERTAD",
    "RALOR": "RED ASISTENCIAL LORETO",
    "RAMD": "RED ASISTENCIAL MADRE DE DIOS",
    "RAMDD": "RED ASISTENCIAL MADRE DE DIOS",
    "RAMOQ": "RED ASISTENCIAL MOQUEGUA",
    "RAMOY": "RED ASISTENCIAL MOYOBAMBA",
    "RAPAS": "RED ASISTENCIAL PASCO",
    "RAPIU": "RED ASISTENCIAL PIURA",
    "RAPUN": "RED ASISTENCIAL PUNO",
    "RATAC": "RED ASISTENCIAL TACNA",
    "RATAR": "RED ASISTENCIAL TARAPOTO",
    "RATUM": "RED ASISTENCIAL TUMBES",
    "RAUCA": "RED ASISTENCIAL UCAYALI",
    # Redes Prestacionales (Lima y Lambayeque)
    "RPA": "RED PRESTACIONAL ALMENARA",
    "RPALM": "RED PRESTACIONAL ALMENARA",
    "RPL": "RED PRESTACIONAL LAMBAYEQUE",
    "RPLAM": "RED PRESTACIONAL LAMBAYEQUE",
    "RPR": "RED PRESTACIONAL REBAGLIATI",
    "RPREB": "RED PRESTACIONAL REBAGLIATI",
    "RPS": "RED PRESTACIONAL SABOGAL",
    "RPSAB": "RED PRESTACIONAL SABOGAL",
}

# Captura el código de red en el título: "...ESSALUD/RAMD-1" -> "RAMD".
_CODE_RE = re.compile(r"ESSALUD[\s/_-]+(R[A-Z]+)")

# Captura el nombre explícito cuando aparece en texto: "...RED ASISTENCIAL MOQUEGUA".
_NAME_RE = re.compile(r"RED\s+(ASISTENCIAL|PRESTACIONAL)\s+([A-Z][A-Z ]*)")

# Máximo de palabras a conservar del nombre capturado (los nombres más largos son
# "MADRE DE DIOS" y "LA LIBERTAD"); el fuzzy match descarta el ruido restante.
_NAME_MAX_WORDS = 4


def match_medicamento(descripcion: str, petitorio_choices: List[str]) -> dict:
    """
    Clasifica una descripción SEACE contra las denominaciones DCI del Petitorio.

    Args:
        descripcion: descripción cruda del ítem OCDS.
        petitorio_choices: lista de DENOMINACION_DCI_NORM (ya normalizadas).

    Returns:
        dict con `dci` (denominación matcheada o None), `score` y `metodo`
        (EXACTO / FUZZY / DUDOSO / HISTORICO).
    """
    q = normalize_text(descripcion)
    if not q:
        return {"dci": None, "score": 0.0, "metodo": "DUDOSO"}

    if "FUERA DEL PETITORIO" in q:
        return {"dci": None, "score": 0.0, "metodo": "HISTORICO"}

    best = process.extractOne(q, petitorio_choices, scorer=fuzz.token_set_ratio)
    if best is None:
        return {"dci": None, "score": 0.0, "metodo": "DUDOSO"}

    dci, score, _ = best
    score = float(score)

    if score >= SCORE_EXACTO:
        return {"dci": dci, "score": score, "metodo": "EXACTO"}
    if score >= SCORE_FUZZY:
        return {"dci": dci, "score": score, "metodo": "FUZZY"}
    return {"dci": None, "score": score, "metodo": "DUDOSO"}


def extract_red_asistencial(
    title: Optional[str], descriptions: Optional[List[str]] = None
) -> dict:
    """
    Extrae un candidato de Red Asistencial desde el OCDS, en cascada:

    1. CODIGO: el título trae el código SEACE (RAMD, RACAJ...) -> tabla CODIGO_RED_MAP.
       Es la fuente canónica: mapea al nombre completo sin truncamiento.
    2. NOMBRE_TEXTO: el texto trae "RED ASISTENCIAL/PRESTACIONAL <nombre>" explícito
       (fallback cuando el código no es mapeable).
    3. SIN_RED / CODIGO_NO_MAPEADO: no se pudo resolver (candidato None).

    Returns:
        dict con `candidato` (nombre de red o None) y `metodo`.
    """
    descriptions = descriptions or []
    title_norm = normalize_text(title) or ""

    # Nivel 1: código SEACE en el título (fuente canónica).
    mc = _CODE_RE.search(title_norm)
    if mc:
        code = mc.group(1)
        mapped = CODIGO_RED_MAP.get(code)
        if mapped:
            return {"candidato": mapped, "metodo": "CODIGO"}
        logger.debug(f"Código de Red no mapeado: {code!r} (título: {title!r})")
        # No retornamos aún: el nombre explícito puede resolverlo.

    # Nivel 2: nombre explícito en el texto.
    full_text = normalize_text(" ".join([title or ""] + [d or "" for d in descriptions])) or ""
    m = _NAME_RE.search(full_text)
    if m:
        nombre = " ".join(m.group(2).split()[:_NAME_MAX_WORDS])
        if nombre:
            return {"candidato": f"RED {m.group(1)} {nombre}", "metodo": "NOMBRE_TEXTO"}

    if mc:
        return {"candidato": None, "metodo": "CODIGO_NO_MAPEADO", "codigo": mc.group(1)}
    return {"candidato": None, "metodo": "SIN_RED"}


def match_red_asistencial(candidato: Optional[str], red_choices: List[str]) -> dict:
    """
    Resuelve el candidato de Red contra los valores canónicos del maestro de
    Establecimientos (columna RED_NORM) por fuzzy match.

    Returns:
        dict con `red` (valor canónico del Excel o None) y `score`.
    """
    if not candidato:
        return {"red": None, "score": 0.0}

    q = normalize_text(candidato)
    best = process.extractOne(q, red_choices, scorer=fuzz.token_set_ratio)
    if best is None:
        return {"red": None, "score": 0.0}

    red, score, _ = best
    score = float(score)
    if score >= SCORE_RED_MIN:
        return {"red": red, "score": score}
    return {"red": None, "score": score}


from pyspark.sql import functions as F

MED_MATCH_SCHEMA = StructType([
    StructField("dci", StringType()),
    StructField("score", DoubleType()),
    StructField("metodo", StringType()),
])

RED_MATCH_SCHEMA = StructType([
    StructField("red", StringType()),
    StructField("score", DoubleType()),
])


def make_match_medicamento_udf(broadcast_choices):
    """
    Devuelve un UDF estándar (PySpark) que clasifica descripciones SEACE contra el Petitorio.
    """
    @F.udf(MED_MATCH_SCHEMA)
    def _udf(desc: str):
        if desc is None:
            return (None, 0.0, "DUDOSO")
        choices = broadcast_choices.value
        try:
            res = match_medicamento(desc, choices)
            return (res["dci"], float(res["score"]), res["metodo"])
        except Exception as exc:
            return (None, 0.0, f"ERROR: {exc}")

    return _udf


def make_match_red_udf(broadcast_choices):
    """
    Devuelve un UDF estándar (PySpark) que resuelve candidatos de Red contra el maestro.
    """
    @F.udf(RED_MATCH_SCHEMA)
    def _udf(cand: str):
        if cand is None:
            return (None, 0.0)
        choices = broadcast_choices.value
        try:
            res = match_red_asistencial(cand, choices)
            return (res["red"], float(res["score"]))
        except Exception:
            return (None, 0.0)

    return _udf

