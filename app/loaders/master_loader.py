import re
import unicodedata
from pathlib import Path
from typing import Optional

import pandas as pd

from app.audit.logger import setup_logger

logger = setup_logger("ocds_framework.loaders.master_loader")

# Nombres de archivo de los maestros en EXTRA_DATA_DIR.
PETITORIO_FILENAME = "Petitorio-Publicar-hasta-Res-N_-063-2026.xls"
ESTABLECIMIENTOS_FILENAME = (
    "5992483-relacion-de-establecimientos-de-salud-por-redes-asistenciales-"
    "y-niveles-de-atencion-julio-2025.xlsx"
)

# Las 8 columnas útiles del Petitorio, renombradas por posición (el .xls trae
# ~22 columnas "Unnamed" vacías a la derecha que se descartan).
PETITORIO_COLUMNS = [
    "N_ITEM",
    "CODIGO_SAP",
    "DENOMINACION_DCI",
    "ESPECIFICACIONES_TECNICAS",
    "UNIDAD_MANEJO",
    "RESTRICCION_USO",
    "ESPECIALIDAD_AUTORIZADA",
    "INDICACIONES",
]

# Las 14 columnas de la hoja CentrosFuncional, renombradas por posición.
ESTABLECIMIENTOS_COLUMNS = [
    "RED",
    "TIPO",
    "COD_EESS",
    "COD_SES",
    "COD_RENAES",
    "TIPO_2",
    "CENTROS_ASISTENCIALES",
    "DPTO",
    "PROVINCIA",
    "DISTRITO",
    "NUMERO_RESOLUCION",
    "ESTADO",
    "CATEGORIA_MINSA",
    "RESOLUCION_CATEGORIZACION",
]


def normalize_text(value) -> Optional[str]:
    """
    Normaliza texto para fuzzy matching: mayúsculas, sin tildes, espacios colapsados.
    Devuelve None si el valor es nulo o queda vacío.
    """
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    text = str(value).strip().upper()
    text = "".join(
        ch for ch in unicodedata.normalize("NFKD", text) if not unicodedata.combining(ch)
    )
    text = re.sub(r"\s+", " ", text)
    return text or None


def _clean_cell(value) -> Optional[str]:
    """
    Convierte una celda a string recortado, preservando códigos con ceros a la
    izquierda (CODIGO_SAP, COD_EESS) tal como los entrega pandas. NaN -> None.
    """
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    text = str(value).strip()
    return text or None


def load_petitorio(path: Path) -> pd.DataFrame:
    """
    Lee el Petitorio Farmacológico EsSalud (.xls antiguo, requiere xlrd).

    Estructura: hoja 0 'PETITORIO ESSALUD 2026'; fila 0 vacía, fila 1 título
    institucional, fila 2 headers (header=2), datos desde la fila 3.

    Returns:
        DataFrame con las 8 columnas de PETITORIO_COLUMNS más
        DENOMINACION_DCI_NORM (clave del fuzzy match contra descripciones SEACE).
    """
    logger.info(f"Cargando Petitorio desde: {path}")
    df = pd.read_excel(path, sheet_name=0, header=2, engine="xlrd")

    # Quedarse solo con las primeras 8 columnas (resto son "Unnamed" vacías).
    df = df.iloc[:, : len(PETITORIO_COLUMNS)].copy()
    df.columns = PETITORIO_COLUMNS

    for col in df.columns:
        df[col] = df[col].map(_clean_cell)

    # Descartar filas sin denominación: notas, observaciones al pie, filas vacías.
    df = df[df["DENOMINACION_DCI"].notna()].copy()

    df["DENOMINACION_DCI_NORM"] = df["DENOMINACION_DCI"].map(normalize_text)
    df = df.reset_index(drop=True)

    logger.info(f"Petitorio cargado: {len(df)} medicamentos.")
    return df


def load_establecimientos(path: Path) -> pd.DataFrame:
    """
    Lee la relación de establecimientos por red asistencial (.xlsx, requiere openpyxl).

    Estructura: hoja 'CentrosFuncional'; encabezado institucional en las primeras
    filas, headers reales en el índice 11 (header=11), datos desde el índice 12.

    Returns:
        DataFrame con las 14 columnas de ESTABLECIMIENTOS_COLUMNS más RED_NORM y
        CENTRO_NORM (claves del fuzzy match para resolver la Red Asistencial).
    """
    logger.info(f"Cargando Establecimientos desde: {path}")
    df = pd.read_excel(path, sheet_name="CentrosFuncional", header=11, engine="openpyxl")

    df = df.iloc[:, : len(ESTABLECIMIENTOS_COLUMNS)].copy()
    df.columns = ESTABLECIMIENTOS_COLUMNS

    for col in df.columns:
        df[col] = df[col].map(_clean_cell)

    # Descartar filas separadoras/subtítulos sin código de establecimiento.
    df = df[df["COD_EESS"].notna()].copy()

    df["RED_NORM"] = df["RED"].map(normalize_text)
    df["CENTRO_NORM"] = df["CENTROS_ASISTENCIALES"].map(normalize_text)
    df = df.reset_index(drop=True)

    logger.info(
        f"Establecimientos cargado: {len(df)} centros | {df['RED'].nunique()} redes."
    )
    return df
