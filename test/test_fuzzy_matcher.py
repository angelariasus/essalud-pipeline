"""Tests de la lógica escalar de fuzzy matching (sin Spark)."""
from app.utils.fuzzy_matcher import (
    extract_red_asistencial,
    match_medicamento,
    match_red_asistencial,
)

PETITORIO_CHOICES = ["PARACETAMOL", "AMOXICILINA", "IBUPROFENO", "ADALIMUMAB"]
RED_CHOICES = [
    "RED ASISTENCIAL MOQUEGUA",
    "RED ASISTENCIAL LIMA",
    "RED PRESTACIONAL ALMENARA",
]


def test_match_medicamento_exacto():
    res = match_medicamento("PARACETAMOL 500 MG TABLETA", PETITORIO_CHOICES)
    assert res["dci"] == "PARACETAMOL"
    assert res["metodo"] in ("EXACTO", "FUZZY")
    assert res["score"] >= 70


def test_match_medicamento_historico():
    res = match_medicamento("ALGO FUERA DEL PETITORIO NACIONAL", PETITORIO_CHOICES)
    assert res["metodo"] == "HISTORICO"
    assert res["dci"] is None


def test_match_medicamento_dudoso_y_vacio():
    assert match_medicamento("XJKQ ZZ 999 RUIDO", PETITORIO_CHOICES)["metodo"] == "DUDOSO"
    assert match_medicamento("", PETITORIO_CHOICES)["metodo"] == "DUDOSO"
    assert match_medicamento(None, PETITORIO_CHOICES)["metodo"] == "DUDOSO"


def test_extract_red_por_codigo():
    res = extract_red_asistencial("SIE-1-2024-ESSALUD/RAMOQ-1", [])
    assert res["candidato"] == "RED ASISTENCIAL MOQUEGUA"
    assert res["metodo"] == "CODIGO"


def test_extract_red_por_nombre_texto():
    res = extract_red_asistencial(
        "PROCESO X", ["ADQUISICION PARA LA RED ASISTENCIAL MOQUEGUA 2024"]
    )
    assert res["candidato"] is not None
    assert "MOQUEGUA" in res["candidato"]
    assert res["metodo"] == "NOMBRE_TEXTO"


def test_extract_red_sin_red():
    res = extract_red_asistencial("CD-1-2024-ESSALUD", [])
    assert res["candidato"] is None
    assert res["metodo"] == "SIN_RED"


def test_match_red_asistencial():
    res = match_red_asistencial("RED ASISTENCIAL MOQUEGUA", RED_CHOICES)
    assert res["red"] == "RED ASISTENCIAL MOQUEGUA"
    assert res["score"] >= 80
    assert match_red_asistencial(None, RED_CHOICES)["red"] is None
