"""Tests de integración de la capa Silver/Gold con Spark (datos sintéticos)."""
import json

import pandas as pd
import pytest

from conftest import requires_spark

pytestmark = requires_spark


# ── Records OCDS sintéticos ──────────────────────────────────────────────────
REC_CON_CONTRATO = {
    "ocid": "t-1",
    "compiledRelease": {
        "tender": {
            "title": "SIE-1-2024-ESSALUD/RAMOQ-1", "description": "COMPRA",
            "procurementMethod": "open", "procurementMethodDetails": "Subasta Inversa Electronica",
            "tenderPeriod": {"startDate": "2024-03-01T00:00:00-05:00"},
            "value": {"amount": 1000.0},
            "items": [{"id": "i1", "description": "PARACETAMOL 500 MG TABLETA",
                       "quantity": 10.0, "unit": {"name": "Unidad"}}],
        },
        "awards": [{"id": "a1", "date": "2024-04-01T00:00:00-05:00", "status": "active",
                    "value": {"amount": 900.0},
                    "suppliers": [{"id": "PE-RUC-20123456789", "name": "FARMA SAC"}],
                    "items": [{"id": "i1"}]}],
        "contracts": [{"id": "c1", "awardID": "a1", "dateSigned": "2024-05-01T00:00:00-05:00",
                       "value": {"amount": 900.0}, "items": [{"id": "i1"}], "amendments": []}],
        "parties": [{"id": "PE-CONSUCODE-1", "name": "ESSALUD", "roles": ["buyer"],
                     "address": {"region": "MOQUEGUA"},
                     "additionalIdentifiers": [{"id": "20131257750", "scheme": "PE-RUC"}]}],
    },
}

REC_DIRECTA_SIN_CONTRATO = {
    "ocid": "t-2",
    "compiledRelease": {
        "tender": {
            "title": "CD-1-2024-ESSALUD", "description": "COMPRA DIRECTA",
            "procurementMethod": "direct", "procurementMethodDetails": "Contratacion Directa",
            "tenderPeriod": {"startDate": "2024-02-01T00:00:00-05:00"},
            "value": {"amount": 500.0},
            "items": [{"id": "i9", "description": "COMBUSTIBLE DIESEL B5",
                       "quantity": 2.0, "unit": {"name": "Galon"}}],
        },
        # award sin items -> _find_award usa el primero (fallback)
        "awards": [{"id": "a9", "date": "2024-02-15T00:00:00-05:00", "status": "active",
                    "value": {"amount": 480.0},
                    "suppliers": [{"id": "PE-RUC-10456", "name": "GRIFO EIRL"}], "items": []}],
        "contracts": [],
        "parties": [{"id": "PE-RUC-20131257750", "name": "ESSALUD", "roles": ["buyer"],
                     "address": {"region": "LIMA"}}],
    },
}

REC_SIN_ITEMS = {
    "ocid": "t-3",
    "compiledRelease": {"tender": {"title": "X", "items": []},
                        "awards": [], "contracts": [], "parties": []},
}


@pytest.fixture
def bronze_dir(tmp_path):
    """Escribe records sintéticos (incluyendo uno corrupto) en un directorio tmp."""
    for rec in (REC_CON_CONTRATO, REC_DIRECTA_SIN_CONTRATO, REC_SIN_ITEMS):
        (tmp_path / f"{rec['ocid']}.json").write_text(json.dumps(rec), encoding="utf-8")
    (tmp_path / "corrupto.json").write_text("{ esto no es json valido ", encoding="utf-8")
    return str(tmp_path).replace("\\", "/")


@pytest.fixture
def petitorio_df():
    cols = {
        "N_ITEM": ["1"], "CODIGO_SAP": ["SAP001"], "DENOMINACION_DCI": ["Paracetamol"],
        "ESPECIFICACIONES_TECNICAS": ["TAB 500MG"], "UNIDAD_MANEJO": ["TAB"],
        "RESTRICCION_USO": [None], "ESPECIALIDAD_AUTORIZADA": [None], "INDICACIONES": [None],
        "DENOMINACION_DCI_NORM": ["PARACETAMOL"],
    }
    return pd.DataFrame(cols)


@pytest.fixture
def establecimientos_df():
    return pd.DataFrame({
        "RED": ["RED ASISTENCIAL MOQUEGUA"],
        "RED_NORM": ["RED ASISTENCIAL MOQUEGUA"],
        "DPTO": ["MOQUEGUA"],
    })


# ── Flattener ────────────────────────────────────────────────────────────────
def test_flatten_grano_y_corrupcion(spark, bronze_dir):
    from app.services.ocds_flattener import read_bronze, split_corrupt, flatten_dataframe

    raw = read_bronze(spark, [bronze_dir]).cache()
    valid, corrupt = split_corrupt(raw)
    assert corrupt.count() == 1  # el archivo corrupto se captura, no aborta

    flat = flatten_dataframe(valid)
    rows = {r["ocid"]: r for r in flat.collect()}
    # t-3 (sin items) no produce filas; t-1 y t-2 sí.
    assert set(rows) == {"t-1", "t-2"}


def test_flatten_cascada_award_contract(spark, bronze_dir):
    from app.services.ocds_flattener import flatten_paths

    rows = {r["ocid"]: r for r in flatten_paths(spark, [bronze_dir]).collect()}

    r1 = rows["t-1"]
    assert r1["ruc_proveedor"] == "20123456789"
    assert r1["monto_adjudicado"] == 900.0
    assert r1["monto_contratado"] == 900.0       # contrato resuelto por awardID
    assert r1["es_contratacion_directa"] is False
    assert r1["tiene_adenda"] is False
    assert r1["red_candidato"] == "RED ASISTENCIAL MOQUEGUA"  # código RAMOQ
    assert str(r1["fecha_suscripcion"]) == "2024-05-01"
    assert r1["anio_fiscal"] == 2024

    r2 = rows["t-2"]
    assert r2["es_contratacion_directa"] is True
    assert r2["monto_adjudicado"] == 480.0       # award fallback (sin items)
    assert r2["monto_contratado"] is None        # sin contrato
    assert r2["ruc_proveedor"] == "10456"
    assert r2["red_candidato"] is None           # SIN_RED


# ── Dim resolver ─────────────────────────────────────────────────────────────
def test_resolve_all_fk_integridad(spark, bronze_dir, petitorio_df, establecimientos_df):
    from app.services.ocds_flattener import flatten_paths
    from app.services import dim_resolver as dr

    flat = flatten_paths(spark, [bronze_dir])
    dims = dr.resolve_all(flat, petitorio_df, establecimientos_df)

    prov = dims["dim_proveedor"].toPandas()
    med = dims["dim_medicamento"].toPandas()
    ent = dims["dim_entidad"].toPandas()
    fact = dims["fact"].toPandas()

    assert len(prov) == 2                         # 20123456789, 10456
    assert len(med) == 1                          # PARACETAMOL (COMBUSTIBLE es DUDOSO)
    assert med.iloc[0]["Metodo_Clasificacion"] in ("EXACTO", "FUZZY")
    assert len(ent) == 1                          # RED ASISTENCIAL MOQUEGUA
    assert int(ent.iloc[0]["FK_Ubigeo"]) == dr.DEPARTAMENTO_SK["MOQUEGUA"]
    assert len(fact) == 2

    # Integridad referencial de la Fact.
    med_sks = set(med["SK_Medicamento"]) | {-1, -2}
    prov_sks = set(prov["SK_Proveedor"]) | {-1}
    ent_sks = set(ent["SK_Entidad"]) | {-1}
    assert set(fact["FK_Medicamento"]) <= med_sks
    assert set(fact["FK_Proveedor"]) <= prov_sks
    assert set(fact["FK_Entidad"]) <= ent_sks
    assert set(fact["FK_Ubigeo_Item"]) <= set(range(1, 26)) | {-1}

    # La fila de contratación directa: flag y tipo de proceso correctos.
    cd = fact[fact["Flag_Contratacion_Directa"] == 1]
    assert len(cd) == 1
    assert int(cd.iloc[0]["FK_Tipo_Proceso"]) == dr.SK_TIPO_PROCESO_CD
    assert int(cd.iloc[0]["FK_Entidad"]) == dr.SK_SENTINEL  # t-2 sin red


# ── Pandas UDF: degradación segura ───────────────────────────────────────────
def test_fuzzy_udf_graceful_degradation(spark):
    from pyspark.sql import functions as F
    from app.utils.fuzzy_matcher import make_match_medicamento_udf

    # Broadcast inválido (int, no lista) -> rapidfuzz lanza; el UDF captura por elemento.
    bad_udf = make_match_medicamento_udf(spark.sparkContext.broadcast(12345))
    df = spark.createDataFrame([("PARACETAMOL",), ("AMOXICILINA",)], ["desc"]).repartition(1)
    metodos = [r["metodo"] for r in df.withColumn("m", bad_udf(F.col("desc"))).select("m.metodo").collect()]
    assert all(m and m.startswith("ERROR") for m in metodos)  # job no aborta
