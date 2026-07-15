# CHANGELOG — EsSalud OCDS Data Framework

Registro completo de todos los cambios realizados en el proyecto, desde la configuración inicial hasta la puesta en producción del pipeline completo (Bronce → Silver → Gold).

---

## Índice

1. [Estructura final del proyecto](#1-estructura-final-del-proyecto)
2. [Archivos creados](#2-archivos-creados)
3. [Archivos modificados](#3-archivos-modificados)
4. [Archivos eliminados / renombrados](#4-archivos-eliminados--renombrados)
5. [Bugs corregidos (Fase 5)](#5-bugs-corregidos-fase-5)
6. [Cómo ejecutar el pipeline completo](#6-cómo-ejecutar-el-pipeline-completo)
7. [Actualización 2026-05-28 — Granularidad nivel Red, idempotencia del DDL y recarga](#7-actualización-2026-05-28--granularidad-nivel-red-idempotencia-del-ddl-y-recarga)
8. [Actualización 2026-05-28 — Migración a PySpark: Silver/Gold distribuido](#8-actualización-2026-05-28--migración-a-pyspark-silvergold-distribuido)
9. [Actualización 2026-07-01 — Fase 4: Modelado Predictivo del Lead Time (ML)](#9-actualización-2026-07-01--fase-4-modelado-predictivo-del-lead-time-ml)
10. [Actualización 2026-07-02 — Debug E2E + Fase 6: Alertas Operativas](#10-actualización-2026-07-02--debug-e2e--fase-6-alertas-operativas)

---

## 1. Estructura final del proyecto

```
essalud-pipeline/
├── .env                          ← Conexión DW: SQL Server localhost:11423
├── .gitignore
├── .github/workflows/
│   └── ci.yml                    ← CI: flake8 (estricto + estilo) + pytest test/ -v
├── AGENTS.md                     ← Hechos de alta señal para agentes de IA
├── CHANGELOG.md                  ← ← ESTE ARCHIVO
├── CLAUDE.md                     ← Guía para Claude Code
├── Dockerfile                    ← Extiende apache/airflow:2.9.1, incluye Java 17 (JRE)
├── GEMINI.md                     ← Descripción del proyecto para Gemini
├── PLAN_MIGRACION_PYSPARK.md     ← Plan de migración a PySpark
├── README.md                     ← README del proyecto
├── docker-compose.yaml           ← Stack completo Airflow (Postgres, Redis, Webserver, Scheduler, Worker, Triggerer, Flower)
├── app/cli.py                       ← CLI entrypoint (targeted / bulk)
├── pytest.ini                    ← Config pytest: pythonpath = .
├── requirements.txt              ← Dependencias runtime
├── requirements-dev.txt          ← Dependencias dev (pytest, flake8, apache-airflow)
│
├── app/
│   ├── audit/
│   │   ├── __init__.py
│   │   └── logger.py             ← Logger dual (consola INFO+, archivo DEBUG+)
│   ├── clients/
│   │   ├── __init__.py
│   │   └── ocds_client.py        ← HTTP client con retry/backoff
│   ├── config/
│   │   ├── __init__.py
│   │   ├── settings.py           ← Config centralizada vía env vars
│   │   └── spark_session.py      ← Singleton SparkSession (PySpark)
│   ├── loaders/
│   │   ├── __init__.py
│   │   ├── dw_loader.py          ← DDL + dim/fact load a SQL Server
│   │   └── master_loader.py      ← Petitorio (996) + Establecimientos (403/35)
│   ├── models/
│   │   ├── __init__.py
│   │   └── data_models.py        ← PaginationData, RecordSummary, CatalogItem
│   ├── pipelines/
│   │   ├── __init__.py
│   │   ├── bronze_layer.py       ← Orchestrador ingesta Bronce
│   │   └── silver_layer.py       ← Orchestrador transformación Silver
│   ├── services/
│   │   ├── __init__.py
│   │   ├── dim_resolver.py       ← Construcción de dimensiones + resolución FK
│   │   ├── extractors.py         ← TargetedExtractor + BulkExtractor
│   │   └── ocds_flattener.py     ← JSON anidado → DataFrame plano + Parquet
│   ├── storage/
│   │   ├── __init__.py
│   │   ├── file_manager.py       ← Persistencia local en disco
│   │   └── r2_manager.py         ← Upload a Cloudflare R2 (S3-compatible)
│   └── utils/
│       ├── __init__.py
│       ├── fuzzy_matcher.py
│       └── helpers.py            ← extract_zip_in_place()
│
├── dags/
│   ├── ocds_dag.py               ← 2 DAGs: targeted (weekly) + bulk (monthly)
│   └── silver_dag.py             ← 1 DAG: silver_pipeline (monthly, trigger desde targeted)
│
├── sql/
│   ├── EsSalud_StarSchema_DDL.sql  ← DDL completo: 7 tablas + 3 vistas + validación post-carga
│   └── EsSalud_Staging_DDL.sql     ← DDL staging + oro.usp_Load_From_Staging (carga atómica)
│
├── data/
│   ├── bronze/
│   │   ├── bulk_files/
│   │   └── records/
│   │       └── 20131257750/
│   │           └── 2024/           ← 100 records (--year 2024 --limit 100)
│   ├── silver/
│   │   ├── staging/
│   │   │   └── ocds_flat_2024.parquet  ← 155 filas-ítem (formato anterior)
│   │   └── staging_flat/               ← Parquet particionado por anio_fiscal (PySpark)
│   └── audit/
│       └── executions/
│           └── ocds_extraction.log
│
├── reference/
│   ├── Petitorio-Publicar-hasta-Res-N_-063-2026.xls     ← 996 medicamentos
│   └── 5992483-*.xlsx                                    ← 403 centros / 35 redes
│
├── test/
│   ├── conftest.py                  ← Fixtures Spark + marcador requires_spark
│   ├── test_api.py                  ← 6 tests con aserciones reales
│   ├── test_bronze_layer.py         ← Mocks, atributos correctos
│   ├── test_dag_integrity.py        ← pytest.importorskip
│   ├── test_dw_loader.py            ← 8 tests dw_loader con mocks
│   ├── test_fuzzy_matcher.py        ← 6 tests fuzzy matching escalar
│   └── test_silver_spark.py         ← 5 tests integración Silver/Gold con Spark
│
├── config/                          ← (mount target Airflow)
├── logs/                            ← logs Airflow (scheduler, dag_processor_manager)
├── plugins/                         ← (mount target Airflow plugins)
└── tmp/                             ← scripts temporales de desarrollo
```

---

## 2. Archivos creados

### 2.1 `.env` — Configuración de entorno

**Creado para**: Proveer la cadena de conexión al Data Warehouse (SQL Server en Docker).

Contenido:
```ini
OCDS_USE_R2=

OCDS_R2_ACCOUNT_ID=
OCDS_R2_ACCESS_KEY=
OCDS_R2_SECRET_KEY=
OCDS_R2_BUCKET_NAME=

OCDS_SILVER_DIR=data/silver
OCDS_EXTRA_DATA_DIR=reference
OCDS_DW_CONN_STRING=mssql+pyodbc://sa:123ABC%40%40@localhost:11423/DW_EsSalud_Adquisiciones?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes
```

### 2.2 `pytest.ini` — Configuración de pytest

**Creado para**: Asegurar que los tests puedan importar módulos desde la raíz del proyecto.

```ini
[pytest]
pythonpath = .
testpaths = test
```

### 2.3 `agentes.md` — Hechos de alta señal (sobrescribe AGENTS.md previo, ahora AGENTS.md)

**Creado para**: Documentar hechos no obvios sobre la arquitectura que un agente de IA podría pasar por alto. Incluye:
- Estado de pruebas y comandos de lint
- Peculiaridades de Airflow (Jinja macros, montura `/opt/airflow/bi`, credenciales)
- Hechos arquitectónicos (R2 lazy import, sin segunda llamada HTTP, filtro farmacéutico, todo el filtrado es client-side)
- Datos cargados en SQL Server (conteos, KPIs)
- Fixes aplicados en Fase 3 (DDL WHILE→CTE, AUTOCOMMIT, method=None, vw_Matriz_Riesgo_HHI)

### 2.4 `data/bronze/bulk_files/` — Directorio para descargas masivas

**Creado para**: Albergar archivos ZIP de descargas bulk del SEACE, según el diseño de datos documentado.

---

## 3. Archivos modificados

### 3.1 `app/cli.py` — CLI entrypoint

**Antes**: Contenía trailing whitespace en múltiples líneas (argumentos de argparse).

**Después**: Límpiado de trailing whitespace. Sin cambios funcionales.

### 3.2 Estructura de datos Bronze

**Antes**: 5 archivos JSON sueltos en `data/bronze/records/20131257750/` sin subdirectorio de año:
- `ocds-dgv273-seacev3-99976.json` (año 2015)
- `ocds-dgv273-seacev3-99986.json` (año 2015)
- `ocds-dgv273-seacev3-99990.json` (año 2015)
- `ocds-dgv273-seacev3-2024-2543-787.json` (año 2024, duplicado)
- `ocds-dgv273-seacev3-999694.json` (año 2024, duplicado)

**Después**:
- Creado `data/bronze/records/20131257750/2015/` con los 3 registros de 2015
- Movidos los 2 registros de 2024 a `data/bronze/records/20131257750/2024/` (ya existían, verificados por hash SHA256)
- Eliminados duplicados en raíz

### 3.3 `app/pipelines/bronze_layer.py` (línea 49)

**Antes**:
```python
path_suffix = f"records/{ruc}/{year}" if year else f"records/{ruc}"
```

**Después**:
```python
path_suffix = f"records/{ruc}/{year}" if year else f"records/{ruc}/unknown"
```

**Bug corregido #5**: Cuando se ejecuta `main.py targeted` sin `--year`, los archivos ahora se guardan en `records/{ruc}/unknown/` en lugar de en la raíz de `records/{ruc}/`. Esto evita que archivos huérfanos sin año ensucien el directorio principal.

### 3.4 `app/pipelines/silver_layer.py` (líneas 74-77)

**Antes**:
```python
load_all(engine, dims, dim_ubigeo_dist, self.ddl_path)
engine.dispose()
logger.info("=== Pipeline Silver completado exitosamente ===")
```

**Después**:
```python
try:
    load_all(engine, dims, dim_ubigeo_dist, self.ddl_path)
finally:
    engine.dispose()
logger.info("=== Pipeline Silver completado exitosamente ===")
```

**Bug corregido #4**: Si `load_all()` lanza una excepción (ej. error de conexión SQL Server, FK violation, timeout), `engine.dispose()` nunca se ejecutaba, dejando el pool de conexiones SQLAlchemy en un estado inconsistent. Ahora `engine.dispose()` se garantiza vía `finally`.

### 3.5 `app/loaders/dw_loader.py` (líneas 79-83)

**Antes**:
```python
conn = engine.connect().execution_options(isolation_level="AUTOCOMMIT")
for i, batch in enumerate(batches, start=1):
    logger.debug(f"Ejecutando lote DDL {i}/{len(batches)}...")
    conn.execute(text(batch))
conn.close()
```

**Después**:
```python
with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
    for i, batch in enumerate(batches, start=1):
        logger.debug(f"Ejecutando lote DDL {i}/{len(batches)}...")
        conn.execute(text(batch))
```

**Bug corregido #3**: La conexión `conn` se abría con `engine.connect()` pero si `conn.execute()` lanzaba una excepción (ej. `ProgrammingError` por una tabla que no se puede dropear por FK), `conn.close()` en la línea 83 nunca se ejecutaba, causando una fuga de conexión. El `with` statement garantiza `conn.close()` incluso en caso de excepción.

### 3.6 `app/models/data_models.py` — Imports no utilizados

**Antes**:
```python
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any
```

**Después**:
```python
from dataclasses import dataclass
from typing import Optional
```

**Motivo**: `field`, `List`, `Dict`, `Any` no se usaban en ninguna definición de dataclass. Limpieza de lint F401.

### 3.7 `app/storage/file_manager.py` — Import no utilizado

**Antes**:
```python
from typing import Dict, Any, Optional, Iterator
```

**Después**:
```python
from typing import Dict, Any, Iterator
```

**Motivo**: `Optional` no se usaba en `FileManager`. Limpieza de lint F401.

### 3.8 `app/storage/r2_manager.py` — Imports no utilizados

**Antes**:
```python
import json
import os
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
from typing import Dict, Any, Iterator
```

**Después**:
```python
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
```

**Motivo**: `json`, `os`, `Dict`, `Any`, `Iterator` no se usaban en `R2Manager`. Limpieza de lint F401.

### 3.9 `dags/ocds_dag.py` — Parámetros dinámicos con Jinja + TriggerDagRunOperator

**Antes**: Años y meses hardcodeados:
```python
bash_command='cd /opt/airflow/bi && python app/cli.py targeted --year 2024 --limit 100',
bash_command='cd /opt/airflow/bi && python app/cli.py bulk --source SEACE --type JSON --year 2023 --month 11',
```

**Después**: Macros Jinja + TriggerDagRunOperator de Targeted → Silver:
```python
bash_command=(
    "cd /opt/airflow/bi && python app/cli.py targeted"
    " --year {{ execution_date.year }}"
    " --limit 100"
),

# ... además, TriggerDagRunOperator:
run_targeted >> trigger_silver
```

**Cambios**:
- `--year 2024` → `--year {{ execution_date.year }}`
- `--year 2023 --month 11` → `--year {{ execution_date.year }} --month {{ execution_date.month }}`
- Agregado `TriggerDagRunOperator` para lanzar `ocds_silver_pipeline` automáticamente tras la ingesta Targeted.

### 3.10 `dags/silver_dag.py` — De BashOperator a PythonOperator con XCom

**Antes**: `run_silver` era un `BashOperator` que llamaba `SilverPipeline().run()` sin argumentos. La tarea `resolve_years` calculaba los años pero nunca los consumía (código muerto).

**Después**: `run_silver` es un `PythonOperator` que extrae los años resueltos vía XCom:
```python
def _resolve_years(**context):
    dag_run = context.get("dag_run")
    if dag_run and dag_run.conf and "year" in dag_run.conf:
        return [dag_run.conf["year"]]
    return None  # None = procesar todos los años

def _run_silver(**context):
    ti = context["ti"]
    years = ti.xcom_pull(task_ids="resolve_years")
    from app.pipelines.silver_layer import SilverPipeline
    SilverPipeline().run(years=years)
```

**Bug corregido #1**: El año resuelto por `dag_run.conf` ahora se pasa efectivamente a `SilverPipeline.run()`. Si se ejecuta mensualmente sin conf, `years=None` y el pipeline procesa `[2022, 2023, 2024, 2025]` (default). Si se trigger desde `ocds_targeted_ingestion` con `conf={"year": 2024}`, procesa solo 2024.

**Nota**: Se eliminó la importación de `BashOperator` (ya no necesaria en este DAG).

### 3.11 `sql/EsSalud_StarSchema_DDL (3).sql` → `EsSalud_StarSchema_DDL.sql`

**Antes**: Nombre con espacio y versión parentética: `EsSalud_StarSchema_DDL (3).sql`

**Después**: Nombre limpio: `EsSalud_StarSchema_DDL.sql`

**Referencia**: `app/pipelines/silver_layer.py:36` actualizada para usar el nuevo nombre.

### 3.12 `sql/EsSalud_StarSchema_DDL.sql` — WHILE loop → CTE recursivo + Sección 9

**Antes (Dim_Tiempo)**:
```sql
DECLARE @d DATE = '2022-01-01';
WHILE @d <= '2025-12-31'
BEGIN
    INSERT INTO oro.Dim_Tiempo VALUES (...);
    SET @d = DATEADD(DAY,1,@d);
END;
```

**Después**:
```sql
WITH Fechas AS (
    SELECT CAST('2022-01-01' AS DATE) AS d
    UNION ALL
    SELECT DATEADD(DAY, 1, d)
    FROM Fechas
    WHERE d < '2025-12-31'
)
INSERT INTO oro.Dim_Tiempo
SELECT ... FROM Fechas OPTION (MAXRECURSION 0);
```

**Bug corregido en sesión previa**: El WHILE loop tomaba ~30s en ejecutarse y SQL Server lo abortaba por timeout después de solo 107 filas (de las 1461 esperadas). El CTE recursivo ejecuta la misma operación en <1s.

**Además**: Se agregó la **Sección 9 — Validación Post-Carga**, que incluye:
- Row counts de todas las tablas
- FK integrity checks (9 constraints, 0 orphans esperados)
- KPIs de negocio (Total Referencial, Adjudicado, Diferencia)
- Validación de vistas analíticas (row counts de `vw_Gasto_Por_Proceso_Y_Red`, `vw_Lead_Time_Por_Proveedor`, `vw_Matriz_Riesgo_HHI`)

### 3.13 `test/test_bronze_layer.py` — Reescrito con mocks

**Antes** (15 líneas, roto):
```python
def test_bronze_pipeline_init():
    pipeline = BronzePipeline()
    assert pipeline.extractor is not None
    assert pipeline.r2_manager is not None

def test_extractors_init():
    targeted = TargetedExtractor()
    bulk = BulkExtractor()
    assert targeted is not None
    assert bulk is not None
```

**Problemas**:
- `pipeline.extractor` no existe (el atributo real es `targeted_extractor`)
- `pipeline.r2_manager` no existe (el atributo real es `cloud_storage`)
- `TargetedExtractor()` y `BulkExtractor()` requieren `client: OCDSClient`
- `BronzePipeline()` internamente crea un `OCDSClient()` que hace HTTP real

**Después** (31 líneas):
```python
from unittest.mock import patch
import pytest
from app.clients.ocds_client import OCDSClient
from app.services.extractors import TargetedExtractor, BulkExtractor

@pytest.fixture
def mock_client():
    with patch("app.pipelines.bronze_layer.OCDSClient") as mock_cls:
        mock_cls.return_value = OCDSClient.__new__(OCDSClient)
        yield mock_cls

def test_bronze_pipeline_init(mock_client):
    from app.pipelines.bronze_layer import BronzePipeline
    pipeline = BronzePipeline()
    assert pipeline.targeted_extractor is not None
    assert pipeline.bulk_extractor is not None
    assert pipeline.cloud_storage is None

def test_extractors_init_with_client():
    client = OCDSClient()
    targeted = TargetedExtractor(client)
    bulk = BulkExtractor(client)
    assert targeted is not None
    assert bulk is not None

def test_extractors_have_pharma_filter():
    assert TargetedExtractor.PHARMA_CATEGORIES == {"goods"}
```

**Cambios**:
- `OCDSClient` parcheado con `unittest.mock.patch` para evitar llamadas HTTP reales
- Atributos corregidos: `.extractor` → `.targeted_extractor`, `.r2_manager` → `.cloud_storage`
- Extractors reciben `client` como parámetro
- Nuevo test: `test_extractors_have_pharma_filter` verifica la constante `PHARMA_CATEGORIES`

### 3.14 `test/test_api.py` — Reescrito con pytest assertions

**Antes** (17 líneas, script manual sin assertions):
```python
def test_endpoint(endpoint, params):
    r = requests.get(...)
    data = r.json()
    records = data.get('records', [])
    if not records:
        print(f'{params} -> No records returned.')
        return
    buyer_name = records[0].get('compiledRelease', {}).get('buyer', {}).get('name')
    print(f'{params} -> First Record Buyer Name: {buyer_name}')

test_endpoint('records', {'buyer.id': '20131257750', 'size': 1})
test_endpoint(...)  # 4 llamadas sin assertions
```

**Después** (81 líneas, 6 tests con assertions):

| Test | Descripción |
|---|---|
| `test_api_responds` | `GET /records` debe responder 200 |
| `test_api_returns_records` | Debe devolver records con `ocid` y `compiledRelease.tender` |
| `test_first_record_has_buyer` | El primer record debe tener `buyer.name` |
| `test_main_procurement_category_exists` | Cada record debe tener `tender.mainProcurementCategory` |
| `test_essalud_records_have_goods_category` | Records EsSalud → `goods` (skips si no hay EsSalud en lote) |
| `test_links_next_exists` | Debe incluir `links.next` para paginación |

**Bug corregido #2**: `test_essalud_records_have_goods_category()` ahora:
- Skipea con `pytest.skip()` si no encuentra EsSalud en los primeros 50 registros (la API no filtra server-side)
- Itera sobre los registros de EsSalud encontrados y assert que `mainProcurementCategory == "goods"`
- Antes era un false-positive que siempre "pasaba" sin verificar nada

### 3.15 `test/test_dag_integrity.py` — Skip automático si no hay Airflow

**Antes**:
```python
from airflow.models import DagBag
```

**Después**:
```python
import pytest
airflow = pytest.importorskip("airflow", reason="apache-airflow not installed")
from airflow.models import DagBag
```

**Cambio**: Si `apache-airflow` no está instalado, el test se salta automáticamente con mensaje claro, en lugar de fallar con `ModuleNotFoundError`.

**Además**: Se agregó verificación de que al menos un DAG fue encontrado:
```python
assert len(dag_bag.dag_ids) >= 1, "No DAGs found in dags/"
```

---

## 4. Archivos eliminados / renombrados

| Acción | Ruta anterior | Ruta nueva | Motivo |
|---|---|---|---|
| Renombrado | `sql/EsSalud_StarSchema_DDL (3).sql` | `sql/EsSalud_StarSchema_DDL.sql` | Espacio y sufijo `(3)` no estándar |
| Eliminado | `data/bronze/records/20131257750/ocds-dgv273-seacev3-2024-2543-787.json` | — | Duplicado exacto (mismo hash SHA256) del existente en `2024/` |
| Eliminado | `data/bronze/records/20131257750/ocds-dgv273-seacev3-999694.json` | — | Duplicado exacto (mismo hash SHA256) del existente en `2024/` |
| Movido | `data/bronze/records/20131257750/ocds-dgv273-seacev3-99976.json` | `data/bronze/records/20131257750/2015/` | Organizado por año |
| Movido | `data/bronze/records/20131257750/ocds-dgv273-seacev3-99986.json` | `data/bronze/records/20131257750/2015/` | Organizado por año |
| Movido | `data/bronze/records/20131257750/ocds-dgv273-seacev3-99990.json` | `data/bronze/records/20131257750/2015/` | Organizado por año |

---

## 5. Bugs corregidos (Fase 5)

### Bug #1 — `silver_dag.py`: Año resuelto nunca consumido

**Severidad**: ALTA
**Archivo**: `dags/silver_dag.py:37-49`
**Síntoma**: `_resolve_years()` retorna el año vía XCom, pero `run_silver` es un `BashOperator` que ejecuta `SilverPipeline().run()` sin argumentos. El año resuelto se pierde.
**Fix**: Convertir `run_silver` a `PythonOperator` que extrae XCom con `ti.xcom_pull("resolve_years")` y llama `SilverPipeline().run(years=years)`.

### Bug #2 — `test_api.py`: Test sin assertion (false positive)

**Severidad**: ALTA
**Archivo**: `test/test_api.py:68-71`
**Síntoma**: `test_essalud_records_have_goods_category` construía una lista de OCIDs y hacía `print()`, pero nunca ejecutaba un `assert`. La prueba SIEMPRE pasaba aunque encontrara 0 registros.
**Fix**: Skip con `pytest.skip()` si no hay EsSalud en el lote; si hay, assert estricto de `mainProcurementCategory == "goods"`.

### Bug #3 — `dw_loader.py`: Connection leak en execute_ddl()

**Severidad**: ALTA
**Archivo**: `app/loaders/dw_loader.py:79-83`
**Síntoma**: Si `conn.execute(text(batch))` lanza excepción (ej. FK conflict), `conn.close()` nunca se ejecuta, fugando la conexión.
**Fix**: Envolver en `with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:`.

### Bug #4 — `silver_layer.py`: engine.dispose() no llamado si falla load_all()

**Severidad**: ALTA
**Archivo**: `app/pipelines/silver_layer.py:74-76`
**Síntoma**: Si `load_all()` lanza excepción, `engine.dispose()` en línea 76 nunca se alcanza, dejando el pool de conexiones abierto.
**Fix**: Envolver en `try/finally`.

### Bug #5 — `bronze_layer.py`: Path mismatch cuando year=None

**Severidad**: ALTA
**Archivo**: `app/pipelines/bronze_layer.py:49`
**Síntoma**: `run_targeted_ingestion()` guarda en `records/{ruc}/` (sin year), pero `flatten_year()` busca en `records/{ruc}/{year}/`. Datos extraídos sin `--year` quedan huérfanos e invisibles para Silver.
**Fix**: Cambiar path a `records/{ruc}/unknown/` cuando year es None, manteniendo consistencia en estructura de directorios.

---

## 6. Cómo ejecutar el pipeline completo

### Prerrequisitos

```bash
# 1. Python 3.12+
python --version

# 2. Instalar dependencias
pip install -r requirements.txt
pip install -r requirements-dev.txt   # (opcional, para tests + Airflow)

# 3. SQL Server en Docker (puerto 11423)
docker run -d --name essalud-sqlserver \
  -e "ACCEPT_EULA=Y" \
  -e "MSSQL_SA_PASSWORD=123ABC@@" \
  -p 11423:1433 \
  mcr.microsoft.com/mssql/server:2022-latest
```

### 6.1 Extraer datos — Capa Bronce

```bash
# Extracción targeted por año (EsSalud, RUC 20131257750)
# Sin --limit, descarga TODOS los registros del año (puede tomar varios minutos)
python app/cli.py targeted --year 2022 --limit 0
python app/cli.py targeted --year 2023 --limit 0
python app/cli.py targeted --year 2024 --limit 0
python app/cli.py targeted --year 2025 --limit 0

# Para pruebas rápidas, limitar a N registros:
python app/cli.py targeted --year 2024 --limit 10
```

Los archivos se guardan en `data/bronze/records/20131257750/<year>/<ocid>.json`.

### 6.2 Crear base de datos en SQL Server

```bash
# Conectar al contenedor y crear la base de datos
docker exec -it essalud-sqlserver /opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U sa -P "123ABC@@" -C \
  -Q "IF NOT EXISTS (SELECT name FROM sys.databases WHERE name='DW_EsSalud_Adquisiciones') CREATE DATABASE DW_EsSalud_Adquisiciones"
```

### 6.3 Transformar y cargar — Capa Silver

```bash
# Opción A: Pipeline completo (todos los años disponibles)
python -c "
from app.pipelines.silver_layer import SilverPipeline
SilverPipeline().run()
"

# Opción B: Solo un año específico
python -c "
from app.pipelines.silver_layer import SilverPipeline
SilverPipeline().run(years=[2024])
"
```

Esto ejecuta:
1. **Carga de maestros**: Petitorio (996 medicamentos) + Establecimientos (403 centros, 35 redes)
2. **Aplanado**: Lee JSONs Bronze → DataFrame plano (29 filas-ítem por año) → Parquet staging
3. **Construcción de dimensiones**: Dim_Tiempo, Dim_Ubigeo, Dim_Entidad_Compradora, Dim_Medicamento, Dim_Proveedor, Dim_Tipo_Proceso + resolución FK
4. **Carga a SQL Server**: DDL (7 tablas + 3 vistas + sentinel rows) → INSERT de dimensiones → INSERT de Fact table

### 6.4 Verificar integridad post-carga

```bash
# Los queries de validación están en la Sección 9 del DDL
# o puedes ejecutar directamente desde Python:
python -c "
from app.loaders.dw_loader import create_sqlalchemy_engine
from sqlalchemy import text

engine = create_sqlalchemy_engine()
with engine.connect() as conn:
    # Row counts
    for t in ['Dim_Tiempo','Dim_Ubigeo','Dim_Entidad_Compradora',
              'Dim_Medicamento','Dim_Proveedor','Dim_Tipo_Proceso',
              'Fact_Ordenes_Y_Contratos']:
        r = conn.execute(text(f'SELECT COUNT(*) FROM oro.{t}'))
        print(f'{t}: {r.scalar()} rows')

    # FK integrity
    fks = ['FK_Tiempo_Convocatoria','FK_Tiempo_Buena_Pro','FK_Tiempo_Suscripcion',
           'FK_Tiempo_Emision_OC','FK_Entidad','FK_Medicamento','FK_Proveedor',
           'FK_Tipo_Proceso','FK_Ubigeo_Item']
    dims = ['Dim_Tiempo']*4 + ['Dim_Entidad_Compradora','Dim_Medicamento',
            'Dim_Proveedor','Dim_Tipo_Proceso','Dim_Ubigeo']
    pks  = ['SK_Tiempo']*4 + ['SK_Entidad','SK_Medicamento',
            'SK_Proveedor','SK_Tipo_Proceso','SK_Ubigeo']
    for fk, dim, pk in zip(fks, dims, pks):
        r = conn.execute(text(
            f'SELECT COUNT(*) FROM oro.Fact_Ordenes_Y_Contratos f '
            f'LEFT JOIN oro.{dim} d ON f.{fk}=d.{pk} WHERE d.{pk} IS NULL'
        ))
        print(f'{fk}: {r.scalar()} orphans')

    # KPIs
    r = conn.execute(text(
        'SELECT SUM(Monto_Referencial_Soles), SUM(Monto_Adjudicado_Soles) '
        'FROM oro.Fact_Ordenes_Y_Contratos'
    ))
    ref, adj = r.fetchone()
    print(f'Referencial: S/ {ref:,.2f}')
    print(f'Adjudicado:  S/ {adj:,.2f}')
    print(f'Cobertura:   {adj/ref*100:.1f}%' if ref else 'N/A')
"
```

Salida esperada (con la muestra de 100 records de 2024; ver Sección 7):
```
Dim_Tiempo: 1462 rows
Dim_Ubigeo: 26 rows           (25 departamentos + sentinel; sin distritos)
Dim_Entidad_Compradora: 20 rows
Dim_Medicamento: 21 rows      (17 clasificados + 4 sentinels)
Dim_Proveedor: 65 rows
Dim_Tipo_Proceso: 12 rows
Fact_Ordenes_Y_Contratos: 155 rows
FK_Tiempo_Convocatoria: 0 orphans
... (9/9 FKs: 0 orphans)
Referencial: S/ 186,531,024
Adjudicado:  S/ 72,073,089
```

### 6.5 Ejecutar tests

```bash
# Todos los tests
pytest test/ -v

# Solo tests de API (requiere conexión a Internet)
pytest test/test_api.py -v

# Solo tests de Bronze (no requiere API real)
pytest test/test_bronze_layer.py -v

# Solo test DAG integrity (requiere apache-airflow instalado)
pytest test/test_dag_integrity.py -v
```

Resultado esperado:
```
9 passed, 1 skipped
```
El skip corresponde a `test_dag_integrity.py` si `apache-airflow` no está instalado.

### 6.6 Lint

```bash
# Lint estricto (errores de sintaxis y nombres indefinidos) — DEBE dar 0
flake8 app dags app/cli.py --count --select=E9,F63,F7,F82 --show-source --statistics

# Lint de estilo (no bloqueante, salida 0 incluso con warnings)
flake8 app dags app/cli.py --count --exit-zero --max-complexity=10 --max-line-length=127 --statistics
```

### 6.7 Airflow (opcional, para orquestación)

```bash
# Iniciar stack Airflow (requiere Docker Compose)
docker compose up -d

# Acceder a http://localhost:8080 (usuario: airflow / contraseña: airflow)
# DAGs disponibles:
#   ocds_targeted_ingestion  → @weekly, extrae año en curso (límite 100)
#   ocds_bulk_ingestion      → @monthly, descarga bulk mensual
#   ocds_silver_pipeline     → @monthly, transforma Bronze → Silver (trigger desde targeted)
```

---

## 7. Actualización (2026-05-28) — Granularidad nivel Red, idempotencia del DDL y recarga

Esta sesión consolidó el modelo a **granularidad nivel Red Asistencial** (el OCDS API solo identifica la Red, no el establecimiento exacto), corrigió un bug de idempotencia del DDL y recargó el DW con una muestra de 100 registros de 2024.

### 7.1 Corrección — Dim_Ubigeo solo a nivel DEPARTAMENTO

**Motivo**: a nivel Red, tanto `Fact.FK_Ubigeo_Item` como `Dim_Entidad_Compradora.FK_Ubigeo` apuntan a los 25 departamentos precargados por el DDL (SK 1-25). Los ~340 distritos que se cargaban no eran referenciados por ninguna FK (filas sin uso analítico).

**Cambios**:
- `app/services/dim_resolver.py`: eliminados `build_dim_ubigeo_distritos()`, `REGION_MAP` y `SK_UBIGEO_DISTRITO_INICIO`.
- `app/loaders/dw_loader.py`: eliminado `load_dim_ubigeo_distritos()`; `load_all()` simplificado a la firma `(engine, dims, ddl_path)` (sin `dim_ubigeo_dist`).
- `app/pipelines/silver_layer.py`: ya no importa ni llama `build_dim_ubigeo_distritos`.

**Impacto**: `Dim_Ubigeo` pasó de **366 → 26 filas** (25 departamentos + sentinel). Las 3 vistas siguen operativas (agrupan por Red / departamento).

### 7.2 Bug #6 — DDL no idempotente al recargar con datos existentes

**Severidad**: ALTA
**Archivo**: `sql/EsSalud_StarSchema_DDL.sql`
**Síntoma**: al re-ejecutar el DDL sobre un DW ya poblado, `DROP TABLE` de una dimensión fallaba con error 3726 (*"Could not drop object ... referenced by a FOREIGN KEY constraint"*), porque la `Fact` (y `Dim_Entidad`) existentes seguían referenciándola. La primera carga no fallaba porque el DW estaba vacío.
**Fix**: bloque de **LIMPIEZA PREVIA** al inicio del DDL que elimina las tablas en orden inverso de dependencias (`Fact` → `Dim_Entidad` → resto) antes de recrearlas. El pipeline Silver ahora se puede re-ejecutar sin intervención manual.

### 7.3 Recarga con muestra de 100 registros de 2024

Bronze re-extraído con `python app/cli.py targeted --year 2024 --limit 100` (los datos previos de 2015 y la muestra anterior de 2024 se reemplazaron por una base limpia). Conteos del DW antes vs. ahora:

| Tabla | Antes | Ahora |
|---|---|---|
| Dim_Ubigeo | 366 | **26** |
| Dim_Entidad_Compradora | 10 | 20 |
| Dim_Medicamento | 7 | 21 (17 clasificados + 4 sentinels) |
| Dim_Proveedor | 17 | 65 |
| Fact_Ordenes_Y_Contratos | 29 | 155 |
| vw_Matriz_Riesgo_HHI | (vacía) | 12 filas |

FK integrity: **0 orphans** en las 9 FKs. KPIs: Referencial S/ 186.5M · Adjudicado S/ 72M · Lead Time calculado en 65 filas.

### 7.4 Hallazgos de datos (relevantes para el informe)

- **Alcance temporal**: el OCDS API no filtra por año en el servidor. Al escanear 1500 registros del portal, **2022 y 2023 no aparecen** y los EsSalud-goods son ~0.87% del total. Extraer "10/año para 2022-2025" es inviable; la muestra práctica es de 2024. Por procesos plurianuales, su `Anio_Fiscal` (derivado de la fecha de convocatoria) reparte en 2022 (1), 2023 (101) y 2024 (53).
- **Proporción de medicamentos**: solo **~12%** de los goods de EsSalud son medicamentos del Petitorio (18 de 155 filas). El ~88% restante son reactivos, dispositivos, insumos y servicios de suministro. El análisis farmacéutico (HHI) opera sobre esa fracción.

---

## 8. Actualización (2026-05-28) — Migración a PySpark: Silver/Gold distribuido

Esta sesión migró el procesamiento de la capa Silver (aplanamiento, fuzzy matching, resolución de dimensiones) de **pandas a PySpark**, incorporando un pipeline de carga atómica vía staging tables JDBC. También se eliminó `PLAN_IMPLEMENTACION.md` (reemplazado por `PLAN_MIGRACION_PYSPARK.md`).

### 8.1 Nuevo módulo: `app/config/spark_session.py`

Singleton `get_spark_session()` que centraliza la configuración de Spark:

- **Arrow** habilitado con fallback (`spark.sql.execution.arrow.pyspark.fallback.enabled=true`) para Pandas UDFs vectorizados.
- **Adaptive Query Execution** para coalesce automático de particiones.
- `spark.sql.shuffle.partitions` acotado a datasets locales pequeños.
- JARs del driver JDBC de SQL Server vía `spark.jars.packages` o `spark.jars` (configurable por env vars).
- `PYSPARK_PYTHON` fijado al mismo intérprete del driver (evita error "Python not found" en Windows con Microsoft Store alias).
- `spark.sql.ansi.enabled=false` (los datos OCDS sucios coercen a NULL como `pd.errors='coerce'`).
- Timezone `America/Lima`, faulthandler en workers UDF.
- `stop_spark_session()` para limpieza ordenada.

### 8.2 Nuevo módulo: `app/utils/fuzzy_matcher.py`

Motor de fuzzy matching basado en `rapidfuzz` con dos variantes:

**Funciones escalares** (usables también sin Spark):

| Función | Propósito |
|---|---|
| `match_medicamento(desc, petitorio_choices)` | Clasifica descripción SEACE contra Denominaciones DCI del Petitorio. Retorna `{dci, score, metodo}` con 4 niveles: EXACTO (≥90), FUZZY (≥70), DUDOSO (<70), HISTORICO ("FUERA DEL PETITORIO") |
| `extract_red_asistencial(title, descriptions)` | Extrae candidato de Red en cascada: (1) código SEACE en título → `CODIGO_RED_MAP`, (2) nombre explícito "RED ASISTENCIAL/PRESTACIONAL..." en texto, (3) SIN_RED |
| `match_red_asistencial(candidato, red_choices)` | Resuelve candidato contra valores canónicos del maestro de Establecimientos (umbral ≥80) |

**Pandas UDFs vectorizados** (Arrow):

| UDF | Schema retorno |
|---|---|
| `make_match_medicamento_udf(broadcast_choices)` | `(dci: String, score: Double, metodo: String)` |
| `make_match_red_udf(broadcast_choices)` | `(red: String, score: Double)` |

Ambos UDFs envuelven cada elemento en `try/except`: si una fila falla, retorna un sentinel (`ERROR: ...`) en lugar de abortar el Job de Spark (*graceful degradation*). Los maestros se transmiten vía `spark.sparkContext.broadcast()`.

**`CODIGO_RED_MAP`**: 34 entradas que mapean códigos SEACE de 3–6 caracteres (p. ej. `RAMOQ`→`RED ASISTENCIAL MOQUEGUA`, `RPREB`→`RED PRESTACIONAL REBAGLIATI`) a los nombres canónicos del Excel de Establecimientos. Resuelve el Gap 2: los códigos en los títulos OCDS no matchean por similitud de caracteres.

### 8.3 Nuevo DDL: `sql/EsSalud_Staging_DDL.sql`

Esquema de staging para carga atómica a producción:

- Crea el esquema `[stg]` (si no existe).
- Define `oro.usp_Load_From_Staging`: stored procedure que en **una transacción**:
  1. `SET IDENTITY_INSERT ON` → INSERT desde `stg.Dim_*` a `oro.Dim_*` con SKs deterministas ya resueltas por Spark.
  2. INSERT desde `stg.Fact_Ordenes_Y_Contratos` a `oro.Fact_Ordenes_Y_Contratos` (SK_Hecho es IDENTITY automática).
  3. `COMMIT` si todo ok; `ROLLBACK` + `THROW` si falla.
- Spark escribe los DataFrames a `stg.*` por JDBC (`.mode("overwrite")`), luego el pipeline invoca el procedure.

**Flujo**: `DDL producción` → `DDL staging` → `write_staging_jdbc()` (Spark escribe stg.*) → `call_load_procedure()` (ejecuta `usp_Load_From_Staging`). Si Spark falla, producción no se toca. Si el procedure falla, ROLLBACK.

### 8.4 `test/conftest.py` — Fixtures compartidos para Spark

- `requires_spark = pytest.mark.skipif(not SPARK_AVAILABLE, ...)` — omitir tests si falta pyspark/Java.
- `spark` fixture (session scope): llama `get_spark_session("pytest_essalud")`, al final `stop_spark_session()`.

### 8.5 Archivos creados

| Archivo | Propósito |
|---|---|
| `app/config/spark_session.py` | Singleton SparkSession (100 líneas) |
| `app/utils/fuzzy_matcher.py` | Fuzzy matching escalar + Pandas UDFs (260 líneas) |
| `sql/EsSalud_Staging_DDL.sql` | Esquema stg + usp_Load_From_Staging (92 líneas) |
| `PLAN_MIGRACION_PYSPARK.md` | Plan de migración (156 líneas, reemplaza implementación previa) |
| `GEMINI.md` | Descripción del proyecto para Gemini (análogo a CLAUDE.md) |
| `test/conftest.py` | Fixtures compartidos Spark (35 líneas) |
| `test/test_silver_spark.py` | 5 tests de integración Silver/Gold con Spark (175 líneas) |
| `test/test_dw_loader.py` | 8 tests unitarios del dw_loader con mocks (79 líneas) |
| `test/test_fuzzy_matcher.py` | 6 tests de lógica escalar fuzzy matching (60 líneas) |
| `data/silver/staging_flat/` | Parquet staging con Hive partitioning por `anio_fiscal` |

### 8.6 Archivos modificados

#### `app/services/ocds_flattener.py` — De pandas a PySpark

**Antes**: `flatten_record()` procesaba un dict JSON con pandas, iterando manualmente items→awards→contracts.

**Después**: Tres funciones Spark:
- `read_bronze(spark, paths)` — Lee directorios de JSONs con `PERMISSIVE` mode, capturando corruptos en `_corrupt_record`.
- `split_corrupt(df)` — Separa filas válidas vs corruptas (auditoría sin abortar).
- `flatten_paths(spark, paths)` — Transformación completa que **explota items** y resuelve award/contract por `awardID` en cascada (Spark SQL joins). Retorna columnas: `ocid`, `descripcion_item`, `cantidad`, `monto_referencial`, `monto_adjudicado`, `monto_contratado`, `ruc_proveedor`, `red_candidato`, `fecha_*`, `es_contratacion_directa`, `tiene_adenda`, `anio_fiscal`, etc.

Se eliminó `flatten_record()` y `flatten_dataframe()` (pandas). La nueva función `flatten_paths()` se integra con el pipeline Silver.

#### `app/services/dim_resolver.py` — De pandas a PySpark con broadcast

**Antes**: `build_dim_*()` retornaba DataFrames pandas; la resolución FK usaba bucles con `fuzzy_matcher.match_medicamento()` llamada fila por fila.

**Después**: `resolve_all(flat_df, petitorio_pdf, establecimientos_pdf)` retorna un dict con 6 DataFrames Spark:

| Dimensión | SKs | Broadcast |
|---|---|---|
| `dim_proveedor` | Determinista (hash de RUC), sentinel -1 | — |
| `dim_medicamento` | Auto-incremental desde 1, sentinels -1, -2 | `petitorio_dci_norm` via broadcast |
| `dim_entidad` | Determinista (hash de Cod_EESS), sentinel -1 | `CODIGO_RED_MAP` + `redes_norm` via broadcast |
| `dim_tiempo` | Derivado de fecha → `SK_Tiempo` | — |
| `dim_tipo_proceso` | Mapeo directo (precargados SK 1–11) | — |
| `fact` | Contiene todas las FKs resueltas | — |

**Fuzzy matching vectorizado**: `make_match_medicamento_udf(broadcast_choices)` se aplica como `withColumn` sobre toda la columna de descripciones en una sola pasada (Pandas UDF sobre Arrow). La resolución de Red usa `extract_red_asistencial()` → `make_match_red_udf()` para extraer y matchear en serie.

SKs de departamento (`DEPARTAMENTO_SK`) mapeados en duro (25 departamentos + Amazonas, SK 1–26), consistente con Dim_Ubigeo del DDL.

#### `app/loaders/dw_loader.py` — Nuevo flujo staging + JDBC

**Antes**: `load_all(engine, dims, ddl_path)` ejecutaba DDL SQL, luego insertaba con `method='multi'` y `chunksize=100` directamente a `oro.*`.

**Después**: Nuevas funciones:

| Función | Propósito |
|---|---|
| `_split_sql_batches(sql)` | Separa batches por `GO` |
| `execute_ddl(engine, ddl_path)` | Ejecuta batches DDL (producción + staging) |
| `_jdbc_conn()` | Deriva URL JDBC desde `DW_JDBC_URL` o desde `DW_CONN_STRING` (parsea con `make_url`) |
| `_jdbc_write_options()` | Dict con 15 opciones JDBC (driver, batchsize, trustServerCertificate, etc.) |
| `write_staging_jdbc(dims_fact, table_name)` | Escribe un DataFrame Spark a `stg.<table>` vía `.write.jdbc()` con `mode="overwrite"` |
| `write_all_staging(dims_fact)` | Itera sobre todas las tablas y escribe cada una a staging |
| `call_load_procedure(engine)` | Ejecuta `EXEC oro.usp_Load_From_Staging` |
| `load_all(dims_fact, ddl_path, engine)` | Orquesta: DDL prod → DDL staging → write_all_staging → call_load_procedure |

**`_jdbc_conn()`** soporta dos modos:
1. `OCDS_DW_JDBC_URL` — URL JDBC directa (ej. para Spark).
2. Fallback a `OCDS_DW_CONN_STRING` — deriva automáticamente `jdbc:sqlserver://<host>:<port>;databaseName=<db>;encrypt=true;trustServerCertificate=true` desde la cadena SQLAlchemy.

#### `app/pipelines/silver_layer.py` — Actualizado para Spark

- **Antes**: `SilverPipeline.run()` usaba `ocds_flattener.flatten_dataframe()`, `dim_resolver.build_dim_*()` (pandas) y `dw_loader.load_all(engine, dims, ddl_path)`.
- **Después**: 
  - Crea `SparkSession` al inicio, la detiene al final (vía `try/finally`).
  - `flatten_paths(spark, ...)` reemplaza al flattening pandas.
  - `resolve_all(flat_df, petitorio_pdf, establecimientos_pdf)` reemplaza a las funciones pandas.
  - `dw_loader.load_all(dims_fact, ddl_path, engine)` recibe el dict de DataFrames Spark + engine SQLAlchemy.
  - Soporta `years=[...]` para filtrar por año fiscal.

#### `app/config/settings.py` — Nuevas variables Spark

Agregadas: `SPARK_APP_NAME`, `SPARK_MASTER`, `SPARK_SHUFFLE_PARTITIONS`, `SPARK_JARS_PACKAGES`, `SPARK_JARS`, `DW_JDBC_URL`, `DW_JDBC_USER`, `DW_JDBC_PASSWORD`, `DW_JDBC_BATCHSIZE`.

#### `requirements.txt` — Nuevas dependencias

```diff
+ pyspark>=3.5.0
- pyarrow>=15.0.0
+ pyarrow>=15.0.0,<19.0.0   # límite superior para compatibilidad con Arrow Java de Spark 4.1
```

#### `Dockerfile` — Java 17 para PySpark

```dockerfile
USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends default-jre-headless procps \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java
USER airflow
```

La imagen base `apache/airflow:2.9.1` (Debian) no trae JRE. `default-jre-headless` instala OpenJDK 17, compatible con PySpark 3.5/4.x.

#### `.github/workflows/ci.yml` — Java 17 en CI

Agregado paso `actions/setup-java@v4` con `distribution: temurin` y `java-version: "17"` antes de instalar dependencias Python.

#### `CLAUDE.md` — Actualizado

Refleja el nuevo stack: PySpark, Rapidfuzz, staging atómico. Sección "Capa Silver" actualizada a "Silver/Gold (Spark + Staging atómico)".

### 8.7 Archivos eliminados

| Archivo | Motivo |
|---|---|
| `PLAN_IMPLEMENTACION.md` | Reemplazado por `PLAN_MIGRACION_PYSPARK.md` |
| `data/silver/staging/ocds_flat_2015.parquet` | Reemplazado por staging_flat particionado |
| `data/silver/staging/ocds_flat_2024.parquet` | Reemplazado por staging_flat particionado |

### 8.8 Tests nuevos — cobertura Silver/Gold

| Archivo | Tests | Descripción |
|---|---|---|
| `test/test_silver_spark.py` | `test_flatten_grano_y_corrupcion` | Lee 3 JSONs sintéticos + 1 corrupto; `split_corrupt` captura el corrupto sin abortar |
| | `test_flatten_cascada_award_contract` | Verifica resolución award→contract por awardID, montos, `es_contratacion_directa`, `tiene_adenda`, `red_candidato` (código RAMOQ) |
| | `test_resolve_all_fk_integridad` | Pipeline completo flatten→resolve→Fact: 2 proveedores, 1 medicamento, 1 entidad, FK sets correctos |
| | `test_fuzzy_udf_graceful_degradation` | Broadcast inválido (int en vez de lista) → UDF retorna `ERROR:*` en vez de abortar el Job |
| `test/test_dw_loader.py` | `test_split_sql_batches_por_go` | Separa batches SQL por `GO` |
| | `test_staging_ddl_existe_y_define_procedure` | Verifica que `EsSalud_Staging_DDL.sql` existe, contiene CREATE SCHEMA y usp_Load_From_Staging |
| | `test_jdbc_write_options` | Construye opciones JDBC correctas desde settings |
| | `test_jdbc_se_deriva_de_conn_string` | Deriva URL JDBC desde SQLAlchemy `DW_CONN_STRING` |
| | `test_write_staging_jdbc_requiere_conexion` | Sin URL ni conn string → ValueError |
| | `test_call_load_procedure_ejecuta_exec` | Verifica que `call_load_procedure()` ejecuta `EXEC oro.usp_Load_From_Staging` |
| | `test_load_all_orquesta_en_orden` | Orden correcto: DDL prod → DDL staging → staging → procedure |
| `test/test_fuzzy_matcher.py` | 6 tests | match_medicamento (exacto, histórico, dudoso/vacío), extract_red (código, nombre_texto, sin_red), match_red_asistencial |

### 8.9 Cambios en el diseño de datos

**Nuevo directorio staging** con particionamiento Hive:
```
data/silver/staging_flat/
├── anio_fiscal=2022/
│   └── part-00000-xxx.snappy.parquet
├── anio_fiscal=2023/
│   ├── part-00000-xxx.snappy.parquet
│   ├── part-00001-xxx.snappy.parquet
│   ├── part-00002-xxx.snappy.parquet
│   └── part-00003-xxx.snappy.parquet
├── anio_fiscal=2024/
│   ├── part-00000-xxx.snappy.parquet
│   ├── part-00001-xxx.snappy.parquet
│   ├── part-00002-xxx.snappy.parquet
│   └── part-00003-xxx.snappy.parquet
└── _SUCCESS
```

Los Parquet planos individuales `ocds_flat_<year>.parquet` fueron reemplazados por el formato particionado que soporta lectura selectiva por año y escalabilidad horizontal con Spark.

### 8.10 Principios de diseño aplicados

1. **Fail-safe**: registros corruptos capturados en `_corrupt_record` sin detener el job; UDFs con `try/except` por elemento retornan sentinels.
2. **Carga atómica**: staging tables + `usp_Load_From_Staging` en transacción única; si Spark falla, producción intacta; si el procedure falla, ROLLBACK.
3. **SKs deterministas**: las dimensiones no dependen de `IDENTITY` de SQL Server; Spark calcula SKs por hash/regla, permitiendo `SET IDENTITY_INSERT` y recargas idempotentes.
4. **Graceful degradation**: ante un error en el fuzzy matching de una fila, se asigna sentinel en lugar de abortar todo el Job.`

---

## 9. Actualización 2026-07-01 — Fase 4: Modelado Predictivo del Lead Time (ML)

Se agrega una capa de **modelado predictivo** que estima cuántos días tarda un proceso
de contratación entre la **convocatoria** y la **suscripción del contrato** (`Lead_Time_Total`),
y publica las predicciones en una nueva tabla Gold para Power BI. **No modifica ningún
archivo del pipeline existente** (Bronze→Silver→Gold): consume las tablas Parquet de `data/mart/`.

### 9.1 Resumen

- **Modelo:** XGBoost vs. Random Forest, seleccionado por RMSE con validación cruzada
  estratificada de 5 folds por Red Asistencial. **Ganó XGBoost.**
- **Entregable:** `data/mart/Pred_Lead_Time.parquet` (9 292 filas) — histórico real + predicho,
  listo para la Vista Táctica de Power BI.
- **Calidad:** MAE 15.8 días · mediana del error 3.6 días · **R² ≈ 0.85** · 10/10 pruebas OK.

### 9.2 ✨ Nueva funcionalidad

- **Predictor de Lead Time contractual** (`machine_learning/lead_time_predictor/LeadTime_Predictor.ipynb`, 13 celdas):
  carga y une las 4 tablas relevantes de `data/mart/`, hace ingeniería de features, EDA (3 gráficos),
  compara dos modelos por CV y exporta predicciones para **todos** los registros — incluidos
  los procesos 2024-2025 aún en curso (sin fecha de suscripción), que es el caso de uso.
- **Nueva tabla Gold `data/mart/Pred_Lead_Time.parquet`** con lead time real, predicho y residual
  por proceso, para graficar histórico vs. predicho por año, Red y categoría.
- **Modelo serializado reutilizable** (`machine_learning/lead_time_predictor/models/best_model.joblib`) cargable con
  `joblib.load` para predecir sobre nuevos procesos.

### 9.3 🔧 Correcciones de calidad de datos

| Problema detectado | Corrección |
|---|---|
| `Lead_Time_Actual` mostraba **valores negativos** (p. ej. −29 d, residual −95.6) porque había fechas inconsistentes (suscripción antes que convocatoria) | Se anulan a `NaN` los Lead Times negativos (**651** en Total, **517** en Comité). Así `Lead_Time_Actual` nunca es negativo y `Residual` solo se calcula donde hay medición real válida |
| Procesos **COMPETITIVO predichos en 0 días** (65-72 filas) por `clip(0)` sobre extrapolaciones negativas del modelo | Se modela el objetivo en escala **log1p** (`TransformedTargetRegressor`): predicciones siempre ≥ 0 y ninguna competitiva en ~0 días (mínimo 4.9 d). Mejora además la calibración de la mediana |
| `N_Item` ~95 % nulo y `Codigo_Convocatoria` ~23 % nulo en el origen → filas sin clave estable | Se agrega **`ID_Registro`** (clave estable por fila) para trazabilidad y relaciones en Power BI |
| Predicciones distintas entre máquinas (host vs. worker Docker) | XGBoost fijado a `n_jobs=1` → **resultado reproducible** entre entornos |
| Texto del notebook con mojibake (`�`) | Notebook regenerado en **UTF-8 limpio** |

### 9.4 Modelo y features

- **Features (11):** 5 categóricas (`Red_Asistencial`, `Categoria_Proceso`, `Especialidad_Autorizada`,
  `Tipo_Red`, `Metodo_Clasificacion`), 4 numéricas (`Monto_Adjudicado_Soles`, `Anio_Fiscal`,
  `Mes_Convocatoria`, `retraso_historico_red`) y 2 binarias (`Flag_Contratacion_Directa`,
  `Flag_Tiene_Adenda`).
- **Ajustes al esquema real de la Fact:** `Flag_Tiene_Adenda` se **deriva** de `Monto_Adicional > 0`
  (la Fact no trae el flag); `retraso_historico_red` (media de lead time de comité por Red) se
  **imputa con la media global** cuando una Red no tiene historial.
- **Preprocesamiento:** `OrdinalEncoder` (categóricas) + `StandardScaler` (numéricas) +
  passthrough (binarias) en un `ColumnTransformer`, todo dentro de un `Pipeline`.
- **Selección por RMSE (CV 5-fold estratificado por Red):**

| Modelo | CV RMSE (días) |
|---|---|
| **XGBoost (log1p)** ✅ | **55.29 ± 3.61** |
| Random Forest (log1p) | 63.86 ± 3.13 |

- **Feature dominante:** `Flag_Contratacion_Directa` (~0.6 de importancia) — la contratación
  directa es el mayor predictor de rapidez.
- **Calibración por año:** MAE 2024 = 11.6 d, MAE 2025 = 2.3 d (2025 es mayormente contratación
  directa, con lead time real ≈ 0, correctamente predicho ≈ 0).

### 9.5 Archivos nuevos

| Archivo | Descripción |
|---|---|
| `machine_learning/lead_time_predictor/LeadTime_Predictor.ipynb` | Notebook del modelo predictivo (13 celdas, UTF-8) |
| `machine_learning/lead_time_predictor/models/best_model.joblib` | Modelo XGBoost (log1p) serializado |
| `machine_learning/lead_time_predictor/test_pred_lead_time.py` | 10 pruebas de verificación del entregable |
| `data/mart/Pred_Lead_Time.parquet` | Tabla Gold de predicciones (9 292 filas) |
| `fase4-lead-time-predictivo.md` | Plan de la fase (incluye guía de integración Power BI) |

### 9.6 Esquema de `data/mart/Pred_Lead_Time.parquet`

| Columna | Tipo | Notas |
|---|---|---|
| `ID_Registro` | int | Clave estable por fila (0…N-1) |
| `Codigo_Convocatoria` | float | Clave natural SEACE (puede ser nula) |
| `N_Item` | float | Número de ítem (escaso en el origen) |
| `Anio_Fiscal` | int | 2022–2025 |
| `Red_Asistencial` | str | Red (o `DESCONOCIDO`) |
| `Categoria_Proceso` | str | COMPETITIVO / DIRECTO / CATALOGO / REGIMEN_ESPECIAL |
| `Lead_Time_Actual` | float | Días reales; `NaN` si falta una fecha o son inconsistentes |
| `Lead_Time_Predicho` | float | Predicción del modelo (siempre presente, ≥ 0) |
| `Residual` | float | Actual − Predicho (`NaN` si Actual es `NaN`) |

### 9.7 🐛 Pruebas y verificación

Suite `machine_learning/lead_time_predictor/test_pred_lead_time.py` (**10/10 OK**), ejecutable de forma aislada
sin Spark ni el `conftest` del proyecto:

```bash
PYTHONUTF8=1 .venv/Scripts/python.exe machine_learning/lead_time_predictor/test_pred_lead_time.py
```

Valida: existencia de entregables · 9 292 filas y esquema · `Lead_Time_Predicho` sin nulos y ≥ 0 ·
**0 actuals negativos** · `Residual` consistente y solo donde hay Actual · `ID_Registro` único ·
el modelo carga y **reproduce el 100 %** del parquet · sanidad por categoría (DIRECTO < CATALOGO < COMPETITIVO) · RMSE < 150.

### 9.8 Cómo ejecutar el notebook (headless)

```bash
# 1) Registrar el .venv como kernel de Jupyter (una sola vez)
.venv/Scripts/python.exe -m ipykernel install --user --name essalud --display-name "EsSalud .venv"

# 2) Ejecutar el notebook de punta a punta (regenera parquet + modelo)
.venv/Scripts/python.exe -m jupyter nbconvert --to notebook --execute --inplace \
  --ExecutePreprocessor.kernel_name=essalud machine_learning/lead_time_predictor/LeadTime_Predictor.ipynb
```

> `nbconvert` fija el CWD del kernel a la carpeta del notebook, por lo que `../bi` y `models/`
> resuelven correctamente.

### 9.9 Nota (fuera del alcance de la Fase 4)

~48 % de las filas de la Fact tienen `Monto_Adjudicado_Soles = 0` (dato faltante de la capa
Gold/sintéticos, aguas arriba). No afecta al modelo de lead time (el monto pesa ~3 % en la
importancia), pero conviene sanearlo en el paso `synth`/`gold` si Power BI usará montos. La
**integración con Power BI queda documentada en el plan pero no se ejecuta** en esta fase.

---

## 10. Actualización 2026-07-02 — Debug E2E + Fase 6: Alertas Operativas

Implementación del plan `debug-endtoend-fase6.md` (con la decisión del usuario:
**Cloudflare = solo R2 opcional para Bronze; el DW se accede local o en
contenedor Docker, sin túnel**). Incluye la **Fase 6**: alertas automáticas por
correo con RUC del proveedor dominante, medicamento y Red Asistencial.

### 10.1 ✨ Fase 6 — motor de alertas (`app/services/alerting.py`)

- **Fuente HHI**: réplica pandas exacta de `oro.vw_Matriz_Riesgo_HHI` sobre
  `data/mart/*.parquet` (HHI ≥ 8000, dominante ≥ 80%, mismos umbrales del semáforo).
  `Es_Uso_Critico` se deriva de `Restriccion_Uso` del Petitorio (en el DW es
  `BIT DEFAULT 0` sin poblar — la vista SQL nunca dispararía CRITICO).
- **Fuente Lead Time** (Fase 4): procesos con `Residual > media + 2σ`
  (`--sigma` configurable); resuelve RUC/medicamento vía `ID_Registro`
  (posicional sobre la Fact).
- **Salidas**: `data/mart/Alertas.parquet` (esquema de 10 columnas para la Vista
  Operativa de Power BI) + correo formal SMTP (texto plano + tabla HTML).
- **CLI**: `python app/cli.py alert [--source hhi|leadtime|all] [--to] [--limit]
  [--sigma] [--dry-run]`. SMTP en `.env` (Gmail App Password o MailHog local).
- **Airflow**: nuevo DAG `ocds_alerting` (disparado por `ocds_silver_pipeline`
  tras Gold); `silver_dag` ganó la tarea **`run_synth`** entre Silver y Gold
  (antes faltaba y Gold quedaba casi vacío en 2024/2025).
- **Guía institucional sin código**: `docs/fase6-powerautomate.md`
  (Power BI Service + visual Power Automate + Send email V2).
- **Tests**: `test/test_alerting.py` (17 pruebas: HHI monopolio/duopolio/
  dominante, outliers de lead time, correo con los 3 campos, dry-run, CLI).

### 10.2 🔧 Fixes del camino `--target sqlserver`

| Problema | Fix |
|---|---|
| `_derive_jdbc_from_conn` derivaba user/password **vacíos** con `trusted_connection=yes` y la escritura Spark fallaba críptica en el executor | `write_staging_jdbc` ahora **aborta temprano** con mensaje accionable (definir `OCDS_DW_JDBC_URL/USER/PASSWORD` con SQL Auth) |
| El `.env` de Windows montado en el contenedor pisaba `JAVA_HOME`/`HADOOP_HOME` y rompía Spark en Linux | `settings.py` soporta **`OCDS_ENV_FILE`**; el compose apunta a **`.env.docker`** (sin rutas Windows, `local[*]`, DW → `sqlserver:1433`) |
| `--profile docker` era no-op silencioso (vars `*_DOCKER` vacías) | `.env` del host ahora define `OCDS_DW_*_DOCKER` → `localhost:11433` (puerto publicado del contenedor) |
| `pyodbc` sin driver del SO en la imagen Airflow | `Dockerfile` instala **msodbcsql18 + unixodbc + mssql-tools18** (repo Microsoft Debian 12) |
| No existía contenedor MSSQL en el compose | Servicios **`sqlserver`** (mcr 2022, `11433:1433`, healthcheck sqlcmd, volumen persistente) + **`sqlserver-init`** (crea `DW_EsSalud_Adquisiciones`) + **`mailhog`** (SMTP pruebas, UI `:8025`) |
| Build context gigante (`.venv/`, `data/`, `data/mart/`) | **`.dockerignore`** whitelist (solo `requirements.txt`) |

### 10.3 Archivos nuevos / modificados

**Nuevos**: `app/services/alerting.py` · `dags/alerting_dag.py` ·
`test/test_alerting.py` · `docs/fase6-powerautomate.md` · `.env.example` ·
`.env.docker` · `.dockerignore`.
**Modificados**: `app/cli.py` (subcomando `alert`) · `dags/silver_dag.py`
(`run_synth` + `trigger_alerting`) · `app/loaders/dw_loader.py` ·
`app/config/settings.py` (OCDS_ENV_FILE + SMTP) · `docker-compose.yaml` ·
`Dockerfile` · `.env` · `README.md`.

### 10.4 Seguridad

- Verificado: **`.env` nunca estuvo trackeado en git** (el hallazgo del plan
  era obsoleto); `.gitignore` ya lo cubría. Se añade `.env.example` sin secretos.
- `.env.docker` sí se versiona: solo contiene credenciales de desarrollo del
  contenedor local (mismo default que el compose), sin secretos reales.

### 10.5 ⚠️ Nota operativa — regeneración vía Airflow vs. host

El DAG `ocds_silver_pipeline` regenera `staging_flat` y `data/mart/` **sobre el volumen
montado**. El paso `synth` es determinista respecto a la semilla (42) pero
**sensible al layout de particiones de Spark** (`orderBy(rand(seed)).limit(n)`):
el contenedor (`local[*]`) muestrea un subconjunto distinto al del host
(`local[4]`) — el total se mantiene en 9 292, pero los actuals válidos de lead
time cambian (host: 5 917; contenedor: 6 303). Si se regenera desde el DAG, hay
que **re-ejecutar el notebook de la Fase 4** (`machine_learning/lead_time_predictor/LeadTime_Predictor.ipynb`)
para realinear `Pred_Lead_Time.parquet`/`best_model.joblib` y ajustar el conteo
esperado en `test_pred_lead_time.py`. El flujo canónico documentado sigue siendo
el del **host Windows** (silver → synth → gold → notebook → test 10/10).

### 10.6 Fixes de orquestación encontrados en la verificación E2E (Capa 9)

| Síntoma | Causa raíz | Fix |
|---|---|---|
| Todo DAG fallaba con "task killed externally"; el worker nunca recibía tareas; scheduler: `module 'redis' has no attribute 'client'` | `AIRFLOW__CELERY__OPERATION_TIMEOUT` default (1 s) expiraba durante el primer import en frío de `kombu.transport.redis`; el SIGALRM dejaba el módulo `redis` a medio importar en `sys.modules` (envenenado para todos los envíos siguientes) | `AIRFLOW__CELERY__OPERATION_TIMEOUT: '30'` en el compose |
| Silver fallaba en el executor: `ModuleNotFoundError: No module named 'app'` (las UDFs no despickean) | Los workers Python de Spark no heredan el `sys.path` del driver (el `sys.path.append` del DAG solo afecta al driver) | `PYTHONPATH: /opt/airflow/bi` en el entorno común del compose |
| Silver fallaba: `UNSUPPORTED_PACKAGE_VERSION: Pandas >= 2.2.0 must be installed; your version is 2.1.4` | La imagen de Airflow 2.9.1 fija pandas 2.1.4; PySpark 4.x exige ≥ 2.2 | `pandas>=2.2,<2.3` explícito en el `pip install` del Dockerfile |

Con los tres fixes, la cadena completa `ocds_silver_pipeline` (resolve → silver
→ synth → gold → trigger) y `ocds_alerting` terminan en **success**, con el
correo de alertas capturado en MailHog.
