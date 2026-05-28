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
├── Dockerfile                    ← Extiende apache/airflow:2.9.1
├── PLAN_IMPLEMENTACION.md        ← Plan original de implementación (10 Pasos)
├── README.md                     ← README del proyecto
├── docker-compose.yaml           ← Stack completo Airflow (Postgres, Redis, Webserver, Scheduler, Worker, Triggerer, Flower)
├── main.py                       ← CLI entrypoint (targeted / bulk)
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
│   │   └── settings.py           ← Config centralizada vía env vars
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
├── star-schema/
│   └── EsSalud_StarSchema_DDL.sql  ← DDL completo: 7 tablas + 3 vistas + validación post-carga
│
├── data/
│   ├── bronze/
│   │   ├── bulk_files/
│   │   └── records/
│   │       └── 20131257750/
│   │           └── 2024/           ← 100 records (--year 2024 --limit 100)
│   ├── silver/
│   │   └── staging/
│   │       └── ocds_flat_2024.parquet  ← 155 filas-ítem
│   └── audit/
│       └── executions/
│           └── ocds_extraction.log
│
├── extra-data/
│   ├── Petitorio-Publicar-hasta-Res-N_-063-2026.xls     ← 996 medicamentos
│   └── 5992483-*.xlsx                                    ← 403 centros / 35 redes
│
├── test/
│   ├── conftest.py                  ← ← CREADO (ahora redundante, ver bugs)
│   ├── test_api.py                  ← ← REESCRITO: 6 tests con aserciones reales
│   ├── test_bronze_layer.py         ← ← REESCRITO: mocks, atributos correctos
│   └── test_dag_integrity.py        ← ← MODIFICADO: pytest.importorskip
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
OCDS_EXTRA_DATA_DIR=extra-data
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

### 3.1 `main.py` — CLI entrypoint

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
bash_command='cd /opt/airflow/bi && python main.py targeted --year 2024 --limit 100',
bash_command='cd /opt/airflow/bi && python main.py bulk --source SEACE --type JSON --year 2023 --month 11',
```

**Después**: Macros Jinja + TriggerDagRunOperator de Targeted → Silver:
```python
bash_command=(
    "cd /opt/airflow/bi && python main.py targeted"
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

### 3.11 `star-schema/EsSalud_StarSchema_DDL (3).sql` → `EsSalud_StarSchema_DDL.sql`

**Antes**: Nombre con espacio y versión parentética: `EsSalud_StarSchema_DDL (3).sql`

**Después**: Nombre limpio: `EsSalud_StarSchema_DDL.sql`

**Referencia**: `app/pipelines/silver_layer.py:36` actualizada para usar el nuevo nombre.

### 3.12 `star-schema/EsSalud_StarSchema_DDL.sql` — WHILE loop → CTE recursivo + Sección 9

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
| Renombrado | `star-schema/EsSalud_StarSchema_DDL (3).sql` | `star-schema/EsSalud_StarSchema_DDL.sql` | Espacio y sufijo `(3)` no estándar |
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
python main.py targeted --year 2022 --limit 0
python main.py targeted --year 2023 --limit 0
python main.py targeted --year 2024 --limit 0
python main.py targeted --year 2025 --limit 0

# Para pruebas rápidas, limitar a N registros:
python main.py targeted --year 2024 --limit 10
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
flake8 app dags main.py --count --select=E9,F63,F7,F82 --show-source --statistics

# Lint de estilo (no bloqueante, salida 0 incluso con warnings)
flake8 app dags main.py --count --exit-zero --max-complexity=10 --max-line-length=127 --statistics
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
**Archivo**: `star-schema/EsSalud_StarSchema_DDL.sql`
**Síntoma**: al re-ejecutar el DDL sobre un DW ya poblado, `DROP TABLE` de una dimensión fallaba con error 3726 (*"Could not drop object ... referenced by a FOREIGN KEY constraint"*), porque la `Fact` (y `Dim_Entidad`) existentes seguían referenciándola. La primera carga no fallaba porque el DW estaba vacío.
**Fix**: bloque de **LIMPIEZA PREVIA** al inicio del DDL que elimina las tablas en orden inverso de dependencias (`Fact` → `Dim_Entidad` → resto) antes de recrearlas. El pipeline Silver ahora se puede re-ejecutar sin intervención manual.

### 7.3 Recarga con muestra de 100 registros de 2024

Bronze re-extraído con `python main.py targeted --year 2024 --limit 100` (los datos previos de 2015 y la muestra anterior de 2024 se reemplazaron por una base limpia). Conteos del DW antes vs. ahora:

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
