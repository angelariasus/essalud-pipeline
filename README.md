# OCDS Data Framework — EsSalud

Framework **ELT** en Python orientado a objetos que extrae datos del **Portal de Contrataciones Abiertas del Perú** (estándar **OCDS**) para evaluar la eficiencia de las compras de medicamentos de **EsSalud**.

Implementa una **arquitectura de Data Lake medallón** completa:

```
Bronze  ──►  Silver  ──►  Gold
(requests)   (PySpark)     (Star Schema en SQL Server)
```

- **Bronze**: extracción robusta de la API OCDS y de volcados masivos del SEACE (`requests`).
- **Silver**: aplanado del JSON anidado, fuzzy matching y construcción de dimensiones (**Apache Spark / PySpark**).
- **Gold**: carga atómica a un **modelo estrella** en SQL Server vía staging tables + stored procedure.

---

## Tabla de contenidos

1. [Características principales](#1-características-principales)
2. [Arquitectura](#2-arquitectura)
3. [Estructura del proyecto](#3-estructura-del-proyecto)
4. [Requisitos previos](#4-requisitos-previos)
5. [Instalación y configuración](#5-instalación-y-configuración)
6. [Variables de entorno](#6-variables-de-entorno)
7. [Uso — Capa Bronze (CLI)](#7-uso--capa-bronze-cli)
8. [Uso — Capa Silver / Gold (PySpark)](#8-uso--capa-silver--gold-pyspark)
9. [Modelo estrella (Data Warehouse)](#9-modelo-estrella-data-warehouse)
10. [Orquestación con Apache Airflow](#10-orquestación-con-apache-airflow)
11. [Tests y CI](#11-tests-y-ci)
12. [Layout de datos](#12-layout-de-datos)
13. [Notas de versionado](#13-notas-de-versionado)

---

## 1. Características principales

- **Arquitectura medallón completa** Bronze → Silver → Gold, con separación clara de responsabilidades por capa.
- **Extracción híbrida (Targeted & Bulk):**
  - *Targeted*: descarga expedientes completos (`RecordPackage`) filtrados por comprador (RUC) y año mediante **filtrado del lado del cliente** (la API OCDS no filtra por RUC/fecha en el servidor).
  - *Bulk*: descarga en streaming de volcados mensuales masivos (ZIP) del SEACE.
- **Almacenamiento híbrido:** persistencia en disco local (`data/bronze/`) con replicación opcional en tiempo real a **Cloudflare R2** (S3-compatible).
- **Procesamiento distribuido con PySpark:** aplanado del JSON OCDS a granularidad **un ítem por fila**, fuzzy matching vectorizado (Pandas UDFs sobre Arrow) y resolución de claves foráneas.
- **Carga atómica al DW:** Spark escribe a tablas `stg.*` vía JDBC; un stored procedure (`oro.usp_Load_From_Staging`) las mueve a producción en **una sola transacción** (si Spark falla a mitad, producción queda intacta).
- **Diseño fail-safe:** JSON corrupto capturado en `_corrupt_record` sin abortar el job; UDFs con *graceful degradation* por elemento; coerción de datos sucios a `NULL` (`spark.sql.ansi.enabled=false`).
- **Orquestación empresarial:** DAGs de **Apache Airflow** (Docker) para ingesta y transformación programadas.

---

## 2. Arquitectura

### Patrón medallón

| Capa | Motor | Entrada | Salida |
|---|---|---|---|
| **Bronze** | `requests` | API OCDS / portal SEACE | JSON / ZIP en `data/bronze/` (+ R2 opcional) |
| **Silver** | PySpark | JSON Bronze | Parquet plano particionado en `data/silver/staging_flat/` |
| **Gold** | PySpark + JDBC | Parquet Silver + maestros Excel | Modelo estrella `oro.*` en SQL Server |

### Flujo de datos

1. **Bronze — Targeted:** pagina la API OCDS → filtra por RUC + año en Python → descarga el record completo → persiste localmente y opcionalmente en R2.
2. **Bronze — Bulk:** descarga el ZIP mensual del SEACE en chunks de 8 KB → replica a R2 → descomprime localmente.
3. **Silver/Gold:** `spark.read.json` (schema estricto, modo `PERMISSIVE` + `_corrupt_record`) → aplana a una fila por ítem de tender (`explode_outer` + cascada award/contract) → Parquet staging → construye dimensiones + resuelve FKs (SKs deterministas con `row_number`, fuzzy matching con Pandas UDFs y maestros en *broadcast*) → escribe tablas staging por JDBC → carga atómica al modelo estrella vía stored procedure.

### Decisiones de diseño clave

- **Filtrado client-side:** `TargetedExtractor` pagina todas las páginas y filtra en Python porque la API no soporta filtros nativos por RUC/fecha.
- **Doble persistencia:** `FileManager` escribe a disco; `R2Manager` (import perezoso para evitar dependencia dura de boto3) replica a R2 cuando `OCDS_USE_R2=True`.
- **Paginación con generadores:** `paginate_records()` produce páginas bajo demanda para datasets arbitrariamente grandes.
- **SKs deterministas:** las dimensiones no dependen de `IDENTITY`; Spark calcula las claves subrogadas por hash/regla, habilitando `SET IDENTITY_INSERT` y recargas idempotentes.
- **Caveats de Spark en Windows:** lanzar muchos workers UDF en paralelo crashea en Windows, por eso `OCDS_SPARK_MASTER` usa `local[4]` por defecto; `pyarrow` está fijado `<19` para alinear con el Arrow Java embebido en Spark 4.1 (18.3). En Linux/Docker no aplica. `PYSPARK_PYTHON` se fuerza al intérprete en ejecución.

---

## 3. Estructura del proyecto

```text
essalud-pipeline/
├── app/
│   ├── audit/          # Logger dual (consola INFO+, archivo DEBUG+)
│   ├── clients/        # Cliente HTTP OCDS con retry/backoff
│   ├── config/         # settings.py (env vars) + spark_session.py (singleton Spark)
│   ├── loaders/        # master_loader (Excel) + dw_loader (DDL, JDBC staging, carga atómica)
│   ├── models/         # Dataclasses: RecordSummary, CatalogItem, PaginationData
│   ├── pipelines/      # BronzePipeline + SilverPipeline (orquestadores)
│   ├── services/       # extractors, ocds_flattener (Spark), dim_resolver (Spark)
│   ├── storage/        # FileManager (disco) + R2Manager (Cloudflare R2)
│   └── utils/          # fuzzy_matcher (rapidfuzz + Pandas UDFs) + helpers
├── dags/               # ocds_dag.py (targeted + bulk) · silver_dag.py (silver)
├── star-schema/        # DDL del modelo estrella + DDL staging y stored procedure
├── extra-data/         # Maestros Excel: Petitorio (medicamentos) + Establecimientos (redes)
├── test/               # Suite pytest (incluye tests de integración Spark)
├── data/               # Data Lake local (bronze/ · silver/ · audit/) — ignorado por git
├── Dockerfile          # Imagen Airflow extendida con Java 17 (JRE) para PySpark
├── docker-compose.yaml # Stack Airflow: Postgres, Redis, Webserver, Scheduler, Worker
├── main.py             # CLI entrypoint (targeted / bulk)
├── pytest.ini          # Config pytest (pythonpath = .)
├── requirements.txt    # Dependencias runtime (requests, boto3, pyspark, rapidfuzz, pyodbc…)
├── requirements-dev.txt# Dependencias dev (pytest, flake8, apache-airflow)
├── CHANGELOG.md        # Historial detallado de cambios
└── PLAN_MIGRACION_PYSPARK.md  # Plan de migración pandas → PySpark
```

> **Nota:** la carpeta `data/` y los archivos de instrucciones de IA (`CLAUDE.md`, `AGENTS.md`, `GEMINI.md`) están en `.gitignore` — ver [§13](#13-notas-de-versionado).

---

## 4. Requisitos previos

- **Python 3.11+**
- **Java JRE 17+** en el `PATH` o vía `JAVA_HOME` (requerido por PySpark para Silver/Gold).
- **SQL Server** accesible (local o en Docker) si se quiere cargar el Data Warehouse.
- **Docker Desktop** (opcional) para levantar Apache Airflow.
- **Cuenta de Cloudflare R2** (opcional) para almacenamiento en la nube.

---

## 5. Instalación y configuración

```powershell
# 1. Crear y activar entorno virtual
python -m venv venv
.\venv\Scripts\activate

# 2. Instalar dependencias de runtime
pip install -r requirements.txt

# 3. (Opcional) Dependencias de desarrollo: tests, lint y Airflow
pip install -r requirements-dev.txt

# 4. Configurar credenciales y conexiones en el archivo .env de la raíz
#    (ver la sección de Variables de entorno)
```

> El archivo `.env` está en la raíz del proyecto y **no se versiona** (está en `.gitignore`). Edítalo con tus credenciales de R2 y la cadena de conexión al Data Warehouse.

---

## 6. Variables de entorno

Configuradas en `.env` (raíz del proyecto):

| Variable | Propósito |
|---|---|
| `OCDS_API_BASE_URL` | URL base de la API OCDS |
| `OCDS_ESSALUD_RUC` | RUC del comprador a filtrar (default: EsSalud) |
| `OCDS_USE_R2` | Habilita replicación a Cloudflare R2 (`True`/`False`) |
| `OCDS_R2_ACCOUNT_ID` / `ACCESS_KEY` / `SECRET_KEY` / `BUCKET_NAME` | Credenciales de R2 |
| `OCDS_LOG_LEVEL` | Nivel de log en consola |
| `OCDS_MAX_RETRIES` / `OCDS_BACKOFF_FACTOR` | Estrategia de reintentos HTTP |
| `OCDS_SILVER_DIR` / `OCDS_EXTRA_DATA_DIR` | Directorio de staging Silver / maestros Excel |
| `OCDS_SPARK_MASTER` | Master de Spark (default `local[4]`; `local[*]`/cluster en Linux) |
| `OCDS_SPARK_JARS_PACKAGES` / `OCDS_SPARK_JARS` | Driver mssql-jdbc (coordenadas Maven / jars locales) |
| `OCDS_DW_CONN_STRING` | Conexión SQLAlchemy/pyodbc para DDL + stored procedure |
| `OCDS_DW_JDBC_URL` / `USER` / `PASSWORD` / `BATCHSIZE` | Conexión JDBC para los writes de Spark (se deriva de `OCDS_DW_CONN_STRING` si no se define) |

---

## 7. Uso — Capa Bronze (CLI)

`main.py` expone una interfaz de terminal (argparse) con dos modos de ingesta.

### Modo Targeted (extracción dirigida)

Descarga expedientes completos de una entidad, filtrando por RUC y año del lado del cliente.

```powershell
# Extraer EsSalud (RUC 20131257750) del año 2024 — todos los registros
python main.py targeted --year 2024

# Limitar a los primeros N registros (útil para pruebas)
python main.py targeted --year 2024 --limit 10

# Otro RUC
python main.py targeted --ruc 20131257750 --year 2023 --limit 0
```

| Argumento | Default | Descripción |
|---|---|---|
| `--ruc` | RUC de EsSalud | RUC de la entidad compradora |
| `--year` | `None` | Año a filtrar (ej. `2024`) |
| `--limit` | `0` | Máximo de registros (`0` = todos) |

Los archivos se guardan en `data/bronze/records/<ruc>/<year>/<ocid>.json`.

### Modo Bulk (extracción masiva)

Descarga catálogos mensuales completos del portal SEACE en formato crudo.

```powershell
python main.py bulk --source SEACE --type JSON --year 2023 --month 11
```

| Argumento | Requerido | Descripción |
|---|---|---|
| `--source` | sí | Fuente (ej. `SEACE`) |
| `--type` | no (`JSON`) | Formato del catálogo |
| `--year` | sí | Año |
| `--month` | sí | Mes (1–12) |

---

## 8. Uso — Capa Silver / Gold (PySpark)

El `SilverPipeline` ejecuta el flujo completo: carga de maestros → aplanado Spark → construcción de dimensiones + resolución de FKs → carga atómica al DW.

> Requiere un **JRE 17+** disponible (PySpark).

```powershell
# Pipeline completo (todos los años disponibles) + carga al DW
python -c "from app.pipelines.silver_layer import SilverPipeline; SilverPipeline().run()"

# Solo un año específico
python -c "from app.pipelines.silver_layer import SilverPipeline; SilverPipeline().run(years=[2024])"

# Validar Silver sin una instancia de SQL Server (omite la carga al DW)
python -c "from app.pipelines.silver_layer import SilverPipeline; SilverPipeline().run(years=[2024], load_dw=False)"
```

Lo que ejecuta el pipeline:

1. **Carga de maestros:** Petitorio de medicamentos + Establecimientos/Redes (Excel, pandas).
2. **Aplanado (Spark):** lee los JSON Bronze → una fila por ítem de tender → Parquet particionado por `anio_fiscal` en `data/silver/staging_flat/`.
3. **Dimensiones + FKs (Spark):** construye `Dim_*`, resuelve claves foráneas con SKs deterministas y fuzzy matching vectorizado (Pandas UDFs con maestros en *broadcast*).
4. **Carga al DW (Gold):** escribe a `stg.*` por JDBC → invoca `oro.usp_Load_From_Staging` (carga atómica).

---

## 9. Modelo estrella (Data Warehouse)

El esquema Gold vive en SQL Server bajo el esquema `oro` y se define en `star-schema/`:

| Archivo | Contenido |
|---|---|
| `EsSalud_StarSchema_DDL.sql` | Modelo estrella completo: dimensiones + tabla de hechos + vistas analíticas + validación post-carga |
| `EsSalud_Staging_DDL.sql` | Esquema `stg` + `oro.usp_Load_From_Staging` (carga atómica transaccional) |

**Dimensiones:** `Dim_Tiempo`, `Dim_Ubigeo`, `Dim_Entidad_Compradora`, `Dim_Medicamento`, `Dim_Proveedor`, `Dim_Tipo_Proceso`.
**Hechos:** `Fact_Ordenes_Y_Contratos`.

**Vistas analíticas (Gold):**

| Vista | Métrica |
|---|---|
| `vw_Gasto_Por_Proceso_Y_Red` | Gasto agregado por tipo de proceso y Red Asistencial |
| `vw_Lead_Time_Por_Proveedor` | **Lead Time** (días entre convocatoria y suscripción) por proveedor |
| `vw_Matriz_Riesgo_HHI` | Índice de concentración **HHI** (Herfindahl-Hirschman) por mercado |

La granularidad del modelo es **nivel Red Asistencial** (la API OCDS identifica la Red, no el establecimiento exacto).

---

## 10. Orquestación con Apache Airflow

Stack completo en Docker (Postgres, Redis, Webserver, Scheduler, Worker). La imagen extiende `apache/airflow:2.9.1` e instala **Java 17 (JRE)** para poder ejecutar PySpark dentro del worker.

```powershell
# 1. Inicializar la base de datos de Airflow
docker compose up airflow-init

# 2. Levantar el stack (construyendo la imagen con las dependencias del proyecto)
docker compose up -d --build

# 3. Acceder al panel: http://localhost:8080  (usuario: airflow / contraseña: airflow)
```

**DAGs disponibles:**

| DAG | Frecuencia | Función |
|---|---|---|
| `ocds_targeted_ingestion` | semanal | Ingesta Targeted del año en curso (límite 100 en dev); dispara el Silver al terminar |
| `ocds_bulk_ingestion` | mensual | Descarga bulk mensual del SEACE |
| `ocds_silver_pipeline` | mensual | Transforma Bronze → Silver/Gold (Spark en modo local dentro del worker); acepta `year` vía `dag_run.conf` |

---

## 11. Tests y CI

```powershell
# Ejecutar toda la suite
pytest

# Un archivo específico
pytest test/test_silver_spark.py

# Lint
flake8 app/ test/
```

- Los tests de **Spark** se saltan automáticamente si no hay un JRE disponible (marcador `requires_spark` en `test/conftest.py`).
- Los tests de integración Silver/Gold usan **registros OCDS sintéticos** (porque `data/bronze/` está ignorado por git).
- **CI** (`.github/workflows/ci.yml`): corre `flake8` y `pytest` en cada push/PR a `main`, configurando **Java 17 (Temurin)** para que los tests de Spark se ejecuten.

---

## 12. Layout de datos

```text
data/
├── bronze/
│   ├── records/        # JSON individuales de la extracción targeted (records/<ruc>/<year>/)
│   └── bulk_files/     # ZIPs crudos y catálogos descomprimidos
├── silver/
│   └── staging_flat/   # Salida Spark aplanada, Parquet particionado por anio_fiscal
└── audit/
    └── executions/     # ocds_extraction.log (nivel debug)
```

Todo el contenido de `data/` se **genera automáticamente** al ejecutar el pipeline (las rutas se crean solas), por lo que no se versiona.

---

## 13. Notas de versionado

Para mantener el repositorio limpio, los siguientes elementos están en `.gitignore` y **no se suben** al remoto:

- **`data/`** completo (`bronze/`, `silver/`, `audit/`) — artefactos generados y regenerables por el pipeline.
- **Archivos de instrucciones de IA:** `CLAUDE.md`, `AGENTS.md`, `GEMINI.md` — son locales; cada quien los edita sin afectar el repo.
- **`.env`** y credenciales.

Archivos de documentación que **sí** se versionan: `README.md`, `CHANGELOG.md` (historial detallado de cambios) y `PLAN_MIGRACION_PYSPARK.md` (plan de migración a PySpark).
