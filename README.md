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
│   ├── audit/              # Logger dual (consola INFO+, archivo DEBUG+)
│   ├── clients/            # Cliente HTTP OCDS con retry/backoff
│   ├── config/             # settings.py (env vars) + spark_session.py (singleton Spark)
│   ├── loaders/
│   │   ├── master_loader.py    # Petitorio + Establecimientos (Excel → pandas)
│   │   ├── dw_loader.py        # DDL idempotente, JDBC staging, carga atómica
│   │   └── targets/            # Abstracción de destinos Gold
│   │       ├── base.py             # GoldTarget (ABC)
│   │       ├── parquet_target.py   # ParquetGoldTarget (default)
│   │       └── sqlserver_target.py # SqlServerTarget (envuelve dw_loader)
│   ├── models/             # Dataclasses: RecordSummary, CatalogItem, PaginationData
│   ├── pipelines/
│   │   ├── bronze_layer.py     # BronzePipeline
│   │   ├── silver_layer.py     # SilverPipeline (flatten→IA→CONOSCE→parquet)
│   │   └── gold_layer.py       # GoldPipeline (read_staging→dims→target)
│   ├── services/
│   │   ├── extractors.py           # Targeted + Bulk extractors
│   │   ├── ocds_flattener.py       # Aplanamiento Spark + read_staging()
│   │   ├── ai_cleaner.py           # Limpieza con Gemini API
│   │   ├── conosce_enricher.py     # Enriquecimiento con Excel CONOSCE (SEACE)
│   │   ├── dim_resolver.py         # Dimensiones + Fact (fuzzy match, SKs)
│   │   ├── static_dims.py          # Dim_Tiempo y Dim_Ubigeo como DataFrames Spark + export bi/
│   │   └── synthetic_generator.py  # Datos sintéticos (2024 boost + 2025 CSV)
│   ├── storage/            # FileManager (disco) + R2Manager (Cloudflare R2)
│   └── utils/
│       ├── fuzzy_matcher.py    # Pandas UDFs RapidFuzz (medicamentos + redes)
│       └── cleaner.py          # Limpieza por capa (bronze/silver/gold)
├── dags/               # ocds_dag.py (targeted + bulk) · silver_dag.py (silver)
├── star-schema/        # DDL del modelo estrella + DDL staging y stored procedure
├── extra-data/
│   ├── Contratos/          # xlsx CONOSCE por año (2022–2025, ~63 000 filas)
│   ├── CONOSCE_2025_essalud.csv  # Resumen EsSalud 2025 bienes (fuente synth)
│   └── gemini_cache.json   # Caché local de respuestas Gemini
├── test/               # Suite pytest (incluye tests de integración Spark)
├── data/               # Data Lake local (bronze/ · silver/ · audit/) — ignorado por git
├── Dockerfile          # Imagen Airflow extendida con Java 17 (JRE) para PySpark
├── docker-compose.yaml # Stack Airflow: Postgres, Redis, Webserver, Scheduler, Worker
├── main.py             # CLI Medallion (todos los subcomandos)
├── pytest.ini          # Config pytest (pythonpath = .)
└── requirements.txt    # Dependencias runtime (requests, boto3, pyspark, rapidfuzz, pyodbc…)
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

## 8. Uso — Capas Silver / Gold y orquestación (CLI)

Cada capa se ejecuta por separado o integrada vía `main.py`. El destino Gold por
defecto es **Parquet** (`data/gold/`), por lo que el flujo completo corre sin
SQL Server. SQL Server queda como opt-in (`--target sqlserver`).

> Requiere **Java JDK 21** en `JAVA_HOME` (PySpark) y `HADOOP_HOME` en Windows.

```powershell
# Activar entorno virtual
.venv\Scripts\Activate.ps1

# ── Flujo completo Bronze → Silver → Gold ────────────────────────
python main.py run-all                              # Parquet Gold (default)
python main.py run-all --target sqlserver --profile local  # carga al DW

# ── Por capas ─────────────────────────────────────────────────────
python main.py silver                              # 4 pasos: flatten+IA+CONOSCE→staging_flat
python main.py silver --years 2022 2023            # solo esos años
python main.py gold                                # Parquet data/gold/ + bi/ (6 tablas, sin SQL Server)
python main.py gold   --target sqlserver           # carga Star Schema en SQL Server + bi/

# ── Datos sintéticos (corre entre silver y gold) ─────────────────
python main.py synth                               # defaults: 2025=2176, 2024=2370
python main.py synth --adicional-rate 0.2          # más adendas para señal ML
python main.py gold  --target sqlserver            # carga los ~9 292 registros totales

# ── Reconstrucción / limpieza ────────────────────────────────────
python main.py silver --rebuild                    # limpia staging y reprocesa
python main.py gold   --rebuild --target sqlserver # re-DDL idempotente + carga
python main.py clean  silver                       # borra solo data/silver/staging_flat
python main.py clean  bronze --yes                 # borra Bronze (requiere --yes)
```

Lo que ejecuta cada capa:

- **Silver** (`silver`): aplana Bronze (una fila por ítem) → limpieza IA con Gemini
  → enriquecimiento CONOSCE (adendas, nombre de contrato, fecha de suscripción) →
  escribe Parquet particionado por `anio_fiscal` en `data/silver/staging_flat/`
  (overwrite **dinámico**: reprocesar un año no borra los demás).
- **Gold** (`gold`): lee `staging_flat` → construye `Dim_*` + `Fact` (SKs
  deterministas + fuzzy matching vectorizado) → carga en el destino elegido
  (`GoldTarget`): Parquet (`data/gold/`) o SQL Server (`stg.*` por JDBC →
  `oro.usp_Load_From_Staging`, carga atómica).

### Datos sintéticos (`synth`)

El Bronze real concentra la actividad en 2022-2023 (el `anio_fiscal` sale de la
fecha de convocatoria), por lo que **Silver casi no produce filas para 2024 (58)
ni 2025 (0)**; además los contratos OCDS reales no traen adendas
(`Monto_Adicional = 0` hasta que CONOSCE los enriquece). El comando `synth`
(separado de `silver`) genera datos sintéticos **fieles** a partir de
**`extra-data/CONOSCE_2025_essalud.csv`** (datos reales de EsSalud 2025, bienes):

- **2025**: ~2 176 filas extraídas del CSV (conserva las adendas reales).
- **2024**: se lleva a 2 370 filas (58 reales + bootstrap del Silver real, ocids
  realistas indistinguibles de los reales).
- **`monto_adicional` 2022-2024**: el modelo preserva los valores reales que
  CONOSCE ya enriqueció en Silver (JOIN por contrato); solo aplica la ocurrencia
  escalada (`--adicional-rate`, default `0.15`, ratios reales ≤ 25%) a las filas
  que aún tienen `monto_adicional = 0`. 2025 conserva sus adendas reales del CSV.

El resultado total en `staging_flat` es **~9 292 filas** (2022=2 274, 2023=2 472,
2024=2 370, 2025=2 176) listas para cargarse con `gold --target sqlserver`.

> **`data/bi/` en cualquier destino**: `python main.py gold` y
> `gold --target sqlserver` generan ambos las **6 tablas completas** en
> `data/bi/*.parquet` (`Dim_Tiempo`, `Dim_Ubigeo`, `Dim_Entidad_Compradora`,
> `Dim_Medicamento`, `Dim_Proveedor`, `Fact_Ordenes_Y_Contratos`).
> `Dim_Tiempo` y `Dim_Ubigeo` se construyen desde `static_dims.py` (sin leer
> SQL Server), por lo que cualquier equipo puede generar los Parquet BI con
> solo Spark. `--target sqlserver` además carga el esquema `oro` del DW.

---

## 9. Modelo estrella (Data Warehouse)

El esquema Gold vive en SQL Server bajo el esquema `oro` y se define en `star-schema/`:

| Archivo | Contenido |
|---|---|
| `EsSalud_StarSchema_DDL.sql` | Modelo estrella completo: dimensiones + tabla de hechos + vistas analíticas + validación post-carga |
| `EsSalud_Staging_DDL.sql` | DROP `stg.*` + `CREATE OR ALTER PROCEDURE oro.usp_Load_From_Staging` (carga atómica transaccional) |

**Dimensiones:** `Dim_Tiempo`, `Dim_Ubigeo`, `Dim_Entidad_Compradora`, `Dim_Medicamento`, `Dim_Proveedor`, `Dim_Tipo_Proceso`.  
**Hechos:** `Fact_Ordenes_Y_Contratos` (35 columnas, ~9 292 filas con datos sintéticos incluidos).

**Métricas de la Fact:**

| Columna | Descripción |
|---|---|
| `Num_Contrato` | Nombre oficial del contrato (del CONOSCE) |
| `Cantidad_Adjudicada` | Unidades adjudicadas por ítem |
| `Monto_Referencial_Soles` | Valor estimado de la convocatoria |
| `Monto_Adjudicado_Soles` | Valor de la buena pro |
| `Monto_Contratado_Item` | Valor firmado en contrato |
| `Monto_Adicional` | Suma de adendas aprobadas (CONOSCE) |
| `Monto_Reduccion` / `Monto_Prorroga` / `Monto_Complementario` | Otros ajustes contractuales (CONOSCE) |
| `Ratio_Sobrecosto_Pct` | `(Monto_Adicional / Monto_Contratado) * 100` |
| `Lead_Time_Total_Dias` | Días entre convocatoria y suscripción |
| `Flag_Contratacion_Directa` | 1 si es contratación directa |
| `Flag_Tiene_Adenda` | 1 si `Monto_Adicional > 0` |
| `Flag_Fuera_Petitorio` | 1 si el ítem no está en el Petitorio 2026 |

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
