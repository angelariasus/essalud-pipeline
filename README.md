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

> [!IMPORTANT]
> **Novedades 2026-07-02** (ver [CHANGELOG §10](CHANGELOG.md)):
> - **Fase 6 — Alertas operativas**: `python main.py alert` consolida riesgos (HHI crítico + Lead Time anómalo) en `bi/Alertas.parquet` y envía correo formal (RUC + medicamento + Red) — ver [§10](#10-modelado-predictivo-del-lead-time-fase-4--ml).
> - **Stack Docker completo**: `docker compose up -d` levanta Airflow **+ SQL Server (DW) + MailHog (SMTP de pruebas)**; el DAG `ocds_silver_pipeline` ahora corre Silver → **Synth** → Gold → alertas de punta a punta.
> - **Cloudflare** queda **solo como réplica opcional del Bronze (R2)**; el DW se trabaja **local o en contenedor** (`--profile local|docker`).
> - **Guía de ejecución paso a paso para el equipo**: [`docs/guia-ejecucion.md`](docs/guia-ejecucion.md) ← *empieza aquí si acabas de hacer pull*.

---

## Tabla de contenidos

1. [Características principales](#1-características-principales)
2. [Arquitectura](#2-arquitectura)
3. [Estructura del proyecto](#3-estructura-del-proyecto)
4. [Requisitos previos](#4-requisitos-previos)
5. [Instalación y configuración](#5-instalación-y-configuración)
6. [Variables de entorno](#6-variables-de-entorno)
7. [Uso — Capa Bronze (CLI)](#7-uso--capa-bronze-cli)
8. [Uso — Capas Silver / Gold y orquestación (CLI)](#8-uso--capas-silver--gold-y-orquestación-cli)
9. [Modelo estrella (Data Warehouse)](#9-modelo-estrella-data-warehouse)
10. [Modelado predictivo del Lead Time (Fase 4 — ML)](#10-modelado-predictivo-del-lead-time-fase-4--ml)
11. [Orquestación con Apache Airflow](#11-orquestación-con-apache-airflow)
12. [Tests y CI](#12-tests-y-ci)
13. [Layout de datos](#13-layout-de-datos)
14. [Notas de versionado](#14-notas-de-versionado)

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
- **Capa de ML predictiva (Fase 4):** modelo **XGBoost** que estima el *Lead Time* contractual (días entre convocatoria y suscripción) y publica las predicciones en `bi/Pred_Lead_Time.parquet` para Power BI — ver [§10](#10-modelado-predictivo-del-lead-time-fase-4--ml).

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
│   │   ├── synthetic_generator.py  # Datos sintéticos (2024 boost + 2025 CSV)
│   │   └── alerting.py             # ★ Fase 6: alertas HHI + lead time → bi/Alertas.parquet + correo SMTP
│   ├── storage/            # FileManager (disco) + R2Manager (Cloudflare R2)
│   └── utils/
│       ├── fuzzy_matcher.py    # Pandas UDFs RapidFuzz (medicamentos + redes)
│       └── cleaner.py          # Limpieza por capa (bronze/silver/gold)
├── dags/
│   ├── ocds_dag.py         # DAGs targeted (semanal) + bulk (mensual)
│   ├── silver_dag.py       # Silver → ★Synth → Gold → ★trigger ocds_alerting
│   └── alerting_dag.py     # ★ Fase 6: DAG ocds_alerting (correo tras Gold)
├── docs/
│   ├── guia-ejecucion.md       # ★ Runbook del equipo: terminal, Docker, Airflow, SQL Server
│   └── fase6-powerautomate.md  # ★ Guía institucional Power BI Service + Power Automate
├── star-schema/        # DDL del modelo estrella + DDL staging y stored procedure
├── extra-data/
│   ├── Contratos/          # xlsx CONOSCE por año (2022–2025, ~63 000 filas)
│   ├── CONOSCE_2025_essalud.csv  # Resumen EsSalud 2025 bienes (fuente synth)
│   └── gemini_cache.json   # Caché local de respuestas Gemini
├── test/               # Suite pytest (incluye tests de integración Spark + ★test_alerting.py)
├── mlpredicts/         # Fase 4 — modelo predictivo de Lead Time (notebook + modelo joblib + tests)
├── ml/                 # requirements.txt del entorno ML de la Fase 4 (xgboost, sklearn, jupyter)
├── bi/                 # Tablas Gold Parquet para BI (7 tablas + Pred_Lead_Time + ★Alertas.parquet)
├── data/               # Data Lake local (bronze/ · silver/ · gold/ · audit/) — ignorado por git
├── Dockerfile          # Imagen Airflow: Java 17 (JRE) + ★driver ODBC msodbcsql18 + ★pandas>=2.2
├── docker-compose.yaml # Stack: Airflow (Postgres/Redis/…) + ★SQL Server DW + ★MailHog
├── .env.example        # ★ Plantilla de variables (copiar a .env)
├── .env.docker         # ★ Dotenv del contenedor (sin rutas Windows; lo carga OCDS_ENV_FILE)
├── .dockerignore       # ★ Build context mínimo (solo requirements.txt)
├── main.py             # CLI Medallion (todos los subcomandos, incluye ★alert)
├── pytest.ini          # Config pytest (pythonpath = .)
└── requirements.txt    # Dependencias runtime (requests, boto3, pyspark, rapidfuzz, pyodbc…)
```

> ★ = nuevo o modificado en la actualización 2026-07-02 (CHANGELOG §10).

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
python -m venv .venv
.\.venv\Scripts\Activate.ps1

# 2. Instalar dependencias de runtime
pip install -r requirements.txt

# 3. (Opcional) Dependencias de desarrollo: tests y lint
pip install -r requirements-dev.txt

# 4. Crear tu .env desde la plantilla y editarlo
Copy-Item .env.example .env
#    (ver la sección de Variables de entorno; los defaults funcionan con el
#     stack Docker local sin tocar nada más que JAVA_HOME/HADOOP_HOME)
```

> El archivo `.env` está en la raíz y **no se versiona** (`.gitignore`); la plantilla
> versionada es **`.env.example`**. El contenedor de Airflow usa su propio dotenv
> (**`.env.docker`**, sin rutas Windows) seleccionado vía `OCDS_ENV_FILE`.
>
> **Guía completa de ejecución para el equipo:** [`docs/guia-ejecucion.md`](docs/guia-ejecucion.md).

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
| `OCDS_DW_JDBC_URL` / `USER` / `PASSWORD` / `BATCHSIZE` | Conexión JDBC para los writes de Spark. **Con `trusted_connection=yes` (Windows Auth) son OBLIGATORIAS**: Spark/JDBC no hereda la autenticación de Windows y el pipeline aborta temprano si faltan |
| `OCDS_DW_*_DOCKER` (`CONN_STRING` / `JDBC_URL` / `JDBC_USER` / `JDBC_PASSWORD`) | ★ Cadenas del perfil `--profile docker`: apuntan al contenedor `sqlserver` (host: `localhost:11433`) |
| `MSSQL_SA_PASSWORD` | ★ Password `sa` del SQL Server en contenedor (default `EsSalud2024!`, solo dev local) |
| `SMTP_HOST` / `PORT` / `USER` / `PASSWORD` / `STARTTLS` / `FROM` / `TO` | ★ Fase 6 — envío del correo de alertas. Default: MailHog local (`localhost:1025`, sin TLS). Para Gmail: `smtp.gmail.com:587` + App Password |
| `OCDS_ENV_FILE` | ★ Dotenv alternativo (el compose lo fija a `.env.docker` dentro del contenedor para no cargar rutas Windows) |
| `GEMINI_API_KEY` / `GEMINI_MODEL` | Limpieza IA en Silver (vacío = no-op con degradación del fuzzy match) |

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

# ── Fase 6: alertas de abastecimiento (tras gold) ────────────────
python main.py alert --dry-run                     # bi/Alertas.parquet + preview del correo
python main.py alert                               # envío real (SMTP del .env)
python main.py alert --source hhi --to correo@x.y  # solo HHI, destinatario puntual

# ── Reconstrucción / limpieza ────────────────────────────────────
python main.py silver --rebuild                    # limpia staging y reprocesa
python main.py gold   --rebuild --target sqlserver # re-DDL idempotente + carga
python main.py clean  silver                       # borra solo data/silver/staging_flat
python main.py clean  bronze --yes                 # borra Bronze (requiere --yes)
```

> ⚠️ **Orden canónico**: `silver → synth → gold` (→ `alert`). Saltarse `synth`
> deja 2024/2025 casi vacíos y rompe el contrato de 9 292 filas que asume el ML.
> Regenerar los datos **siempre desde el host Windows**: el `synth` del contenedor
> muestrea un subconjunto distinto (mismo total, otros ítems) y desalinea
> `Pred_Lead_Time.parquet` — ver CHANGELOG §10.5.

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

## 10. Modelado predictivo del Lead Time (Fase 4 — ML)

Una capa de **Machine Learning aditiva** —no toca el pipeline— que estima el **Lead Time
contractual** (días entre la convocatoria y la suscripción del contrato) de cada ítem,
incluidos los procesos 2024-2025 aún en curso. Vive en `mlpredicts/` y consume las tablas
Parquet de `bi/` producidas por la capa Gold.

> [!NOTE]
> Esta capa usa el venv de ML (`.venv`, `ml/requirements.txt`: pandas, scikit-learn,
> **xgboost**), **no** el entorno de Spark. Requiere haber ejecutado `python main.py gold`.

**Modelo.** XGBoost vs. Random Forest, seleccionados por RMSE con validación cruzada
estratificada (5 folds por Red Asistencial). Gana **XGBoost**, con el objetivo modelado en
escala **log1p** (`TransformedTargetRegressor`) para que las predicciones sean siempre ≥ 0
(ningún proceso competitivo predicho en 0 días). Métricas: **RMSE ≈ 55 d · MAE 15.8 d · R² ≈ 0.85**.

**Entregable:** `bi/Pred_Lead_Time.parquet` (9 292 filas) — histórico real + predicho por
proceso, listo para la Vista Táctica de Power BI.

| Columna | Descripción |
|---|---|
| `ID_Registro` | Clave estable por fila (para relaciones en Power BI) |
| `Anio_Fiscal` · `Red_Asistencial` · `Categoria_Proceso` | Dimensiones de corte |
| `Lead_Time_Actual` | Días reales (`NaN` si falta una fecha o son inconsistentes) |
| `Lead_Time_Predicho` | Predicción del modelo (siempre ≥ 0) |
| `Residual` | `Actual − Predicho` (`NaN` si no hay Actual) |

```powershell
# Registrar el .venv como kernel de Jupyter (una sola vez)
.venv\Scripts\python.exe -m ipykernel install --user --name essalud --display-name "EsSalud .venv"

# Ejecutar el notebook de punta a punta (regenera parquet + modelo)
.venv\Scripts\python.exe -m jupyter nbconvert --to notebook --execute --inplace --ExecutePreprocessor.kernel_name=essalud mlpredicts\LeadTime_Predictor.ipynb

# Verificar el entregable (10 comprobaciones; .venv no trae pytest → se corre como script)
$env:PYTHONUTF8 = 1; .venv\Scripts\python.exe mlpredicts\test_pred_lead_time.py
```

El plan completo y la guía de integración con Power BI están en
[`fase4-lead-time-predictivo.md`](fase4-lead-time-predictivo.md).

### Fase 6 — Alertas operativas de abastecimiento

Capa de respuesta operativa que consolida dos fuentes de riesgo en
**`bi/Alertas.parquet`** y notifica por **correo formal** (RUC del proveedor
dominante + medicamento + Red Asistencial) al área de abastecimiento:

- **HHI crítico** (réplica pandas de `oro.vw_Matriz_Riesgo_HHI`): mercados con
  HHI ≥ 8000, medicamento de uso restringido y proveedor dominante ≥ 80%.
- **Lead Time anómalo** (Fase 4): procesos cuyo residual (real − predicho)
  excede `media + 2σ`.

```powershell
# Vista previa sin enviar (genera bi/Alertas.parquet e imprime el correo)
python main.py alert --dry-run

# Envío real (SMTP del .env: MailHog local o Gmail App Password)
python main.py alert --to fernando.barrera@unmsm.edu.pe
python main.py alert --source hhi --limit 30   # solo HHI, hasta 30 filas en el correo
```

SMTP se configura en `.env` (`SMTP_HOST/PORT/USER/PASSWORD/STARTTLS/FROM/TO`).
Para pruebas sin credenciales, el stack Docker incluye **MailHog**
(`docker compose up -d mailhog`, UI en <http://localhost:8025>). En Airflow, el
DAG **`ocds_alerting`** corre automáticamente tras Gold. La variante
institucional sin código (Power BI Service + Power Automate) está documentada en
[`docs/fase6-powerautomate.md`](docs/fase6-powerautomate.md).

---

## 11. Orquestación con Apache Airflow

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
| `ocds_silver_pipeline` | mensual | Bronze → Silver → **Synth** → Gold (Spark local dentro del worker); acepta `years`/`target`/`profile`/`skip_synth` vía `dag_run.conf`; al terminar dispara `ocds_alerting` |
| `ocds_alerting` | on-trigger | Fase 6: consolida `bi/Alertas.parquet` y envía el correo de alertas (SMTP → MailHog en el stack) |

**Servicios del stack** (además de los propios de Airflow):

| Servicio | Puerto host | Función |
|---|---|---|
| `sqlserver` | `11433` → 1433 | SQL Server 2022 (DW en contenedor, volumen persistente) |
| `sqlserver-init` | — | One-shot: crea la BD `DW_EsSalud_Adquisiciones` |
| `mailhog` | UI `8025` · SMTP `1025` | Captura los correos de la Fase 6 sin credenciales |

**Comandos de operación y revisión:**

```powershell
# Estado de los servicios (todos deben quedar healthy)
docker compose ps

# Disparar el pipeline completo y las alertas manualmente
docker exec essalud-pipeline-airflow-scheduler-1 airflow dags trigger ocds_silver_pipeline
docker exec essalud-pipeline-airflow-scheduler-1 airflow dags trigger ocds_alerting

# Revisar corridas y errores de import de DAGs
docker exec essalud-pipeline-airflow-scheduler-1 airflow dags list-runs -d ocds_silver_pipeline
docker exec essalud-pipeline-airflow-scheduler-1 airflow dags list-import-errors

# Log de una tarea (la ruta se arma con dag_id/run_id/task_id)
docker exec essalud-pipeline-airflow-worker-1 bash -c "ls /opt/airflow/logs/dag_id=ocds_silver_pipeline/"
```

**Detalles de entorno del contenedor** (configurados en `docker-compose.yaml`, no tocar sin motivo):

- `OCDS_ENV_FILE=/opt/airflow/bi/.env.docker` — dentro del contenedor `settings.py`
  carga `.env.docker` en lugar del `.env` de Windows (cuyo `JAVA_HOME`/`HADOOP_HOME`
  con rutas `C:\` romperían Spark en Linux).
- `PYTHONPATH=/opt/airflow/bi` — los workers Python de Spark no heredan el
  `sys.path` del driver; sin esto las UDFs fallan con `No module named 'app'`.
- `AIRFLOW__CELERY__OPERATION_TIMEOUT=30` — el default (1 s) corta el primer
  envío a Celery en frío y deja el módulo `redis` corrupto (todo DAG fallaría
  con *"task killed externally"*).

---

## 12. Tests y CI

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

## 13. Layout de datos

```text
data/
├── bronze/
│   ├── records/        # JSON individuales de la extracción targeted (records/<ruc>/<year>/)
│   └── bulk_files/     # ZIPs crudos y catálogos descomprimidos
├── silver/
│   └── staging_flat/   # Salida Spark aplanada, Parquet particionado por anio_fiscal
├── gold/               # Modelo estrella materializado en Parquet (target default)
└── audit/
    └── executions/     # ocds_extraction.log (nivel debug)

bi/                     # (raíz del repo, gitignored) Parquet para Power BI:
├── Dim_*.parquet            # 6 dimensiones
├── Fact_Ordenes_Y_Contratos.parquet  # 9 292 filas
├── Pred_Lead_Time.parquet    # Fase 4 (lo genera el notebook de mlpredicts/)
└── Alertas.parquet           # Fase 6 (lo genera `python main.py alert`)
```

Todo el contenido de `data/` se **genera automáticamente** al ejecutar el pipeline (las rutas se crean solas), por lo que no se versiona.

---

## 14. Notas de versionado

Para mantener el repositorio limpio, los siguientes elementos están en `.gitignore` y **no se suben** al remoto:

- **`data/`** completo (`bronze/`, `silver/`, `gold/`, `audit/`) y **`bi/`** — artefactos generados y regenerables por el pipeline (ver [`docs/guia-ejecucion.md`](docs/guia-ejecucion.md) §2 para regenerarlos tras el pull).
- **Notebooks** (`*.ipynb`), incluido `mlpredicts/LeadTime_Predictor.ipynb` — el modelo serializado y los tests sí se versionan.
- **Archivos de instrucciones de IA:** `CLAUDE.md`, `AGENTS.md`, `GEMINI.md` — son locales; cada quien los edita sin afectar el repo.
- **`.env`** y credenciales.

Archivos de configuración/documentación que **sí** se versionan: `README.md`, `CHANGELOG.md` (historial detallado), `fase4-lead-time-predictivo.md`, **`docs/guia-ejecucion.md`** (runbook del equipo), **`docs/fase6-powerautomate.md`**, **`.env.example`** (plantilla sin secretos) y **`.env.docker`** (dotenv del contenedor, solo credenciales dev del stack local).
