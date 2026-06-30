# Pipeline EsSalud — Arquitectura Medallion

Pipeline ELT para el análisis de adquisiciones de EsSalud (Perú), construido sobre la **Arquitectura Medallion** (Bronze → Silver → Gold). Extrae contratos públicos del estándar OCDS, los transforma con Apache Spark y los carga en un Data Warehouse de SQL Server para su consumo en BI y modelos de Machine Learning.

---

## Visión general

```
API OCDS (SEACE)
      │
      ▼
┌─────────────┐     JSON por contrato     ┌──────────────────────┐
│   BRONZE    │ ─────────────────────────▶│  data/bronze/records │
│  Extracción │                           │  /20131257750/{año}/ │
└─────────────┘                           └──────────────────────┘
                                                     │
                                                     ▼
┌─────────────┐   Spark: aplanar + AI    ┌──────────────────────┐
│   SILVER    │ ─────────────────────────▶│  data/silver/        │
│Transformación│  clean + fuzzy match     │  staging_flat/       │
└─────────────┘                           │  (Parquet por año)   │
                                          └──────────────────────┘
                                                     │
                                                     ▼
┌─────────────┐  Spark JDBC + stored proc ┌──────────────────────┐
│    GOLD     │ ─────────────────────────▶│  SQL Server          │
│  DW / BI   │                           │  DW_EsSalud_         │
└─────────────┘                           │  Adquisiciones       │
                                          └──────────────────────┘
```

---

## Capa Bronze — Extracción

**Objetivo:** descargar los contratos públicos de EsSalud desde la API OCDS de SEACE y persistirlos como JSON sin transformación.

### Fuente de datos

| Campo | Valor |
|---|---|
| API | `https://contratacionesabiertas.oece.gob.pe/api/v1` |
| Estándar | OCDS (Open Contracting Data Standard) |
| RUC EsSalud | `20131257750` |

### Proceso

1. `OCDSClient` realiza peticiones GET con reintentos exponenciales (hasta 5 intentos, backoff 0.5s) ante errores 429/5xx.
2. `BronzePipeline` pagina la API y guarda cada release OCDS como un archivo JSON individual.
3. Los archivos se organizan en `data/bronze/records/{RUC}/{año}/{ocid}.json`.

### Modos de ingesta

```
# Ingesta dirigida por RUC y año
python main.py targeted --ruc 20131257750 --year 2022 --limit 100

# Ingesta masiva por fuente y período
python main.py bulk --source SEACE --type JSON --year 2022 --month 6
```

### Archivos clave

| Archivo | Rol |
|---|---|
| [main.py](main.py) | CLI de entrada a la capa Bronze |
| [app/clients/ocds_client.py](app/clients/ocds_client.py) | Cliente HTTP resiliente |
| [app/pipelines/bronze_layer.py](app/pipelines/bronze_layer.py) | Orquestador de extracción |
| [app/services/extractors.py](app/services/extractors.py) | Estrategias de extracción (targeted / bulk) |
| [app/storage/file_manager.py](app/storage/file_manager.py) | Escritura local de JSON |
| [app/storage/r2_manager.py](app/storage/r2_manager.py) | Escritura opcional en Cloudflare R2 |

---

## Capa Silver — Transformación

**Objetivo:** convertir los JSON crudos en un DataFrame tabular limpio y enriquecido, listo para construir el modelo dimensional.

### Pasos del pipeline

#### Paso 1 — Carga de maestros (pandas)

Se cargan dos archivos de referencia desde `extra-data/`:

- **Petitorio Farmacológico 2026** (`.xls`): ~1 400 medicamentos con Denominación Común Internacional (DCI), código SAP y especificaciones técnicas.
- **Relación de Establecimientos Jul 2025** (`.xlsx`): ~800 centros asistenciales con su red, departamento y tipo.

#### Paso 2 — Aplanamiento Bronze con Spark

`ocds_flattener.py` lee los JSON con un esquema estricto en modo `PERMISSIVE`, separando los registros corruptos (no aborta el job). Luego aplana la estructura anidada OCDS mediante `explode_outer` de ítems y resuelve la cascada award → contract por ítem usando funciones de orden superior de Spark (`filter`, `exists`, `aggregate`).

El grano de salida es **una fila por ítem de licitación** con los campos provenientes del JSON OCDS:

| Grupo | Campos |
|---|---|
| Identificación | `ocid`, `n_item`, `codigoconvocatoria`, `n_cod_contrato` |
| Descripción | `descripcion_item`, `cantidad`, `unidad_medida` |
| Fechas | `fecha_convocatoria`, `fecha_buena_pro`, `fecha_suscripcion` |
| Montos | `monto_referencial`, `monto_adjudicado`, `monto_contratado` |
| Comprador | `ruc_comprador`, `nombre_comprador`, `departamento_comprador` |
| Proveedor | `ruc_proveedor`, `nombre_proveedor` |
| Proceso | `metodo_contratacion`, `detalles_metodo`, `es_contratacion_directa`, `tiene_adenda` |
| Red | `red_candidato`, `red_metodo` |

#### Paso 2.5 — Limpieza con IA (Gemini)

`GeminiCleaner` toma las descripciones únicas del campo `descripcion_item` y las envía en lotes de 50 a la API de Gemini, que las normaliza al nombre genérico DCI (ej: `"PARACETAML TABL 500"` → `"PARACETAMOL 500 MG"`). Los resultados se cachean localmente en `extra-data/gemini_cache.json` para evitar llamadas repetidas.

Requiere configurar `GEMINI_API_KEY` en el `.env`. Si no está configurada, el pipeline continúa sin limpiar.

#### Paso 3 — Enriquecimiento CONOSCE (Spark)

`conosce_enricher.py` carga los archivos Excel de `extra-data/Contratos/` (un xlsx por año disponible: 2022–2025, ~63 000 filas en total) y hace un LEFT JOIN sobre `(codigoconvocatoria, n_cod_contrato)`. Este paso añade las columnas que la API OCDS no reporta:

| Columna | Descripción |
|---|---|
| `num_contrato` | Nombre oficial del contrato (hasta 200 caracteres) |
| `monto_adicional` | Suma de adendas aprobadas |
| `monto_reduccion` | Reducciones de contrato |
| `monto_prorroga` | Prórrogas de contrato |
| `monto_complementario` | Contratos complementarios |
| `fecha_suscripcion` | Fallback cuando la API OCDS no la reporta |
| `tiene_resolucion` | Indicador SI/NO de resolución aprobatoria (solo en Silver; no sube a Gold) |

El Parquet resultante se escribe en `data/silver/staging_flat/` con **overwrite dinámico por partición** (`anio_fiscal`): procesar un año no borra los demás.

#### Paso 4 — Fuzzy matching y construcción de dimensiones (Spark)

`dim_resolver.py` construye tres dimensiones y resuelve las claves foráneas de la tabla de hechos:

- **Dim_Proveedor**: una fila por RUC único; clasifica como Persona Natural (RUC 10x), Persona Jurídica (RUC 20x) o Consorcio. SK generada con `row_number()`.
- **Dim_Medicamento**: fuzzy match vectorizado (RapidFuzz) entre las descripciones SEACE y el Petitorio 2026. Clasifica cada ítem como EXACTO, FUZZY, HISTORICO o no farmacológico.
- **Dim_Entidad_Compradora**: extrae la Red Asistencial desde el título del proceso usando regex y fuzzy match contra el maestro de establecimientos; resuelve el departamento dominante de cada red.

### Archivos clave

| Archivo | Rol |
|---|---|
| [app/pipelines/silver_layer.py](app/pipelines/silver_layer.py) | Orquestador Silver (4 pasos) |
| [app/services/ocds_flattener.py](app/services/ocds_flattener.py) | Aplanamiento Spark + reader de staging |
| [app/services/ai_cleaner.py](app/services/ai_cleaner.py) | Limpieza con Gemini API |
| [app/services/conosce_enricher.py](app/services/conosce_enricher.py) | Enriquecimiento con Excel CONOSCE |
| [app/services/dim_resolver.py](app/services/dim_resolver.py) | Dimensiones + Fact con Spark |
| [app/utils/fuzzy_matcher.py](app/utils/fuzzy_matcher.py) | Pandas UDFs de fuzzy match (RapidFuzz) |
| [app/loaders/master_loader.py](app/loaders/master_loader.py) | Carga de Petitorio y Establecimientos |
| [app/config/spark_session.py](app/config/spark_session.py) | SparkSession singleton |

---

## Capa Gold — Data Warehouse

**Objetivo:** cargar el modelo dimensional en SQL Server de forma transaccional y atómica.

### Modelo estrella

```
                    Dim_Tiempo (SK=YYYYMMDD)
                         │
          Dim_Ubigeo ────┤
                         │
Dim_Entidad_Compradora ──┼──── Fact_Ordenes_Y_Contratos ────── Dim_Medicamento
                         │
         Dim_Proveedor ──┤
                         │
       Dim_Tipo_Proceso ─┘
```

**Fact_Ordenes_Y_Contratos** — métricas principales (35 columnas):

| Métrica | Descripción |
|---|---|
| `Num_Contrato` | Nombre oficial del contrato (CONOSCE) |
| `Cantidad_Adjudicada` | Unidades adjudicadas por ítem |
| `Monto_Referencial_Soles` | Valor estimado de la convocatoria |
| `Monto_Adjudicado_Soles` | Valor de la buena pro |
| `Monto_Contratado_Item` | Valor firmado en contrato |
| `Monto_Adicional` | Suma de adendas aprobadas (CONOSCE) |
| `Monto_Reduccion` | Reducciones de contrato (CONOSCE) |
| `Monto_Prorroga` | Prórrogas de contrato (CONOSCE) |
| `Monto_Complementario` | Contratos complementarios (CONOSCE) |
| `Ratio_Sobrecosto_Pct` | `(Monto_Adicional / Monto_Contratado) * 100` |
| `Lead_Time_Total_Dias` | Días entre convocatoria y suscripción |
| `Flag_Contratacion_Directa` | 1 si es contratación directa |
| `Flag_Tiene_Adenda` | 1 si `Monto_Adicional > 0` |
| `Flag_Fuera_Petitorio` | 1 si el ítem no está en el petitorio 2026 |

### Estrategia de carga (fail-safe)

1. **DDL idempotente**: `EsSalud_StarSchema_DDL.sql` (DROP + CREATE atómico sobre `oro.*`) y `EsSalud_Staging_DDL.sql` (DROP `stg.*` + `CREATE OR ALTER PROCEDURE`). El DROP previo del staging garantiza que SQL Server no valide el cuerpo del SP contra una tabla antigua.
2. **Staging JDBC**: Spark escribe cada DataFrame por JDBC a tablas `stg.*`. Si una escritura falla, la producción no se toca.
3. **Carga atómica**: se ejecuta `oro.usp_Load_From_Staging`, que mueve los datos de staging a producción dentro de una única transacción con `ROLLBACK` ante cualquier error.

### Archivos clave

| Archivo | Rol |
|---|---|
| [app/pipelines/gold_layer.py](app/pipelines/gold_layer.py) | Orquestador Gold (lee staging → dims → target) |
| [app/loaders/targets/](app/loaders/targets/) | Abstracción de destinos: `ParquetGoldTarget`, `SqlServerTarget` |
| [app/loaders/dw_loader.py](app/loaders/dw_loader.py) | Escritura JDBC + ejecución del stored procedure |
| [star-schema/EsSalud_StarSchema_DDL.sql](star-schema/EsSalud_StarSchema_DDL.sql) | DDL del modelo estrella en SQL Server |
| [star-schema/EsSalud_Staging_DDL.sql](star-schema/EsSalud_Staging_DDL.sql) | DDL del esquema staging + stored procedure |

---

## Cómo ejecutar el pipeline completo

### Prerrequisitos

| Requisito | Versión | Notas |
|---|---|---|
| Python | 3.12 | Entorno virtual `.venv/` |
| Java (JDK) | 21 | `JAVA_HOME` en `.env` |
| Hadoop winutils | — | Solo Windows; `HADOOP_HOME` en `.env` |
| SQL Server | 2019+ | Solo para la capa Gold |
| GEMINI_API_KEY | — | Opcional; activa limpieza con IA |

### Variables de entorno (`.env`)

```env
# SQL Server (capa Gold)
OCDS_DW_CONN_STRING=mssql+pyodbc://localhost/DW_EsSalud_Adquisiciones?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes
OCDS_DW_JDBC_URL=jdbc:sqlserver://localhost;databaseName=DW_EsSalud_Adquisiciones;encrypt=true;trustServerCertificate=true
OCDS_DW_JDBC_USER=essalud_user
OCDS_DW_JDBC_PASSWORD=EsSalud2024!

# Spark + JDBC driver
OCDS_SPARK_JARS_PACKAGES=com.microsoft.sqlserver:mssql-jdbc:12.4.2.jre11
JAVA_HOME=C:\Program Files\Java\jdk-21
HADOOP_HOME=C:\hadoop

# IA (opcional)
GEMINI_API_KEY=AIza...
GEMINI_MODEL=gemini-2.5-flash
```

### Comandos (CLI Medallion)

Cada capa se ejecuta de forma independiente o integrada. El destino Gold por
defecto es **Parquet** (`data/gold/`), sin necesidad de SQL Server.

```powershell
# Activar entorno virtual (Windows)
.venv\Scripts\Activate.ps1

# ──────────────────────────────────────────────────────────────────
# FLUJO COMPLETO (Bronze → Silver → Gold)
# ──────────────────────────────────────────────────────────────────
python main.py run-all                                  # Parquet Gold (default)
python main.py run-all --target sqlserver --profile local  # carga al DW

# ──────────────────────────────────────────────────────────────────
# POR CAPAS (flujo recomendado para desarrollo)
# ──────────────────────────────────────────────────────────────────

# 1. Bronze — ingesta por año (--limit 0 = todos los registros)
python main.py bronze --years 2022 2023 2024 2025
python main.py bronze --years 2024 --limit 50         # prueba rápida

# 2. Silver — aplana + limpia IA + enriquece CONOSCE → staging_flat
python main.py silver                                  # todos los años del default
python main.py silver --years 2022 2023

# 3. Gold — lee staging_flat → dimensiones → destino
python main.py gold                                    # Parquet en data/gold/
python main.py gold --target sqlserver                 # carga DW (Star Schema)
python main.py gold --target sqlserver --profile local # equivalente a anterior

# ──────────────────────────────────────────────────────────────────
# DATOS SINTÉTICOS (comando separado; corre después de silver)
# ──────────────────────────────────────────────────────────────────
# Genera datos para enriquecer staging_flat antes de correr gold:
#   2025 → ~2 176 filas reales de EsSalud desde extra-data/CONOSCE_2025_essalud.csv
#   2024 → boost a 2 370 filas (58 reales + bootstrap del Silver real)
#   2022/2023 → añade monto_adicional sintético (ocurrencia ~15%, ratio real ≤25%)
python main.py synth                                   # defaults
python main.py synth --adicional-rate 0.2              # más adendas para señal ML
python main.py synth --n-2025 2000 --n-2024 2200       # ajuste de conteos

# Luego cargar Gold (consume los 9 292 registros totales):
python main.py gold --target sqlserver

# ──────────────────────────────────────────────────────────────────
# RECONSTRUCCIÓN / LIMPIEZA POR CAPA
# ──────────────────────────────────────────────────────────────────
python main.py silver --rebuild                        # limpia staging y reprocesa
python main.py gold   --rebuild --target sqlserver     # re-DDL idempotente + recarga
python main.py clean  silver                           # borra data/silver/staging_flat
python main.py clean  gold --target parquet            # borra data/gold/
python main.py clean  bronze --yes                     # borra Bronze (requiere --yes)

# ──────────────────────────────────────────────────────────────────
# COMPATIBILIDAD HISTÓRICA (sin cambios respecto a versiones previas)
# ──────────────────────────────────────────────────────────────────
python main.py targeted --year 2022 --limit 100
python main.py bulk --source SEACE --year 2022 --month 6
```

| Destino Gold | Flag | Produce | Requiere |
|---|---|---|---|
| Parquet (default) | `--target parquet` | `data/gold/` + **`data/bi/` (6 tablas)** | nada (solo Spark) |
| SQL Server local | `--target sqlserver --profile local` | esquema `oro` (6 tablas) **+ `bi/` Parquet** | instancia en `localhost` |
| SQL Server Docker | `--target sqlserver --profile docker` | igual que local | vars `*_DOCKER` en `.env` |

> **`data/bi/` en ambos destinos**: `python main.py gold` y
> `gold --target sqlserver` producen los mismos 6 archivos en `data/bi/`
> (`Dim_Tiempo.parquet`, `Dim_Ubigeo.parquet`, `Dim_Entidad_Compradora.parquet`,
> `Dim_Medicamento.parquet`, `Dim_Proveedor.parquet`, `Fact_Ordenes_Y_Contratos.parquet`).
> `Dim_Tiempo` (spine 2010–2030, 7 671 filas) y `Dim_Ubigeo` (25 dptos. + centinela)
> se generan en memoria desde `static_dims.py` — no se leen desde SQL Server —
> por lo que los Parquet BI son reproducibles en cualquier entorno con solo Spark.

> **Nota — datos sintéticos**: el Bronze real concentra la actividad en 2022-2023
> (el `anio_fiscal` sale de la fecha de convocatoria), por lo que Silver casi no
> produce filas para 2024 (58) ni 2025 (0); además los contratos OCDS reales no
> traen adendas (`Monto_Adicional = 0`). El comando `synth` genera datos
> sintéticos **fieles** a partir de **`extra-data/CONOSCE_2025_essalud.csv`** (data
> real de EsSalud 2025, bienes):
> - **2025**: se extrae del CSV (~2 176 filas), conservando las adendas reales.
> - **2024**: se lleva a 2 370 filas (reales + bootstrap del Silver real, con ocids
>   realistas indistinguibles de los reales).
> - **`monto_adicional` 2022-2024**: el generador **preserva** los valores que
>   CONOSCE ya enriqueció en Silver (JOIN por contrato, ~3–5% de filas). El modelo
>   de ocurrencia escalada (`--adicional-rate`, default 0.15, ratios reales ≤ 25%)
>   se aplica **solo a las filas con `monto_adicional = 0`**, sin sobrescribir los
>   reales. Las filas bootstrap de 2024 parten de `monto_adicional = 0` para que el
>   modelo las trate desde cero. 2025 conserva sus adendas reales del CSV.

---

## Módulo ML (`ml/`)

Proyecto independiente que consume los datos del DW para entrenar modelos GBDT de gestión de riesgo en adquisiciones. Incluye una aplicación Streamlit para visualización de resultados.

Modelos implementados: **XGBoost**, **LightGBM**, **CatBoost**.

```powershell
cd ml
pip install -r requirements.txt
streamlit run app.py
```

---

## Estructura del repositorio

```
essalud-pipeline/
├── app/
│   ├── clients/            # OCDSClient (HTTP resiliente, retry/backoff)
│   ├── config/             # settings.py (env vars) + spark_session.py (singleton)
│   ├── loaders/
│   │   ├── master_loader.py    # Petitorio + Establecimientos (Excel → pandas)
│   │   ├── dw_loader.py        # DDL idempotente + JDBC staging + SP atómico
│   │   └── targets/            # Abstracción de destinos Gold
│   │       ├── base.py             # GoldTarget (ABC)
│   │       ├── parquet_target.py   # ParquetGoldTarget (default, sin SQL Server)
│   │       └── sqlserver_target.py # SqlServerTarget (envuelve dw_loader)
│   ├── models/             # Dataclasses de dominio
│   ├── pipelines/
│   │   ├── bronze_layer.py     # BronzePipeline (extracción)
│   │   ├── silver_layer.py     # SilverPipeline (4 pasos: flatten→IA→CONOSCE→parquet)
│   │   └── gold_layer.py       # GoldPipeline (read_staging → dims → target.load)
│   ├── services/
│   │   ├── extractors.py           # Targeted + Bulk extractors
│   │   ├── ocds_flattener.py       # Aplanamiento Spark + read_staging()
│   │   ├── ai_cleaner.py           # Limpieza con Gemini API
│   │   ├── conosce_enricher.py     # Enriquecimiento con Excel CONOSCE (SEACE)
│   │   ├── dim_resolver.py         # Dimensiones + Fact (fuzzy match, SKs)
│   │   ├── static_dims.py          # Dim_Tiempo y Dim_Ubigeo como DataFrames Spark + export_dims_to_bi()
│   │   └── synthetic_generator.py  # Datos sintéticos (2024 boost + 2025 CSV)
│   ├── storage/            # FileManager (local) y R2Manager (Cloudflare R2)
│   └── utils/
│       ├── fuzzy_matcher.py    # Pandas UDFs RapidFuzz (medicamentos + redes)
│       └── cleaner.py          # Limpieza por capa (bronze/silver/gold)
├── dags/               # DAGs de Apache Airflow (orquestación Docker)
├── extra-data/
│   ├── Contratos/          # xlsx CONOSCE por año (2022–2025, ~63 000 filas)
│   ├── CONOSCE_2025_essalud.csv  # Resumen EsSalud 2025 bienes (fuente synth 2025)
│   └── gemini_cache.json   # Caché local de respuestas Gemini
├── ml/                 # Modelos GBDT + app Streamlit (XGBoost/LightGBM/CatBoost)
├── star-schema/
│   ├── EsSalud_StarSchema_DDL.sql  # Modelo estrella (DROP+CREATE idempotente)
│   └── EsSalud_Staging_DDL.sql     # stg.* + oro.usp_Load_From_Staging (SP atómico)
├── test/               # Suite pytest (unitario + integración Spark)
├── main.py             # CLI Medallion (todos los subcomandos)
├── Dockerfile          # Imagen Airflow + Java 17
├── docker-compose.yaml # Stack completo (Airflow + Redis + PostgreSQL)
└── .env                # Variables de entorno locales (no versionado)
```
