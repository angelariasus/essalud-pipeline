# Plan de Implementación — Bronze → Silver → DW

Proyecto: **Arquitectura BI para la Evaluación Predictiva de Eficiencia en Adquisiciones de Medicamentos EsSalud**  
Alcance: Fases 1–3 (Bronze ingestion, Silver ETL, Gold/DW star schema)

---

## Inventario de Cambios

### Archivos a modificar

| Archivo | Cambio |
|---|---|
| `app/services/extractors.py` | Agregar filtro por `mainProcurementCategory` en `paginate_records()` |
| `app/pipelines/bronze_layer.py` | Agregar proyección de campos antes de `save_json()` |
| `app/config/settings.py` | Agregar `SILVER_DIR`, `EXTRA_DATA_DIR`, `DW_CONN_STRING` |
| `requirements.txt` | Agregar nuevas dependencias Silver |

### Archivos nuevos

| Archivo | Propósito |
|---|---|
| `app/loaders/master_loader.py` | Lee Petitorio (.xls) y Establecimientos (.xlsx) |
| `app/utils/fuzzy_matcher.py` | Match fuzzy: medicamentos vs DCI, comprador vs Red Asistencial |
| `app/services/ocds_flattener.py` | JSON Bronze anidado → DataFrame plano por ítem |
| `app/services/dim_resolver.py` | Construye todas las dimensiones y resuelve FKs |
| `app/loaders/dw_loader.py` | INSERT a SQL Server en orden de dependencia FK |
| `app/pipelines/silver_layer.py` | Orquestador Silver (encadena todos los pasos) |
| `dags/silver_dag.py` | DAG Airflow para el pipeline Silver |

---

## Paso 0 — Inspección de campo

**Tipo:** Ejecución manual, sin cambios de código.

```bash
python main.py targeted --limit 10
```

**Qué verificar en los JSONs generados en `data/bronze/records/`:**

1. Valor de `compiledRelease.tender.mainProcurementCategory`  
   → Confirmar si es `"goods"`, `"medicine"`, `"pharmaceuticals"` u otro  
   → Ese valor se hardcodea como `PHARMA_CATEGORIES` en el Paso 1

2. Campo `compiledRelease.parties[role=buyer].id` y `.name`  
   → Cuántos RUC/nombres distintos de compradores aparecen  
   → Escenario A: múltiples RUCs por Red → fuzzy match por nombre  
   → Escenario B: un solo RUC central → usar nombre de `procuringEntity`

3. Si existe `compiledRelease.tender.items[i].deliveryLocation`  
   → Si existe: usar ese departamento directamente (anula el fallback del Gap 3)  
   → Si no existe: confirmar fallback vía departamento del comprador

4. Cómo se vinculan `awards[]` con `tender.items[]`  
   → Por `relatedLot`, por índice posicional, o por `awardID`

**Artefacto resultante:** Conocimiento de los valores reales que usan los Pasos 1 y 6.

---

## Paso 1 — Modificar Bronze: filtro farmacéutico + proyección

### `app/services/extractors.py`

**Dónde:** dentro del `for rec in records_data:` loop, después de los filtros de RUC y año existentes.

**Qué agregar:**

```python
# Filtro farmacéutico (valor confirmado en Paso 0)
PHARMA_CATEGORIES = {"goods"}  # ajustar tras inspección

category = rec.get("compiledRelease", {}).get("tender", {}).get("mainProcurementCategory", "")
if category.lower() not in PHARMA_CATEGORIES:
    continue
```

### `app/pipelines/bronze_layer.py`

**Dónde:** en `run_targeted_ingestion()`, después de obtener `detail` y antes de llamar a `save_json()`.

**Qué agregar:**

```python
# Proyectar solo los nodos necesarios para Silver (~70% reducción de tamaño)
cr = detail.get("compiledRelease", {})
detail_projected = {
    "ocid": detail.get("ocid"),
    "compiledRelease": {
        "date": cr.get("date"),
        "tender": {
            "id": cr.get("tender", {}).get("id"),
            "title": cr.get("tender", {}).get("title"),
            "status": cr.get("tender", {}).get("status"),
            "mainProcurementCategory": cr.get("tender", {}).get("mainProcurementCategory"),
            "procurementMethod": cr.get("tender", {}).get("procurementMethod"),
            "procurementMethodDetails": cr.get("tender", {}).get("procurementMethodDetails"),
            "procurementMethodRationale": cr.get("tender", {}).get("procurementMethodRationale"),
            "tenderPeriod": cr.get("tender", {}).get("tenderPeriod"),
            "value": cr.get("tender", {}).get("value"),
            "items": cr.get("tender", {}).get("items", []),
        },
        "awards": cr.get("awards", []),
        "contracts": cr.get("contracts", []),
        "parties": [
            p for p in cr.get("parties", [])
            if any(r in p.get("roles", []) for r in ("buyer", "supplier", "tenderer", "procuringEntity"))
        ],
    }
}
# Reemplazar detail por la versión proyectada antes de guardar
detail = detail_projected
```

**Ejecución Bronze completa (por año):**

```bash
python main.py targeted --year 2022
python main.py targeted --year 2023
python main.py targeted --year 2024
python main.py targeted --year 2025
```

---

## Paso 2 — Actualizar dependencias

### `requirements.txt`

Agregar al final del archivo:

```
# Silver pipeline dependencies
pandas>=2.0.0
rapidfuzz>=3.6.0
pyodbc>=5.0.0
sqlalchemy>=2.0.0
xlrd>=2.0.1
openpyxl>=3.1.0
pyarrow>=15.0.0
```

```bash
pip install -r requirements.txt
```

---

## Paso 3 — Extender Settings

### `app/config/settings.py`

**Agregar 3 variables a la clase `Settings` y al `.env.sample`:**

```python
SILVER_DIR: str = "data/silver/staging"
EXTRA_DATA_DIR: str = "extra-data"
DW_CONN_STRING: str = ""  # "mssql+pyodbc://user:pass@server/DW_EsSalud_Adquisiciones?driver=ODBC+Driver+17+for+SQL+Server"
```

**Agregar al `.env.sample`:**

```
SILVER_DIR=data/silver/staging
EXTRA_DATA_DIR=extra-data
DW_CONN_STRING=mssql+pyodbc://user:pass@localhost/DW_EsSalud_Adquisiciones?driver=ODBC+Driver+17+for+SQL+Server
```

---

## Paso 4 — `app/loaders/master_loader.py` (nuevo)

Lee y limpia los dos archivos maestros de `extra-data/`.

### Función `load_petitorio(path: str) -> pd.DataFrame`

- Usa `xlrd` (archivo `.xls` antiguo — openpyxl no soporta este formato)
- Salta filas 0–1 (título institucional), fila 2 = headers reales
- Columnas resultantes: `N_ITEM`, `CODIGO_SAP`, `DENOMINACION_DCI`, `ESPECIFICACIONES_TECNICAS`, `UNIDAD_MANEJO`, `RESTRICCION_USO`, `ESPECIALIDAD_AUTORIZADA`, `INDICACIONES`
- Limpieza: strip de espacios, eliminar filas completamente vacías
- Normalización: `DENOMINACION_DCI` → mayúsculas, sin tildes, sin caracteres especiales (base del fuzzy match)
- Output: ~998 filas

### Función `load_establecimientos(path: str) -> pd.DataFrame`

- Usa `openpyxl`, sheet `CentrosFuncional`
- Salta filas 0–11 (encabezado institucional multi-fila), fila 12 = headers reales
- Columnas: `RED`, `TIPO`, `COD_EESS`, `COD_SES`, `COD_RENAES`, `TIPO_2`, `CENTROS_ASISTENCIALES`, `DPTO`, `PROVINCIA`, `DISTRITO`, `NUMERO_RESOLUCION`, `ESTADO`, `CATEGORIA_MINSA`
- Limpieza: strip de espacios, eliminar filas de totales/subtítulos (donde `COD_EESS` sea NaN), normalizar nombres de región
- **Nota importante:** Esta tabla NO tiene columna RUC. El RUC del comprador viene del OCDS (`parties[buyer].id`). El vínculo se construye en el Paso 5 (fuzzy match por nombre).
- Output: ~419 filas

---

## Paso 5 — `app/utils/fuzzy_matcher.py` (nuevo)

Centraliza toda la lógica de matching difuso. Requiere `rapidfuzz`.

### Función `match_medicamento(descripcion: str, petitorio_df: pd.DataFrame) -> dict`

Compara la descripción SEACE contra `DENOMINACION_DCI` del Petitorio.

| Score | `Metodo_Clasificacion` | Acción |
|---|---|---|
| ≥ 90 | `EXACTO` | Asigna SK del medicamento |
| 70–89 | `FUZZY` | Asigna SK, marca para revisión manual |
| < 70 | `DUDOSO` | FK_Medicamento = -1 (Sin Clasificar) |
| Contiene "FUERA DEL PETITORIO" | `HISTORICO` | FK_Medicamento = -2 |
| Es lote/paquete (heurística) | `PAQUETE` | FK_Medicamento = -3 |

Retorna: `{"sk": int, "score": float, "metodo": str, "dci_match": str}`

### Función `extract_red_asistencial(tender_title: str, descriptions: list) -> str | None`

**⚠️ Hallazgo Paso 0:** El buyer en el OCDS es SIEMPRE `"SEGURO SOCIAL DE SALUD"` (entidad central, RUC `20131257750`, región LIMA). El fuzzy match por nombre de comprador no sirve. La Red Asistencial está codificada en el título del tender y en las descripciones de los ítems.

Estrategia en cascada:

1. **Regex en `tender.title`**: busca patrón `ESSALUD/R([A-Z]+)` → extrae código SEACE  
   (ej: `/RAMOQ` → `"MOQUEGUA"`, `/RAARQ` → `"AREQUIPA"`, `/RAALL` → `"LA LIBERTAD"`)
2. **Regex en `tender.description` / `items[i].description`**: busca `"RED ASISTENCIAL\s+([A-Z\s]+)"` → extrae nombre directamente

Retorna la cadena del nombre de la Red (ej: `"MOQUEGUA"`, `"AREQUIPA"`) o `None` si no se encuentra.

### Función `match_red_asistencial(red_name: str, establecimientos_df: pd.DataFrame) -> dict`

**Esta función resuelve el Gap 2.**

- Recibe el nombre extraído por `extract_red_asistencial()`
- Aplica rapidfuzz contra columna `RED` del Excel de Establecimientos
- Umbral ≥ 80 → retorna el valor normalizado de `RED` (ej: `"REBAGLIATI"`, `"MOQUEGUA"`)
- Si score < 80 → retorna `None` (el registro tendrá `Red_Asistencial = NULL`)

Retorna: `{"red": str, "score": float}`

---

## Paso 6 — `app/services/ocds_flattener.py` (nuevo)

Lee todos los JSONs del Bronze y produce un DataFrame plano con una fila por ítem de licitación.

### Lógica de vinculación

```
compiledRelease.tender.items[i]     → una fila por ítem
      ↕ vinculado por relatedLot (o índice si no hay lots)
compiledRelease.awards[j]           → fecha buena pro, proveedor, monto adjudicado
      ↕ vinculado por awards[j].id → contracts[k].awardID
compiledRelease.contracts[k]        → fecha suscripción, monto contratado, adendas
```

### Función `flatten_year(year: int, bronze_dir: str) -> pd.DataFrame`

Lee todos los `.json` de `data/bronze/records/<ruc>/<year>/` y retorna un DataFrame con las columnas:

| Columna | Fuente OCDS |
|---|---|
| `ocid` | raíz del record |
| `n_item` | `tender.items[i].id` |
| `descripcion_item` | `tender.items[i].description` |
| `cantidad` | `tender.items[i].quantity` |
| `unidad_medida` | `tender.items[i].unit.name` |
| `fecha_convocatoria` | `tender.tenderPeriod.startDate` |
| `fecha_buena_pro` | `awards[j].date` |
| `fecha_suscripcion` | `contracts[k].dateSigned` |
| `monto_referencial` | `tender.value.amount` |
| `monto_adjudicado` | `awards[j].value.amount` |
| `monto_contratado` | `contracts[k].value.amount` |
| `monto_adicional` | suma de `contracts[k].amendments[].value.amount` |
| `ruc_comprador` | `parties[buyer].id` |
| `nombre_comprador` | `parties[buyer].name` |
| `departamento_comprador` | `parties[buyer].address.region` (siempre `"LIMA"` — sede central) |
| `red_asistencial` | extraído de `tender.title` (regex `/RAXX`) o descripciones (`"RED ASISTENCIAL XXXX"`) |
| `ruc_proveedor` | `awards[j].suppliers[0].id` |
| `nombre_proveedor` | `awards[j].suppliers[0].name` |
| `metodo_contratacion` | `tender.procurementMethod` |
| `detalles_metodo` | `tender.procurementMethodDetails` |
| `causal_cd` | `tender.procurementMethodRationale` |
| `es_contratacion_directa` | derivado: `metodo_contratacion == "direct"` |
| `anio_fiscal` | año extraído de `fecha_convocatoria` |
| `estado_adjudicacion` | `awards[j].status` |
| `tiene_adenda` | `len(contracts[k].amendments) > 0` |

Guarda resultado como `data/silver/staging/ocds_flat_{year}.parquet`.

### Función `flatten_all(years: list, bronze_dir: str) -> pd.DataFrame`

Llama a `flatten_year()` para cada año y concatena.

---

## Paso 7 — `app/services/dim_resolver.py` (nuevo)

Recibe los DataFrames planos y maestros. Construye DataFrames listos para INSERT en SQL Server y resuelve todos los FKs para la Fact.

### `build_dim_ubigeo_distritos(establecimientos_df) -> pd.DataFrame`

- Extrae tripletas únicas `(DPTO, PROVINCIA, DISTRITO)` del Excel de Establecimientos
- Asigna `Region_Natural` (COSTA / SIERRA / SELVA) y `Macroregion` (NORTE / CENTRO / SUR / ORIENTE) usando tabla de referencia por departamento
- Nivel = `'DISTRITO'`
- Se insertarán en `Dim_Ubigeo` (los 25 departamentos ya están pre-cargados por el DDL)

### `build_dim_entidad(establecimientos_df, flat_df, fuzzy_matcher) -> pd.DataFrame`

**Integra la solución al Gap 2.**

- Itera los ~419 establecimientos del Excel
- La columna `red_asistencial` en `flat_df` ya fue extraída del `tender.title` / descripciones (por `extract_red_asistencial()` durante el aplanamiento)
- Aplica `match_red_asistencial(red_asistencial, estab_df)` para vincular cada transacción con su `RED` del Excel
- `RUC_Entidad` = `20131257750` para todos (único RUC de EsSalud en el OCDS) — el identificador real de la Red Asistencial es la columna `Red_Asistencial` (nombre)
- Vincula el `DPTO` del establecimiento → FK a `Dim_Ubigeo`
- Las transacciones sin Red Asistencial identificable quedan con `Red_Asistencial = NULL`

### `build_dim_medicamento(flat_df, petitorio_df, fuzzy_matcher) -> pd.DataFrame`

- Extrae todas las `descripcion_item` únicas del Bronze aplanado
- Aplica `match_medicamento()` a cada descripción única
- Genera filas para `Dim_Medicamento` con `Nombre_Descripcion_SEACE`, `Score_Fuzzy_Match`, `Metodo_Clasificacion`
- Los sentineles -1, -2, -3, -4 ya están pre-insertados por el DDL

### `build_dim_proveedor(flat_df) -> pd.DataFrame`

- Extrae pares `(ruc_proveedor, nombre_proveedor)` únicos del Bronze
- Detecta consorcios: `Es_Consorcio = nombre.contains("CONSORCIO", case=False)`
- El sentinel -1 ya está pre-insertado por el DDL

### `resolve_all_fks(flat_df, dims) -> pd.DataFrame`

Agrega todas las columnas FK al DataFrame plano para construir la Fact.

| FK | Lógica |
|---|---|
| `FK_Tiempo_Convocatoria` | `int(fecha_convocatoria.strftime('%Y%m%d'))` |
| `FK_Tiempo_Buena_Pro` | `int(fecha_buena_pro.strftime('%Y%m%d'))`, o -1 si NULL |
| `FK_Tiempo_Suscripcion` | `int(fecha_suscripcion.strftime('%Y%m%d'))`, o -1 si NULL |
| `FK_Ubigeo_Entidad` | lookup `ruc_comprador` → `Dim_Entidad.FK_Ubigeo` |
| `FK_Ubigeo_Item` | **Gap 3**: intenta `departamento_comprador` del OCDS → si NULL, usa `FK_Ubigeo_Entidad` como fallback |
| `FK_Entidad_Compradora` | lookup `ruc_comprador` → `Dim_Entidad.SK` |
| `FK_Medicamento` | resultado de `match_medicamento()` |
| `FK_Proveedor` | lookup `ruc_proveedor` → `Dim_Proveedor.SK` |
| `FK_Tipo_Proceso` | mapeo `metodo_contratacion` → `Dim_Tipo_Proceso.SK` |
| `Fecha_Emision_OC` | **Gap 1**: `NULL` (la API no expone OC) |
| `Monto_Total_OC` | **Gap 1**: `0` |
| `Nro_Orden_Compra` | **Gap 1**: `NULL` |

---

## Paso 8 — `app/loaders/dw_loader.py` (nuevo)

Ejecuta el DDL y carga los datos en SQL Server.

### Función `execute_ddl(engine, ddl_path: str)`

Ejecuta el script `star-schema/EsSalud_StarSchema_DDL (3).sql` completo.  
Crea la BD, tablas, sentineles pre-cargados y vistas analíticas.  
Idempotente: usa `IF NOT EXISTS` / `IF OBJECT_ID IS NULL`.

### Orden de carga (respeta dependencias FK)

```
1. execute_ddl()                    → crea esquema, pre-carga Dim_Tiempo y 25 Dim_Ubigeo depts.
2. load_dim_ubigeo_distritos()      → INSERT DISTRITO-level rows
3. load_dim_entidad_compradora()    → depende de Dim_Ubigeo
4. load_dim_medicamento()           → independiente
5. load_dim_proveedor()             → independiente
6. load_fact()                      → depende de todas las Dims
```

### Estrategia de carga

- Por lotes de 500 filas (`chunksize=500`) vía `pandas.DataFrame.to_sql()`
- Transacciones explícitas por año: si falla el año N, hace rollback y continúa con N+1
- Usa `method='multi'` para INSERT bulk eficiente

---

## Paso 9 — `app/pipelines/silver_layer.py` (nuevo)

Orquesta todos los pasos Silver en secuencia.

```python
class SilverPipeline:
    def run(self, years: list = [2022, 2023, 2024, 2025]):
        # 1. Cargar maestros
        petitorio_df = load_petitorio(...)
        estab_df = load_establecimientos(...)

        # 2. Aplanar Bronze por año
        flat_df = flatten_all(years, bronze_dir)

        # 3. Construir dimensiones
        dim_ubigeo_dist = build_dim_ubigeo_distritos(estab_df)
        dim_entidad = build_dim_entidad(estab_df, flat_df, fuzzy_matcher)
        dim_medicamento = build_dim_medicamento(flat_df, petitorio_df, fuzzy_matcher)
        dim_proveedor = build_dim_proveedor(flat_df)

        # 4. Resolver FKs
        fact_df = resolve_all_fks(flat_df, dims)

        # 5. Ejecutar DDL y cargar DW
        execute_ddl(engine, ddl_path)
        load_dims(engine, ...)
        load_fact(engine, fact_df)
```

Expone `run()` equivalente al `BronzePipeline.run_targeted_ingestion()`.

---

## Paso 10 — `dags/silver_dag.py` (nuevo)

DAG Airflow que encadena Bronze y Silver.

```
bronze_2022 >> bronze_2023 >> bronze_2024 >> bronze_2025
    >> flatten_all_years
    >> build_dims
    >> load_dw
```

- ID del DAG: `ocds_silver_pipeline`
- Schedule: mensual (después de que el Bronze termine)
- Depende de que el DAG `ocds_targeted_ingestion` haya completado

---

## Gaps conocidos y su solución

### Gap 1 — Órdenes de Compra (no bloqueante)

**Problema:** La API OCDS no expone datos de órdenes de compra (`Fecha_Emision_OC`, `Monto_Total_OC`, `Nro_Orden_Compra`).  
**Solución:** Estos campos quedan en `NULL/0` en la Fact.  
**Impacto:** Las columnas PERSISTED `Lead_Time_Total_Dias`, `Lead_Time_Comite_Dias`, `Lead_Time_Formalizacion_Dias` se computan igual con las fechas disponibles (convocatoria → buena pro → suscripción).  
**Documentar en informe:** Limitación de fuente de datos; la SEACE no publica OC en el API OCDS.

### Gap 2 — Red Asistencial (crítico, resuelto en Paso 5)

**Problema original:** El Excel de Establecimientos no tiene columna RUC, y se planificó hacer fuzzy match por nombre de comprador.

**Hallazgo Paso 0 (corrección):** El buyer en el OCDS es SIEMPRE `"SEGURO SOCIAL DE SALUD"` con un único RUC `20131257750`. El nombre del comprador no varía por Red Asistencial — no sirve para el match.

**Solución revisada:** La Red Asistencial está en el `tender.title` (código SEACE `ESSALUD/RAMOQ`) y en las descripciones de ítems (`"RED ASISTENCIAL MOQUEGUA"`). Se extrae con regex en `extract_red_asistencial()` y luego se hace fuzzy match contra la columna `RED` del Excel.

**Implementado en:** `app/utils/fuzzy_matcher.py` + `app/services/ocds_flattener.py` (extracción) + `app/services/dim_resolver.py:build_dim_entidad()` (resolución)

### Gap 3 — Departamento del ítem (no bloqueante, resuelto en Paso 7)

**Problema:** El OCDS no siempre incluye `deliveryLocation` por ítem.  
**Solución:** Fallback en `resolve_all_fks()`: si `departamento_comprador` del OCDS es NULL, usar el departamento de la entidad compradora vía `FK_Ubigeo_Entidad`.  
**Impacto:** Aceptable para proyecto académico; la entidad compradora está en la misma región que los ítems en la mayoría de casos.

---

## Estructura de directorios resultante

```
essalud-pipeline/
├── app/
│   ├── clients/
│   │   └── ocds_client.py          (sin cambios)
│   ├── config/
│   │   └── settings.py             (modificado: +3 vars)
│   ├── loaders/
│   │   ├── __init__.py             (nuevo)
│   │   ├── master_loader.py        (nuevo)
│   │   └── dw_loader.py            (nuevo)
│   ├── models/
│   │   └── data_models.py          (sin cambios)
│   ├── pipelines/
│   │   ├── bronze_layer.py         (modificado: proyección)
│   │   └── silver_layer.py         (nuevo)
│   ├── services/
│   │   ├── extractors.py           (modificado: filtro farmacéutico)
│   │   ├── ocds_flattener.py       (nuevo)
│   │   └── dim_resolver.py         (nuevo)
│   ├── storage/
│   │   ├── file_manager.py         (sin cambios)
│   │   └── r2_manager.py           (sin cambios)
│   ├── utils/
│   │   ├── helpers.py              (sin cambios)
│   │   └── fuzzy_matcher.py        (nuevo)
│   └── audit/
│       └── logger.py               (sin cambios)
├── dags/
│   ├── ocds_dag.py                 (sin cambios)
│   └── silver_dag.py               (nuevo)
├── data/
│   ├── bronze/
│   │   └── records/<ruc>/<year>/   (JSONs por año)
│   └── silver/
│       └── staging/                (nuevo: .parquet por año)
├── extra-data/
│   ├── Petitorio-Publicar-*.xls    (sin cambios)
│   └── 5992483-relacion-*.xlsx     (sin cambios)
├── star-schema/
│   └── EsSalud_StarSchema_DDL.sql  (sin cambios)
├── requirements.txt                (modificado)
├── .env.sample                     (modificado)
├── main.py                         (sin cambios)
└── PLAN_IMPLEMENTACION.md          (este archivo)
```

---

## Orden de ejecución completo

```bash
# 1. Inspección
python main.py targeted --limit 10

# 2. Bronze completo (tras confirmar filtro farmacéutico)
python main.py targeted --year 2022
python main.py targeted --year 2023
python main.py targeted --year 2024
python main.py targeted --year 2025

# 3. Silver + carga DW
python -c "from app.pipelines.silver_layer import SilverPipeline; SilverPipeline().run()"

# 4. Airflow (alternativa orquestada)
docker compose up --build
# Activar DAG ocds_silver_pipeline en localhost:8080
```
