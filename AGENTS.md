# AGENTS.md

Complementa `CLAUDE.md`. Solo incluye hechos de alta señal que un agente probablemente pasaría por alto. Si no está aquí, está en `CLAUDE.md` o es estándar para el lenguaje/herramienta.

## Estado de las pruebas ✅ (actualizadas en Fase 3)

- **9/10 pasan**, 1 se salta si no hay `apache-airflow` instalado.
- `test_bronze_layer.py` ✅ — inicilización del pipeline, extractors con client mock, filtro farmacéutico.
- `test_api.py` ✅ — 6 tests reales con aserciones contra la API real (responds, estructura records, paginación, categorías).
- `test_dag_integrity.py` ⏭️ — se salta automáticamente si `apache-airflow` no está instalado (`pytest.importorskip`).
- CI ejecuta `pytest test/ -v` — **debería pasar completamente** en CI (donde Airflow sí está instalado).

## Comandos

```bash
# lint (coincide con CI — no prueba/):
flake8 app dags main.py --count --select=E9,F63,F7,F82 --show-source --statistics
flake8 app dags main.py --count --exit-zero --max-complexity=10 --max-line-length=127 --statistics

# install dev extras (includes Airflow):
pip install -r requirements-dev.txt
```

## Arquitectura — Hechos que no son obvios

- **Importación perezosa de R2Manager**: Se importa dentro de `BronzePipeline.__init__()` (`bronze_layer.py:21`) para evitar una dependencia dura de `boto3`. Solo se instancia cuando `OCDS_USE_R2=True`.
- **Sin segunda llamada HTTP**: `paginate_records()` en `extractors.py` emite el `compiledRelease` ya proyectado del listado `/recordsAfter` — no se realiza una llamada separada a `/record/{ocid}`. La respuesta del listado ya contiene el release completo.
- **Filtro farmacéutico**: `PHARMA_CATEGORIES = {"goods"}` en `extractors.py:16`. Solo se conservan registros con `mainProcurementCategory == "goods"`.
- **No hay soporte nativo de la API para filtrado**: La API de OCDS no soporta filtros por RUC, fecha o categoría. Todo el filtrado se realiza del lado del cliente en `paginate_records()`.

## Capa Silver — Estado actual (Fase 3 completada ✅)

| Componente | Estado |
|---|---|
| `app/pipelines/silver_layer.py` | ✅ Orquesta maestros→flatten→dims→DW |
| `app/loaders/dw_loader.py` | ✅ `execute_ddl()` con AUTOCOMMIT; `_load_dim()`/`load_fact()` con `method=None` (evita límite 2100 params de SQL Server) |
| `dags/silver_dag.py` | ✅ Creado, `@monthly`, sigue patrón de `ocds_dag.py` |
| Pipeline end-to-end (Fase 3) | ✅ Ejecutado exitosamente: 29 fact rows, todas las dimensiones cargadas |
| Post-load integrity (Fase 4) | ✅ FK integrity PASS (9/9), KPIs calculados, vw_Matriz_Riesgo_HHI funciona |

### Fixes aplicados durante Fase 3
1. **DDL WHILE loop → CTE recursivo**: `EsSalud_StarSchema_DDL.sql` — WHILE loop timeout (~30s) solo producía 107/1461 días; reemplazado con `WITH Fechas AS (...)` recursivo + `OPTION (MAXRECURSION 0)`.
2. **AUTOCOMMIT en DDL**: `dw_loader.py:execute_ddl()` cambió a `isolation_level="AUTOCOMMIT"` para que fallos en vistas no reviertan tablas ya creadas.
3. **method=None**: `_load_dim()` y `load_fact()` usan `method=None` + `chunksize=100` para evitar error "COUNT field incorrect" por límite de 2100 parámetros de SQL Server con `method='multi'`.
4. **vw_Matriz_Riesgo_HHI**: CTE `MarketTotals` extraída para eliminar window function anidada dentro de agregado (error 4109).

### Datos cargados en SQL Server (oro.*)

| Tabla | Filas |
|---|---|
| Dim_Tiempo (2022-2025 + centinela) | 1,462 |
| Dim_Ubigeo (340 distritos + 26 dptos + centinela) | 366 |
| Dim_Entidad_Compradora (9 redes + centinela) | 10 |
| Dim_Medicamento (3 clasificados + centinela + 3 sin clasif?) | 7 |
| Dim_Proveedor (16 proveedores + centinela) | 17 |
| Dim_Tipo_Proceso (precargados + centinela) | 12 |
| Fact_Ordenes_Y_Contratos | 29 |

### KPIs (20 records EsSalud 2024)
- Referencial: S/ 89,379,289.79
- Adjudicado: S/ 27,695,880.60
- Diferencia: S/ 61,683,409.19 (~69% no adjudicado)

## Hallazgos de la Evaluación Fase 1–2 (histórico)

### ✅ Correcto
- **API OCDS**: responde 200, `mainProcurementCategory='goods'` para EsSalud. Filtro farmacéutico funciona.
- **Extracción targeted**: `paginate_records()` proyecta correctamente con `_project_release()`. Filtros RUC/año/categoría operativos.
- **Master data**: Petitorio 996 filas, Establecimientos 403 centros / 35 redes.
- **Flatten**: `flatten_record()` produce 25 columnas por ítem de licitación.
- **Dim resolution**: `dim_entidad` resuelve Red Asistencial correctamente (ej: "RED ASISTENCIAL MADRE DE DIOS" → SK=1, FK_Ubigeo=17).
- **Ubigeo distritos**: 340 filas, SK 26–365.
- **DDL compatibilidad**: todas las columnas mapean correctamente. Columnas faltantes en Python usan DEFAULTs del DDL.

### ⚠️ Requiere SQL Server (Fases 3–4)
- `DW_CONN_STRING` no configurada en `.env` (no existe archivo `.env`, solo `.env.sample`).
- Fase 3 (Silver end-to-end) y Fase 4 (post-load integrity) no se pueden ejecutar sin conexión a SQL Server.

### 🔴 Tests desactualizados (documentado)
- `test_bronze_layer.py` (líneas 8–9, 12–13): referencias inválidas a `.extractor`, `.r2_manager`, constructores sin `client`.
- `test_dag_integrity.py`: requiere `apache-airflow`.
- `test_api.py`: script manual sin aserciones pytest.

## Conexión DW (Fase 3)
- `.env` creado con: `mssql+pyodbc://sa:123ABC%40%40@localhost:11423/DW_EsSalud_Adquisiciones?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes`
- Puerto: 11423 (mapeado al contenedor Docker `essalud-pipeline-sqlserver-1`)

## Cambios en esta sesión

### Tests
- `test_bronze_layer.py` reescrito: usa monkeypatched OCDSClient, `.targeted_extractor` (antes `.extractor`).
- `test_api.py` reescrito: 6 tests con aserciones reales (API responde, estructura records, paginación, `mainProcurementCategory`).
- `test_dag_integrity.py`: usa `pytest.importorskip("airflow")` para salto automático.
- `pytest.ini` agregado con `pythonpath = .` para importaciones relativas.

### DAGs
- `ocds_dag.py`: parámetros dinámicos con Jinja (`{{ execution_date.year }}`, `{{ execution_date.month }}`); `TriggerDagRunOperator` en Targeted → Silver.
- `silver_dag.py`: parámetro `year` vía `dag_run.conf`; `PythonOperator` para resolver años.

### DDL
- `EsSalud_StarSchema_DDL (3).sql` renombrado a `EsSalud_StarSchema_DDL.sql`.
- Sección 9 agregada: validación post-carga (row counts, FK integrity, KPIs, vistas).
- WHILE loop de Dim_Tiempo reemplazado por CTE recursivo (`OPTION MAXRECURSION 0`).

### Data cleanup
- Records 2015 movidos a `data/bronze/records/20131257750/2015/`.
- Records 2024 duplicados en raíz eliminados.
- `data/bronze/bulk_files/` creado.

## Peculiaridades de Airflow

- Los DAGs usan Jinja macros: `--year {{ execution_date.year }}`, `--month {{ execution_date.month }}`.
- DAG `ocds_targeted_ingestion` → `TriggerDagRunOperator` → `ocds_silver_pipeline`.
- La raíz del proyecto se monta en `/opt/airflow/bi` dentro del contenedor (`docker-compose.yaml:80`). Los comandos de DAG usan `cd /opt/airflow/bi && python main.py ...`.
- `test_dag_integrity.py` requiere que Airflow esté instalado en el entorno de pruebas.
- Autenticación del webserver de Airflow: `airflow` / `airflow` en `localhost:8080`.

## Diseño de datos

```
data/bronze/records/<ruc>/<year>/<ocid>.json              (targeted)
data/bronze/bulk_files/<source>_<type>_<year>_<month>.zip  (bulk)
data/audit/executions/ocds_extraction.log
data/silver/staging/ocds_flat_<year>.parquet               (generado por ocds_flattener)
```
