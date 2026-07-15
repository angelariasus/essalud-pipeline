# Guía de ejecución — EsSalud Pipeline (runbook del equipo)

Guía paso a paso para ejecutar y revisar **todo** el proyecto después de hacer
`git pull`: pipeline por terminal, stack Docker (Airflow + SQL Server + MailHog),
carga al Data Warehouse, modelo predictivo ML y alertas operativas.

Todo está verificado end-to-end (2026-07-02, CHANGELOG §10). Los comandos son
PowerShell sobre Windows salvo que se indique lo contrario.

---

## 0. Prerrequisitos

| Software | Versión | Notas |
|---|---|---|
| Python | 3.11+ (probado con 3.12) | |
| Java JDK | 17+ (probado con 21) | `JAVA_HOME` apuntando al JDK |
| Hadoop winutils | — | `HADOOP_HOME` (p. ej. `C:\hadoop` con `bin\winutils.exe`) |
| Docker Desktop | reciente | para Airflow / SQL Server en contenedor / MailHog |
| SQL Server | 2019+ (opcional) | solo si quieres el DW **local** además del contenedor |
| ODBC Driver 17/18 for SQL Server | — | requerido por `pyodbc` en el host |

## 1. Setup inicial (una sola vez)

```powershell
git clone <repo> ; cd essalud-pipeline

# Entorno virtual + dependencias
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
pip install -r requirements-dev.txt        # pytest + flake8 (opcional)

# Variables de entorno
Copy-Item .env.example .env
# Editar .env: como mínimo JAVA_HOME y HADOOP_HOME de TU máquina.
# Los defaults de DW/SMTP funcionan con el stack Docker sin cambios.
```

> `.env` no se versiona. `.env.docker` (dotenv del contenedor) y `.env.example`
> sí están en el repo — no pongas secretos reales en ellos.

**Verificación rápida del entorno:**

```powershell
.\.venv\Scripts\python.exe -c "import pyspark, pandas, pyodbc, google.genai; print('entorno OK')"
```

## 1.5. Variables de entorno

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
| `OCDS_DW_JDBC_URL` / `USER` / `PASSWORD` / `BATCHSIZE` | Conexión JDBC para los writes de Spark. |
| `OCDS_DW_*_DOCKER` (`CONN_STRING` / `JDBC_URL` / `JDBC_USER` / `JDBC_PASSWORD`) | ★ Cadenas del perfil `--profile docker`: apuntan al contenedor `sqlserver` |
| `MSSQL_SA_PASSWORD` | ★ Password `sa` del SQL Server en contenedor (default `EsSalud2024!`) |
| `SMTP_HOST` / `PORT` / `USER` / `PASSWORD` / `STARTTLS` / `FROM` / `TO` | ★ Envío del correo de alertas operativas. |
| `OCDS_ENV_FILE` | ★ Dotenv alternativo (el compose lo fija a `.env.docker` dentro del contenedor) |
| `GEMINI_API_KEY` / `GEMINI_MODEL` | Limpieza IA en Silver (vacío = no-op) |

## 2. Pipeline completo por terminal (sin Docker)

El destino Gold por defecto es **Parquet** — no necesita SQL Server.

```powershell
# (a) Bronze — solo si data/bronze/ está vacío (~2 300 JSON, tarda por la API)
# Modo Targeted (extracción dirigida por RUC y año):
python app/cli.py targeted --year 2024 --limit 10
python app/cli.py targeted --ruc 20131257750 --year 2023 --limit 0
# Modo Bulk (extracción masiva SEACE):
python app/cli.py bulk --source SEACE --type JSON --year 2023 --month 11
# Alias:
python app/cli.py bronze --years 2022 2023 2024 2025

# (b) Orden canónico: Silver → Synth → Gold
python app/cli.py silver --rebuild     # aplana Bronze → data/silver/staging_flat
python app/cli.py synth                # sintéticos 2024/2025 + adendas (¡OBLIGATORIO antes de gold!)
python app/cli.py gold                 # dims + Fact → data/gold/ + data/mart/*.parquet

# (c) Verificar el resultado
python -c "import pandas as pd; f=pd.read_parquet('data/mart/Fact_Ordenes_Y_Contratos.parquet'); print('Fact:', len(f))"
# Esperado: Fact: 9292
```

> ⚠️ **Nunca** corras `gold` sin `synth` después de un `silver --rebuild`: 2024/2025
> quedarían casi vacíos y el ML (que asume 9 292 filas) se rompe.
> ⚠️ Regenera los datos **siempre desde el host**, no desde el DAG de Airflow
> (el `synth` del contenedor muestrea un subconjunto distinto — CHANGELOG §10.5).

## 3. Modelo Predictivo (ML de Lead Time)

```powershell
# Requiere data/mart/*.parquet (paso 2). El notebook reentrena y publica data/mart/Pred_Lead_Time.parquet
.\.venv\Scripts\python.exe -m jupyter nbconvert --to notebook --execute --inplace `
  --ExecutePreprocessor.kernel_name=essalud mlpredicts\LeadTime_Predictor.ipynb

# Verificación del entregable (esperado: 10/10 OK)
$env:PYTHONUTF8 = 1; .\.venv\Scripts\python.exe mlpredicts\test_pred_lead_time.py
```

> El notebook está **gitignored** (`*.ipynb`); si no lo tienes, pide el archivo o
> reconstrúyelo según `modelo-predictivo.md`. El `best_model.joblib`
> commiteado requiere scikit-learn compatible con el que lo entrenó: si `joblib.load`
> falla con `AttributeError`, re-ejecuta el notebook (reentrena y lo regenera).

## 4. Alertas de Abastecimiento

```powershell
# Vista previa (genera data/mart/Alertas.parquet e imprime el correo, no envía nada)
python app/cli.py alert --dry-run

# Envío real. Default del .env: MailHog local (levántalo antes: paso 5)
python app/cli.py alert

# Variantes
python app/cli.py alert --source hhi                    # solo concentración HHI
python app/cli.py alert --source leadtime --sigma 2.5   # solo lead time, umbral más estricto
python app/cli.py alert --to otra.persona@unmsm.edu.pe  # destinatario puntual
```

Ver el correo capturado: **http://localhost:8025** (UI de MailHog).
Para Gmail real: en `.env` configura `SMTP_HOST=smtp.gmail.com`, `SMTP_PORT=587`,
`SMTP_STARTTLS=true`, `SMTP_USER`/`SMTP_PASSWORD` (App Password).
La variante institucional sin código: [`alertas-automatizadas.md`](alertas-automatizadas.md).

## 5. Stack Docker (Airflow + SQL Server + MailHog)

```powershell
# Primera vez: construir la imagen (Java 17 + ODBC + pandas>=2.2) e inicializar
docker compose build
docker compose up airflow-init

# Levantar todo (Airflow + sqlserver + sqlserver-init + mailhog)
docker compose up -d

# Verificar: todos healthy (sqlserver-init y airflow-init terminan y salen)
docker compose ps
```

**Accesos:**

| Servicio | URL / puerto | Credenciales |
|---|---|---|
| Airflow UI | http://localhost:8080 | `airflow` / `airflow` |
| MailHog UI | http://localhost:8025 | — |
| SQL Server (contenedor) | `localhost,11433` | `sa` / `EsSalud2024!` (dev) |

## 6. Airflow — operación y revisión

Los DAGs nacen **pausados** (salvo los ya despausados). Desde la UI o por CLI:

```powershell
# Despausar
docker exec essalud-pipeline-airflow-scheduler-1 airflow dags unpause ocds_silver_pipeline
docker exec essalud-pipeline-airflow-scheduler-1 airflow dags unpause ocds_alerting

# Pipeline completo: Silver → Synth → Gold → trigger ocds_alerting (~15 min)
docker exec essalud-pipeline-airflow-scheduler-1 airflow dags trigger ocds_silver_pipeline

# Con parámetros (años puntuales, carga al DW del contenedor, sin synth)
docker exec essalud-pipeline-airflow-scheduler-1 airflow dags trigger ocds_silver_pipeline `
  --conf '{\"years\": [2022, 2023], \"target\": \"sqlserver\", \"profile\": \"docker\", \"skip_synth\": true}'

# Solo las alertas (rápido, ~10 s; el correo cae en MailHog)
docker exec essalud-pipeline-airflow-scheduler-1 airflow dags trigger ocds_alerting
```

**Revisión de corridas:**

```powershell
docker exec essalud-pipeline-airflow-scheduler-1 airflow dags list                 # 4 DAGs
docker exec essalud-pipeline-airflow-scheduler-1 airflow dags list-import-errors  # esperado: "No data found"
docker exec essalud-pipeline-airflow-scheduler-1 airflow dags list-runs -d ocds_silver_pipeline
docker exec essalud-pipeline-airflow-scheduler-1 airflow tasks states-for-dag-run ocds_silver_pipeline <run_id>

# Log de una tarea que falló
docker exec essalud-pipeline-airflow-worker-1 bash -c `
  "cat /opt/airflow/logs/dag_id=ocds_silver_pipeline/run_id=<run_id>/task_id=run_silver_pipeline/attempt=1.log | tail -50"
```

## 7. SQL Server — carga y revisión del DW

### 7a. DW en el contenedor (recomendado: cero instalación)

```powershell
# Con el stack arriba (paso 5), cargar el modelo estrella desde el host:
python app/cli.py gold --target sqlserver --profile docker
```

Revisión (desde el host, con sqlcmd o cualquier cliente en `localhost,11433`):

```powershell
docker exec essalud-pipeline-sqlserver-1 /opt/mssql-tools18/bin/sqlcmd `
  -S localhost -U sa -P "EsSalud2024!" -C -d DW_EsSalud_Adquisiciones `
  -Q "SELECT COUNT(*) AS Fact FROM oro.Fact_Ordenes_Y_Contratos; SELECT TOP 5 * FROM oro.vw_Matriz_Riesgo_HHI ORDER BY HHI DESC;"
```

### 7b. DW en SQL Server local

Requiere un login SQL (la escritura de Spark **no** funciona con Windows Auth):

```sql
-- Ejecutar una vez como admin (SSMS/sqlcmd):
CREATE LOGIN essalud_user WITH PASSWORD = 'EsSalud2024!';
ALTER SERVER ROLE dbcreator ADD MEMBER essalud_user;   -- o crear la BD a mano
```

```powershell
# .env: OCDS_DW_CONN_STRING (pyodbc) + OCDS_DW_JDBC_URL/USER/PASSWORD (ver .env.example)
python app/cli.py gold --target sqlserver --profile local
```

**Validaciones esperadas** (local o contenedor):

| Consulta | Esperado |
|---|---|
| `SELECT COUNT(*) FROM oro.Fact_Ordenes_Y_Contratos` | 9 292 |
| `SELECT COUNT(*) FROM oro.Dim_Medicamento` | 910 (906 + sentinelas) |
| `SELECT COUNT(*) FROM oro.vw_Matriz_Riesgo_HHI` | ~963 |
| `SELECT COUNT(*) FROM oro.vw_Lead_Time_Por_Proveedor` | ~6 582 |

## 8. Tests

```powershell
.\.venv\Scripts\python.exe -m pytest -q                       # suite completa (~12 min, incluye Spark)
.\.venv\Scripts\python.exe -m pytest -q --ignore=test/test_silver_spark.py   # rápida (~4 min)
.\.venv\Scripts\python.exe -m pytest test/test_alerting.py -q # solo alertas
flake8 app dags app/cli.py --max-line-length=127                 # lint (criterio del CI)
```

Esperado: **65 passed, 2 skipped** (los 2 skips son DAG-integrity/API si no aplican).

## 9. Cloudflare R2 (opcional — solo réplica del Bronze)

Por decisión de proyecto, Cloudflare se usa **únicamente** como almacén espejo del
Bronze y **solo si hay credenciales**. Sin credenciales todo corre 100 % local.

```powershell
# .env: OCDS_USE_R2=true + OCDS_R2_ACCOUNT_ID/ACCESS_KEY/SECRET_KEY/BUCKET_NAME
python app/cli.py targeted --year 2024 --limit 10   # cada JSON se replica al bucket
```

## 10. Troubleshooting

| Síntoma | Causa | Solución |
|---|---|---|
| `ValueError: Credenciales JDBC incompletas` al cargar el DW | `.env` usa `trusted_connection=yes` sin `OCDS_DW_JDBC_USER/PASSWORD` | Define las tres `OCDS_DW_JDBC_*` con un login SQL (§7b) |
| Tareas de Airflow fallan al instante con *"task killed externally"* | Módulo `redis` corrupto por timeout de Celery en frío | Ya mitigado (`AIRFLOW__CELERY__OPERATION_TIMEOUT=30`); si reaparece, `docker compose restart airflow-scheduler` |
| `No module named 'app'` dentro de una tarea Spark en Airflow | Falta `PYTHONPATH` en el contenedor | Ya fijado en el compose; verifica `docker exec ...-worker-1 python -c "import app"` |
| `Pandas >= 2.2.0 must be installed` en el worker | Imagen vieja sin el pin de pandas | `docker compose build && docker compose up -d` |
| Spark en Windows crashea con muchos workers UDF | Bug conocido de Windows | Mantén `OCDS_SPARK_MASTER=local[4]` (default) |
| `test_pred_lead_time.py` falla en conteos o reproducibilidad | `data/mart/` regenerado desde el DAG (subset synth distinto) o `joblib` de otro sklearn | Regenera desde el host (§2) y re-ejecuta el notebook (§3) |
| El correo no llega | `SMTP_HOST` vacío o MailHog abajo | `docker compose up -d mailhog` y revisa `.env` (§4) |
| `gold` casi vacío en 2024/2025 | Se saltó `synth` | `python app/cli.py synth` y repite `gold` |

## 11. Referencias

- [`README.md`](../README.md) — visión general y arquitectura
- [`CHANGELOG.md`](../CHANGELOG.md) — §10: detalle de todos los cambios 2026-07-02
- [`modelo-predictivo.md`](modelo-predictivo.md) — modelo ML + Power BI
- [`alertas-automatizadas.md`](alertas-automatizadas.md) — alertas vía Power BI Service (sin código)
- [`arquitectura.md`](arquitectura.md) — diseño detallado del framework
