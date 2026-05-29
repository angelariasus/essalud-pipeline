# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

OCDS Data Framework for EsSalud — an ELT pipeline that extracts procurement data from Peru's Open Contracting Standard (OCDS) portal to evaluate medicine purchase efficiency. Implements a medallion data lake architecture (Bronze → Silver → Gold): **Bronze** (extraction with `requests`) and **Silver/Gold** (transformation + DW load with **Apache Spark / PySpark**).

## Common Commands

```bash
# Install runtime dependencies (PySpark requires a JRE 17+ on PATH / JAVA_HOME)
pip install -r requirements.txt

# Install dev dependencies (includes Airflow)
pip install -r requirements-dev.txt

# Copy and fill in credentials
cp .env.sample .env

# Run targeted extraction (EsSalud, specific year)
python main.py targeted --year 2024 [--limit N]

# Run bulk catalog download
python main.py bulk --source SEACE --type JSON --year 2023 --month 11

# Run Silver/Gold pipeline (Spark): flatten + dims + DW load
python -c "from app.pipelines.silver_layer import SilverPipeline; SilverPipeline().run()"
# Validate Silver without a SQL Server instance (no DW load):
python -c "from app.pipelines.silver_layer import SilverPipeline; SilverPipeline().run(years=[2024], load_dw=False)"

# Lint
flake8 app/ test/

# Run all tests
pytest

# Run a single test file
pytest test/test_bronze_layer.py

# Start full Airflow stack (Postgres, Redis, Webserver @ :8080, Scheduler, Worker)
docker compose up --build
```

## Architecture

### Medallion Pattern

**Bronze** (extraction) and **Silver/Gold** (Spark transformation + DW load) are implemented. Data flows:

1. **Bronze — Targeted mode**: Paginate OCDS API → client-side filter by RUC + year → fetch full record → persist locally and optionally to Cloudflare R2.
2. **Bronze — Bulk mode**: Download monthly ZIP from SEACE portal → stream to disk → replicate to R2 → decompress locally.
3. **Silver/Gold (Spark)**: `spark.read.json` (strict schema, PERMISSIVE + `_corrupt_record`) → flatten one row per tender item (`explode_outer` + higher-order award/contract cascade) → Parquet staging → build dimensions + resolve FKs (`row_number` SKs, fuzzy matching via Pandas UDFs with broadcast masters) → write JDBC staging tables → atomic load to the SQL Server star schema via a stored procedure.

### Key Design Decisions

- **Client-side filtering**: The OCDS API has no native RUC/date filter support, so `TargetedExtractor` paginates all pages and filters in Python.
- **Dual persistence**: `FileManager` writes to `data/bronze/`, and `R2Manager` replicates to Cloudflare R2 when `OCDS_USE_R2=True`. R2Manager is imported lazily to avoid hard boto3 dependency.
- **Generator pagination**: `paginate_records()` in `ocds_client.py` yields pages on demand to handle arbitrarily large datasets.
- **Streaming downloads**: Bulk ZIPs are downloaded in 8192-byte chunks.
- **Spark fail-safe**: `spark.sql.ansi.enabled=false` (dirty OCDS data coerces to NULL like pandas `errors="coerce"`); corrupt JSON is captured in `_corrupt_record`, never aborts the job; fuzzy Pandas UDFs degrade gracefully per element (`metodo='ERROR: ...'`).
- **Windows Spark caveat**: spawning many Python UDF workers concurrently crashes on Windows, so `OCDS_SPARK_MASTER` defaults to `local[4]`; `pyarrow` is pinned `<19` to match Spark 4.1's bundled Arrow Java (18.3). On Linux/Docker neither applies. `PYSPARK_PYTHON` is forced to the running interpreter in `spark_session.py`.
- **Atomic DW load**: Spark writes to `stg.*` staging tables via JDBC; a stored procedure (`oro.usp_Load_From_Staging`) moves them to production in one transaction — if Spark fails mid-write, production is untouched. Surrogate keys are deterministic INT (`row_number`), inserted with `SET IDENTITY_INSERT`.

### Module Map

| Module | Role |
|---|---|
| `app/pipelines/bronze_layer.py` | Bronze orchestrator (`BronzePipeline`) |
| `app/pipelines/silver_layer.py` | Silver/Gold orchestrator (`SilverPipeline`) — Spark |
| `app/services/extractors.py` | `TargetedExtractor` and `BulkExtractor` |
| `app/services/ocds_flattener.py` | Spark flattener: read JSON → one row per item (Parquet staging) |
| `app/services/dim_resolver.py` | Spark dimension builder + FK resolution (star schema) |
| `app/utils/fuzzy_matcher.py` | `rapidfuzz` scalar logic + Pandas UDF factories (medicamento/red) |
| `app/loaders/master_loader.py` | Loads Petitorio/Establecimientos Excel masters (pandas) |
| `app/loaders/dw_loader.py` | DDL exec + Spark JDBC staging writes + atomic load procedure |
| `app/clients/ocds_client.py` | HTTP client with retry/backoff (`HTTPAdapter`) |
| `app/storage/file_manager.py` | Local disk persistence |
| `app/storage/r2_manager.py` | Cloudflare R2 (S3-compatible) upload |
| `app/config/settings.py` | All env-var config (loaded from `.env`) |
| `app/config/spark_session.py` | `get_spark_session()` singleton (Arrow, AQE, JDBC, Windows fixes) |
| `app/models/data_models.py` | `RecordSummary`, `CatalogItem`, `PaginationData` |
| `app/audit/logger.py` | Dual logger: console (INFO+), file (DEBUG+) at `data/audit/` |
| `dags/ocds_dag.py` | Airflow DAGs — weekly targeted, monthly bulk |
| `dags/silver_dag.py` | Airflow DAG — monthly Silver/Gold (`ocds_silver_pipeline`) |
| `star-schema/EsSalud_StarSchema_DDL.sql` | Gold star schema (oro.*) + analytic views |
| `star-schema/EsSalud_Staging_DDL.sql` | `stg` schema + `oro.usp_Load_From_Staging` (atomic load) |
| `main.py` | CLI entry point (argparse) |

### Airflow DAGs

- `ocds_targeted_ingestion` — weekly; hardcoded `limit=100` for dev (remove for production).
- `ocds_bulk_ingestion` — monthly; date is **hardcoded** to Nov 2023. Should be replaced with Jinja macros (`{{ macros.ds_format(...) }}`) for dynamic scheduling.
- `ocds_silver_pipeline` (`dags/silver_dag.py`) — monthly; runs `SilverPipeline().run()` (Spark local mode in the worker container, which has a JRE via the Dockerfile). Accepts a `year` via `dag_run.conf`.

### Environment Variables

Defined in `.env.sample`:

| Variable | Purpose |
|---|---|
| `OCDS_API_BASE_URL` | OCDS API base URL |
| `OCDS_ESSALUD_RUC` | Buyer RUC to filter (default: EsSalud) |
| `OCDS_USE_R2` | Enable Cloudflare R2 replication (`True`/`False`) |
| `OCDS_R2_ACCOUNT_ID/ACCESS_KEY/SECRET_KEY/BUCKET_NAME` | R2 credentials |
| `OCDS_LOG_LEVEL` | Console log level |
| `OCDS_MAX_RETRIES` / `OCDS_BACKOFF_FACTOR` | HTTP retry strategy |
| `OCDS_SILVER_DIR` / `OCDS_EXTRA_DATA_DIR` | Silver staging dir / Excel masters dir |
| `OCDS_SPARK_MASTER` | Spark master (default `local[4]`; `local[*]`/cluster on Linux) |
| `OCDS_SPARK_JARS_PACKAGES` / `OCDS_SPARK_JARS` | mssql-jdbc driver (Maven coords / local jars) |
| `OCDS_DW_CONN_STRING` | SQLAlchemy/pyodbc conn for DDL + load procedure |
| `OCDS_DW_JDBC_URL/USER/PASSWORD/BATCHSIZE` | JDBC conn for Spark staging writes (auto-derived from `OCDS_DW_CONN_STRING` if unset) |

### Data Layout

```
data/
  bronze/
    records/        # Individual JSON records from targeted extraction
    bulk_files/     # Raw ZIPs and decompressed catalogs
  silver/
    staging_flat/   # Spark flattened output, Parquet partitioned by anio_fiscal
  audit/
    executions/     # ocds_extraction.log (debug-level)
```

## CI/CD

GitHub Actions (`.github/workflows/ci.yml`) runs `flake8` and `pytest` on push/PR to `main`. It sets up **Java 17 (Temurin)** so the Spark tests run; Spark tests skip gracefully if no JRE is present (see `test/conftest.py` `requires_spark`). Spark integration tests use synthetic OCDS records (since `data/bronze/` is git-ignored).
