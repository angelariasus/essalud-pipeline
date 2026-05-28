# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

OCDS Data Framework for EsSalud — an ELT pipeline that extracts procurement data from Peru's Open Contracting Standard (OCDS) portal to evaluate medicine purchase efficiency. Implements the **Bronze layer** of a medallion data lake architecture (Bronze → Silver → Gold).

## Common Commands

```bash
# Install runtime dependencies
pip install -r requirements.txt

# Install dev dependencies (includes Airflow)
pip install -r requirements-dev.txt

# Copy and fill in credentials
cp .env.sample .env

# Run targeted extraction (EsSalud, specific year)
python main.py targeted --year 2024 [--limit N]

# Run bulk catalog download
python main.py bulk --source SEACE --type JSON --year 2023 --month 11

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

Only the Bronze layer exists currently. Data flows:

1. **Targeted mode**: Paginate OCDS API → client-side filter by RUC + year → fetch full record → persist locally and optionally to Cloudflare R2.
2. **Bulk mode**: Download monthly ZIP from SEACE portal → stream to disk → replicate to R2 → decompress locally.

### Key Design Decisions

- **Client-side filtering**: The OCDS API has no native RUC/date filter support, so `TargetedExtractor` paginates all pages and filters in Python.
- **Dual persistence**: `FileManager` writes to `data/bronze/`, and `R2Manager` replicates to Cloudflare R2 when `OCDS_USE_R2=True`. R2Manager is imported lazily to avoid hard boto3 dependency.
- **Generator pagination**: `paginate_records()` in `ocds_client.py` yields pages on demand to handle arbitrarily large datasets.
- **Streaming downloads**: Bulk ZIPs are downloaded in 8192-byte chunks.

### Module Map

| Module | Role |
|---|---|
| `app/pipelines/bronze_layer.py` | Top-level orchestrator (`BronzePipeline`) |
| `app/services/extractors.py` | `TargetedExtractor` and `BulkExtractor` |
| `app/clients/ocds_client.py` | HTTP client with retry/backoff (`HTTPAdapter`) |
| `app/storage/file_manager.py` | Local disk persistence |
| `app/storage/r2_manager.py` | Cloudflare R2 (S3-compatible) upload |
| `app/config/settings.py` | All env-var config (loaded from `.env`) |
| `app/models/data_models.py` | `RecordSummary`, `CatalogItem`, `PaginationData` |
| `app/audit/logger.py` | Dual logger: console (INFO+), file (DEBUG+) at `data/audit/` |
| `dags/ocds_dag.py` | Airflow DAGs — weekly targeted, monthly bulk |
| `main.py` | CLI entry point (argparse) |

### Airflow DAGs

- `ocds_targeted_ingestion` — weekly; hardcoded `limit=100` for dev (remove for production).
- `ocds_bulk_ingestion` — monthly; date is **hardcoded** to Nov 2023. Should be replaced with Jinja macros (`{{ macros.ds_format(...) }}`) for dynamic scheduling.

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

### Data Layout

```
data/
  bronze/
    records/        # Individual JSON records from targeted extraction
    bulk_files/     # Raw ZIPs and decompressed catalogs
  audit/
    executions/     # ocds_extraction.log (debug-level)
```

## CI/CD

GitHub Actions (`.github/workflows/ci.yml`) runs `flake8` and `pytest` on push/PR to `main`.
