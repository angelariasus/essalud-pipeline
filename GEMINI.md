# OCDS Data Framework - EsSalud (Data Pipeline)

## Project Overview
This project is an Object-Oriented Python framework designed to extract, process, and store data from the Peruvian Open Contracting Data Standard (OCDS) API (SEACE). It serves as the main ELT (Extract, Load, Transform) engine to feed a Data Lake (local and cloud) and evaluate the efficiency of medicine purchases in "EsSalud".

The project follows a **Medallion Architecture**:
*   **Bronze Layer**: Hybrid extraction (Targeted/Bulk) of JSON records from the API, storing them locally (`data/bronze/`) with optional replication to Cloudflare R2.
*   **Silver Layer**: Flattening nested JSONs into DataFrames (using Pandas), fuzzy matching for data normalization (using Rapidfuzz), and dimension resolution.
*   **Gold/DW Layer**: Loading the processed data into a Star Schema Data Warehouse (SQL Server) for BI and predictive evaluation.

**Key Technologies:**
*   **Language**: Python 3.11+
*   **Core Libraries**: `requests`, `pandas`, `rapidfuzz`, `sqlalchemy`, `pyodbc`, `boto3` (for R2)
*   **Orchestration**: Apache Airflow
*   **Infrastructure**: Docker Compose (for Airflow), Cloudflare R2 (S3-compatible)

## Directory Structure
*   `app/`: Core Python module containing the ELT logic.
    *   `clients/`: HTTP client for OCDS API.
    *   `loaders/`: Loads master data and handles DW insertion.
    *   `models/`: Data models (Pydantic / Dataclasses).
    *   `pipelines/`: Pipeline orchestrators (Bronze and Silver layers).
    *   `services/`: Business logic, extraction, flattening, and dimension resolution.
    *   `storage/`: Persistence controllers (Local, R2).
    *   `utils/`: Helpers and fuzzy matching logic.
*   `dags/`: Apache Airflow DAGs (`ocds_dag.py`, `silver_dag.py`).
*   `data/`: Local Data Lake storage (`bronze/`, `silver/`, `audit/`).
*   `extra-data/`: Master files (Excel) for cross-referencing.
*   `star-schema/`: DDL scripts for the SQL Server Data Warehouse.

## Building and Running

### Prerequisites
*   Python 3.11+
*   Docker Desktop (for Apache Airflow)
*   Optional: Cloudflare R2 credentials (configured in `.env`)
*   Optional: SQL Server instance (configured in `.env` as `DW_CONN_STRING`)

### Local Setup
1.  Create and activate a virtual environment:
    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows: .\venv\Scripts\activate
    ```
2.  Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```
3.  Set up the `.env` file based on `.env.sample`.

### Execution via CLI
*   **Targeted Extraction (Bronze):**
    ```bash
    python main.py targeted --year 2024
    python main.py targeted --year 2024 --limit 10
    ```
*   **Bulk Extraction (Bronze):**
    ```bash
    python main.py bulk --source SEACE --type JSON --year 2023 --month 11
    ```
*   **Run Silver Layer & DW Load:**
    ```bash
    python -c "from app.pipelines.silver_layer import SilverPipeline; SilverPipeline().run()"
    ```

### Execution via Airflow
1.  Initialize the Airflow database:
    ```bash
    docker-compose up airflow-init
    ```
2.  Start the Airflow containers:
    ```bash
    docker-compose up -d --build
    ```
3.  Access the web interface at `http://localhost:8080` (User/Pass: `airflow`/`airflow`) and enable the DAGs (`ocds_targeted_ingestion`, `ocds_bulk_ingestion`, `ocds_silver_pipeline`).

## Development Conventions
*   **Architecture Strategy:** Explicit separation of concerns into Extractors, Flatteners, Resolvers, and Loaders.
*   **Data Matching:** Employs explicit fuzzy matching (`app/utils/fuzzy_matcher.py`) to map raw textual data to master records, resolving data quality gaps.
*   **Database Loading:** Respects Foreign Key (FK) dependencies by inserting data in a specific dependency order using explicit chunking and transaction management.
*   **Cloud Storage:** Supports toggling R2 cloud persistence via environment variables.
