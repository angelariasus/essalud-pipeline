# OCDS Data Framework - EsSalud (Capa Bronze)

Un framework en Python orientado a objetos (OOP) diseñado para extraer, procesar y almacenar datos del Portal de Contrataciones Abiertas de Perú (estándar OCDS). Este proyecto actúa como el motor ELT principal para alimentar un Data Lake local y en la nube, con el objetivo de evaluar la eficiencia de compras de medicamentos en "EsSalud".

## 🚀 Características Principales
- **Arquitectura Medallón:** Enfoque actual en la ingesta robusta de la Capa Bronze.
- **Extracción Híbrida (Targeted & Bulk):**
  - *Targeted*: Descarga expedientes completos (`RecordPackage`) filtrados estrictamente por comprador (RUC) y año.
  - *Bulk*: Descarga automática de volcados mensuales masivos (ZIP) del SEACE.
- **Almacenamiento Híbrido:** Persistencia en disco local (`data/bronze/`) con replicación automática en tiempo real hacia la nube mediante **Cloudflare R2** (S3-compatible).
- **Orquestación Empresarial:** Integración nativa con **Apache Airflow** (Docker) para programación de DAGs, manejo de reintentos y monitoreo gráfico.

## 📁 Estructura del Proyecto

```text
.
├── app/
│   ├── audit/          # Sistema de logs y control de ejecuciones
│   ├── clients/        # Cliente HTTP robusto (retry policy, backoff) para la API OCDS
│   ├── config/         # Configuraciones dinámicas y lectura de variables de entorno
│   ├── models/         # Clases de datos (Pydantic / Dataclasses)
│   ├── pipelines/      # Orquestador principal (BronzePipeline)
│   ├── services/       # Lógica de extracción (Client-Side Filtering de RUC y Fechas)
│   ├── storage/        # Controladores de persistencia (FileManager, R2Manager)
│   └── utils/          # Utilidades (descompresión ZIP, helpers)
├── dags/               # Grafos dirigidos para Apache Airflow (ocds_dag.py)
├── data/               # Data Lake Local (bronze/, audit/)
├── docker-compose.yaml # Infraestructura de contenedores de Airflow
├── Dockerfile          # Imagen de Airflow extendida con dependencias del proyecto
├── main.py             # CLI Entrypoint principal
├── requirements.txt    # Dependencias de Python (requests, boto3, python-dotenv)
└── .env                # Credenciales y configuración local (R2)
```

## ⚙️ Requisitos Previos
- Python 3.11 o superior.
- Docker Desktop (Para levantar Apache Airflow).
- Cuenta de Cloudflare R2 (Opcional, para almacenamiento en la nube).

## 🛠️ Instalación y Configuración Local

1. **Clonar y preparar el entorno:**
   ```powershell
   python -m venv venv
   .\venv\Scripts\activate
   pip install -r requirements.txt
   ```

2. **Configurar el archivo `.env`:**
   Abre el archivo `.env` en la raíz del proyecto e ingresa tus credenciales de almacenamiento en la nube:
   ```env
   OCDS_USE_R2=True
   OCDS_R2_ACCOUNT_ID=tu_account_id
   OCDS_R2_ACCESS_KEY=tu_access_key
   OCDS_R2_SECRET_KEY=tu_secret_key
   OCDS_R2_BUCKET_NAME=essalud-bronze-lake
   ```

## 💻 Uso por Línea de Comandos (CLI)

El framework proporciona una potente interfaz de terminal para ejecuciones manuales o de prueba.

### Modo Targeted (Extracción Dirigida)
Filtra y descarga expedientes completos de una entidad específica. Aplica filtrado del lado del cliente (Client-Side Filtering) para procesar únicamente la data relevante, superando las limitaciones nativas de la API.
```powershell
# Extraer datos de EsSalud (RUC 20131257750) del año 2024
python main.py targeted --year 2024

# Limitar la extracción a los primeros 10 expedientes encontrados
python main.py targeted --year 2024 --limit 10
```

### Modo Bulk (Extracción Masiva)
Descarga catálogos mensuales completos de la página oficial de OCDS en formato crudo.
```powershell
python main.py bulk --source SEACE --type JSON --year 2023 --month 11
```

## 📅 Orquestación con Apache Airflow
Para levantar la infraestructura de orquestación y dejar el proceso corriendo automáticamente de manera programada:

1. **Inicializar la base de datos de Airflow:**
   ```powershell
   docker-compose up airflow-init
   ```
2. **Levantar los contenedores (construyendo la imagen con tus dependencias):**
   ```powershell
   docker-compose up -d --build
   ```
3. **Acceder al panel web:**
   Ingresa en tu navegador a `http://localhost:8080`. Inicia sesión con el usuario `airflow` y la contraseña `airflow`. Enciende los DAGs `ocds_targeted_ingestion` y `ocds_bulk_ingestion`.
