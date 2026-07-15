# Guía de Desarrollo y Contribución

Este documento establece los lineamientos para desarrolladores e ingenieros de datos que necesiten extender, depurar o testear el `essalud-pipeline`.

---

## 1. Entorno de Desarrollo Local

Se asume el uso de **Windows** y **PowerShell** en el equipo local, que es el estándar del equipo.

### Clonar y preparar entorno
```powershell
git clone <repo_url>
cd essalud-pipeline
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

### Instalar dependencias
Para desarrollo, se requiere instalar los requerimientos base y los de testing/linting:
```powershell
pip install -r requirements.txt
pip install -r requirements-dev.txt
```

---

## 2. Testing y Calidad de Código

El repositorio cuenta con una suite completa de pruebas unitarias y de integración, ubicada en el directorio `test/`. El pipeline CI/CD (GitHub Actions) asume que todas estas pruebas pasen exitosamente.

### Ejecutar Pruebas (Pytest)
Para ejecutar la suite completa:
```powershell
pytest test/ -v
```

> [!WARNING]
> Las pruebas que involucran `PySpark` toman un poco más de tiempo. Para correr una prueba rápida excluyendo Spark:
> ```powershell
> pytest test/ -q --ignore=test/test_silver_spark.py
> ```

### Reglas de Estilo (Flake8)
El proyecto utiliza `flake8` para el linting estático. 
```powershell
flake8 app dags app/cli.py --max-line-length=127
```

---

## 3. Estructura de Código Core (`app/`)

Si necesitas añadir un nuevo paso al pipeline o modificar reglas de transformación, los módulos principales son:

- `app/services/extractors.py`: Estrategias de extracción de la API OCDS (Bulk / Targeted).
- `app/services/ocds_flattener.py`: Lógica de PySpark para el aplanado de JSON anidado (Silver).
- `app/services/ai_cleaner.py`: Integración con LLM (Gemini) para normalización de Redes Asistenciales.
- `app/services/dim_resolver.py`: Lógica de cruces para generar las dimensiones y el esquema estrella.
- `app/loaders/dw_loader.py`: Abstracción de conexión a bases de datos JDBC.

---

## 4. Pipeline de CI/CD (GitHub Actions)

El repositorio incluye un flujo automatizado en `.github/workflows/` que reacciona a los **Push** en la rama `main` o en los **Pull Requests**.

El workflow ejecuta:
1. **Linting**: Valida la sintaxis con `flake8` y reporta fallos si el código no cumple con el estándar (límite de 127 caracteres, entre otros).
2. **Setup de Java y Python**: Prepara las versiones correctas (Java 17, Python 3.11).
3. **Tests**: Ejecuta toda la suite de `pytest`. 
   *Nota: Las variables del `.env` (como contraseñas dummy o flags de API) están mockeadas en los tests y se inyectan a nivel de entorno en CI.*

> [!IMPORTANT]
> **No** realizar push directo a `main`. Todo cambio en las lógicas de PySpark o extracciones debe ser introducido a través de un PR (Pull Request) validando que la suite de GitHub Actions finalice en verde.
