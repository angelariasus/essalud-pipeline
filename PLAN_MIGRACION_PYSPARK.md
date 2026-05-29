# Plan de Migración a PySpark (Capa Silver y Gold/DW)

## 1. Objetivo
Migrar el procesamiento de datos tabulares (aplanamiento) de la capa Silver y la carga a la capa Gold (DW) del framework actual (basado en `pandas`) hacia **Apache Spark (PySpark)**. Esto permitirá procesar los archivos JSON masivos de la capa Bronze de manera distribuida, escalable y tolerante a fallos, preparando el pipeline para grandes volúmenes de datos.

## 2. Principios de Diseño para Continuidad sin Errores
Para asegurar que el flujo continúe sin excepciones fatales ("fail-safe") se aplicarán las siguientes estrategias:
*   **Esquemas Estrictos y Manejo de Corrupción (`DROPMALFORMED` / `PERMISSIVE`):** Al leer JSONs, Spark usará un esquema (`StructType`) predefinido. Los registros corruptos serán capturados en una columna `_corrupt_record` para auditoría, sin detener el job.
*   **Pandas UDFs Vectorizados con Graceful Degradation:** El *Fuzzy Matching* (`rapidfuzz`) se encapsulará en `pandas_udf`. Si el match falla para un registro, retornará un "Sentinel" (ej. `-1` o `-2`) en lugar de arrojar una excepción.
*   **Checkpoints y Staging (Parquet):** Después de transformaciones complejas (como aplanamiento), el resultado intermedio se guardará en disco (Parquet). Esto evita recalcular todo si ocurre un fallo en fases posteriores.
*   **Carga Transaccional (Staging Tables):** La escritura por JDBC a SQL Server se hará hacia tablas *Staging* temporales. Posteriormente, un procedimiento almacenado (`MERGE` o `INSERT`) moverá los datos a producción de forma atómica.

---

## 3. Impacto Arquitectónico y Dependencias

### Modificaciones en `requirements.txt` / Docker
*   Añadir `pyspark>=3.5.0` y `pyarrow>=15.0.0` (vital para Pandas UDFs).
*   Descargar el driver JDBC de SQL Server (`mssql-jdbc.jar`) en una ruta accesible o configurarlo vía coordenadas Maven.
*   Asegurar que la imagen de Docker base tenga Java (JRE 11 o superior).

### Nuevo Módulo: `app/config/spark_session.py`
Se creará un Singleton para la inicialización de Spark:
```python
from pyspark.sql import SparkSession

def get_spark_session(app_name="EsSalud_Pipeline"):
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars.packages", "com.microsoft.sqlserver:mssql-jdbc:12.4.2.jre11") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
```

---

## 4. Diseño Detallado por Componente

### A. Aplanamiento Robusto (`app/services/ocds_flattener.py`)
Reemplazar la lectura iterativa manual por la API de DataFrames de Spark.

1. **Lectura Segura:** 
   Se usará la lectura de directorios completos de JSONs con soporte multilínea.
   ```python
   df_bronze = spark.read.option("multiline", "true") \
                    .option("mode", "PERMISSIVE") \
                    .option("columnNameOfCorruptRecord", "_corrupt_record") \
                    .json("data/bronze/records/*/*/*.json")
   ```
2. **Transformación (Explode):**
   Uso de `explode_outer` para mantener las licitaciones incluso si no tienen arrays anidados (evita pérdida de datos silenciosa).
   ```python
   from pyspark.sql.functions import col, explode_outer
   
   df_items = df_bronze.withColumn("item", explode_outer("compiledRelease.tender.items"))
   # ... Selecciones (select, alias) para construir las columnas planas
   ```
3. **Punto de Control (Checkpoint):**
   Escribir el resultado aplanado (Silver Staging).
   ```python
   df_flat.write.mode("overwrite").partitionBy("anio_fiscal").parquet("data/silver/staging_flat/")
   ```

### B. Lógica de Maestros y Fuzzy Matching (`app/utils/fuzzy_matcher.py`)
El `rapidfuzz` es una librería en C++ pensada para un hilo. En Spark, debe vectorizarse usando Apache Arrow.

1. **Broadcasting de Maestros:**
   Los Excels del Petitorio y Establecimientos se leen con `pandas` (ya que son < 1000 filas) y se envían como variables *Broadcast* a todos los nodos de Spark.
   ```python
   petitorio_pd = pd.read_excel("...")
   broadcast_petitorio = spark.sparkContext.broadcast(petitorio_pd.to_dict('records'))
   ```

2. **Pandas UDF Vectorizado con Try/Except:**
   Para escalar, se define una función escalar.
   ```python
   from pyspark.sql.functions import pandas_udf, PandasUDFType
   import pandas as pd

   @pandas_udf("struct<sk:int, score:float, metodo:string>", PandasUDFType.SCALAR)
   def match_medicamento_udf(descripciones: pd.Series) -> pd.DataFrame:
       resultados = []
       maestro = broadcast_petitorio.value
       for desc in descripciones:
           try:
               if pd.isna(desc):
                   resultados.append({"sk": -1, "score": 0.0, "metodo": "NULL"})
                   continue
               # ... lógica de rapidfuzz contra maestro ...
               resultados.append({"sk": id_encontrado, "score": max_score, "metodo": "FUZZY"})
           except Exception as e:
               # Fallback seguro, no detiene el Job de Spark
               resultados.append({"sk": -1, "score": 0.0, "metodo": f"ERROR: {str(e)}"})
       return pd.DataFrame(resultados)
   ```

### C. Resolución de Dimensiones (`app/services/dim_resolver.py`)
Spark generará los DataFrames de dimensiones mediante uniones (`join`) y agregaciones (`groupBy`).
En lugar de generar un "SK" numérico secuencial en memoria (lo cual es costoso en sistemas distribuidos), **confiaremos en la generación de Claves Hash consistentes (MD5)** o delegaremos la creación del SK autonumérico a SQL Server.

Para consistencia idemptotente en Spark, la generación de Surrogate Keys puede ser:
```python
from pyspark.sql.functions import md5, concat_ws
dim_proveedor = df_flat.select("ruc_proveedor", "nombre_proveedor").distinct()
dim_proveedor = dim_proveedor.withColumn("hash_sk", md5(concat_ws("|", "ruc_proveedor", "nombre_proveedor")))
```

### D. Carga al Data Warehouse sin interrupciones (`app/loaders/dw_loader.py`)
Para evitar el escenario donde el job de Spark inserta la mitad de los datos, falla, y deja la tabla productiva inconsistente:

1. **Tablas de Staging Temporales en BD:**
   Spark escribirá en `stg_Dim_Proveedor`, `stg_Fact_Adjudicaciones`, etc.
   ```python
   # Opciones seguras para base de datos
   jdbc_options = {
       "url": DW_CONN_STRING,
       "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
       "user": user,
       "password": password,
       "batchsize": "10000",
       "tableLock": "true" 
   }
   
   df_dim.write.mode("overwrite").jdbc(table="stg_Dim_Proveedor", **jdbc_options)
   ```

2. **Carga Atómica (Stored Procedures):**
   Tras la escritura exitosa en las tablas staging, se invoca a PyODBC o a una tarea separada en Airflow para ejecutar un Procedimiento Almacenado de SQL Server que hace un `MERGE` de Staging a Producción. Si Spark falla, la tabla productiva no se toca.

---

## 5. Plan de Ejecución Paso a Paso

1. **Fase 1: Entorno y Core (1-2 días)**
   * Actualizar el Dockerfile (añadir JRE 11) y `requirements.txt` (pyspark).
   * Crear `spark_session.py`.
   * Adaptar Airflow para soportar memoria en el Celery/Local worker para Spark local.

2. **Fase 2: Aplanamiento - El Flattener (2-3 días)**
   * Reescribir `ocds_flattener.py` usando `spark.read.json`.
   * Validar que el schema capture registros defectuosos en `_corrupt_record`.
   * Verificar salida a Parquet particionada.

3. **Fase 3: UDFs y Lógica Difusa (2-3 días)**
   * Adaptar `fuzzy_matcher.py` a `pandas_udf`.
   * Probar el fallback de errores inyectando strings inválidos.

4. **Fase 4: Dim Resolver y DW Loader (3 días)**
   * Reescribir `dim_resolver.py` a sentencias de PySpark SQL/DataFrames.
   * Crear el esquema Staging en SQL Server (`star-schema/EsSalud_StarSchema_DDL.sql`).
   * Integrar la escritura JDBC en lotes.

5. **Fase 5: Pruebas y DAG (2 días)**
   * Ejecutar el nuevo `silver_layer.py` que encadena el flujo de Spark.
   * Modificar el DAG `silver_dag.py` para levantar un job de Spark (`SparkSubmitOperator` u operador Python si es memoria local en el mismo contenedor).