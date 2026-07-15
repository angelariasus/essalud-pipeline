# Diccionario de Datos

Este documento detalla el esquema de las tablas producidas por el pipeline en la **Capa Gold**, las cuales se persisten tanto en archivos Parquet (carpeta `data/mart/`) como en **SQL Server** (esquema `oro`).

---

## 1. Modelo Estrella (Data Warehouse)

El pipeline genera un esquema estrella clásico, optimizado para consumo directo desde Power BI o herramientas analíticas.

### Tabla de Hechos: `Fact_Ordenes_Y_Contratos`

Contiene el detalle transaccional de las adquisiciones OCDS a nivel de ítem.

| Columna | Tipo SQL / Parquet | Descripción |
|---|---|---|
| `Codigo_Convocatoria` | `VARCHAR(100)` | ID original del proceso en SEACE (Nomenclatura). |
| `N_Item` | `INT` | Número de ítem adjudicado dentro del proceso. |
| `Anio_Fiscal` | `INT` | Año extraído de la convocatoria (2022, 2023, 2024, 2025). |
| `Monto_Referencial_Soles` | `DECIMAL(18,2)` | Valor referencial del ítem antes de adjudicar. |
| `Monto_Adjudicado_Soles` | `DECIMAL(18,2)` | Monto final adjudicado en el contrato. |
| `Ahorro_Soles` | `DECIMAL(18,2)` | `Monto_Referencial_Soles` - `Monto_Adjudicado_Soles`. |
| `Flag_Contratacion_Directa` | `INT` (0 o 1) | `1` si el proceso es contratación directa (exoneración). |
| `Flag_Tiene_Adenda` | `INT` (0 o 1) | `1` si el contrato original sufrió modificaciones/adendas. |
| `Lead_Time_Total_Dias` | `INT` | Días calendario entre `Fecha_Convocatoria` y `Fecha_Suscripcion`. |
| `Lead_Time_Comite_Dias` | `INT` | Días entre `Fecha_Convocatoria` y `Fecha_Buena_Pro`. |
| `Lead_Time_Firma_Dias` | `INT` | Días entre `Fecha_Buena_Pro` y `Fecha_Suscripcion`. |
| `FK_Proveedor` | `BIGINT` | Clave foránea hacia `Dim_Proveedor`. |
| `FK_Entidad` | `BIGINT` | Clave foránea hacia `Dim_Entidad_Compradora`. |
| `FK_Medicamento` | `BIGINT` | Clave foránea hacia `Dim_Medicamento`. |
| `FK_Tipo_Proceso` | `BIGINT` | Clave foránea hacia `Dim_Tipo_Proceso`. |
| `Fecha_Convocatoria` | `DATE` | Fecha de publicación en SEACE. |
| `Fecha_Buena_Pro` | `DATE` | Fecha de adjudicación (award). |
| `Fecha_Suscripcion` | `DATE` | Fecha de firma del contrato. |

### Dimensiones

#### `Dim_Proveedor`
*Catálogo de proveedores del Estado.*
- `SK_Proveedor`: Surrogate key.
- `RUC_Proveedor`: Documento de identidad tributario.
- `Nombre_Razon_Social`: Nombre del proveedor.

#### `Dim_Entidad_Compradora`
*Catálogo de Redes Asistenciales y Sedes Centrales de EsSalud.*
- `SK_Entidad`: Surrogate key.
- `RUC_Entidad`: RUC (usualmente de EsSalud).
- `Nombre_Entidad`: Nombre oficial en SEACE.
- `Red_Asistencial`: Red a la que pertenece (limpiada vía IA/Excel).
- `Tipo_Red`: LIMA o PROVINCIA.

#### `Dim_Medicamento`
*Catálogo extraído del Petitorio Nacional.*
- `SK_Medicamento`: Surrogate key.
- `Codigo_Siga`: Código interno del petitorio.
- `Nombre_Medicamento`: Nombre estandarizado (Denominación Común Internacional).
- `Especialidad_Autorizada`: Área médica primaria (Oncología, Cardiología, etc.).
- `Metodo_Clasificacion`: Método usado para limpieza (EXACT_MATCH, FUZZY_MATCH, AI_CLEANED).

#### `Dim_Tipo_Proceso`
*Categorización de los métodos de compra.*
- `SK_Tipo_Proceso`: Surrogate key.
- `Siglas_Proceso`: Siglas en SEACE (ej. LP, CP, RES, AMC).
- `Categoria_Proceso`: COMPETITIVO, DIRECTO, CATALOGO, REGIMEN_ESPECIAL.
- `Es_Contratacion_Directa`: Booleano descriptivo.

---

## 2. Vistas Analíticas de Negocio

En SQL Server y a través del motor de procesamiento, se derivan tablas adicionales consolidadas.

### Riesgo y Concentración (`vw_Matriz_Riesgo_HHI`)
Calcula el índice Herfindahl-Hirschman (HHI) por Año, Red Asistencial y Medicamento.
- `HHI`: Suma del cuadrado de la cuota de mercado (`(Monto_Proveedor / Monto_Total) * 100 ^ 2`).
  - `HHI < 1500`: Mercado competitivo.
  - `1500 <= HHI <= 2500`: Mercado moderadamente concentrado.
  - `HHI > 2500`: Mercado altamente concentrado (Riesgo de monopolio).

### Predicciones Machine Learning (`Pred_Lead_Time.parquet`)
Generado por el módulo de Machine Learning.
- `Lead_Time_Actual`: Lead Time real si se conoce.
- `Lead_Time_Predicho`: Predicción del modelo XGBoost de la duración del proceso de compra.
- `Residual`: Diferencia entre actual y predicho (útil para detectar ineficiencias atípicas).

---

## 3. Manejo de Valores Faltantes (Sentinelas)

Para mantener la integridad referencial en el modelo estrella cuando faltan datos (ej. un registro no logra hacer match con el catálogo de medicamentos):

- **`-1`**: `DESCONOCIDO` (Falta información en la fuente original OCDS).
- **`-2`**: `NO_APLICA` (El campo no aplica lógicamente).
- **`-3`**: `ERROR_EXTRACCION` (Error de parseo durante la etapa Silver).
