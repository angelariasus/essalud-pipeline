# Plan: Fase 4 — Modelado Predictivo del Lead Time Contractual

## Contexto

Las Fases 1-3 del pipeline ya están implementadas y verificadas: Bronze→Silver→Gold produce
7 tablas Parquet en `bi/` (9 292 filas en `Fact_Ordenes_Y_Contratos.parquet`, datos reales
2022-2023 + sintéticos 2024-2025). Esta fase agrega un modelo regresivo de Lead Time
sin tocar ningún archivo del pipeline existente.

**Objetivo:** predecir cuántos días tardará un proceso de contratación entre convocatoria y
suscripción del contrato, exponer las predicciones en `bi/Pred_Lead_Time.parquet` y
documentar la integración en Power BI (Vista Táctica: histórico + predicho).

**Prerrequisito de ejecución:** `python main.py gold` debe haber generado los 7 Parquet en
`bi/`. El notebook se ejecuta desde `mlpredicts/` con el venv del proyecto (`.venv/`), que
ya tiene `xgboost`, `scikit-learn`, `pandas`, `numpy`, `matplotlib`, `seaborn`, `joblib`
(ver `ml/requirements.txt`).

## Datos disponibles en `bi/`

| Parquet | Filas | Columnas relevantes para ML |
|---|---|---|
| `Fact_Ordenes_Y_Contratos.parquet` | 9 292 | `Fecha_Convocatoria`, `Fecha_Buena_Pro`, `Fecha_Suscripcion`, `Monto_Adjudicado_Soles`, `Monto_Referencial_Soles`, `Flag_Contratacion_Directa`, `Flag_Tiene_Adenda`, `Anio_Fiscal`, `FK_Entidad`, `FK_Medicamento`, `FK_Tipo_Proceso` |
| `Dim_Entidad_Compradora.parquet` | 30 | `SK_Entidad`, `Red_Asistencial`, `Tipo_Red` |
| `Dim_Medicamento.parquet` | 906 | `SK_Medicamento`, `Especialidad_Autorizada`, `Metodo_Clasificacion` |
| `Dim_Tipo_Proceso.parquet` | 12 | `SK_Tipo_Proceso`, `Categoria_Proceso`, `Es_Contratacion_Directa` |

**Target:** `Lead_Time_Total = (Fecha_Suscripcion − Fecha_Convocatoria).dt.days`
- Registros válidos (ambas fechas no nulas, resultado ≥ 0): ~6 582 de 9 292 (70.8%)
- Distribución: media ≈ 46 días, mediana 33, std 124, rango 0-995 (sesgado a la derecha)
- Las columnas `Lead_Time_*_Dias` son computed columns de SQL Server y **no están** en el
  Parquet; se calculan en el notebook desde las columnas DATE.

## Archivo a crear

```
mlpredicts/
└── LeadTime_Predictor.ipynb     ← único archivo nuevo de código
models/                          ← creado por el notebook al ejecutar
    best_model.joblib
```

Y el notebook escribe:
```
bi/Pred_Lead_Time.parquet        ← nueva tabla Gold para Power BI
```

## Estructura del notebook (13 celdas)

### Celda 1 — Markdown: título y contexto
Describe objetivo, dataset y prerrequisitos.

### Celda 2 — Imports y rutas
```python
import pandas as pd, numpy as np, joblib, warnings
from pathlib import Path
import matplotlib.pyplot as plt, seaborn as sns
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import OrdinalEncoder, StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.model_selection import StratifiedKFold
from sklearn.metrics import root_mean_squared_error
import xgboost as xgb

warnings.filterwarnings("ignore")
BI_DIR  = Path("../bi")
MDL_DIR = Path("models"); MDL_DIR.mkdir(exist_ok=True)
```

### Celda 3 — Carga y join de Parquets
```python
fact    = pd.read_parquet(BI_DIR / "Fact_Ordenes_Y_Contratos.parquet")
entidad = pd.read_parquet(BI_DIR / "Dim_Entidad_Compradora.parquet")
med     = pd.read_parquet(BI_DIR / "Dim_Medicamento.parquet")
tipo    = pd.read_parquet(BI_DIR / "Dim_Tipo_Proceso.parquet")

df = (fact
  .merge(entidad[["SK_Entidad","Red_Asistencial","Tipo_Red"]],
         left_on="FK_Entidad", right_on="SK_Entidad", how="left")
  .merge(med[["SK_Medicamento","Especialidad_Autorizada","Metodo_Clasificacion"]],
         left_on="FK_Medicamento", right_on="SK_Medicamento", how="left")
  .merge(tipo[["SK_Tipo_Proceso","Categoria_Proceso"]],
         left_on="FK_Tipo_Proceso", right_on="SK_Tipo_Proceso", how="left")
)
```

### Celda 4 — Ingeniería de features
```python
to_dt = lambda c: pd.to_datetime(df[c], errors="coerce")
df["Lead_Time_Total"]  = (to_dt("Fecha_Suscripcion") - to_dt("Fecha_Convocatoria")).dt.days
df["Lead_Time_Comite"] = (to_dt("Fecha_Buena_Pro")   - to_dt("Fecha_Convocatoria")).dt.days
df["Mes_Convocatoria"] = to_dt("Fecha_Convocatoria").dt.month

# "Historial de retrasos del comité" por Red Asistencial (media global por grupo)
hist = df.groupby("Red_Asistencial")["Lead_Time_Comite"].mean().rename("retraso_historico_red")
df = df.merge(hist, on="Red_Asistencial", how="left")

df_ml = df[df["Lead_Time_Total"].notna() & (df["Lead_Time_Total"] >= 0)].copy()
print(f"Registros para ML: {len(df_ml)} / {len(df)}")
```

### Celda 5 — Análisis exploratorio (3 gráficos)
- Histograma `Lead_Time_Total` (distribución sesgada, justifica log-transform opcional)
- Boxplot por `Red_Asistencial` (variabilidad entre Redes — evidencia de heterogeneidad)
- Barras de promedio por `Categoria_Proceso` (DIRECTO ≈ 2 d vs. COMPETITIVO ≈ 70 d)

### Celda 6 — Definición de features y preprocesador
```python
CAT = ["Red_Asistencial","Categoria_Proceso","Especialidad_Autorizada",
       "Tipo_Red","Metodo_Clasificacion"]
NUM = ["Monto_Adjudicado_Soles","Anio_Fiscal","Mes_Convocatoria","retraso_historico_red"]
BIN = ["Flag_Contratacion_Directa","Flag_Tiene_Adenda"]
TARGET = "Lead_Time_Total"

for c in CAT: df_ml[c] = df_ml[c].fillna("DESCONOCIDO")
X = df_ml[CAT + NUM + BIN].copy()
y = df_ml[TARGET].astype(float)

pre = ColumnTransformer([
    ("cat", OrdinalEncoder(handle_unknown="use_encoded_value", unknown_value=-1), CAT),
    ("num", StandardScaler(), NUM),
    ("bin", "passthrough", BIN),
])
```

### Celda 7 — Cross-validation estratificado por Red Asistencial
```python
from sklearn.preprocessing import LabelEncoder
strat_labels = LabelEncoder().fit_transform(df_ml["Red_Asistencial"])
skf = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
cv_splits = list(skf.split(X, strat_labels))
# StratifiedKFold con Red_Asistencial como etiqueta asegura que cada fold
# tenga distribución similar de Redes (evita que una Red quede solo en train o val)
```

### Celda 8 — XGBoost regresión con CV
```python
pipe_xgb = Pipeline([("pre", pre), ("xgb", xgb.XGBRegressor(
    n_estimators=300, max_depth=6, learning_rate=0.1,
    subsample=0.8, colsample_bytree=0.8, random_state=42, n_jobs=-1))])

rmse_xgb = []
for tr, val in cv_splits:
    pipe_xgb.fit(X.iloc[tr], y.iloc[tr])
    rmse_xgb.append(root_mean_squared_error(y.iloc[val], pipe_xgb.predict(X.iloc[val])))
print(f"XGBoost  CV RMSE: {np.mean(rmse_xgb):.2f} ± {np.std(rmse_xgb):.2f} días")
```

### Celda 9 — Random Forest regresión con CV
```python
pipe_rf = Pipeline([("pre", pre), ("rf", RandomForestRegressor(
    n_estimators=300, max_depth=12, min_samples_leaf=5, random_state=42, n_jobs=-1))])

rmse_rf = []
for tr, val in cv_splits:
    pipe_rf.fit(X.iloc[tr], y.iloc[tr])
    rmse_rf.append(root_mean_squared_error(y.iloc[val], pipe_rf.predict(X.iloc[val])))
print(f"Random Forest CV RMSE: {np.mean(rmse_rf):.2f} ± {np.std(rmse_rf):.2f} días")
```

### Celda 10 — Comparación y selección por RMSE
```python
resultados = pd.DataFrame({
    "Modelo": ["XGBoost", "Random Forest"],
    "RMSE_Mean": [np.mean(rmse_xgb), np.mean(rmse_rf)],
    "RMSE_Std":  [np.std(rmse_xgb),  np.std(rmse_rf)],
}).sort_values("RMSE_Mean")
display(resultados)

best_pipe = pipe_xgb if np.mean(rmse_xgb) < np.mean(rmse_rf) else pipe_rf
best_name = "XGBoost" if best_pipe is pipe_xgb else "Random Forest"
best_pipe.fit(X, y)    # reentrenar sobre datos completos
print(f"\nModelo seleccionado: {best_name}")
```

### Celda 11 — Importancia de features
```python
step = "xgb" if best_name == "XGBoost" else "rf"
importances = best_pipe.named_steps[step].feature_importances_
(pd.DataFrame({"feature": CAT + NUM + BIN, "importance": importances})
   .sort_values("importance")
   .plot.barh(x="feature", y="importance", figsize=(8,6), legend=False,
              title="Importancia de Features — " + best_name))
plt.tight_layout(); plt.show()
```

### Celda 12 — Generación de tabla de predicciones y export a `bi/`
```python
# Predice sobre TODOS los registros (también los sin fecha de suscripción)
df_pred = df.copy()
for c in CAT: df_pred[c] = df_pred[c].fillna("DESCONOCIDO")
df_pred["Lead_Time_Predicho"] = best_pipe.predict(df_pred[CAT + NUM + BIN]).clip(0)

pred_table = df_pred[[
    "Codigo_Convocatoria","N_Item","Anio_Fiscal",
    "Red_Asistencial","Categoria_Proceso",
    "Lead_Time_Total","Lead_Time_Predicho",
]].rename(columns={"Lead_Time_Total": "Lead_Time_Actual"})
pred_table["Residual"] = pred_table["Lead_Time_Actual"] - pred_table["Lead_Time_Predicho"]

out = BI_DIR / "Pred_Lead_Time.parquet"
pred_table.to_parquet(out, index=False)
print(f"Exportado: {out}  ({len(pred_table)} registros)")
```

**Schema de `bi/Pred_Lead_Time.parquet`:**

| Columna | Tipo | Notas |
|---|---|---|
| `Codigo_Convocatoria` | float | Clave natural SEACE |
| `N_Item` | float | Número de ítem |
| `Anio_Fiscal` | int | 2022–2025 |
| `Red_Asistencial` | str | Nombre de la Red |
| `Categoria_Proceso` | str | COMPETITIVO / DIRECTO / CATALOGO / REGIMEN_ESPECIAL |
| `Lead_Time_Actual` | float | Días reales (NaN si alguna fecha falta) |
| `Lead_Time_Predicho` | float | Predicción del modelo (siempre presente, ≥ 0) |
| `Residual` | float | Actual − Predicho (NaN si Actual es NaN) |

### Celda 13 — Serialización del modelo
```python
joblib.dump(best_pipe, MDL_DIR / "best_model.joblib")
print("Guardado: mlpredicts/models/best_model.joblib")
print("Carga: model = joblib.load('models/best_model.joblib')")
print("Uso:   pred  = model.predict(X_new)  # mismas columnas CAT+NUM+BIN")
```

## Integración Power BI — paso a paso

### 1. Importar tabla de predicciones
Power BI Desktop → **Obtener datos** → **Parquet** → `<proyecto>/bi/Pred_Lead_Time.parquet`
→ Renombrar tabla como **"Predicción Lead Time"**

### 2. Crear relación
Vista Modelo → `Predicción Lead Time[Red_Asistencial]` ↔ `Dim_Entidad_Compradora[Red_Asistencial]`
(Muchos a uno, filtro bidireccional)

### 3. Medidas DAX (en tabla "Predicción Lead Time")
```dax
Lead Time Real (días)     = AVERAGE('Predicción Lead Time'[Lead_Time_Actual])
Lead Time Predicho (días) = AVERAGE('Predicción Lead Time'[Lead_Time_Predicho])
Error Absoluto Medio      = AVERAGEX(
    FILTER('Predicción Lead Time', NOT ISBLANK([Lead_Time_Actual])),
    ABS([Lead_Time_Actual] - [Lead_Time_Predicho])
)
```

### 4. Vista Táctica — página "Lead Time Contractual"
| Visual | Configuración |
|---|---|
| Gráfico de líneas | Eje X: `Anio_Fiscal`; líneas: `[Lead Time Real]` y `[Lead Time Predicho]` |
| Segmentador | Campo: `Red_Asistencial` (lista, selección múltiple) |
| Barras horizontales | Eje Y: `Categoria_Proceso`; valor: `[Lead Time Predicho (días)]` |
| Tarjetas KPI | Lead Time Real Promedio / Lead Time Predicho 2025 / Error Absoluto Medio |


## Verificación end-to-end

1. Ejecutar todas las celdas sin error → se imprime RMSE de ambos modelos
2. RMSE razonable: esperar < 150 días en CV (la distribución tiene std=124)
3. `bi/Pred_Lead_Time.parquet` existe con 9 292 filas; columna `Lead_Time_Predicho` sin nulos
4. `mlpredicts/models/best_model.joblib` cargable con `joblib.load`
5. En Power BI: importar el Parquet, crear relación, añadir las 3 medidas DAX y verificar
   que el gráfico de líneas muestra dos series por año con slicer de Red funcional
