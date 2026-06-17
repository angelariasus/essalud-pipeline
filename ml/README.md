# GBDT Risk Management System
## Sistema Predictivo de Riesgo de Sobrecosto en Contratos de Medicamentos

**Universidad Nacional Mayor de San Marcos (UNMSM)**  
Facultad de Ingeniería de Sistemas e Informática — Ingeniería de Software  
Línea de Investigación: Aprendizaje Supervisado Aplicado a Gestión de Riesgos en Adquisiciones Públicas

---

## Ficha Técnica del Proyecto

| Atributo | Descripción |
|----------|-------------|
| **Nombre** | GBDT Risk Management System — Adquisiciones Farmacéuticas |
| **Tipo de Problema** | Clasificación Binaria Supervisada |
| **Variable Objetivo** | `flag_tiene_adenda` (0: Ejecución limpia / 1: Requiere adenda) |
| **Dataset** | Pharmaceutical Procurement and Healthcare Tenders Dataset |
| **Registros** | 12,500 contratos de adquisición |
| **Dimensionalidad** | 10 atributos predictores + 1 variable objetivo |
| **Algoritmos** | LightGBM · CatBoost · XGBoost (Gradient Boosted Decision Trees) |
| **Optimización** | Grid Search + Stratified 5-Fold Cross Validation |
| **Métrica Principal** | F1-Score (clase positiva) |
| **Lenguaje** | Python 3.10+ |

---

## Arquitectura Lógica del Flujo de Datos

```
staging_flat.csv (12,500 registros × 11 columnas)
        │
        ▼
┌─────────────────────────────────────────────────┐
│  NOTEBOOK 01: Preprocesamiento                  │
│  ┌─────────────────────────────────────────┐    │
│  │ 1. EDA + Diagnóstico de calidad         │    │
│  │ 2. Validación variable objetivo         │    │
│  │ 3. Eliminación duplicados               │    │
│  │ 4. Corrección inconsistencias lógicas   │    │
│  │ 5. Imputación (mediana / moda)          │    │
│  │ 6. Transformación log1p(monto)          │    │
│  │ 7. IQR Winsorización outliers           │    │
│  │ 8. RobustScaler (numéricas)             │    │
│  │ 9. Target Encoding (rama XGBoost)       │    │
│  └─────────────────────────────────────────┘    │
└─────────────────────────────────────────────────┘
        │
        ├──► processed_features.csv  (LightGBM / CatBoost)
        └──► processed_xgboost.csv   (XGBoost — todo numérico)
        │
        ▼
┌─────────────────────────────────────────────────┐
│  NOTEBOOK 02: Entrenamiento y Evaluación        │
│  ┌─────────────────────────────────────────┐    │
│  │ 1. Split Estratificado 80/20            │    │
│  │ 2. Grid Search + Stratified KFold CV=5  │    │
│  │    ├─ LightGBM (categorías nativas)     │    │
│  │    ├─ CatBoost (Ordered Target Stats)   │    │
│  │    └─ XGBoost  (Target Encoded)         │    │
│  │ 3. Evaluación en Test Set               │    │
│  │    (Accuracy, Precision, Recall, F1,    │    │
│  │     ROC-AUC, Brier Score, Log-Loss)     │    │
│  │ 4. Visualizaciones (5 gráficas)         │    │
│  │ 5. Serialización de modelos (.joblib)   │    │
│  └─────────────────────────────────────────┘    │
└─────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────┐
│  APP.PY: Streamlit — Inferencia en Tiempo Real  │
│  ┌─────────────────────────────────────────┐    │
│  │ Sidebar: Formulario 9 atributos         │    │
│  │ Tab 1: Predicción + Gauge de riesgo     │    │
│  │ Tab 2: Métricas comparativas            │    │
│  │ Tab 3: Curvas ROC / PR / Calibración    │    │
│  │ Tab 4: Feature Importance               │    │
│  └─────────────────────────────────────────┘    │
└─────────────────────────────────────────────────┘
```

---

## Estructura del Directorio del Proyecto

```
GBDT_Risk_Management/
├── data/
│   ├── staging_flat.csv            ← Dataset original (12,500 registros)
│   ├── processed_features.csv      ← Dataset procesado (LightGBM/CatBoost)
│   ├── processed_xgboost.csv       ← Dataset codificado (XGBoost)
│   ├── viz_feature_importance.png  ← Gráfica importancia de características
│   ├── viz_roc_pr_curves.png       ← Curvas ROC y Precision-Recall
│   ├── viz_confusion_matrix.png    ← Matriz de confusión normalizada
│   ├── viz_calibration_curves.png  ← Curvas de calibración
│   └── viz_metrics_comparison.png  ← Comparativa de métricas
│
├── Documentacion/
│   ├── 01_preprocessing_procurement.ipynb
│   └── 02_training_evaluation_gbdt.ipynb
│
├── Modelos/
│   ├── best_lightgbm.joblib        ← Mejor LightGBM serializado
│   ├── best_catboost.joblib        ← Mejor CatBoost serializado
│   ├── best_catboost.cbm           ← CatBoost formato nativo
│   ├── best_xgboost.joblib         ← Mejor XGBoost serializado
│   ├── best_xgboost.json           ← XGBoost formato nativo
│   ├── robust_scaler.joblib        ← RobustScaler ajustado
│   ├── num_imputer.joblib          ← Imputador numérico (mediana)
│   ├── cat_imputer.joblib          ← Imputador categórico (moda)
│   ├── bin_imputer.joblib          ← Imputador binario (moda)
│   ├── target_encoder_global.joblib← Mapa de Target Encoding
│   ├── model_config.joblib         ← Configuración y mejores parámetros
│   └── metrics_df.joblib           ← DataFrame de métricas del test set
│
├── app.py                          ← Aplicación Streamlit
├── requirements.txt                ← Dependencias del proyecto
└── README.md                       ← Este archivo
```

---

## Comparativa Teórica de los Modelos GBDT Implementados

### Gradient Boosted Decision Trees — Marco General

Los tres algoritmos comparten el principio fundamental del **Boosting Aditivo**:

$$F_M(x) = F_0(x) + \sum_{m=1}^{M} \eta \cdot h_m(x)$$

donde $\eta$ es la tasa de aprendizaje (*learning rate*) y $h_m$ es el árbol $m$-ésimo entrenado para corregir los residuos del ensemble anterior. La diferencia reside en cómo cada framework implementa el crecimiento del árbol, el manejo de categorías y la regularización.

### LightGBM — Histogramas y Crecimiento Leaf-Wise

LightGBM introduce dos innovaciones algorítmicas sobre XGBoost:

1. **Gradient-based One-Side Sampling (GOSS):** Retiene todas las instancias con gradientes grandes (error alto) y muestrea las de gradiente pequeño, manteniendo la distribución de datos con una fracción del costo computacional.

2. **Exclusive Feature Bundling (EFB):** Agrupa features mutuamente exclusivas (raras de ser no-cero simultáneamente) en un único feature, reduciendo la dimensionalidad sin pérdida de información.

3. **Crecimiento Leaf-Wise:** En lugar de crecer nivel por nivel (*level-wise*), LightGBM expande la hoja con mayor ganancia de pérdida en cada iteración, produciendo árboles más profundos y expresivos. El parámetro `num_leaves` controla la complejidad máxima.

**Manejo de categorías:** LightGBM convierte internamente las categorías a enteros y encuentra el split óptimo evaluando todas las particiones posibles del conjunto de categorías en $O(k \log k)$.

### CatBoost — Ordered Target Statistics

CatBoost resuelve el problema de *target leakage* en la codificación de categorías mediante **Ordered Target Statistics (OTS)**:

$$\hat{x}_i^j = \frac{\sum_{k \in D_q, x_k^j = x_i^j} y_k + a \cdot p}{\sum_{k \in D_q, x_k^j = x_i^j} 1 + a}$$

donde $D_q$ es un subconjunto ordenado temporalmente que garantiza que la estadística del registro $i$ no incluya su propio target. Esto elimina el data leakage inherente a los enfoques de Target Encoding estándar.

CatBoost también implementa **Oblivious Trees** (árboles simétricos), donde todos los nodos del mismo nivel usan el mismo split. Esto reduce el overfitting y acelera la predicción en producción.

### XGBoost — Regularización Explícita y Level-Wise

XGBoost optimiza una función objetivo regularizada de segunda derivada de Taylor:

$$\mathcal{L}^{(m)} = \sum_{i=1}^{n}\left[g_i f_m(x_i) + \frac{1}{2}h_i f_m^2(x_i)\right] + \Omega(f_m)$$

$$\Omega(f_m) = \gamma T + \frac{1}{2}\lambda\sum_{j=1}^{T}w_j^2$$

donde $g_i$ y $h_i$ son la primera y segunda derivada de la pérdida, $T$ es el número de hojas, $w_j$ son los pesos de las hojas, y $\gamma, \lambda$ son los parámetros de regularización. El crecimiento **level-wise** garantiza que todos los nodos al mismo nivel sean procesados antes de proceder al siguiente, produciendo árboles más balanceados que LightGBM.

---

## Instalación del Entorno Virtual

### Requisitos del Sistema

- Python 3.10 o superior
- pip 24.0 o superior
- 4 GB RAM mínimo (8 GB recomendado para Grid Search completo)
- Sistema operativo: Windows 10/11, macOS 12+, Ubuntu 20.04+

### Paso 1 — Clonar o descomprimir el proyecto

```bash
# Si tiene el proyecto en un ZIP:
unzip GBDT_Risk_Management.zip -d GBDT_Risk_Management
cd GBDT_Risk_Management

# O si usa git:
git clone <url_repositorio>
cd GBDT_Risk_Management
```

### Paso 2 — Crear el entorno virtual

```bash
# Windows (PowerShell)
python -m venv .venv
.\.venv\Scripts\Activate.ps1

# macOS / Linux
python3 -m venv .venv
source .venv/bin/activate
```

### Paso 3 — Instalar dependencias

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

> **Nota para macOS con Apple Silicon (M1/M2/M3):** LightGBM puede requerir compilación desde fuente. Si la instalación falla, pruebe:
> ```bash
> pip install lightgbm --no-binary lightgbm
> ```

### Paso 4 — Registrar el kernel en JupyterLab

```bash
python -m ipykernel install --user --name=gbdt_env --display-name "Python (GBDT Risk)"
```

---

## Ejecución Secuencial del Pipeline

### Paso 1 — Notebook de Preprocesamiento

```bash
# Activar entorno (si no está activo)
source .venv/bin/activate  # macOS/Linux
# o
.\.venv\Scripts\Activate.ps1  # Windows

# Lanzar JupyterLab
jupyter lab
```

En JupyterLab, abra `Documentacion/01_preprocessing_procurement.ipynb` y ejecute todas las celdas en orden (`Kernel → Restart Kernel and Run All Cells`).

**Salidas esperadas:**
- `data/processed_features.csv`
- `data/processed_xgboost.csv`
- `Modelos/robust_scaler.joblib`
- `Modelos/num_imputer.joblib`
- `Modelos/cat_imputer.joblib`
- `Modelos/bin_imputer.joblib`
- `Modelos/target_encoder_global.joblib`

### Paso 2 — Notebook de Entrenamiento y Evaluación

> ⏱️ **Tiempo estimado:** 15–45 minutos dependiendo del hardware (Grid Search exhaustivo).

Abra `Documentacion/02_training_evaluation_gbdt.ipynb` y ejecute todas las celdas.

**Salidas esperadas:**
- `Modelos/best_lightgbm.joblib`
- `Modelos/best_catboost.joblib` + `.cbm`
- `Modelos/best_xgboost.joblib` + `.json`
- `Modelos/model_config.joblib`
- `Modelos/metrics_df.joblib`
- `data/viz_*.png` (5 gráficas)

### Paso 3 — Aplicación Streamlit

```bash
streamlit run app.py
```

La aplicación se desplegará automáticamente en `http://localhost:8501`.

---

## Despliegue Local de la Aplicación Streamlit

```bash
# Desde el directorio raíz del proyecto (donde está app.py)
streamlit run app.py --server.port 8501 --server.address localhost
```

### Configuración opcional (`.streamlit/config.toml`)

```toml
[server]
port = 8501
headless = true
enableCORS = false

[theme]
primaryColor = "#1565C0"
backgroundColor = "#F8F9FA"
secondaryBackgroundColor = "#FFFFFF"
textColor = "#212121"
font = "sans serif"
```

---

## Descripción de la Arquitectura del Dataset

### Variables Predictoras

| Variable | Tipo | Cardinalidad | Descripción |
|----------|------|-------------|-------------|
| `red_asistencial` | Categórica | 9 | Red de EsSalud responsable del proceso |
| `dpto_entrega_item` | Categórica | 10 | Departamento de entrega del medicamento |
| `tipo_proveedor` | Categórica | 3 | Naturaleza jurídica del proveedor |
| `denominacion_dci` | Categórica | 15 | Principio activo (INN/OMS) |
| `tipo_proceso_seleccion` | Categórica | 4 | Modalidad de contratación |
| `es_consorcio` | Binaria | 2 | Flag de consorcio empresarial |
| `es_uso_critico` | Binaria | 2 | Medicamento en lista de uso crítico |
| `es_uso_intrahospitalario` | Binaria | 2 | Requiere administración hospitalaria |
| `monto_adjudicado_soles` | Numérica | Continua | Valor del contrato en S/. |
| `anio` | Numérica temporal | 4 | Año de adjudicación (2022–2025) |

### Variable Objetivo

| Variable | Tipo | Distribución | Descripción |
|----------|------|-------------|-------------|
| `flag_tiene_adenda` | Binaria | ~77.2% clase 0 / ~22.8% clase 1 | 1 si el contrato requirió adenda o ampliación presupuestaria |

---

## Notas Técnicas Adicionales

### Manejo del Desbalance de Clases

- **LightGBM:** `class_weight='balanced'` — repondera automáticamente las clases inversamente proporcional a su frecuencia.
- **CatBoost:** `auto_class_weights='Balanced'` — implementa una versión similar internamente.
- **XGBoost:** `scale_pos_weight = n_negative / n_positive` (≈3.38) — pondera la pérdida de la clase positiva para compensar el desbalance.

### Anti-Leakage en Target Encoding

El Target Encoding para XGBoost se ajusta **dentro de cada fold de entrenamiento** durante el Cross-Validation, nunca sobre el conjunto de prueba. El mapa global exportado en `target_encoder_global.joblib` se usa únicamente para inferencia en producción.

---

*Generado automáticamente por el pipeline de documentación GBDT Risk Management System.*  
*© 2024 UNMSM — Ingeniería de Software — Aprendizaje Supervisado Aplicado*
