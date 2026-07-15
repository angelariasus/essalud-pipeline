# Sistema Predictivo de Riesgo de Sobrecosto en Contratos de Medicamentos
### Aprendizaje Supervisado aplicado a la Gestión de Riesgos en Adquisiciones Públicas del Sector Salud (EsSalud)

**Institución:** Universidad Nacional Mayor de San Marcos (UNMSM) — Facultad de Ingeniería de Sistemas e Informática
**Programa:** Ingeniería de Software
**Tipo de proyecto:** Investigación académica avanzada en Machine Learning aplicado

---

## 1. Ficha Técnica del Proyecto

| Campo | Detalle |
|---|---|
| **Dataset** | *Pharmaceutical Procurement and Healthcare Tenders Dataset* |
| **Archivo fuente** | `data/staging_flat.csv` |
| **Origen de los datos** | Registros administrativos de procesos de adquisición de medicamentos gestionados por redes prestacionales y asistenciales de EsSalud (Seguro Social de Salud del Perú), consolidando los hitos temporales de convocatoria, otorgamiento de buena pro y suscripción contractual de cada proceso de selección |
| **N.º de registros** | 12,500 contratos de adquisición |
| **Variable objetivo** | `flag_tiene_adenda` (0 = Sin sobrecosto / Sin adenda, 1 = Con sobrecosto / Con adenda) — Clasificación Binaria |
| **Balance de clases** | ≈ 77% clase negativa / 23% clase positiva (desbalanceado) |
| **Variables de fecha crudas** | `fecha_convocatoria`, `fecha_buena_pro`, `fecha_suscripcion` |
| **Variables de Lead Time derivadas** | `lead_time_adjudicacion`, `lead_time_formalizacion`, `lead_time_total_proceso` |
| **Variables predictoras del sistema** | `red_asistencial`, `dpto_entrega_item`, `tipo_proveedor`, `denominacion_dci`, `tipo_proceso_seleccion`, `es_consorcio`, `es_uso_critico`, `es_uso_intrahospitalario`, `monto_adjudicado_soles`, `anio` |
| **Algoritmos evaluados** | XGBoost Classifier (Gradient Boosting regularizado) · Random Forest Classifier (Bagging paralelo) |
| **Métrica de optimización** | F1-Score (maximizada vía Grid Search + Stratified K-Fold CV, K=5) |

---

## 2. Arquitectura Lógica del Flujo de Datos

```
staging_flat.csv (crudo)
        │
        ▼
┌───────────────────────────────────────────────────┐
│  01_preprocessing_leadtime.ipynb                    │
│  ─────────────────────────────────────────────────  │
│  • Conversión segura de fechas (pd.to_datetime,      │
│    errors='coerce')                                  │
│  • Cálculo de Lead Times (adjudicación,               │
│    formalización, total)                              │
│  • Corrección de inconsistencias lógicas               │
│    (imputación por mediana)                            │
│  • Tratamiento de outliers (capping IQR)                │
│  • Target Encoding (denominacion_dci) +                  │
│    One-Hot Encoding (baja cardinalidad)                   │
│  • Escalamiento RobustScaler                                │
└───────────────────────────┬───────────────────────────────┘
                             ▼
              data/medicine_overrun_dataset.csv
                             │
                             ▼
┌───────────────────────────────────────────────────────────┐
│  02_training_evaluation_gbdt_lt.ipynb                        │
│  ───────────────────────────────────────────────────────────│
│  • Partición estratificada 80/20                              │
│  • Grid Search (ParameterGrid) + Stratified K-Fold CV (K=5)    │
│  • Entrenamiento XGBoost y Random Forest                        │
│  • Evaluación en test: Accuracy, Precision, Recall, F1,          │
│    ROC-AUC, Matriz de Confusión                                   │
│  • Visualizaciones: Feature Importance, KDE Lead Time,             │
│    ROC/PR comparativo, Matriz de Confusión normalizada,             │
│    Barras comparativas de métricas                                   │
└───────────────────────────┬───────────────────────────────────────┘
                             ▼
              Modelos/*.pkl  (modelos, encoders, columnas)
                             │
                             ▼
                        app.py (Streamlit)
              Inferencia interactiva en tiempo real
```

---

## 3. Comparativa Teórica: XGBoost (Boosting) vs. Random Forest (Bagging) ante Variables de Lead Time

| Dimensión | Random Forest (Bagging) | XGBoost (Gradient Boosting) |
|---|---|---|
| **Construcción de árboles** | Paralela e independiente, sobre muestras bootstrap | Secuencial, cada árbol corrige los residuos del gradiente del ensamble previo |
| **Objetivo estadístico** | Reducción de **varianza** por promediación de estimadores débilmente correlacionados | Reducción de **sesgo** mediante optimización aditiva por descenso de gradiente |
| **Sensibilidad a relaciones no lineales en Lead Time** | Alta — captura umbrales discontinuos de riesgo sin necesidad de suavidad en la función | Alta, pero además modela **interacciones acumulativas** entre Lead Time y otras variables (p. ej. `es_uso_critico`) a través de residuos sucesivos |
| **Regularización** | Implícita, vía profundidad máxima y submuestreo de variables (`max_features`) | Explícita, vía `reg_alpha` (L1) y `reg_lambda` (L2) sobre los pesos de las hojas |
| **Riesgo de sobreajuste con muestra moderada (12.5k registros)** | Moderado, mitigado por el promedio de múltiples árboles | Mayor si no se regulariza adecuadamente; controlado en este proyecto vía Grid Search sobre los términos de penalización |
| **Interpretabilidad de Feature Importance** | Directa (Mean Decrease in Impurity) | Directa (Gain-based), generalmente más concentrada en pocas variables dominantes |

La hipótesis de ingeniería validada empíricamente en `02_training_evaluation_gbdt_lt.ipynb` es que el **Lead Time de Formalización** concentra el mayor peso predictivo relativo en ambos ensambles, dado que refleja fricciones contractuales de negociación —terreno directo para adendas por ajuste de precios, plazos o condiciones de entrega—, mientras que el Lead Time de Adjudicación captura primordialmente fricciones administrativas/burocráticas de menor correlación directa con el sobrecosto final.

---

## 4. Estructura del Repositorio

```
GBDT_Risk_Management/
├── data/
│   ├── staging_flat.csv                    # Dataset original (crudo)
│   └── medicine_overrun_dataset.csv        # Dataset enriquecido (salida del Notebook 01)
├── Documentacion/
│   ├── 01_preprocessing_leadtime.ipynb
│   └── 02_training_evaluation_gbdt_lt.ipynb
├── Modelos/
│   ├── best_xgboost_model.pkl
│   ├── best_random_forest_model.pkl
│   ├── feature_columns.pkl
│   ├── best_model_name.pkl
│   └── test_metrics_summary.csv
├── assets/                                  # Gráficas exportadas por el Notebook 02
├── app.py                                    # Aplicación Streamlit de inferencia
├── requirements.txt
└── README.md
```

---

## 5. Instalación del Entorno Virtual

```bash
# 1. Clonar o descomprimir el repositorio del proyecto
cd GBDT_Risk_Management

# 2. Crear el entorno virtual
python3 -m venv venv

# 3. Activar el entorno virtual
source venv/bin/activate          # Linux / macOS
venv\Scripts\activate             # Windows

# 4. Instalar las dependencias
pip install -r requirements.txt
```

---

## 6. Ejecución Secuencial de los Notebooks

> **Importante:** los notebooks deben ejecutarse en orden estricto, dado que el Notebook 02 consume el artefacto (`medicine_overrun_dataset.csv`) exportado por el Notebook 01.

```bash
cd Documentacion

# Paso 1 — Preprocesamiento e ingeniería de Lead Time
jupyter nbconvert --to notebook --execute --inplace 01_preprocessing_leadtime.ipynb

# Paso 2 — Entrenamiento, Grid Search y evaluación
jupyter nbconvert --to notebook --execute --inplace 02_training_evaluation_gbdt_lt.ipynb
```

Alternativamente, ábralos de forma interactiva con `jupyter lab` o `jupyter notebook` y ejecute todas las celdas en orden (`Kernel → Restart & Run All`).

Al finalizar la ejecución del Notebook 02, la carpeta `Modelos/` contendrá los estimadores optimizados serializados, requeridos por la aplicación Streamlit.

---

## 7. Despliegue Local de la Aplicación Streamlit

Desde la raíz del proyecto (`GBDT_Risk_Management/`), con el entorno virtual activado y habiendo ejecutado previamente ambos notebooks:

```bash
streamlit run app.py
```

La aplicación se expondrá por defecto en `http://localhost:8501`, habilitando:

- **Panel de Simulación:** formulario lateral con sliders para los 3 Lead Times y los 8 predictores del sistema, con cálculo instantáneo de la probabilidad de riesgo de sobrecosto.
- **Panel de Curvas de Evaluación:** exploración interactiva de ROC, Precision-Recall y Matriz de Confusión normalizada.
- **Panel de Impacto de Lead Time:** distribución comparativa de los tiempos de ciclo segmentada por la variable objetivo.

---

## 8. Notas de Rigor Metodológico

- El ajuste del `TargetEncoder` sobre `denominacion_dci` para efectos de modelado se realiza exclusivamente sobre la partición de entrenamiento (80%) para prevenir fuga de información (*target leakage*); el ajuste global documentado en el Notebook 01 es únicamente de referencia exploratoria.
- El tratamiento de outliers en las variables de Lead Time emplea *capping* (winsorización) vía el método IQR, preservando el tamaño muestral y la señal de procesos administrativamente anómalos.
- La selección de hiperparámetros se valida mediante Stratified K-Fold Cross-Validation (K=5) para mitigar la varianza de estimación asociada al desbalance de clases, maximizando el F1-Score como criterio de decisión.
