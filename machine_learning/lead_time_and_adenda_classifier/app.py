"""
app.py
------
Aplicación web interactiva (Streamlit) para el sistema predictivo de Riesgo de
Sobrecosto en Contratos de Medicamentos (EsSalud), enriquecido con variables de
Lead Time (tiempos de ciclo administrativo).

Módulos funcionales:
    1. Panel lateral (sidebar) de simulación de un nuevo contrato de adquisición,
       con sliders independientes para los 3 Lead Times y los 8 predictores del sistema.
    2. Motor de inferencia en tiempo real usando el mejor estimador serializado
       (XGBoost o Random Forest, seleccionado automáticamente por F1-Score en el
       Notebook 02_training_evaluation_gbdt_lt.ipynb).
    3. Panel de exploración de las curvas de evaluación (ROC, Precision-Recall,
       Matriz de Confusión) generadas sobre el conjunto de prueba.
    4. Panel de exploración del impacto de los tiempos de ciclo sobre el riesgo.

Ejecución local:
    streamlit run app.py
"""

import os
import joblib
import numpy as np
import pandas as pd
import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
from sklearn.metrics import roc_curve, precision_recall_curve, roc_auc_score, confusion_matrix

# ---------------------------------------------------------------------------
# Configuración general de la página
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Riesgo de Sobrecosto — Contratos de Medicamentos EsSalud",
    page_icon="💊",
    layout="wide",
)

MODELOS_DIR = "Modelos"
DATA_DIR = "data"

# ---------------------------------------------------------------------------
# Carga de artefactos serializados (cacheada para evitar recarga en cada interacción)
# ---------------------------------------------------------------------------
@st.cache_resource
def load_artifacts():
    xgb_model = joblib.load(os.path.join(MODELOS_DIR, "best_xgboost_model.pkl"))
    rf_model = joblib.load(os.path.join(MODELOS_DIR, "best_random_forest_model.pkl"))
    feature_cols = joblib.load(os.path.join(MODELOS_DIR, "feature_columns.pkl"))
    best_model_name = joblib.load(os.path.join(MODELOS_DIR, "best_model_name.pkl"))
    metrics_summary = pd.read_csv(os.path.join(MODELOS_DIR, "test_metrics_summary.csv"), index_col=0)
    return xgb_model, rf_model, feature_cols, best_model_name, metrics_summary


@st.cache_data
def load_dataset():
    return pd.read_csv(os.path.join(DATA_DIR, "medicine_overrun_dataset.csv"))


try:
    xgb_model, rf_model, feature_cols, best_model_name, metrics_summary = load_artifacts()
    dataset = load_dataset()
    ARTIFACTS_OK = True
except FileNotFoundError as e:
    ARTIFACTS_OK = False
    MISSING_MSG = str(e)

MODELS = {"XGBoost": xgb_model if ARTIFACTS_OK else None,
          "Random Forest": rf_model if ARTIFACTS_OK else None}

# ---------------------------------------------------------------------------
# Categorías válidas (derivadas del one-hot encoding del Notebook 01)
# ---------------------------------------------------------------------------
RED_ASISTENCIAL_OPTS = [
    "red asistencial arequipa", "red asistencial la libertad", "red asistencial loreto",
    "red asistencial madre de dios", "red prestacional almenara", "red prestacional lambayeque",
    "red prestacional rebagliati", "red prestacional sabogal", "red prestacional cusco (base)",
]
DPTO_OPTS = [
    "apurimac", "arequipa", "cusco", "la libertad", "lambayeque",
    "lima", "loreto", "madre de dios", "piura", "otro (base)",
]
TIPO_PROVEEDOR_OPTS = ["legal entity", "natural person", "consortium (base)"]
TIPO_PROCESO_OPTS = [
    "contratacion directa", "licitacion publica",
    "subasta inversa electronica", "adjudicacion simplificada (base)",
]

st.title("💊 Sistema Predictivo de Riesgo de Sobrecosto en Contratos de Medicamentos")
st.caption(
    "Aprendizaje Supervisado aplicado a la gestión de riesgos en adquisiciones públicas del "
    "sector salud — Ingeniería de características temporales (Lead Time) · UNMSM"
)

if not ARTIFACTS_OK:
    st.error(
        "⚠️ No se encontraron los artefactos serializados en `Modelos/`. "
        "Ejecute previamente los notebooks `01_preprocessing_leadtime.ipynb` y "
        "`02_training_evaluation_gbdt_lt.ipynb` en la carpeta `Documentacion/`.\n\n"
        f"Detalle técnico: {MISSING_MSG}"
    )
    st.stop()

tab_infer, tab_eval, tab_impact = st.tabs(
    ["🔮 Simulación de Nuevo Contrato", "📊 Curvas de Evaluación", "⏱️ Impacto de Lead Time"]
)

# ===========================================================================
# SIDEBAR — Formulario dinámico del nuevo contrato de adquisición
# ===========================================================================
with st.sidebar:
    st.header("🧾 Nuevo Contrato de Adquisición")
    st.markdown("Configure los parámetros del proceso para estimar el riesgo de sobrecosto.")

    model_choice = st.selectbox(
        "Estimador de inferencia",
        options=["Automático (mejor F1)"] + list(MODELS.keys()),
        index=0,
    )

    st.subheader("⏱️ Tiempos de Ciclo (Lead Time)")
    lt_adjudicacion = st.slider("Lead Time de Adjudicación (días)", 0, 150, 30, 1)
    lt_formalizacion = st.slider("Lead Time de Formalización (días)", 0, 150, 22, 1)
    lt_total_default = lt_adjudicacion + lt_formalizacion
    lt_total = st.slider(
        "Lead Time Total del Proceso (días)", 0, 300, lt_total_default, 1,
        help="Por consistencia administrativa se sugiere = Adjudicación + Formalización, "
             "aunque puede ajustarse manualmente para simular escenarios."
    )

    st.subheader("🏥 Variables Predictoras del Sistema")
    red_asistencial = st.selectbox("Red Asistencial", RED_ASISTENCIAL_OPTS)
    dpto_entrega = st.selectbox("Departamento de Entrega", DPTO_OPTS)
    tipo_proveedor = st.selectbox("Tipo de Proveedor", TIPO_PROVEEDOR_OPTS)
    tipo_proceso = st.selectbox("Tipo de Proceso de Selección", TIPO_PROCESO_OPTS)
    denominacion_dci = st.selectbox(
        "Denominación DCI (principio activo)",
        sorted([c.replace("_target_enc", "").replace("denominacion_dci_", "")
                for c in ["referencial"]]) if False else
        ["cefalexina 500 mg", "hipromelosa 0.3%", "meropenem 500 mg", "ceftriaxona 1 g",
         "paracetamol 500 mg", "metformina 850 mg", "insulina glargina", "omeprazol 20 mg",
         "losartan 50 mg", "amoxicilina 500 mg", "otro principio activo"],
    )
    es_consorcio = st.toggle("¿Es Consorcio?", value=False)
    es_uso_critico = st.toggle("¿Es de Uso Crítico?", value=False)
    es_uso_intrahosp = st.toggle("¿Es de Uso Intrahospitalario?", value=True)
    monto_adjudicado = st.number_input(
        "Monto Adjudicado (S/.)", min_value=0.0, max_value=5_000_000.0,
        value=150_000.0, step=1000.0
    )
    anio = st.selectbox("Año del Proceso", [2022, 2023, 2024, 2025], index=1)

    infer_btn = st.button("🔍 Calcular Riesgo de Sobrecosto", type="primary", use_container_width=True)


# ---------------------------------------------------------------------------
# Construcción del vector de features alineado a feature_cols del entrenamiento
# ---------------------------------------------------------------------------
def build_feature_vector():
    row = {c: 0 for c in feature_cols}

    # Lead times (escalados de forma aproximada con estadísticos robustos del dataset
    # de referencia, replicando la transformación RobustScaler del Notebook 01)
    for raw_col, value in [
        ("lead_time_adjudicacion", lt_adjudicacion),
        ("lead_time_formalizacion", lt_formalizacion),
        ("lead_time_total_proceso", lt_total),
        ("monto_adjudicado_soles", monto_adjudicado),
    ]:
        if raw_col in dataset.columns:
            median = dataset[f"{raw_col}_raw"].median() if f"{raw_col}_raw" in dataset.columns else dataset[raw_col].median()
            q1 = dataset[f"{raw_col}_raw"].quantile(0.25) if f"{raw_col}_raw" in dataset.columns else dataset[raw_col].quantile(0.25)
            q3 = dataset[f"{raw_col}_raw"].quantile(0.75) if f"{raw_col}_raw" in dataset.columns else dataset[raw_col].quantile(0.75)
            iqr = max(q3 - q1, 1e-6)
            row[raw_col] = (value - median) / iqr

    row["es_consorcio"] = int(es_consorcio)
    row["es_uso_critico"] = int(es_uso_critico)
    row["es_uso_intrahospitalario"] = int(es_uso_intrahosp)
    row["anio"] = anio

    ohe_map = {
        f"red_asistencial_{red_asistencial}": True,
        f"dpto_entrega_item_{dpto_entrega}": True,
        f"tipo_proveedor_{tipo_proveedor}": True,
        f"tipo_proceso_seleccion_{tipo_proceso}": True,
    }
    for col_name in ohe_map:
        if col_name in row:
            row[col_name] = 1

    # Target Encoding aproximado del principio activo: se usa la tasa histórica
    # promedio observada en el dataset de referencia como proxy en tiempo real.
    if "denominacion_dci_target_enc" in row:
        row["denominacion_dci_target_enc"] = dataset["denominacion_dci_target_enc"].mean()

    vector = pd.DataFrame([row])[feature_cols]
    return vector


# ===========================================================================
# TAB 1 — Inferencia en tiempo real
# ===========================================================================
with tab_infer:
    st.subheader("Resultado de la Inferencia")

    if infer_btn:
        X_new = build_feature_vector()

        if model_choice == "Automático (mejor F1)":
            active_model = MODELS[best_model_name]
            active_name = best_model_name
        else:
            active_model = MODELS[model_choice]
            active_name = model_choice

        proba = active_model.predict_proba(X_new)[0, 1]
        pred = int(proba >= 0.5)

        col1, col2, col3 = st.columns(3)
        col1.metric("Modelo utilizado", active_name)
        col2.metric("Probabilidad de Sobrecosto", f"{proba*100:.2f}%")
        col3.metric("Clasificación", "🔴 Con Sobrecosto" if pred == 1 else "🟢 Sin Sobrecosto")

        fig_gauge = go.Figure(go.Indicator(
            mode="gauge+number",
            value=proba * 100,
            title={"text": "Probabilidad de Riesgo de Sobrecosto (%)"},
            gauge={
                "axis": {"range": [0, 100]},
                "bar": {"color": "#d62728" if pred == 1 else "#2ca02c"},
                "steps": [
                    {"range": [0, 40], "color": "#d4f4dd"},
                    {"range": [40, 70], "color": "#fff3cd"},
                    {"range": [70, 100], "color": "#f8d7da"},
                ],
            },
        ))
        fig_gauge.update_layout(height=350)
        st.plotly_chart(fig_gauge, use_container_width=True)

        st.info(
            f"El proceso simulado presenta un **Lead Time Total de {lt_total} días** "
            f"({lt_adjudicacion} días de adjudicación + {lt_formalizacion} días de formalización). "
            "Recuerde que ciclos de formalización prolongados están asociados a mayor probabilidad "
            "histórica de adendas contractuales."
        )
    else:
        st.info("⬅️ Configure los parámetros del contrato en el panel lateral y presione "
                "**'Calcular Riesgo de Sobrecosto'** para ejecutar la inferencia.")

    st.divider()
    st.subheader("Métricas de Desempeño en Conjunto de Prueba")
    st.dataframe(metrics_summary.style.format("{:.4f}"), use_container_width=True)

# ===========================================================================
# TAB 2 — Curvas de evaluación interactivas
# ===========================================================================
with tab_eval:
    st.subheader("Curvas de Evaluación del Modelo (Conjunto de Prueba, 20%)")

    feature_cols_no_target = [c for c in dataset.columns if not c.endswith("_raw") and c != "flag_tiene_adenda"]
    from sklearn.model_selection import train_test_split
    X_all = dataset[feature_cols]
    y_all = dataset["flag_tiene_adenda"]
    _, X_test_viz, _, y_test_viz = train_test_split(
        X_all, y_all, test_size=0.20, stratify=y_all, random_state=42
    )

    eval_model_choice = st.radio("Seleccionar modelo a graficar", list(MODELS.keys()), horizontal=True)
    proba_viz = MODELS[eval_model_choice].predict_proba(X_test_viz)[:, 1]
    preds_viz = MODELS[eval_model_choice].predict(X_test_viz)

    col_a, col_b = st.columns(2)

    with col_a:
        fpr, tpr, _ = roc_curve(y_test_viz, proba_viz)
        auc_val = roc_auc_score(y_test_viz, proba_viz)
        fig_roc = px.area(x=fpr, y=tpr, title=f"Curva ROC — {eval_model_choice} (AUC = {auc_val:.3f})",
                           labels={"x": "Tasa de Falsos Positivos", "y": "Tasa de Verdaderos Positivos"})
        fig_roc.add_shape(type="line", line=dict(dash="dash"), x0=0, x1=1, y0=0, y1=1)
        st.plotly_chart(fig_roc, use_container_width=True)

    with col_b:
        prec, rec, _ = precision_recall_curve(y_test_viz, proba_viz)
        fig_pr = px.area(x=rec, y=prec, title=f"Curva Precision-Recall — {eval_model_choice}",
                          labels={"x": "Recall", "y": "Precision"})
        st.plotly_chart(fig_pr, use_container_width=True)

    cm = confusion_matrix(y_test_viz, preds_viz, normalize="true")
    fig_cm = px.imshow(
        cm, text_auto=".2%", color_continuous_scale="Blues",
        x=["Sin Sobrecosto", "Con Sobrecosto"], y=["Sin Sobrecosto", "Con Sobrecosto"],
        labels=dict(x="Predicción", y="Real", color="Proporción"),
        title=f"Matriz de Confusión Normalizada — {eval_model_choice}",
    )
    st.plotly_chart(fig_cm, use_container_width=True)

# ===========================================================================
# TAB 3 — Exploración del impacto del Lead Time
# ===========================================================================
with tab_impact:
    st.subheader("Impacto de los Tiempos de Ciclo sobre el Riesgo de Sobrecosto")

    raw_cols_available = [c for c in dataset.columns if c.endswith("_raw") and "lead_time" in c]
    lt_variable = st.selectbox(
        "Variable de Lead Time a explorar",
        raw_cols_available,
        format_func=lambda c: c.replace("_raw", "").replace("_", " ").title(),
    )

    fig_kde = px.histogram(
        dataset, x=lt_variable, color="flag_tiene_adenda", marginal="box",
        barmode="overlay", opacity=0.6,
        color_discrete_map={0: "#1f77b4", 1: "#d62728"},
        labels={"flag_tiene_adenda": "Con Adenda", lt_variable: "Días"},
        title=f"Distribución de {lt_variable.replace('_raw','').replace('_',' ').title()} por Estado de Adenda",
    )
    st.plotly_chart(fig_kde, use_container_width=True)

    st.markdown(
        "El histograma superpuesto (con marginal tipo *box-plot*) permite contrastar la mediana y "
        "dispersión del tiempo de ciclo seleccionado entre contratos con y sin adenda contractual, "
        "validando la hipótesis de ingeniería de características: **a mayor Lead Time, mayor "
        "probabilidad histórica de sobrecosto.**"
    )

    corr = dataset[raw_cols_available + ["flag_tiene_adenda"]].corr()["flag_tiene_adenda"].drop("flag_tiene_adenda")
    st.write("**Correlación punto-biserial (Pearson) entre Lead Times crudos y el target:**")
    st.dataframe(corr.to_frame("Correlación con flag_tiene_adenda").style.format("{:.4f}"))

st.divider()
st.caption(
    "Proyecto académico de investigación — Universidad Nacional Mayor de San Marcos (UNMSM) · "
    "Ingeniería de Software · Aprendizaje Supervisado en Gestión de Riesgos de Adquisiciones Públicas."
)
