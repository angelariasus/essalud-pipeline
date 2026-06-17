"""
app.py — Aplicación Streamlit: Predictor de Riesgo de Sobrecosto en Adquisiciones Farmacéuticas
GBDT Risk Management System — UNMSM
"""

# ── Librerías ─────────────────────────────────────────────────────────────────
import os
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
import joblib
import streamlit as st
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns

warnings.filterwarnings("ignore")

# ── Configuración de la app ───────────────────────────────────────────────────
st.set_page_config(
    page_title="Modelo Predictivo de Sobrecostos en Contrataciones Públicas de Essalud",
    page_icon="💊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Rutas ─────────────────────────────────────────────────────────────────────
BASE_DIR   = Path(__file__).parent.resolve()
DATA_DIR   = BASE_DIR / "data"
MODELS_DIR = BASE_DIR / "Modelos"

# ── Paleta corporativa ────────────────────────────────────────────────────────
COLOR_PRIMARY   = "#1565C0"
COLOR_SECONDARY = "#FF5722"
COLOR_SUCCESS   = "#2E7D32"
COLOR_WARNING   = "#F57F17"
COLOR_BG        = "#F8F9FA"

sns.set_theme(style="whitegrid", palette="muted", font_scale=1.0)


# ═══════════════════════════════════════════════════════════════════════════════
# CARGA DE ARTEFACTOS (cacheada)
# ═══════════════════════════════════════════════════════════════════════════════

@st.cache_resource(show_spinner="Cargando modelos y transformadores...")
def load_artifacts():
    """Carga todos los artefactos serializados del pipeline."""
    artifacts = {}

    # Modelos GBDT
    for name, fname in [
        ("LightGBM", "best_lightgbm.joblib"),
        ("CatBoost", "best_catboost.joblib"),
        ("XGBoost",  "best_xgboost.joblib"),
    ]:
        path = MODELS_DIR / fname
        if path.exists():
            artifacts[name] = joblib.load(path)
        else:
            artifacts[name] = None

    # Transformadores
    for key, fname in [
        ("scaler",         "robust_scaler.joblib"),
        ("num_imputer",    "num_imputer.joblib"),
        ("cat_imputer",    "cat_imputer.joblib"),
        ("bin_imputer",    "bin_imputer.joblib"),
        ("te_map",         "target_encoder_global.joblib"),
        ("model_config",   "model_config.joblib"),
        ("metrics_df",     "metrics_df.joblib"),
    ]:
        path = MODELS_DIR / fname
        if path.exists():
            artifacts[key] = joblib.load(path)
        else:
            artifacts[key] = None

    return artifacts


@st.cache_data(show_spinner="Cargando dataset...")
def load_processed_data():
    path = DATA_DIR / "processed_features.csv"
    if path.exists():
        return pd.read_csv(path)
    return None


# ═══════════════════════════════════════════════════════════════════════════════
# FUNCIÓN DE INFERENCIA
# ═══════════════════════════════════════════════════════════════════════════════

CAT_COLS = [
    "red_asistencial", "dpto_entrega_item", "tipo_proveedor",
    "denominacion_dci", "tipo_proceso_seleccion"
]
NUM_COLS = ["log_monto_adjudicado", "anio"]
BIN_COLS = ["es_consorcio", "es_uso_critico", "es_uso_intrahospitalario"]
ALL_FEAT = NUM_COLS + BIN_COLS + CAT_COLS


def preprocess_input(
    raw_input: dict,
    scaler,
    te_map: dict,
    model_type: str
) -> pd.DataFrame:
    """
    Aplica el pipeline completo de preprocesamiento a una observación nueva.
    Retorna un DataFrame listo para predict_proba.
    """
    row = pd.DataFrame([raw_input])

    # 1. Transformación log1p del monto
    row["log_monto_adjudicado"] = np.log1p(row["monto_adjudicado_soles"])
    row.drop(columns=["monto_adjudicado_soles"], inplace=True)

    # 2. Escalado de numéricas
    row[NUM_COLS] = scaler.transform(row[NUM_COLS])

    # 3. Codificación según modelo
    if model_type == "XGBoost":
        global_mean = te_map.get("global_mean", 0.5)
        for col in CAT_COLS:
            val = row[col].iloc[0]
            col_map = te_map.get(col, {})
            row[col] = col_map.get(val, global_mean)
        # Asegurar tipos numéricos
        for col in BIN_COLS:
            row[col] = row[col].astype(int)
        row = row[ALL_FEAT].astype(float)
    else:
        # LightGBM / CatBoost: categorías como string
        for col in CAT_COLS:
            row[col] = row[col].astype(str)
        if model_type == "LightGBM":
            for col in CAT_COLS:
                row[col] = row[col].astype("category")
        row = row[ALL_FEAT]

    return row


def predict_risk(
    raw_input: dict,
    selected_model_name: str,
    artifacts: dict
) -> dict:
    """Ejecuta inferencia y retorna probabilidad y clasificación."""
    model  = artifacts.get(selected_model_name)
    scaler = artifacts.get("scaler")
    te_map = artifacts.get("te_map")

    if model is None or scaler is None:
        return {"error": "Modelo o transformador no disponible."}

    X_input = preprocess_input(raw_input, scaler, te_map, selected_model_name)
    prob    = float(model.predict_proba(X_input)[0, 1])
    label   = int(prob >= 0.5)

    return {
        "probability" : prob,
        "label"       : label,
        "risk_pct"    : prob * 100,
        "model_used"  : selected_model_name
    }


# ═══════════════════════════════════════════════════════════════════════════════
# LAYOUT PRINCIPAL
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    artifacts  = load_artifacts()
    df_proc    = load_processed_data()
    config     = artifacts.get("model_config") or {}
    metrics_df = artifacts.get("metrics_df")

    # ── Header ────────────────────────────────────────────────────────────────
    st.markdown(
        """
        <div style='background: linear-gradient(135deg, #1565C0 0%, #0D47A1 100%);
                    padding: 1.5rem 2rem; border-radius: 12px; margin-bottom: 1.5rem;'>
            <h1 style='color:white; margin:0; font-size:1.8rem;'>
                💊 Modelo Predictivo de Sobrecostos en Contrataciones Públicas de Essalud
            </h1>
            <p style='color:#BBDEFB; margin:0.5rem 0 0 0; font-size:0.95rem;'>
                Sistema Predictivo de Riesgo de Sobrecosto en Contratos de Medicamentos &nbsp;|&nbsp;
                Universidad Nacional Mayor de San Marcos (UNMSM)
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # ── Tabs ──────────────────────────────────────────────────────────────────
    tab1, tab2, tab3, tab4 = st.tabs([
        "🔮 Predicción en Tiempo Real",
        "📊 Métricas del Proyecto",
        "📈 Curvas de Evaluación",
        "🔍 Importancia de Variables"
    ])

    # ═══════════════════════════════════════════════════════════════════════════
    # TAB 1: PREDICCIÓN EN TIEMPO REAL
    # ═══════════════════════════════════════════════════════════════════════════
    with tab1:
        st.markdown("### Formulario de Evaluación — Nuevo Contrato de Adquisición")
        st.info(
            "Complete los 9 atributos del contrato en el panel lateral izquierdo. "
            "El sistema calculará en tiempo real la probabilidad de riesgo de sobrecosto "
            "utilizando los modelos GBDT entrenados."
        )

        # ── Sidebar ───────────────────────────────────────────────────────────
        with st.sidebar:
            st.markdown("## ⚙️ Parámetros del Contrato")
            st.markdown("---")

            # Selección de modelo
            available_models = [
                name for name in ["LightGBM", "CatBoost", "XGBoost"]
                if artifacts.get(name) is not None
            ]
            if not available_models:
                available_models = ["LightGBM", "CatBoost", "XGBoost"]

            best_name = config.get("best_model_name", "LightGBM")
            default_idx = available_models.index(best_name) if best_name in available_models else 0

            selected_model = st.selectbox(
                "🤖 Modelo Predictor",
                options=available_models,
                index=default_idx,
                help="Seleccione el estimador GBDT a utilizar para la predicción."
            )

            st.markdown("---")
            st.markdown("**📋 Variables Categóricas**")

            red_asistencial = st.selectbox(
                "Red Asistencial",
                options=[
                    "red prestacional sabogal",
                    "red prestacional lambayeque",
                    "red prestacional almenara",
                    "red prestacional arequipa",
                    "red prestacional piura",
                    "red prestacional cusco",
                    "red prestacional la libertad",
                    "red prestacional junin",
                    "red prestacional ica"
                ],
                help="Red asistencial de EsSalud responsable del proceso de compra."
            )

            dpto_entrega = st.selectbox(
                "Departamento de Entrega",
                options=["lima", "cusco", "lambayeque", "arequipa",
                         "piura", "la libertad", "junin", "ica", "puno", "ancash"],
                help="Región geográfica de destino para la entrega del medicamento."
            )

            tipo_proveedor = st.selectbox(
                "Tipo de Proveedor",
                options=["legal entity", "natural person", "consortium"],
                help="Naturaleza jurídica del proveedor adjudicado."
            )

            denominacion_dci = st.selectbox(
                "Denominación Común Internacional (DCI)",
                options=[
                    "cefalexina 500 mg", "hipromelosa 0.3%", "meropenem 500 mg",
                    "amoxicilina 500 mg", "ciprofloxacino 500 mg", "metformina 850 mg",
                    "omeprazol 20 mg", "paracetamol 500 mg", "ibuprofeno 400 mg",
                    "atorvastatina 20 mg", "losartan 50 mg", "metoprolol 50 mg",
                    "insulina glargina 100 ui/ml", "salbutamol 100 mcg/dosis", "furosemida 40 mg"
                ],
                help="Principio activo del medicamento según denominación INN/OMS."
            )

            tipo_proceso = st.selectbox(
                "Tipo de Proceso de Selección",
                options=[
                    "adjudicacion simplificada",
                    "licitacion publica",
                    "comparacion de precios",
                    "contratacion directa"
                ],
                help="Modalidad de contratación según la Ley de Contrataciones del Estado."
            )

            st.markdown("---")
            st.markdown("**🔢 Variables Numéricas**")

            monto_adjudicado = st.slider(
                "Monto Adjudicado (S/.)",
                min_value=1_000,
                max_value=5_000_000,
                value=300_000,
                step=10_000,
                format="S/. %d",
                help="Valor total del contrato adjudicado en Soles peruanos."
            )

            anio = st.slider(
                "Año del Contrato",
                min_value=2022,
                max_value=2025,
                value=2023,
                step=1,
                help="Año de publicación y adjudicación del proceso."
            )

            st.markdown("---")
            st.markdown("**🏷️ Indicadores de Criticidad**")

            es_consorcio = 1 if tipo_proveedor == "consortium" else st.selectbox(
                "¿Es Consorcio? (es_consorcio)",
                options=[0, 1],
                index=0,
                format_func=lambda x: "Sí (1)" if x == 1 else "No (0)"
            )

            es_uso_critico = st.selectbox(
                "¿Medicamento de Uso Crítico?",
                options=[0, 1],
                index=0,
                format_func=lambda x: "Sí — Crítico (1)" if x == 1 else "No — Estándar (0)",
                help="Indica si el fármaco está en la lista de medicamentos críticos del MINSA."
            )

            es_intrahospitalario = st.selectbox(
                "¿Uso Intrahospitalario?",
                options=[0, 1],
                index=0,
                format_func=lambda x: "Sí — Intrahospitalario (1)" if x == 1 else "No — Ambulatorio (0)",
                help="Indica si el medicamento requiere administración hospitalaria."
            )

            st.markdown("---")
            btn_predict = st.button(
                "🔮 CALCULAR RIESGO", type="primary", use_container_width=True
            )

        # ── Panel de resultados ────────────────────────────────────────────────
        raw_input = {
            "red_asistencial"          : red_asistencial,
            "dpto_entrega_item"        : dpto_entrega,
            "tipo_proveedor"           : tipo_proveedor,
            "es_consorcio"             : int(es_consorcio),
            "denominacion_dci"         : denominacion_dci,
            "es_uso_critico"           : int(es_uso_critico),
            "es_uso_intrahospitalario" : int(es_intrahospitalario),
            "tipo_proceso_seleccion"   : tipo_proceso,
            "monto_adjudicado_soles"   : float(monto_adjudicado),
            "anio"                     : int(anio),
        }

        col_left, col_right = st.columns([1, 1], gap="large")

        with col_left:
            st.markdown("#### 📋 Resumen del Contrato Ingresado")
            input_display = {
                "Red Asistencial"        : red_asistencial.title(),
                "Departamento Entrega"   : dpto_entrega.title(),
                "Tipo Proveedor"         : tipo_proveedor.title(),
                "Es Consorcio"           : "Sí" if es_consorcio else "No",
                "Medicamento (DCI)"      : denominacion_dci.title(),
                "Tipo Proceso"           : tipo_proceso.title(),
                "Monto Adjudicado"       : f"S/. {monto_adjudicado:,.0f}",
                "Año"                    : str(anio),
                "Uso Crítico"            : "Sí" if es_uso_critico else "No",
                "Uso Intrahospitalario"  : "Sí" if es_intrahospitalario else "No",
            }
            df_display = pd.DataFrame(
                list(input_display.items()), columns=["Atributo", "Valor"]
            )
            st.dataframe(df_display, hide_index=True, use_container_width=True)

        with col_right:
            st.markdown("#### 🎯 Resultado de la Evaluación de Riesgo")

            if btn_predict or True:  # Always show prediction on load
                result = predict_risk(raw_input, selected_model, artifacts)

                if "error" in result:
                    st.error(f"⚠️ Error en predicción: {result['error']}")
                    st.info("Ejecute primero el Notebook 02 para entrenar y serializar los modelos.")
                else:
                    prob    = result["probability"]
                    risk_pct = result["risk_pct"]
                    label   = result["label"]

                    # Color semafórico según nivel de riesgo
                    if risk_pct < 25:
                        risk_color = COLOR_SUCCESS
                        risk_level = "BAJO"
                        risk_icon  = "🟢"
                        risk_msg   = "Contrato de bajo riesgo. Ejecución limpia probable."
                    elif risk_pct < 50:
                        risk_color = "#F9A825"
                        risk_level = "MODERADO"
                        risk_icon  = "🟡"
                        risk_msg   = "Riesgo moderado. Monitoreo recomendado."
                    elif risk_pct < 75:
                        risk_color = COLOR_WARNING
                        risk_level = "ALTO"
                        risk_icon  = "🟠"
                        risk_msg   = "Riesgo alto. Revisión contractual preventiva sugerida."
                    else:
                        risk_color = COLOR_SECONDARY
                        risk_level = "MUY ALTO"
                        risk_icon  = "🔴"
                        risk_msg   = "Riesgo crítico. Alta probabilidad de requerir adenda."

                    st.markdown(
                        f"""
                        <div style='background:{risk_color}20; border:2px solid {risk_color};
                                    border-radius:12px; padding:1.5rem; text-align:center;'>
                            <div style='font-size:3.5rem; font-weight:900; color:{risk_color};'>
                                {risk_pct:.1f}%
                            </div>
                            <div style='font-size:1.2rem; font-weight:700; color:{risk_color}; margin-top:0.3rem;'>
                                {risk_icon} RIESGO {risk_level}
                            </div>
                            <div style='font-size:0.9rem; color:#555; margin-top:0.5rem;'>
                                {risk_msg}
                            </div>
                            <div style='font-size:0.8rem; color:#777; margin-top:0.8rem;'>
                                Clasificación binaria: <strong>{'1 — Con Adenda' if label else '0 — Sin Adenda'}</strong>
                                &nbsp;|&nbsp; Modelo: <strong>{selected_model}</strong>
                            </div>
                        </div>
                        """,
                        unsafe_allow_html=True
                    )

                    # Gauge chart usando matplotlib
                    st.markdown("")
                    fig_gauge, ax_gauge = plt.subplots(figsize=(6, 2.5))
                    ax_gauge.set_xlim(0, 1)
                    ax_gauge.set_ylim(0, 1)
                    ax_gauge.axis("off")

                    # Barra de progreso
                    ax_gauge.barh(0.5, 1.0, height=0.35, color="#E0E0E0", left=0)
                    ax_gauge.barh(0.5, prob, height=0.35, color=risk_color, left=0)
                    ax_gauge.text(prob, 0.5, f"  {risk_pct:.1f}%",
                                  va="center", ha="left", fontsize=14,
                                  fontweight="bold", color=risk_color)
                    ax_gauge.text(0, 0.15, "0%", va="center", ha="left", fontsize=9, color="gray")
                    ax_gauge.text(0.5, 0.15, "50%", va="center", ha="center", fontsize=9, color="gray")
                    ax_gauge.text(1.0, 0.15, "100%", va="center", ha="right", fontsize=9, color="gray")
                    ax_gauge.axvline(x=0.5, color="navy", linestyle="--", linewidth=1, alpha=0.5)
                    ax_gauge.set_title("Probabilidad de Riesgo de Sobrecosto",
                                       fontsize=10, fontweight="bold")
                    plt.tight_layout()
                    st.pyplot(fig_gauge, use_container_width=True)
                    plt.close()

                    # Predicción con todos los modelos
                    st.markdown("---")
                    st.markdown("**Comparativa entre modelos:**")
                    all_preds = {}
                    for mname in ["LightGBM", "CatBoost", "XGBoost"]:
                        r = predict_risk(raw_input, mname, artifacts)
                        if "error" not in r:
                            all_preds[mname] = r["risk_pct"]

                    if all_preds:
                        comp_df = pd.DataFrame(
                            [(k, f"{v:.1f}%") for k, v in all_preds.items()],
                            columns=["Modelo", "Prob. Riesgo (%)"]
                        )
                        st.dataframe(comp_df, hide_index=True, use_container_width=True)

    # ═══════════════════════════════════════════════════════════════════════════
    # TAB 2: MÉTRICAS DEL PROYECTO
    # ═══════════════════════════════════════════════════════════════════════════
    with tab2:
        st.markdown("### 📊 Rendimiento de Modelos — Conjunto de Prueba (20%)")

        if metrics_df is not None:
            # KPIs del mejor modelo
            best_name = config.get("best_model_name", "LightGBM") if config else "LightGBM"
            if best_name in metrics_df.index:
                best_metrics = metrics_df.loc[best_name]
                c1, c2, c3, c4, c5 = st.columns(5)
                for col, metric, icon in zip(
                    [c1, c2, c3, c4, c5],
                    ["Accuracy", "Precision", "Recall", "F1-Score", "ROC-AUC"],
                    ["✅", "🎯", "📡", "⚡", "📈"]
                ):
                    with col:
                        st.metric(
                            label=f"{icon} {metric}",
                            value=f"{best_metrics[metric]:.3f}",
                            delta=f"Mejor: {best_name}"
                        )

            st.markdown("---")
            st.markdown("#### Tabla Comparativa Completa")

            styled_metrics = metrics_df.style.format("{:.4f}").background_gradient(
                cmap="YlGn", subset=["Accuracy", "Precision", "Recall", "F1-Score", "ROC-AUC"]
            ).background_gradient(
                cmap="YlOrRd_r", subset=["Log-Loss", "Brier Score"]
            )
            st.dataframe(styled_metrics, use_container_width=True)

            # Gráfico de barras
            st.markdown("#### Gráfico Comparativo de Métricas")
            metrics_to_show = ["Accuracy", "Precision", "Recall", "F1-Score", "ROC-AUC"]
            fig_bar, ax_bar = plt.subplots(figsize=(11, 5))
            x = np.arange(len(metrics_to_show))
            width = 0.25
            colors = ["#2196F3", "#FF5722", "#4CAF50"]

            for i, (mname, color) in enumerate(zip(metrics_df.index, colors)):
                vals = [metrics_df.loc[mname, m] for m in metrics_to_show]
                bars = ax_bar.bar(x + (i - 1) * width, vals, width,
                                  label=mname, color=color, alpha=0.85, edgecolor="white")
                for bar, val in zip(bars, vals):
                    ax_bar.text(bar.get_x() + bar.get_width() / 2,
                                bar.get_height() + 0.005,
                                f"{val:.3f}", ha="center", va="bottom",
                                fontsize=8, fontweight="bold")

            ax_bar.set_xticks(x)
            ax_bar.set_xticklabels(metrics_to_show)
            ax_bar.set_ylim([0, 1.15])
            ax_bar.set_title("Comparativa de Métricas — LightGBM vs CatBoost vs XGBoost",
                             fontweight="bold")
            ax_bar.legend()
            ax_bar.axhline(y=0.5, color="gray", linestyle=":", linewidth=1, alpha=0.5)
            plt.tight_layout()
            st.pyplot(fig_bar, use_container_width=True)
            plt.close()
        else:
            st.warning("⚠️ Métricas no disponibles. Ejecute primero el Notebook 02.")

    # ═══════════════════════════════════════════════════════════════════════════
    # TAB 3: CURVAS DE EVALUACIÓN
    # ═══════════════════════════════════════════════════════════════════════════
    with tab3:
        st.markdown("### 📈 Curvas de Evaluación del Sistema Predictivo")

        for img_name, title in [
            ("viz_roc_pr_curves.png",   "Curvas ROC y Precision-Recall"),
            ("viz_calibration_curves.png", "Curvas de Calibración (Reliability Diagram)"),
            ("viz_confusion_matrix.png",   "Matriz de Confusión — Mejor Modelo"),
        ]:
            img_path = DATA_DIR / img_name
            if img_path.exists():
                st.markdown(f"#### {title}")
                st.image(str(img_path), use_column_width=True)
            else:
                st.info(f"📌 Gráfica '{img_name}' no encontrada. Ejecute el Notebook 02.")

    # ═══════════════════════════════════════════════════════════════════════════
    # TAB 4: IMPORTANCIA DE VARIABLES
    # ═══════════════════════════════════════════════════════════════════════════
    with tab4:
        st.markdown("### 🔍 Importancia de Características por Modelo GBDT")
        st.info(
            "Las gráficas de Feature Importance revelan qué atributos del contrato "
            "tienen mayor poder predictivo para detectar el riesgo de sobrecosto."
        )

        img_path = DATA_DIR / "viz_feature_importance.png"
        if img_path.exists():
            st.image(str(img_path), use_column_width=True)
        else:
            st.info("📌 Ejecute el Notebook 02 para generar las gráficas de importancia.")

        if df_proc is not None:
            st.markdown("---")
            st.markdown("#### Vista Rápida del Dataset Procesado")
            st.dataframe(df_proc.head(100), use_container_width=True)

            # Estadísticas básicas
            target_col = "flag_tiene_adenda"
            if target_col in df_proc.columns:
                col_s1, col_s2, col_s3 = st.columns(3)
                with col_s1:
                    st.metric("Total Registros", f"{len(df_proc):,}")
                with col_s2:
                    st.metric("Sin Adenda (clase 0)",
                              f"{(df_proc[target_col]==0).sum():,} ({(df_proc[target_col]==0).mean():.1%})")
                with col_s3:
                    st.metric("Con Adenda (clase 1)",
                              f"{(df_proc[target_col]==1).sum():,} ({(df_proc[target_col]==1).mean():.1%})")

    # ── Footer ────────────────────────────────────────────────────────────────
    st.markdown("---")
    st.markdown(
        "<div style='text-align:center; color:#9E9E9E; font-size:0.8rem;'>"
        "GBDT Risk Management System · Universidad Nacional Mayor de San Marcos (UNMSM) · "
        "Ingeniería de Software · Aprendizaje Supervisado Aplicado"
        "</div>",
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()
