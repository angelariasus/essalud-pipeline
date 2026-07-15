"""
Fase 6 — Motor de alertas operativas (HHI + Lead Time) y envío por correo.

Transforma el análisis pasivo en respuesta operativa: detecta mercados de
medicamentos en riesgo (concentración HHI crítica) y procesos con Lead Time
anómalo (retraso real muy superior al predicho por el modelo de la Fase 4), y
notifica por correo formal al área de abastecimiento con los 3 campos clave:
**RUC del proveedor dominante, denominación del medicamento y Red Asistencial**.

Fuentes (Parquet de `data/mart/`, sin Spark ni SQL Server — pandas puro):
  - `Fact_Ordenes_Y_Contratos.parquet` + dims -> réplica pandas de la vista
    `oro.vw_Matriz_Riesgo_HHI` (mismos umbrales: HHI>=8000, dominante>=80%).
  - `Pred_Lead_Time.parquet` (Fase 4) -> Residual = Actual - Predicho; una fila
    es anómala si su residual excede `media + sigma*desviación` (default 2σ).

`Es_Uso_Critico` no viene poblado en el Parquet (en el DW es BIT DEFAULT 0);
se deriva de `Restriccion_Uso` del Petitorio: un medicamento con códigos de
restricción de uso se trata como de uso crítico para el semáforo.

Salidas:
  - `data/mart/Alertas.parquet` — consolidado (ambas fuentes) para la Vista Operativa
    de Power BI (visual Power Automate lee estas columnas).
  - Correo SMTP (Gmail App Password o MailHog local), ver `send_alerts()`.

CLI: `python main.py alert [--source hhi|leadtime|all] [--to correo] [--dry-run]`.
"""
from __future__ import annotations

import smtplib
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Optional

import pandas as pd

from app.audit.logger import setup_logger
from app.config.settings import settings

logger = setup_logger("ocds_framework.services.alerting")

# Umbrales del semáforo HHI (idénticos a oro.vw_Matriz_Riesgo_HHI).
HHI_ALTO = 8000
HHI_MODERADO = 2500
DOMINANTE_PCT = 80.0
# Umbral de anomalía de Lead Time: residual > media + LEADTIME_SIGMA * std.
LEADTIME_SIGMA = 2.0

ALERTAS_FILENAME = "Alertas.parquet"

# Columnas del consolidado data/mart/Alertas.parquet (contrato con Power BI / correo).
ALERTAS_COLS = [
    "Tipo_Alerta", "Anio", "Red_Asistencial", "Medicamento",
    "RUC_Proveedor", "Nombre_Proveedor", "Metrica", "Valor", "Umbral", "Detalle",
]


def _read_bi(name: str, bi_dir: Optional[Path] = None) -> pd.DataFrame:
    path = Path(bi_dir or settings.BI_DIR) / f"{name}.parquet"
    if not path.exists():
        raise FileNotFoundError(
            f"No existe {path}. Ejecuta `python main.py gold` "
            f"(y el notebook de la Fase 4 para Pred_Lead_Time) antes de alertar."
        )
    return pd.read_parquet(path)


def _es_uso_critico(med: pd.DataFrame) -> pd.Series:
    """Deriva Es_Uso_Critico de Restriccion_Uso (el DW la tiene en BIT DEFAULT 0)."""
    r = med.get("Restriccion_Uso")
    if r is None:
        return pd.Series(False, index=med.index)
    return r.notna() & (r.astype(str).str.strip() != "") & (r.astype(str) != "nan")


# ── Fuente 1: concentración de mercado (HHI) ─────────────────────────────────
def build_hhi_alerts(
    fact: Optional[pd.DataFrame] = None,
    med: Optional[pd.DataFrame] = None,
    prov: Optional[pd.DataFrame] = None,
    ent: Optional[pd.DataFrame] = None,
    bi_dir: Optional[Path] = None,
) -> pd.DataFrame:
    """
    Réplica pandas de `oro.vw_Matriz_Riesgo_HHI`: HHI por (año, entidad,
    medicamento), proveedor dominante y semáforo. Retorna TODAS las filas con
    su `Nivel_Alerta_HHI` (el llamador filtra CRITICO/Disparar_Alerta).
    """
    fact = _read_bi("Fact_Ordenes_Y_Contratos", bi_dir) if fact is None else fact
    med = _read_bi("Dim_Medicamento", bi_dir) if med is None else med
    prov = _read_bi("Dim_Proveedor", bi_dir) if prov is None else prov
    ent = _read_bi("Dim_Entidad_Compradora", bi_dir) if ent is None else ent

    # Mismos filtros que la vista: monto > 0, medicamento y proveedor resueltos.
    f = fact[
        (fact["Monto_Adjudicado_Soles"] > 0)
        & (fact["FK_Medicamento"] > 0)
        & (fact["FK_Proveedor"] > 0)
    ]
    if f.empty:
        return pd.DataFrame()

    keys = ["Anio_Fiscal", "FK_Entidad", "FK_Medicamento"]
    part = (
        f.groupby(keys + ["FK_Proveedor"], as_index=False)["Monto_Adjudicado_Soles"]
        .sum()
        .rename(columns={"Monto_Adjudicado_Soles": "Monto_Proveedor"})
    )
    part["Monto_Total_Mercado"] = part.groupby(keys)["Monto_Proveedor"].transform("sum")
    part["Share"] = part["Monto_Proveedor"] / part["Monto_Total_Mercado"]

    hhi = (
        part.assign(Share2=part["Share"] ** 2)
        .groupby(keys)
        .agg(
            HHI=("Share2", lambda s: round(s.sum() * 10000)),
            Num_Proveedores=("Share2", "size"),
            Monto_Total_Mercado=("Monto_Total_Mercado", "first"),
        )
        .reset_index()
    )

    # Proveedor dominante por mercado (equivalente al ROW_NUMBER ... ORDER BY DESC).
    dom = part.sort_values("Monto_Proveedor", ascending=False).drop_duplicates(keys)
    dom = dom.assign(Participacion_Pct_Dominante=(dom["Share"] * 100).round(1))
    hhi = hhi.merge(
        dom[keys + ["FK_Proveedor", "Participacion_Pct_Dominante"]], on=keys, how="left"
    )

    med = med.assign(Es_Uso_Critico=_es_uso_critico(med))
    hhi = (
        hhi.merge(
            ent[["SK_Entidad", "Red_Asistencial"]],
            left_on="FK_Entidad", right_on="SK_Entidad", how="left",
        )
        .merge(
            med[["SK_Medicamento", "Denominacion_DCI", "Es_Uso_Critico"]],
            left_on="FK_Medicamento", right_on="SK_Medicamento", how="left",
        )
        .merge(
            prov[["SK_Proveedor", "RUC_Proveedor", "Nombre_Proveedor"]],
            left_on="FK_Proveedor", right_on="SK_Proveedor", how="left",
        )
    )
    hhi["Es_Uso_Critico"] = hhi["Es_Uso_Critico"].fillna(False)

    critico = (hhi["HHI"] >= HHI_ALTO) & hhi["Es_Uso_Critico"]
    hhi["Nivel_Alerta_HHI"] = "BAJO"
    hhi.loc[hhi["HHI"] >= HHI_MODERADO, "Nivel_Alerta_HHI"] = "MODERADO"
    hhi.loc[hhi["HHI"] >= HHI_ALTO, "Nivel_Alerta_HHI"] = "ALTO"
    hhi.loc[critico, "Nivel_Alerta_HHI"] = "CRITICO"
    hhi["Disparar_Alerta"] = (
        critico & (hhi["Participacion_Pct_Dominante"] >= DOMINANTE_PCT)
    )
    return hhi


# ── Fuente 2: Lead Time anómalo (Fase 4) ─────────────────────────────────────
def build_leadtime_alerts(
    pred: Optional[pd.DataFrame] = None,
    fact: Optional[pd.DataFrame] = None,
    med: Optional[pd.DataFrame] = None,
    prov: Optional[pd.DataFrame] = None,
    sigma: float = LEADTIME_SIGMA,
    bi_dir: Optional[Path] = None,
) -> pd.DataFrame:
    """
    Procesos con retraso anómalo: `Residual > media + sigma*std` (solo retrasos,
    Residual > 0 = tardó más de lo predicho). `ID_Registro` es posicional sobre
    la Fact (así lo construye el notebook de la Fase 4), lo que permite resolver
    RUC del proveedor y medicamento de cada proceso alertado.
    """
    pred = _read_bi("Pred_Lead_Time", bi_dir) if pred is None else pred
    fact = _read_bi("Fact_Ordenes_Y_Contratos", bi_dir) if fact is None else fact
    med = _read_bi("Dim_Medicamento", bi_dir) if med is None else med
    prov = _read_bi("Dim_Proveedor", bi_dir) if prov is None else prov

    valid = pred["Residual"].notna()
    if not valid.any():
        return pd.DataFrame()
    mu, sd = pred.loc[valid, "Residual"].mean(), pred.loc[valid, "Residual"].std()
    umbral = mu + sigma * sd
    anom = pred[valid & (pred["Residual"] > umbral)].copy()
    if anom.empty:
        return anom
    anom["Umbral_Dias"] = round(umbral, 1)

    # ID_Registro -> fila de la Fact (posicional) -> FKs -> RUC / medicamento.
    fk = fact.reset_index(drop=True)[["FK_Medicamento", "FK_Proveedor"]]
    anom = anom.merge(
        fk, left_on="ID_Registro", right_index=True, how="left"
    ).merge(
        med[["SK_Medicamento", "Denominacion_DCI"]],
        left_on="FK_Medicamento", right_on="SK_Medicamento", how="left",
    ).merge(
        prov[["SK_Proveedor", "RUC_Proveedor", "Nombre_Proveedor"]],
        left_on="FK_Proveedor", right_on="SK_Proveedor", how="left",
    )
    return anom


# ── Consolidado data/mart/Alertas.parquet ───────────────────────────────────────────
def build_alertas(
    bi_dir: Optional[Path] = None,
    sigma: float = LEADTIME_SIGMA,
    save: bool = True,
) -> pd.DataFrame:
    """
    Une ambas fuentes en el esquema `ALERTAS_COLS` y (opcional) lo persiste en
    `data/mart/Alertas.parquet` para la Vista Operativa de Power BI.
    """
    frames = []

    hhi = build_hhi_alerts(bi_dir=bi_dir)
    if len(hhi):
        top = hhi[hhi["Disparar_Alerta"]]
        frames.append(pd.DataFrame({
            "Tipo_Alerta": "HHI_CRITICO",
            "Anio": top["Anio_Fiscal"].astype(int),
            "Red_Asistencial": top["Red_Asistencial"].fillna("DESCONOCIDO"),
            "Medicamento": top["Denominacion_DCI"].fillna("DESCONOCIDO"),
            "RUC_Proveedor": top["RUC_Proveedor"].astype(str),
            "Nombre_Proveedor": top["Nombre_Proveedor"].fillna(""),
            "Metrica": "HHI",
            "Valor": top["HHI"].astype(float),
            "Umbral": float(HHI_ALTO),
            "Detalle": (
                "Mercado monopolizado (dominante "
                + top["Participacion_Pct_Dominante"].round(1).astype(str)
                + "% del monto adjudicado, medicamento de uso restringido)"
            ),
        }))

    lt = build_leadtime_alerts(sigma=sigma, bi_dir=bi_dir)
    if len(lt):
        frames.append(pd.DataFrame({
            "Tipo_Alerta": "LEAD_TIME_ANOMALO",
            "Anio": lt["Anio_Fiscal"].astype(int),
            "Red_Asistencial": lt["Red_Asistencial"].fillna("DESCONOCIDO"),
            "Medicamento": lt["Denominacion_DCI"].fillna("DESCONOCIDO"),
            "RUC_Proveedor": lt["RUC_Proveedor"].astype(str),
            "Nombre_Proveedor": lt["Nombre_Proveedor"].fillna(""),
            "Metrica": "Residual_Dias",
            "Valor": lt["Residual"].round(1).astype(float),
            "Umbral": lt["Umbral_Dias"].astype(float),
            "Detalle": (
                "Proceso demoró " + lt["Lead_Time_Actual"].round(0).astype(int).astype(str)
                + " días vs. " + lt["Lead_Time_Predicho"].round(0).astype(int).astype(str)
                + " predichos (" + lt["Categoria_Proceso"].fillna("?") + ")"
            ),
        }))

    alertas = (
        pd.concat(frames, ignore_index=True)[ALERTAS_COLS]
        if frames else pd.DataFrame(columns=ALERTAS_COLS)
    )
    # Sanea saltos de línea del origen SEACE (rompen la tabla del correo).
    for col in ("Medicamento", "Nombre_Proveedor", "Detalle"):
        if len(alertas):
            alertas[col] = (
                alertas[col].astype(str).str.replace(r"\s+", " ", regex=True).str.strip()
            )
    alertas = alertas.sort_values(
        ["Tipo_Alerta", "Valor"], ascending=[True, False]
    ).reset_index(drop=True)

    if save:
        out = Path(bi_dir or settings.BI_DIR) / ALERTAS_FILENAME
        alertas.to_parquet(out, index=False)
        logger.info(f"Consolidado de alertas -> {out} ({len(alertas)} filas).")
    return alertas


# ── Correo ───────────────────────────────────────────────────────────────────
def render_email(alertas: pd.DataFrame, to: str, limit: int = 20) -> MIMEMultipart:
    """Arma el correo formal (texto plano + HTML) con las alertas activas."""
    shown = alertas.head(limit)
    n_total, n_hhi = len(alertas), int((alertas["Tipo_Alerta"] == "HHI_CRITICO").sum())
    n_lt = n_total - n_hhi
    fecha = datetime.now().strftime("%d/%m/%Y %H:%M")

    msg = MIMEMultipart("alternative")
    msg["Subject"] = (
        f"[ALERTA EsSalud] {n_total} alertas de abastecimiento "
        f"({n_hhi} HHI crítico, {n_lt} lead time anómalo)"
    )
    msg["From"] = settings.SMTP_FROM or "alertas@essalud-pipeline.local"
    msg["To"] = to

    lines = [
        "Estimados, Área de Abastecimiento:",
        "",
        f"El monitoreo automático del {fecha} detectó {n_total} alertas activas "
        f"en las adquisiciones de medicamentos de EsSalud:",
        "",
    ]
    for _, r in shown.iterrows():
        lines.append(
            f"- [{r['Tipo_Alerta']}] {r['Medicamento']} | Red: {r['Red_Asistencial']} | "
            f"Proveedor: {r['Nombre_Proveedor']} (RUC {r['RUC_Proveedor']}) | "
            f"{r['Metrica']}={r['Valor']:.0f} (umbral {r['Umbral']:.0f}). {r['Detalle']}"
        )
    if n_total > limit:
        lines.append(f"... y {n_total - limit} alertas adicionales (ver data/mart/Alertas.parquet).")
    lines += [
        "",
        "Se solicita evaluar acciones de diversificación de proveedores y/o "
        "seguimiento del proceso según corresponda.",
        "",
        "Atentamente,",
        "Sistema de Monitoreo BI — EsSalud Pipeline (generado automáticamente)",
    ]
    msg.attach(MIMEText("\n".join(lines), "plain", "utf-8"))

    rows = "".join(
        "<tr>"
        f"<td>{r['Tipo_Alerta']}</td><td>{r['Anio']}</td><td>{r['Medicamento']}</td>"
        f"<td>{r['Red_Asistencial']}</td><td>{r['RUC_Proveedor']}</td>"
        f"<td>{r['Nombre_Proveedor']}</td><td align='right'>{r['Valor']:.0f}</td>"
        f"<td>{r['Detalle']}</td>"
        "</tr>"
        for _, r in shown.iterrows()
    )
    html = f"""
    <p>Estimados, Área de Abastecimiento:</p>
    <p>El monitoreo automático del <b>{fecha}</b> detectó <b>{n_total}</b> alertas activas
    ({n_hhi} de concentración HHI crítica, {n_lt} de lead time anómalo):</p>
    <table border="1" cellpadding="4" cellspacing="0" style="border-collapse:collapse;font-size:13px">
      <tr style="background:#8B0000;color:#fff">
        <th>Tipo</th><th>Año</th><th>Medicamento</th><th>Red Asistencial</th>
        <th>RUC Proveedor</th><th>Proveedor</th><th>Valor</th><th>Detalle</th>
      </tr>
      {rows}
    </table>
    {f"<p><i>... y {n_total - limit} alertas adicionales (ver data/mart/Alertas.parquet).</i></p>" if n_total > limit else ""}
    <p>Se solicita evaluar acciones de diversificación de proveedores y/o seguimiento
    del proceso según corresponda.</p>
    <p>Atentamente,<br/>Sistema de Monitoreo BI — EsSalud Pipeline
    <i>(generado automáticamente)</i></p>
    """
    msg.attach(MIMEText(html, "html", "utf-8"))
    return msg


def send_email(msg: MIMEMultipart) -> None:
    """Envía por SMTP según settings (STARTTLS y login solo si están configurados)."""
    if not settings.SMTP_HOST:
        raise ValueError(
            "SMTP_HOST no está configurado en .env. Para Gmail: SMTP_HOST=smtp.gmail.com, "
            "SMTP_PORT=587, SMTP_USER/SMTP_PASSWORD (App Password). Para MailHog local: "
            "SMTP_HOST=localhost, SMTP_PORT=1025, SMTP_STARTTLS=false."
        )
    logger.info(f"Enviando correo vía {settings.SMTP_HOST}:{settings.SMTP_PORT} -> {msg['To']}")
    with smtplib.SMTP(settings.SMTP_HOST, settings.SMTP_PORT, timeout=30) as server:
        if settings.SMTP_STARTTLS:
            server.starttls()
        if settings.SMTP_USER and settings.SMTP_PASSWORD:
            server.login(settings.SMTP_USER, settings.SMTP_PASSWORD)
        server.send_message(msg)
    logger.info("Correo de alertas enviado.")


def send_alerts(
    source: str = "all",
    to: Optional[str] = None,
    dry_run: bool = False,
    limit: int = 20,
    sigma: float = LEADTIME_SIGMA,
    bi_dir: Optional[Path] = None,
) -> int:
    """
    Flujo completo: construye `data/mart/Alertas.parquet`, filtra por `source`
    (hhi | leadtime | all) y envía (o imprime, con `dry_run`) el correo.
    Retorna el número de alertas notificadas.
    """
    alertas = build_alertas(bi_dir=bi_dir, sigma=sigma, save=True)
    if source == "hhi":
        alertas = alertas[alertas["Tipo_Alerta"] == "HHI_CRITICO"]
    elif source == "leadtime":
        alertas = alertas[alertas["Tipo_Alerta"] == "LEAD_TIME_ANOMALO"]

    if alertas.empty:
        logger.info(f"Sin alertas activas para source='{source}'; no se envía correo.")
        return 0

    to = to or settings.SMTP_TO
    if not to:
        raise ValueError("Destinatario vacío: pasa --to o define SMTP_TO en .env.")
    msg = render_email(alertas, to=to, limit=limit)

    if dry_run:
        logger.info(f"[dry-run] {len(alertas)} alertas; correo NO enviado. Vista previa:")
        print(f"Subject: {msg['Subject']}\nTo: {msg['To']}\n")
        print(msg.get_payload(0).get_payload(decode=True).decode("utf-8"))
    else:
        send_email(msg)
    return len(alertas)
