"""Tests de la Fase 6 (alerting): HHI, lead time anómalo y correo (sin red)."""
from unittest.mock import patch

import pandas as pd
import pytest

from app.services import alerting


# ── Fixtures mínimas (esquema de bi/*.parquet) ───────────────────────────────
def _dims():
    med = pd.DataFrame({
        "SK_Medicamento": [1, 2],
        "Denominacion_DCI": ["PARACETAMOL 500MG", "INSULINA HUMANA"],
        "Restriccion_Uso": [None, "3,8"],  # solo la insulina es de uso restringido
    })
    prov = pd.DataFrame({
        "SK_Proveedor": [10, 20],
        "RUC_Proveedor": ["20100000001", "20100000002"],
        "Nombre_Proveedor": ["FARMA UNO S.A.", "MONOPOLIO PHARMA S.A.C."],
    })
    ent = pd.DataFrame({
        "SK_Entidad": [100],
        "Red_Asistencial": ["RED PRESTACIONAL REBAGLIATI"],
    })
    return med, prov, ent


def _fact(rows):
    """rows: lista de (medicamento, proveedor, monto)."""
    return pd.DataFrame({
        "Anio_Fiscal": [2024] * len(rows),
        "FK_Entidad": [100] * len(rows),
        "FK_Medicamento": [r[0] for r in rows],
        "FK_Proveedor": [r[1] for r in rows],
        "Monto_Adjudicado_Soles": [r[2] for r in rows],
    })


# ── HHI ──────────────────────────────────────────────────────────────────────
def test_hhi_monopolio_critico():
    # Un solo proveedor (100% del mercado) en medicamento restringido -> HHI 10000, CRITICO.
    med, prov, ent = _dims()
    fact = _fact([(2, 20, 50000.0)])
    hhi = alerting.build_hhi_alerts(fact, med, prov, ent)
    assert len(hhi) == 1
    row = hhi.iloc[0]
    assert row["HHI"] == 10000
    assert row["Nivel_Alerta_HHI"] == "CRITICO"
    assert bool(row["Disparar_Alerta"]) is True
    assert row["RUC_Proveedor"] == "20100000002"
    assert row["Red_Asistencial"] == "RED PRESTACIONAL REBAGLIATI"


def test_hhi_monopolio_no_critico_sin_restriccion():
    # Monopolio en medicamento SIN restricción -> ALTO (no CRITICO, no dispara).
    med, prov, ent = _dims()
    fact = _fact([(1, 10, 50000.0)])
    hhi = alerting.build_hhi_alerts(fact, med, prov, ent)
    assert hhi.iloc[0]["Nivel_Alerta_HHI"] == "ALTO"
    assert bool(hhi.iloc[0]["Disparar_Alerta"]) is False


def test_hhi_duopolio_equilibrado_moderado():
    # Dos proveedores 50/50 -> HHI = 2*(0.5^2)*10000 = 5000 -> MODERADO.
    med, prov, ent = _dims()
    fact = _fact([(2, 10, 30000.0), (2, 20, 30000.0)])
    hhi = alerting.build_hhi_alerts(fact, med, prov, ent)
    row = hhi.iloc[0]
    assert row["HHI"] == 5000
    assert row["Nivel_Alerta_HHI"] == "MODERADO"
    assert row["Participacion_Pct_Dominante"] == 50.0


def test_hhi_dominante_correcto():
    # 90/10 -> dominante es el de 90% (HHI 8200 >= 8000 y restringido -> dispara).
    med, prov, ent = _dims()
    fact = _fact([(2, 20, 90000.0), (2, 10, 10000.0)])
    hhi = alerting.build_hhi_alerts(fact, med, prov, ent)
    row = hhi.iloc[0]
    assert row["Nombre_Proveedor"] == "MONOPOLIO PHARMA S.A.C."
    assert row["Participacion_Pct_Dominante"] == 90.0
    assert row["HHI"] == 8200
    assert bool(row["Disparar_Alerta"]) is True


def test_hhi_ignora_montos_cero_y_fks_sentinela():
    med, prov, ent = _dims()
    fact = _fact([(2, 20, 0.0), (-1, 20, 5000.0), (2, -1, 5000.0)])
    hhi = alerting.build_hhi_alerts(fact, med, prov, ent)
    assert len(hhi) == 0


# ── Lead time ────────────────────────────────────────────────────────────────
def _pred(residuals):
    n = len(residuals)
    return pd.DataFrame({
        "ID_Registro": range(n),
        "Anio_Fiscal": [2024] * n,
        "Red_Asistencial": ["RED X"] * n,
        "Categoria_Proceso": ["COMPETITIVO"] * n,
        "Lead_Time_Actual": [100.0 + (r or 0) for r in residuals],
        "Lead_Time_Predicho": [100.0] * n,
        "Residual": residuals,
    })


def test_leadtime_detecta_outlier():
    med, prov, _ = _dims()
    residuals = [0.0, 1.0, -1.0, 2.0, -2.0, 0.5, -0.5, 1.5, -1.5, 300.0]
    pred = _pred(residuals)
    fact = _fact([(2, 20, 1000.0)] * len(residuals))
    out = alerting.build_leadtime_alerts(pred, fact, med, prov, sigma=2.0)
    assert len(out) == 1
    assert out.iloc[0]["Residual"] == 300.0
    assert out.iloc[0]["RUC_Proveedor"] == "20100000002"
    assert out.iloc[0]["Denominacion_DCI"] == "INSULINA HUMANA"


def test_leadtime_sin_anomalias():
    med, prov, _ = _dims()
    pred = _pred([0.0, 1.0, -1.0, 0.5])
    fact = _fact([(1, 10, 1000.0)] * 4)
    out = alerting.build_leadtime_alerts(pred, fact, med, prov, sigma=2.0)
    assert len(out) == 0


def test_leadtime_ignora_residual_nan():
    med, prov, _ = _dims()
    pred = _pred([None] * 5)
    fact = _fact([(1, 10, 1000.0)] * 5)
    out = alerting.build_leadtime_alerts(pred, fact, med, prov)
    assert len(out) == 0


# ── Correo ───────────────────────────────────────────────────────────────────
def _alertas_df():
    return pd.DataFrame({
        "Tipo_Alerta": ["HHI_CRITICO"],
        "Anio": [2024],
        "Red_Asistencial": ["RED PRESTACIONAL REBAGLIATI"],
        "Medicamento": ["INSULINA HUMANA"],
        "RUC_Proveedor": ["20100000002"],
        "Nombre_Proveedor": ["MONOPOLIO PHARMA S.A.C."],
        "Metrica": ["HHI"],
        "Valor": [10000.0],
        "Umbral": [8000.0],
        "Detalle": ["Mercado monopolizado"],
    })


def test_render_email_contiene_los_tres_campos():
    msg = alerting.render_email(_alertas_df(), to="abastecimiento@example.com")
    assert "abastecimiento@example.com" == msg["To"]
    assert "1 alertas" in msg["Subject"]
    plain = msg.get_payload(0).get_payload(decode=True).decode("utf-8")
    # Los 3 campos que exige la Fase 6: RUC, medicamento y Red Asistencial.
    assert "20100000002" in plain
    assert "INSULINA HUMANA" in plain
    assert "RED PRESTACIONAL REBAGLIATI" in plain


def test_send_email_requiere_smtp_host(monkeypatch):
    monkeypatch.setattr(alerting.settings, "SMTP_HOST", "")
    with pytest.raises(ValueError, match="SMTP_HOST"):
        alerting.send_email(alerting.render_email(_alertas_df(), to="x@y.z"))


def test_send_email_usa_smtp(monkeypatch):
    monkeypatch.setattr(alerting.settings, "SMTP_HOST", "localhost")
    monkeypatch.setattr(alerting.settings, "SMTP_PORT", 1025)
    monkeypatch.setattr(alerting.settings, "SMTP_STARTTLS", False)
    monkeypatch.setattr(alerting.settings, "SMTP_USER", "")
    with patch("app.services.alerting.smtplib.SMTP") as smtp_cls:
        server = smtp_cls.return_value.__enter__.return_value
        alerting.send_email(alerting.render_email(_alertas_df(), to="x@y.z"))
        smtp_cls.assert_called_once_with("localhost", 1025, timeout=30)
        server.send_message.assert_called_once()
        server.starttls.assert_not_called()  # STARTTLS deshabilitado
        server.login.assert_not_called()     # sin credenciales -> sin login


def test_send_alerts_dry_run_no_envia(monkeypatch, capsys):
    monkeypatch.setattr(alerting, "build_alertas", lambda **kw: _alertas_df())
    with patch("app.services.alerting.smtplib.SMTP") as smtp_cls:
        n = alerting.send_alerts(source="all", to="x@y.z", dry_run=True)
        assert n == 1
        smtp_cls.assert_not_called()
    out = capsys.readouterr().out
    assert "INSULINA HUMANA" in out


def test_send_alerts_filtra_por_source(monkeypatch):
    monkeypatch.setattr(alerting, "build_alertas", lambda **kw: _alertas_df())
    # El df solo trae HHI_CRITICO -> con source=leadtime no hay nada que enviar.
    n = alerting.send_alerts(source="leadtime", to="x@y.z", dry_run=True)
    assert n == 0


def test_send_alerts_requiere_destinatario(monkeypatch):
    monkeypatch.setattr(alerting, "build_alertas", lambda **kw: _alertas_df())
    monkeypatch.setattr(alerting.settings, "SMTP_TO", "")
    with pytest.raises(ValueError, match="Destinatario"):
        alerting.send_alerts(source="all", to=None, dry_run=True)


def test_build_alertas_schema(monkeypatch, tmp_path):
    # Consolidado con ambas fuentes vacías -> DataFrame vacío con el esquema contrato.
    monkeypatch.setattr(alerting, "build_hhi_alerts", lambda **kw: pd.DataFrame())
    monkeypatch.setattr(alerting, "build_leadtime_alerts", lambda **kw: pd.DataFrame())
    out = alerting.build_alertas(bi_dir=tmp_path, save=True)
    assert list(out.columns) == alerting.ALERTAS_COLS
    assert (tmp_path / alerting.ALERTAS_FILENAME).exists()


def test_cli_alert_handler(monkeypatch):
    # El subcomando alert debe delegar en send_alerts con los args del parser.
    import main as cli
    called = {}

    def fake_send_alerts(**kw):
        called.update(kw)
        return 3

    monkeypatch.setattr("app.services.alerting.send_alerts", fake_send_alerts)
    parser = cli.build_parser()
    args = parser.parse_args(["alert", "--source", "hhi", "--dry-run", "--to", "a@b.c"])
    args.func(args)
    assert called["source"] == "hhi"
    assert called["dry_run"] is True
    assert called["to"] == "a@b.c"


def test_render_email_trunca_al_limite():
    df = pd.concat([_alertas_df()] * 30, ignore_index=True)
    msg = alerting.render_email(df, to="x@y.z", limit=5)
    plain = msg.get_payload(0).get_payload(decode=True).decode("utf-8")
    assert "25 alertas adicionales" in plain
