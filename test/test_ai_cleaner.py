"""Tests del GeminiCleaner (sin red): gating por API y fail-fast."""
from app.services import ai_cleaner as ac


def test_sin_api_key_devuelve_identidad(monkeypatch, tmp_path):
    """Sin API key, el cleaner no se configura y devuelve identidad."""
    monkeypatch.setattr(ac.settings, "GEMINI_API_KEY", "")
    monkeypatch.setattr(ac.settings, "EXTRA_DATA_DIR", tmp_path)
    cleaner = ac.GeminiCleaner()
    assert cleaner.is_configured is False
    out = cleaner.clean_descriptions(["PARACETAMOL", "IBUPROFENO"])
    assert out == {"PARACETAMOL": "PARACETAMOL", "IBUPROFENO": "IBUPROFENO"}


def test_failfast_si_api_falla(monkeypatch, tmp_path):
    """Con API key pero fallo (p.ej. 429), aborta la limpieza tras el 1er lote."""
    monkeypatch.setattr(ac.settings, "GEMINI_API_KEY", "fake-key")
    monkeypatch.setattr(ac.settings, "EXTRA_DATA_DIR", tmp_path)
    monkeypatch.setattr(ac.genai, "Client", lambda api_key: object())

    cleaner = ac.GeminiCleaner()
    assert cleaner.is_configured is True

    calls = {"n": 0}

    class _Models:
        def generate_content(self, **kwargs):
            calls["n"] += 1
            raise RuntimeError("429 RESOURCE_EXHAUSTED")

    cleaner.client = type("_C", (), {"models": _Models()})()

    items = [f"ITEM_{i}" for i in range(120)]  # 3 lotes de 50
    out = cleaner.clean_descriptions(items)

    # Solo se intentó UN lote (fail-fast); no se molieron los 3.
    assert calls["n"] == 1
    # Todos los ítems quedan como identidad (sin limpiar).
    assert all(out[it] == it for it in items)
