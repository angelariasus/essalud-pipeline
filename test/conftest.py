import os
import shutil
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def _spark_available() -> bool:
    """True si pyspark está instalado y hay un Java disponible (JAVA_HOME o en PATH)."""
    try:
        import pyspark  # noqa: F401
    except Exception:
        return False
    return bool(os.environ.get("JAVA_HOME") or shutil.which("java"))


SPARK_AVAILABLE = _spark_available()

# Marcador para tests que requieren Spark (se omiten si falta pyspark/Java).
requires_spark = pytest.mark.skipif(
    not SPARK_AVAILABLE, reason="pyspark/Java no disponible"
)


@pytest.fixture(scope="session")
def spark():
    """SparkSession compartida para toda la sesión de tests."""
    from app.config.spark_session import get_spark_session, stop_spark_session

    session = get_spark_session("pytest_essalud")
    yield session
    stop_spark_session()
