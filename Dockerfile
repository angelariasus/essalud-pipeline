FROM apache/airflow:2.9.1

# Spark (PySpark) requiere un JRE. La imagen base de Airflow (Debian) no lo trae.
# default-jre-headless instala OpenJDK 17, compatible con PySpark 3.5/4.x.
# msodbcsql18 + unixodbc: driver ODBC de Microsoft para pyodbc (sin él, cualquier
# `gold --target sqlserver` dentro del contenedor falla con "Can't open lib").
# mssql-tools18 aporta sqlcmd para diagnóstico manual.
USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      default-jre-headless procps curl gnupg2 apt-transport-https \
 && curl -fsSL https://packages.microsoft.com/keys/microsoft.asc \
      | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
 && echo "deb [signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" \
      > /etc/apt/sources.list.d/mssql-release.list \
 && apt-get update \
 && ACCEPT_EULA=Y apt-get install -y --no-install-recommends \
      msodbcsql18 unixodbc mssql-tools18 \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="$PATH:/opt/mssql-tools18/bin"
USER airflow

COPY requirements.txt /
# pandas>=2.2 explícito: la imagen base de Airflow 2.9.1 fija pandas 2.1.4, pero
# PySpark 4.x exige >=2.2 (falla con UNSUPPORTED_PACKAGE_VERSION al usar Arrow).
# Genera un warning inocuo con apache-airflow-providers-google (no se usa).
RUN pip install --no-cache-dir -r /requirements.txt "pandas>=2.2,<2.3"
