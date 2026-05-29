FROM apache/airflow:2.9.1

# Spark (PySpark) requiere un JRE. La imagen base de Airflow (Debian) no lo trae.
# default-jre-headless instala OpenJDK 17, compatible con PySpark 3.5/4.x.
USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends default-jre-headless procps \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java
USER airflow

COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt
