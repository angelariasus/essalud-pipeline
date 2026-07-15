# Guía Power BI Service + Power Automate (Alertas Operativas)

Camino **sin código** para que el dashboard dispare el correo de alerta desde la
cuenta corporativa. Es el equivalente institucional del prototipo Python ya
operativo (`python app/cli.py alert`); ambos consumen las mismas fuentes de `data/mart/`.

> **Requisitos**: licencia **Power BI Pro o PPU** en la cuenta institucional,
> permisos para crear flujos en **Power Automate** y el conector "Office 365
> Outlook" habilitado por el tenant. Sin esto, usar el prototipo Python.

---

## 1. Construir el `.pbix` (si aún no existe)

1. Power BI Desktop → **Obtener datos → Parquet** e importar, desde `<repo>/data/mart/`:
   - `Fact_Ordenes_Y_Contratos.parquet`, las 6 `Dim_*.parquet`
   - `Pred_Lead_Time.parquet` (Modelo predictivo — ver medidas DAX en `modelo-predictivo.md`)
   - **`Alertas.parquet`** ← fuente de la Vista Operativa (la genera
     `python app/cli.py alert` o el DAG `ocds_alerting`)
2. Vista Modelo → relaciones:
   - `Fact[FK_*]` ↔ `Dim_*[SK_*]` (muchos a uno)
   - `Alertas[Red_Asistencial]` ↔ `Dim_Entidad_Compradora[Red_Asistencial]`
     (muchos a uno, bidireccional)

## 2. Vista Operativa (página nueva)

| Visual | Configuración |
|---|---|
| **Tabla de alertas** | Columnas de `Alertas`: `Tipo_Alerta`, `Anio`, `Medicamento`, `Red_Asistencial`, `RUC_Proveedor`, `Nombre_Proveedor`, `Valor`, `Detalle` |
| Formato condicional | Fondo rojo si `Tipo_Alerta = "HHI_CRITICO"`, ámbar si `LEAD_TIME_ANOMALO` |
| Segmentadores | `Anio`, `Red_Asistencial`, `Tipo_Alerta` |
| Tarjetas KPI | `COUNTROWS(Alertas)`, `CALCULATE(COUNTROWS(Alertas), Alertas[Tipo_Alerta]="HHI_CRITICO")` |

## 3. Publicar en Power BI Service

1. Power BI Desktop → **Publicar** → área de trabajo institucional.
2. (Opcional) Configurar **actualización programada** del dataset apuntando a un
   gateway con acceso a la carpeta `data/mart/` (o republicar tras cada corrida del pipeline).

## 4. Flujo Power Automate (botón en el informe)

1. En el informe (Service o Desktop) → **Insertar → Power Automate (versión preliminar)**.
2. Arrastrar al visual los **campos de datos** que el flujo capturará de la fila
   seleccionada: `RUC_Proveedor`, `Medicamento`, `Red_Asistencial`, `Tipo_Alerta`,
   `Valor`, `Detalle`.
3. En el visual → **Editar** → **Nuevo flujo** con plantilla
   *"Ejecutar un flujo cuando se hace clic en un botón de Power BI"*. El trigger
   **Power BI button clicked** expone los campos arrastrados en `Power BI data`.
4. Añadir **Condición**: `Tipo_Alerta` *es igual a* `HHI_CRITICO`
   **O** `Tipo_Alerta` *es igual a* `LEAD_TIME_ANOMALO` (rama "Sí" continúa;
   rama "No" → acción *Terminar* con estado `Cancelled`).
5. Rama "Sí" → acción **Enviar un correo electrónico (V2)** (Office 365 Outlook):
   - **Para**: correo del área de abastecimiento (prueba: `fernando.barrera@unmsm.edu.pe`)
   - **Asunto**: `[ALERTA EsSalud] @{triggerBody()?['entity']?['Power BI values'][0]?['Tipo_Alerta']} — abastecimiento en riesgo`
   - **Cuerpo** (usar los campos dinámicos de `Power BI data`):

     ```text
     Estimados, Área de Abastecimiento:

     Se ha detectado una alerta activa en las adquisiciones de medicamentos:

       • Tipo de alerta : {Tipo_Alerta}
       • Medicamento    : {Medicamento}
       • Red Asistencial: {Red_Asistencial}
       • Proveedor (RUC): {RUC_Proveedor}
       • Valor / umbral : {Valor} — {Detalle}

     Se solicita evaluar acciones de diversificación de proveedores y/o
     seguimiento del proceso según corresponda.

     Atentamente,
     Sistema de Monitoreo BI — EsSalud (Power BI + Power Automate)
     ```
6. **Guardar y aplicar**. Renombrar el botón del visual a **"Notificar Abastecimiento"**.

## 5. Prueba de aceptación

1. En la Vista Operativa, filtrar `Tipo_Alerta = HHI_CRITICO` y **seleccionar una fila**
   (la selección es lo que alimenta `Power BI data` del flujo).
2. Clic en **Notificar Abastecimiento**.
3. Verificar en Power Automate → **Historial de ejecuciones** que el flujo corrió
   en verde, y que el correo llegó con **RUC, medicamento y Red** correctos.

## Notas

- El visual Power Automate envía **las filas seleccionadas** (o todas las del
  contexto de filtro si no hay selección) — instruir al usuario a seleccionar
  la fila de la alerta antes de pulsar el botón.
- El paso 4 (condición) evita notificaciones accidentales desde filas sin alerta
  real si el visual recibiera el dataset completo.
- Alternativa ya operativa sin licencias: `python app/cli.py alert --to <correo>`
  (SMTP Gmail/MailHog) o el DAG `ocds_alerting` de Airflow, que corre
  automáticamente tras cada carga Gold.
