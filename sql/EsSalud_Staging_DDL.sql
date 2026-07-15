/* ============================================================
   CAPA STAGING + CARGA ATÓMICA  |  MOTOR: SQL Server 2019+
   Proyecto BI EsSalud — soporte para la ingesta distribuida con Spark.

   Flujo (ver app/loaders/dw_loader.py):
     1. Spark escribe los DataFrames de dimensiones/Fact por JDBC en el esquema
        [stg] (tablas creadas/sobrescritas automáticamente por Spark).
     2. Este script crea el esquema [stg] y el stored procedure de carga.
     3. Tras una escritura exitosa de TODAS las staging, se ejecuta
        oro.usp_Load_From_Staging, que mueve los datos a producción dentro de
        UNA transacción (atómica). Si Spark falla a mitad, producción no se toca;
        si el procedure falla, hace ROLLBACK y producción queda intacta.

   Las SK llegan resueltas desde Spark (deterministas) => SET IDENTITY_INSERT.
   ============================================================ */
USE DW_EsSalud_Adquisiciones;
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'stg')
    EXEC('CREATE SCHEMA stg');
GO

/* Limpiar tablas stg.* del ciclo anterior para que el SP se compile
   con resolución diferida (deferred name resolution). Si las tablas
   stg.* existen con el schema viejo, SQL Server las valida al hacer
   CREATE OR ALTER PROCEDURE y falla cuando los nombres de columna
   cambiaron. Spark las recrea en el paso siguiente (write_all_staging). */
DROP TABLE IF EXISTS stg.Fact_Ordenes_Y_Contratos;
DROP TABLE IF EXISTS stg.Dim_Entidad_Compradora;
DROP TABLE IF EXISTS stg.Dim_Medicamento;
DROP TABLE IF EXISTS stg.Dim_Proveedor;
GO

/* ------------------------------------------------------------
   Carga atómica de Staging -> Producción.
   Resolución diferida de nombres: las tablas stg.* las crea Spark
   en tiempo de ejecución, antes de invocar este procedure.
   ------------------------------------------------------------ */
CREATE OR ALTER PROCEDURE oro.usp_Load_From_Staging AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    BEGIN TRY
        BEGIN TRANSACTION;

        -- ── Dim_Entidad_Compradora ──
        SET IDENTITY_INSERT oro.Dim_Entidad_Compradora ON;
        INSERT INTO oro.Dim_Entidad_Compradora
            (SK_Entidad, FK_Ubigeo, Cod_EESS, RUC_Entidad, Red_Asistencial,
             Tipo_Red, Nombre_Centro, Estado_Operativo)
        SELECT SK_Entidad, FK_Ubigeo, Cod_EESS, RUC_Entidad, Red_Asistencial,
               Tipo_Red, Nombre_Centro, Estado_Operativo
        FROM stg.Dim_Entidad_Compradora;
        SET IDENTITY_INSERT oro.Dim_Entidad_Compradora OFF;

        -- ── Dim_Medicamento ──
        SET IDENTITY_INSERT oro.Dim_Medicamento ON;
        INSERT INTO oro.Dim_Medicamento
            (SK_Medicamento, N_Item_Petitorio, Codigo_SAP, Denominacion_DCI,
             Especificaciones_Tecnicas, Unidad_De_Manejo, Restriccion_Uso,
             Especialidad_Autorizada, Indicaciones_Observaciones, En_Petitorio_2026,
             Descripcion_SEACE_Original, Score_Fuzzy_Match, Metodo_Clasificacion)
        SELECT SK_Medicamento, N_Item_Petitorio, Codigo_SAP, Denominacion_DCI,
               Especificaciones_Tecnicas, Unidad_De_Manejo, Restriccion_Uso,
               Especialidad_Autorizada, Indicaciones_Observaciones, En_Petitorio_2026,
               Descripcion_SEACE_Original, Score_Fuzzy_Match, Metodo_Clasificacion
        FROM stg.Dim_Medicamento;
        SET IDENTITY_INSERT oro.Dim_Medicamento OFF;

        -- ── Dim_Proveedor ──
        SET IDENTITY_INSERT oro.Dim_Proveedor ON;
        INSERT INTO oro.Dim_Proveedor
            (SK_Proveedor, RUC_Proveedor, Nombre_Proveedor, Tipo_Proveedor, Es_Consorcio)
        SELECT SK_Proveedor, RUC_Proveedor, Nombre_Proveedor, Tipo_Proveedor, Es_Consorcio
        FROM stg.Dim_Proveedor;
        SET IDENTITY_INSERT oro.Dim_Proveedor OFF;

        -- ── Fact_Ordenes_Y_Contratos (SK_Hecho es IDENTITY automática) ──
        INSERT INTO oro.Fact_Ordenes_Y_Contratos
            (FK_Tiempo_Convocatoria, FK_Tiempo_Buena_Pro, FK_Tiempo_Suscripcion,
             FK_Entidad, FK_Medicamento, FK_Proveedor,
             FK_Tipo_Proceso, FK_Ubigeo_Item, Fecha_Convocatoria, Fecha_Buena_Pro,
             Fecha_Suscripcion,
             Codigo_Convocatoria, N_Item, N_Cod_Contrato,
             Num_Contrato,
             Cantidad_Adjudicada,
             Monto_Referencial_Soles, Monto_Adjudicado_Soles, Monto_Contratado_Item,
             Monto_Adicional, Monto_Reduccion, Monto_Prorroga, Monto_Complementario,
             Flag_Contratacion_Directa,
             Flag_Fuera_Petitorio, Moneda_Original, Fuente_Dataset, Anio_Fiscal)
        SELECT FK_Tiempo_Convocatoria, FK_Tiempo_Buena_Pro, FK_Tiempo_Suscripcion,
               FK_Entidad, FK_Medicamento, FK_Proveedor,
               FK_Tipo_Proceso, FK_Ubigeo_Item, Fecha_Convocatoria, Fecha_Buena_Pro,
               Fecha_Suscripcion,
               Codigo_Convocatoria, N_Item, N_Cod_Contrato,
               Num_Contrato,
               Cantidad_Adjudicada,
               Monto_Referencial_Soles, Monto_Adjudicado_Soles, Monto_Contratado_Item,
               Monto_Adicional, Monto_Reduccion, Monto_Prorroga, Monto_Complementario,
               Flag_Contratacion_Directa,
               Flag_Fuera_Petitorio, Moneda_Original, Fuente_Dataset, Anio_Fiscal
        FROM stg.Fact_Ordenes_Y_Contratos;

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        THROW;  -- propaga el error a PyODBC para abortar el pipeline
    END CATCH
END;
GO
