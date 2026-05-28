/* ============================================================
   PROYECTO: Arquitectura BI - EsSalud Adquisiciones 2022-2025
   ASIGNATURA: Inteligencia de Negocios - FISI UNMSM
   CAPA: ORO  |  MOTOR: SQL Server 2019+

   FUENTES DEL DICCIONARIO:
     [1] Adjudicacion         [2] ContratacionDirecta
     [3] Contratos            [4] OrdenesServicios
   MAESTROS:
     [5] Petitorio 2026       [6] Establecimientos Jul 2025
     [7] INEI Ubigeo

   CORRECCIONES v3:
   - Lead Time sobre columnas DATE reales (determinístico → PERSISTED OK)
   - FK_Ubigeo_Entidad eliminada de la Fact (se accede por FK_Entidad
     → Dim_Entidad_Compradora.FK_Ubigeo, evitando FK redundante)
   ============================================================ */

USE master;
GO
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = N'DW_EsSalud_Adquisiciones')
    CREATE DATABASE DW_EsSalud_Adquisiciones COLLATE Modern_Spanish_CI_AI;
GO
USE DW_EsSalud_Adquisiciones;
GO
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'oro')
    EXEC('CREATE SCHEMA oro');
GO

/* ============================================================
   LIMPIEZA PREVIA (idempotencia en recargas):
   Se eliminan todas las tablas en ORDEN INVERSO de dependencias
   (Fact → Dim_Entidad → resto) para liberar las FOREIGN KEYs; de lo
   contrario, DROP TABLE falla con error 3726 cuando el DW ya contiene
   datos de una carga previa.
   ============================================================ */
DROP TABLE IF EXISTS oro.Fact_Ordenes_Y_Contratos;
GO
DROP TABLE IF EXISTS oro.Dim_Entidad_Compradora;
GO
DROP TABLE IF EXISTS oro.Dim_Tipo_Proceso;
GO
DROP TABLE IF EXISTS oro.Dim_Proveedor;
GO
DROP TABLE IF EXISTS oro.Dim_Medicamento;
GO
DROP TABLE IF EXISTS oro.Dim_Ubigeo;
GO
DROP TABLE IF EXISTS oro.Dim_Tiempo;
GO

/* ============================================================
   ORDEN DE CREACIÓN (respetar por dependencias FK):
   1. Dim_Tiempo
   2. Dim_Ubigeo
   3. Dim_Entidad_Compradora  (→ Dim_Ubigeo)
   4. Dim_Medicamento
   5. Dim_Proveedor
   6. Dim_Tipo_Proceso
   7. Fact_Ordenes_Y_Contratos (→ todas las dims anteriores)
   ============================================================ */

-- ============================================================
-- 1. DIM_TIEMPO
-- SK = YYYYMMDD (entero). Permite hacer la FK directamente
-- desde Python: int(fecha.strftime('%Y%m%d'))
-- ============================================================
DROP TABLE IF EXISTS oro.Dim_Tiempo;
GO
CREATE TABLE oro.Dim_Tiempo (
    SK_Tiempo        INT          NOT NULL,   -- PK = YYYYMMDD
    Fecha_Completa   DATE         NOT NULL,
    Anio             SMALLINT     NOT NULL,
    Trimestre        TINYINT      NOT NULL,
    Nombre_Trimestre CHAR(2)      NOT NULL,
    Mes              TINYINT      NOT NULL,
    Nombre_Mes       VARCHAR(15)  NOT NULL,
    Semana_Anio      TINYINT      NOT NULL,
    Dia_Del_Mes      TINYINT      NOT NULL,
    Dia_Semana       TINYINT      NOT NULL,   -- 1=Lun … 7=Dom
    Nombre_Dia       VARCHAR(12)  NOT NULL,
    Es_Fin_Semana    BIT          NOT NULL DEFAULT 0,
    CONSTRAINT PK_Dim_Tiempo PRIMARY KEY CLUSTERED (SK_Tiempo)
);
GO

-- Población 2022-2025 (CTE recursivo, más rápido que WHILE)
WITH Fechas AS (
    SELECT CAST('2022-01-01' AS DATE) AS d
    UNION ALL
    SELECT DATEADD(DAY, 1, d)
    FROM Fechas
    WHERE d < '2025-12-31'
)
INSERT INTO oro.Dim_Tiempo
SELECT
    CAST(FORMAT(d,'yyyyMMdd') AS INT),
    d,
    YEAR(d),
    DATEPART(QUARTER,d),
    'Q' + CAST(DATEPART(QUARTER,d) AS CHAR(1)),
    MONTH(d),
    DATENAME(MONTH,d),
    DATEPART(ISO_WEEK,d),
    DAY(d),
    CASE DATEPART(WEEKDAY,d) WHEN 1 THEN 7 ELSE DATEPART(WEEKDAY,d)-1 END,
    DATENAME(WEEKDAY,d),
    CASE WHEN DATEPART(WEEKDAY,d) IN (1,7) THEN 1 ELSE 0 END
FROM Fechas
OPTION (MAXRECURSION 0);
GO

-- Registro centinela para fechas nulas / 1900-01-01 del SEACE
INSERT INTO oro.Dim_Tiempo
VALUES (-1,'1900-01-01',1900,1,'Q1',1,'Enero',1,1,1,'Lunes',0);
GO


-- ============================================================
-- 2. DIM_UBIGEO
-- Jerarquía: País → Departamento → Provincia → Distrito
-- Fuente [7] INEI + [6] Establecimientos EsSalud Jul 2025
--
-- Nivel_Geo = 'DEPARTAMENTO':
--   Solo se conoce el dpto. Usado en FK_Ubigeo_Item de la Fact
--   (Adjudicacion.DEPARTAMENTO_ITEM solo trae el departamento)
--
-- Nivel_Geo = 'DISTRITO':
--   Nivel completo. Cargado desde el archivo de Establecimientos.
--   Usado en Dim_Entidad_Compradora.FK_Ubigeo
-- ============================================================
DROP TABLE IF EXISTS oro.Dim_Ubigeo;
GO
CREATE TABLE oro.Dim_Ubigeo (
    SK_Ubigeo      INT          NOT NULL IDENTITY(1,1),
    Ubigeo         CHAR(6)      NULL,         -- código INEI 6 dígitos (NULL si solo dpto.)
    Departamento   VARCHAR(50)  NOT NULL,
    Provincia      VARCHAR(80)  NULL,          -- NULL si Nivel_Geo = 'DEPARTAMENTO'
    Distrito       VARCHAR(80)  NULL,          -- NULL si Nivel_Geo = 'DEPARTAMENTO'
    Region_Natural VARCHAR(20)  NOT NULL,      -- COSTA / SIERRA / SELVA
    Macroregion    VARCHAR(30)  NOT NULL,      -- NORTE / CENTRO / SUR / ORIENTE
    Nivel_Geo      VARCHAR(15)  NOT NULL,      -- 'DEPARTAMENTO' / 'DISTRITO'
    CONSTRAINT PK_Dim_Ubigeo PRIMARY KEY CLUSTERED (SK_Ubigeo)
);
GO
CREATE NONCLUSTERED INDEX IX_Ubigeo_Dpto
    ON oro.Dim_Ubigeo (Departamento) INCLUDE (Provincia, Distrito, Region_Natural);
CREATE NONCLUSTERED INDEX IX_Ubigeo_Codigo
    ON oro.Dim_Ubigeo (Ubigeo);

-- Registro centinela
SET IDENTITY_INSERT oro.Dim_Ubigeo ON;
INSERT INTO oro.Dim_Ubigeo
    (SK_Ubigeo,Ubigeo,Departamento,Provincia,Distrito,Region_Natural,Macroregion,Nivel_Geo)
VALUES (-1,NULL,'SIN UBICACION',NULL,NULL,'SIN DATO','SIN DATO','DEPARTAMENTO');
SET IDENTITY_INSERT oro.Dim_Ubigeo OFF;
GO

-- 25 departamentos precargados (nivel DEPARTAMENTO → para FK_Ubigeo_Item)
INSERT INTO oro.Dim_Ubigeo
    (Ubigeo,Departamento,Region_Natural,Macroregion,Nivel_Geo)
VALUES
    ('010000','AMAZONAS',     'SELVA',  'NORTE',   'DEPARTAMENTO'),
    ('020000','ANCASH',       'SIERRA', 'CENTRO',  'DEPARTAMENTO'),
    ('030000','APURIMAC',     'SIERRA', 'SUR',     'DEPARTAMENTO'),
    ('040000','AREQUIPA',     'SIERRA', 'SUR',     'DEPARTAMENTO'),
    ('050000','AYACUCHO',     'SIERRA', 'SUR',     'DEPARTAMENTO'),
    ('060000','CAJAMARCA',    'SIERRA', 'NORTE',   'DEPARTAMENTO'),
    ('070000','CALLAO',       'COSTA',  'CENTRO',  'DEPARTAMENTO'),
    ('080000','CUSCO',        'SIERRA', 'SUR',     'DEPARTAMENTO'),
    ('090000','HUANCAVELICA', 'SIERRA', 'CENTRO',  'DEPARTAMENTO'),
    ('100000','HUANUCO',      'SIERRA', 'CENTRO',  'DEPARTAMENTO'),
    ('110000','ICA',          'COSTA',  'SUR',     'DEPARTAMENTO'),
    ('120000','JUNIN',        'SIERRA', 'CENTRO',  'DEPARTAMENTO'),
    ('130000','LA LIBERTAD',  'COSTA',  'NORTE',   'DEPARTAMENTO'),
    ('140000','LAMBAYEQUE',   'COSTA',  'NORTE',   'DEPARTAMENTO'),
    ('150000','LIMA',         'COSTA',  'CENTRO',  'DEPARTAMENTO'),
    ('160000','LORETO',       'SELVA',  'ORIENTE', 'DEPARTAMENTO'),
    ('170000','MADRE DE DIOS','SELVA',  'ORIENTE', 'DEPARTAMENTO'),
    ('180000','MOQUEGUA',     'SIERRA', 'SUR',     'DEPARTAMENTO'),
    ('190000','PASCO',        'SIERRA', 'CENTRO',  'DEPARTAMENTO'),
    ('200000','PIURA',        'COSTA',  'NORTE',   'DEPARTAMENTO'),
    ('210000','PUNO',         'SIERRA', 'SUR',     'DEPARTAMENTO'),
    ('220000','SAN MARTIN',   'SELVA',  'NORTE',   'DEPARTAMENTO'),
    ('230000','TACNA',        'SIERRA', 'SUR',     'DEPARTAMENTO'),
    ('240000','TUMBES',       'COSTA',  'NORTE',   'DEPARTAMENTO'),
    ('250000','UCAYALI',      'SELVA',  'ORIENTE', 'DEPARTAMENTO');
-- Los registros nivel DISTRITO se cargan en Capa Plata desde [6]
GO


-- ============================================================
-- 3. DIM_ENTIDAD_COMPRADORA
-- Fuente [6] Establecimientos EsSalud Jul 2025
-- Cruce con SEACE: ENTIDAD_RUC [1][2] / RUC_ENTIDAD [4] = RUC_Entidad
-- ============================================================
DROP TABLE IF EXISTS oro.Dim_Entidad_Compradora;
GO
CREATE TABLE oro.Dim_Entidad_Compradora (
    SK_Entidad       INT          NOT NULL IDENTITY(1,1),
    FK_Ubigeo        INT          NOT NULL DEFAULT -1,  -- → Dim_Ubigeo (nivel DISTRITO)
    Cod_EESS         VARCHAR(10)  NOT NULL,
    Cod_SES          VARCHAR(10)  NULL,
    Cod_RENAES       VARCHAR(15)  NULL,
    RUC_Entidad      VARCHAR(11)  NULL,      -- clave de cruce con datasets SEACE
    Red_Asistencial  VARCHAR(100) NOT NULL,
    Tipo_Red         VARCHAR(10)  NULL,
    Nombre_Centro    VARCHAR(200) NOT NULL,
    Tipo_Estab       VARCHAR(20)  NULL,      -- H.N., H.III, CAP.III, P.M. …
    Categoria_MINSA  VARCHAR(80)  NULL,      -- III-E, II-2, I-4 …
    Estado_Operativo VARCHAR(30)  NOT NULL DEFAULT 'FUNCIONA',
    Fecha_Carga      DATETIME2    NOT NULL DEFAULT GETDATE(),
    CONSTRAINT PK_Dim_Entidad PRIMARY KEY CLUSTERED (SK_Entidad),
    CONSTRAINT UQ_Entidad_CodEESS UNIQUE (Cod_EESS),
    CONSTRAINT FK_Entidad_Ubigeo
        FOREIGN KEY (FK_Ubigeo) REFERENCES oro.Dim_Ubigeo (SK_Ubigeo)
);
GO
CREATE NONCLUSTERED INDEX IX_Entidad_RUC
    ON oro.Dim_Entidad_Compradora (RUC_Entidad)
    INCLUDE (Red_Asistencial, FK_Ubigeo);

SET IDENTITY_INSERT oro.Dim_Entidad_Compradora ON;
INSERT INTO oro.Dim_Entidad_Compradora
    (SK_Entidad,FK_Ubigeo,Cod_EESS,Nombre_Centro,Red_Asistencial,Estado_Operativo)
VALUES (-1,-1,'UNKNOWN','Sin Clasificar','Sin Red','DESCONOCIDO');
SET IDENTITY_INSERT oro.Dim_Entidad_Compradora OFF;
GO


-- ============================================================
-- 4. DIM_MEDICAMENTO
-- Fuente [5] Petitorio Farmacológico EsSalud (Res. 063-2026)
-- ============================================================
DROP TABLE IF EXISTS oro.Dim_Medicamento;
GO
CREATE TABLE oro.Dim_Medicamento (
    SK_Medicamento            INT           NOT NULL IDENTITY(1,1),
    N_Item_Petitorio          SMALLINT      NULL,
    Codigo_SAP                VARCHAR(15)   NULL,
    Denominacion_DCI          VARCHAR(300)  NOT NULL,
    Especificaciones_Tecnicas VARCHAR(500)  NULL,
    Unidad_De_Manejo          VARCHAR(20)   NULL,
    Restriccion_Uso           VARCHAR(50)   NULL,
    Especialidad_Autorizada   VARCHAR(300)  NULL,
    Indicaciones_Observaciones VARCHAR(1000) NULL,
    En_Petitorio_2026         BIT           NOT NULL DEFAULT 1,
    Es_Uso_Critico            BIT           NOT NULL DEFAULT 0,
    Es_Uso_Intrahospitalario  BIT           NOT NULL DEFAULT 0,
    -- Trazabilidad del fuzzy matching (viene de Capa Plata)
    Descripcion_SEACE_Original VARCHAR(600) NULL,
    Score_Fuzzy_Match         DECIMAL(5,2)  NULL,
    Metodo_Clasificacion      VARCHAR(20)   NULL,
        -- 'EXACTO','FUZZY','DUDOSO','HISTORICO','NO_FARMA','PAQUETE','GENERICO'
    Fecha_Carga               DATETIME2     NOT NULL DEFAULT GETDATE(),
    CONSTRAINT PK_Dim_Medicamento PRIMARY KEY CLUSTERED (SK_Medicamento)
);
GO
CREATE NONCLUSTERED INDEX IX_Med_SAP
    ON oro.Dim_Medicamento (Codigo_SAP) INCLUDE (Denominacion_DCI);
CREATE NONCLUSTERED INDEX IX_Med_Flags
    ON oro.Dim_Medicamento (Es_Uso_Critico, En_Petitorio_2026);

SET IDENTITY_INSERT oro.Dim_Medicamento ON;
INSERT INTO oro.Dim_Medicamento
    (SK_Medicamento,Denominacion_DCI,En_Petitorio_2026,Metodo_Clasificacion)
VALUES
    (-1,'Sin Clasificar / No Farmacéutico',         0,'NO_FARMA'),
    (-2,'Histórico / No Vigente en Petitorio 2026', 0,'HISTORICO'),
    (-3,'Ítem Paquete - No Clasificable',            0,'PAQUETE'),
    (-4,'Descripción Genérica de Proceso',           0,'GENERICO');
SET IDENTITY_INSERT oro.Dim_Medicamento OFF;
GO


-- ============================================================
-- 5. DIM_PROVEEDOR
-- Fuente [1][2]: RUC_PROVEEDOR + PROVEEDOR + TIPO_PROVEEDOR
--        [3][4]: RUC_CONTRATISTA + NOMBRE_RAZON_CONTRATISTA
-- ============================================================
DROP TABLE IF EXISTS oro.Dim_Proveedor;
GO
CREATE TABLE oro.Dim_Proveedor (
    SK_Proveedor     INT          NOT NULL IDENTITY(1,1),
    RUC_Proveedor    VARCHAR(11)  NOT NULL,
    Nombre_Proveedor VARCHAR(250) NOT NULL,
    Tipo_Proveedor   VARCHAR(50)  NULL,
        -- Persona Natural / Persona Jurídica / No domiciliada / Consorcio
    Es_Consorcio     BIT          NOT NULL DEFAULT 0,
    Fecha_Carga      DATETIME2    NOT NULL DEFAULT GETDATE(),
    CONSTRAINT PK_Dim_Proveedor PRIMARY KEY CLUSTERED (SK_Proveedor),
    CONSTRAINT UQ_Proveedor_RUC UNIQUE (RUC_Proveedor)
);
GO
SET IDENTITY_INSERT oro.Dim_Proveedor ON;
INSERT INTO oro.Dim_Proveedor (SK_Proveedor,RUC_Proveedor,Nombre_Proveedor)
VALUES (-1,'00000000000','Proveedor Desconocido');
SET IDENTITY_INSERT oro.Dim_Proveedor OFF;
GO


-- ============================================================
-- 6. DIM_TIPO_PROCESO
-- Fuente [1]: Adjudicacion.PROCESO (texto)
--        [2]: ContratacionDirecta.TIPOPROCESOSELECCION + CAUSAL
-- ============================================================
DROP TABLE IF EXISTS oro.Dim_Tipo_Proceso;
GO
CREATE TABLE oro.Dim_Tipo_Proceso (
    SK_Tipo_Proceso         INT          NOT NULL IDENTITY(1,1),
    Tipo_Proceso_Seleccion  VARCHAR(100) NOT NULL,
    Objeto_Contractual      VARCHAR(50)  NOT NULL,   -- BIEN / SERVICIO
    Es_Contratacion_Directa BIT          NOT NULL DEFAULT 0,
    Causal_CD               VARCHAR(300) NULL,        -- solo si Es_CD=1, de [2].CAUSAL
    Categoria_Proceso       VARCHAR(30)  NOT NULL,
        -- 'COMPETITIVO' / 'DIRECTO' / 'CATALOGO' / 'REGIMEN_ESPECIAL'
    Fecha_Carga             DATETIME2    NOT NULL DEFAULT GETDATE(),
    CONSTRAINT PK_Dim_Tipo_Proceso PRIMARY KEY CLUSTERED (SK_Tipo_Proceso)
);
GO
INSERT INTO oro.Dim_Tipo_Proceso
    (Tipo_Proceso_Seleccion,Objeto_Contractual,Es_Contratacion_Directa,Categoria_Proceso)
VALUES
    ('LICITACION PUBLICA',                   'BIEN',     0,'COMPETITIVO'),
    ('CONCURSO PUBLICO',                     'SERVICIO', 0,'COMPETITIVO'),
    ('ADJUDICACION SIMPLIFICADA',            'BIEN',     0,'COMPETITIVO'),
    ('ADJUDICACION SIMPLIFICADA',            'SERVICIO', 0,'COMPETITIVO'),
    ('SUBASTA INVERSA ELECTRONICA',          'BIEN',     0,'CATALOGO'),
    ('CONVENIO MARCO',                       'BIEN',     0,'CATALOGO'),
    ('COMPARACION DE PRECIOS',               'BIEN',     0,'COMPETITIVO'),
    ('CONTRATACION DIRECTA',                 'BIEN',     1,'DIRECTO'),
    ('CONTRATACION DIRECTA',                 'SERVICIO', 1,'DIRECTO'),
    ('SELECCION DE CONSULTORES INDIVIDUALES','SERVICIO', 0,'COMPETITIVO'),
    ('REGIMEN ESPECIAL',                     'BIEN',     0,'REGIMEN_ESPECIAL'),
    ('DESCONOCIDO',                          'BIEN',     0,'REGIMEN_ESPECIAL');
GO


-- ============================================================
-- 7. FACT_ORDENES_Y_CONTRATOS
--
-- CORRECCIÓN PRINCIPAL:
--   Las columnas de Lead Time eran calculadas convirtiendo el
--   SK_Tiempo (INT) a DATE via CAST(VARCHAR), lo cual SQL Server
--   no considera determinístico y no permite PERSISTED.
--
--   SOLUCIÓN: agregar columnas DATE reales (Fecha_Convocatoria,
--   Fecha_Buena_Pro, Fecha_Suscripcion, Fecha_Emision_OC).
--   El ETL de Python las carga junto con el SK_Tiempo.
--   DATEDIFF sobre columnas DATE es 100% determinístico → PERSISTED OK.
--
--   Los SK_Tiempo_* se mantienen para el join con Dim_Tiempo
--   (slicers de año/mes/trimestre en Power BI).
--
-- FKs ELIMINADAS:
--   FK_Ubigeo_Entidad eliminada de la Fact. La geografía de la
--   entidad se accede por: FK_Entidad → Dim_Entidad_Compradora.FK_Ubigeo
--   → Dim_Ubigeo. Una FK redundante en la Fact generaría ambigüedad
--   en Power BI al detectar rutas múltiples hacia Dim_Ubigeo.
--
-- TOTAL DE FKs EN LA FACT: 9
--   4 temporales + FK_Entidad + FK_Medicamento +
--   FK_Proveedor + FK_Tipo_Proceso + FK_Ubigeo_Item
-- ============================================================
DROP TABLE IF EXISTS oro.Fact_Ordenes_Y_Contratos;
GO
CREATE TABLE oro.Fact_Ordenes_Y_Contratos (
    SK_Hecho                 BIGINT        NOT NULL IDENTITY(1,1),

    -- ── Claves foráneas (9 en total) ──────────────────────────
    -- Temporales → Dim_Tiempo
    FK_Tiempo_Convocatoria   INT           NOT NULL DEFAULT -1,
    FK_Tiempo_Buena_Pro      INT           NOT NULL DEFAULT -1,
    FK_Tiempo_Suscripcion    INT           NOT NULL DEFAULT -1,
    FK_Tiempo_Emision_OC     INT           NOT NULL DEFAULT -1,
    -- Dimensiones
    FK_Entidad               INT           NOT NULL DEFAULT -1,  -- → Dim_Entidad_Compradora
    FK_Medicamento           INT           NOT NULL DEFAULT -1,  -- → Dim_Medicamento
    FK_Proveedor             INT           NOT NULL DEFAULT -1,  -- → Dim_Proveedor
    FK_Tipo_Proceso          INT           NOT NULL DEFAULT -1,  -- → Dim_Tipo_Proceso
    FK_Ubigeo_Item           INT           NOT NULL DEFAULT -1,  -- → Dim_Ubigeo (dpto. entrega)
        -- Origen: Adjudicacion.DEPARTAMENTO_ITEM
        --         ContratacionDirecta.DEPARTAMENTO_ITEM
        -- Solo nivel DEPARTAMENTO (es lo que trae el SEACE)
        -- La geo de la entidad se llega por FK_Entidad → Dim_Entidad_Compradora.FK_Ubigeo

    -- ── Fechas reales (DATE) ───────────────────────────────────
    -- PROPÓSITO: base determinística para las columnas calculadas.
    -- El ETL Python las asigna junto con los SK_Tiempo correspondientes.
    -- Ejemplo: Fecha_Convocatoria = '2022-03-15'
    --          FK_Tiempo_Convocatoria = 20220315
    Fecha_Convocatoria       DATE          NULL,   -- Adjudicacion.FECHA_CONVOCATORIA  [1][2]
    Fecha_Buena_Pro          DATE          NULL,   -- Adjudicacion.FECHA_BUENAPRO       [1][2]
    Fecha_Suscripcion        DATE          NULL,   -- Contratos.FECHA_SUSCRIPCION_CONTRATO [3]
    Fecha_Emision_OC         DATE          NULL,   -- OrdenesServicios.FECHA_DE_EMISION [4]

    -- ── Claves naturales (trazabilidad SEACE) ─────────────────
    Codigo_Convocatoria      BIGINT        NULL,   -- CODIGOCONVOCATORIA [1][2][3]
    N_Item                   SMALLINT      NULL,   -- N_ITEM [1][2] / NUM_ITEM [3]
    N_Cod_Contrato           BIGINT        NULL,   -- N_COD_CONTRATO [3]
    Num_Contrato             VARCHAR(50)   NULL,   -- NUM_CONTRATO [3]
    Tiene_Resolucion         CHAR(2)       NULL,   -- TIENERESOLOCION [3]
    -- OC: sin CODIGOCONVOCATORIA → join en ETL por RUC_ENTIDAD + Anio_Fiscal
    Nro_Orden_Compra         VARCHAR(20)   NULL,   -- NRO_DE_ORDEN [4]
    Tipo_Orden               VARCHAR(50)   NULL,   -- TIPOORDEN [4]
    Estado_Contratacion_OC   VARCHAR(20)   NULL,   -- ESTADOCONTRATACION [4]

    -- ── Métricas financieras (soles) ──────────────────────────
    Cantidad_Adjudicada      DECIMAL(18,4) NOT NULL DEFAULT 0,
    Monto_Referencial_Soles  DECIMAL(18,2) NOT NULL DEFAULT 0,  -- [1][2]
    Monto_Adjudicado_Soles   DECIMAL(18,2) NOT NULL DEFAULT 0,  -- [1][2]
    Monto_Contratado_Item    DECIMAL(18,2) NOT NULL DEFAULT 0,  -- [3]
    Monto_Adicional          DECIMAL(18,2) NOT NULL DEFAULT 0,  -- [3] sobrecosto adendas
    Monto_Reduccion          DECIMAL(18,2) NOT NULL DEFAULT 0,  -- [3]
    Monto_Prorroga           DECIMAL(18,2) NOT NULL DEFAULT 0,  -- [3]
    Monto_Complementario     DECIMAL(18,2) NOT NULL DEFAULT 0,  -- [3]
    Monto_Total_OC           DECIMAL(18,2) NOT NULL DEFAULT 0,  -- [4]

    -- ── Columnas calculadas PERSISTIDAS ───────────────────────
    -- DATEDIFF sobre DATE es determinístico: no lanza Msg 4936

    -- % de sobrecosto por adendas respecto al monto contratado
    Ratio_Sobrecosto_Pct AS (
        CASE WHEN Monto_Contratado_Item > 0
             THEN ROUND(Monto_Adicional / Monto_Contratado_Item * 100, 2)
             ELSE 0 END
    ) PERSISTED,

    -- MÉTRICA CORE: burocracia total (convocatoria → firma de contrato)
    Lead_Time_Total_Dias AS (
        CASE WHEN Fecha_Convocatoria IS NOT NULL AND Fecha_Suscripcion IS NOT NULL
             THEN DATEDIFF(DAY, Fecha_Convocatoria, Fecha_Suscripcion)
             ELSE NULL END
    ) PERSISTED,

    -- Subfase 1: tiempo del comité (convocatoria → buena pro)
    Lead_Time_Comite_Dias AS (
        CASE WHEN Fecha_Convocatoria IS NOT NULL AND Fecha_Buena_Pro IS NOT NULL
             THEN DATEDIFF(DAY, Fecha_Convocatoria, Fecha_Buena_Pro)
             ELSE NULL END
    ) PERSISTED,

    -- Subfase 2: formalización legal (buena pro → suscripción contrato)
    Lead_Time_Formalizacion_Dias AS (
        CASE WHEN Fecha_Buena_Pro IS NOT NULL AND Fecha_Suscripcion IS NOT NULL
             THEN DATEDIFF(DAY, Fecha_Buena_Pro, Fecha_Suscripcion)
             ELSE NULL END
    ) PERSISTED,

    -- ── Flags ─────────────────────────────────────────────────
    Flag_Contratacion_Directa BIT          NOT NULL DEFAULT 0,
        -- 1 si el registro viene de [2] ContratacionDirecta
        -- Desnormalizado aquí para rendimiento en DAX
    Flag_Tiene_Adenda AS (
        CASE WHEN Monto_Adicional > 0 THEN 1 ELSE 0 END
    ) PERSISTED,
    Flag_Fuera_Petitorio      BIT          NOT NULL DEFAULT 0,
        -- 1 cuando DESCRIPCION_ITEM contenía "FUERA DEL PETITORIO"

    Estado_Item               VARCHAR(30)  NULL,   -- Adjudicado/Consentido/Contratado
    Moneda_Original           CHAR(3)      NOT NULL DEFAULT 'PEN',

    -- ── Auditoría ─────────────────────────────────────────────
    Fuente_Dataset            VARCHAR(25)  NOT NULL,
        -- 'ADJUDICACION' / 'CONTRATACION_DIRECTA' / 'CONTRATO' / 'ORDEN_COMPRA'
    Anio_Fiscal               SMALLINT     NOT NULL,
    Fecha_Carga               DATETIME2    NOT NULL DEFAULT GETDATE(),

    CONSTRAINT PK_Fact PRIMARY KEY CLUSTERED (SK_Hecho),

    -- Integridad referencial
    CONSTRAINT FK_Fact_TConvocatoria
        FOREIGN KEY (FK_Tiempo_Convocatoria) REFERENCES oro.Dim_Tiempo (SK_Tiempo),
    CONSTRAINT FK_Fact_TBuenaPro
        FOREIGN KEY (FK_Tiempo_Buena_Pro)    REFERENCES oro.Dim_Tiempo (SK_Tiempo),
    CONSTRAINT FK_Fact_TSuscripcion
        FOREIGN KEY (FK_Tiempo_Suscripcion)  REFERENCES oro.Dim_Tiempo (SK_Tiempo),
    CONSTRAINT FK_Fact_TEmisionOC
        FOREIGN KEY (FK_Tiempo_Emision_OC)   REFERENCES oro.Dim_Tiempo (SK_Tiempo),
    CONSTRAINT FK_Fact_Entidad
        FOREIGN KEY (FK_Entidad)             REFERENCES oro.Dim_Entidad_Compradora (SK_Entidad),
    CONSTRAINT FK_Fact_Medicamento
        FOREIGN KEY (FK_Medicamento)         REFERENCES oro.Dim_Medicamento (SK_Medicamento),
    CONSTRAINT FK_Fact_Proveedor
        FOREIGN KEY (FK_Proveedor)           REFERENCES oro.Dim_Proveedor (SK_Proveedor),
    CONSTRAINT FK_Fact_TipoProceso
        FOREIGN KEY (FK_Tipo_Proceso)        REFERENCES oro.Dim_Tipo_Proceso (SK_Tipo_Proceso),
    CONSTRAINT FK_Fact_UbigeoItem
        FOREIGN KEY (FK_Ubigeo_Item)         REFERENCES oro.Dim_Ubigeo (SK_Ubigeo)
);
GO

-- Índices de rendimiento para Power BI
CREATE NONCLUSTERED INDEX IX_Fact_Tiempo_Entidad
    ON oro.Fact_Ordenes_Y_Contratos (FK_Tiempo_Convocatoria, FK_Entidad)
    INCLUDE (Monto_Adjudicado_Soles, Flag_Contratacion_Directa, Anio_Fiscal);

CREATE NONCLUSTERED INDEX IX_Fact_LeadTime
    ON oro.Fact_Ordenes_Y_Contratos (Lead_Time_Total_Dias, FK_Proveedor)
    INCLUDE (FK_Entidad, FK_Medicamento, Anio_Fiscal);

CREATE NONCLUSTERED INDEX IX_Fact_HHI
    ON oro.Fact_Ordenes_Y_Contratos (FK_Medicamento, FK_Entidad, FK_Proveedor)
    INCLUDE (Monto_Adjudicado_Soles, Anio_Fiscal);

CREATE NONCLUSTERED INDEX IX_Fact_UbigeoItem
    ON oro.Fact_Ordenes_Y_Contratos (FK_Ubigeo_Item)
    INCLUDE (Monto_Adjudicado_Soles, FK_Medicamento);

CREATE NONCLUSTERED INDEX IX_Fact_CD
    ON oro.Fact_Ordenes_Y_Contratos (Flag_Contratacion_Directa, Anio_Fiscal)
    INCLUDE (Monto_Adjudicado_Soles, FK_Entidad);
GO


-- ============================================================
-- 8. VISTAS ANALÍTICAS
-- Las vistas crean DESPUÉS de la Fact para evitar Msg 208.
-- ============================================================

-- 8.1 Vista ESTRATÉGICA
-- ¿Cuánto gastó cada Red en concursos vs. contrataciones directas?
CREATE OR ALTER VIEW oro.vw_Gasto_Por_Proceso_Y_Red AS
SELECT
    t.Anio, t.Trimestre, t.Nombre_Trimestre, t.Mes, t.Nombre_Mes,
    e.Red_Asistencial,
    ue.Departamento     AS Dpto_Entidad,
    ue.Provincia        AS Provincia_Entidad,
    ue.Distrito         AS Distrito_Entidad,
    ue.Macroregion      AS Macroregion_Entidad,
    ui.Departamento     AS Dpto_Entrega_Item,
    ui.Region_Natural   AS Region_Entrega,
    tp.Tipo_Proceso_Seleccion,
    tp.Categoria_Proceso,
    tp.Es_Contratacion_Directa,
    tp.Causal_CD,
    m.Especialidad_Autorizada,
    m.Es_Uso_Critico,
    COUNT(*)                                    AS Cantidad_Items,
    SUM(f.Monto_Adjudicado_Soles)               AS Monto_Adjudicado_Total,
    SUM(f.Monto_Adicional)                      AS Sobrecosto_Adendas,
    SUM(f.Monto_Total_OC)                       AS Monto_Ejecutado_OC,
    AVG(CAST(f.Lead_Time_Total_Dias AS FLOAT))  AS Promedio_Lead_Time_Dias,
    MAX(f.Lead_Time_Total_Dias)                 AS Max_Lead_Time_Dias
FROM       oro.Fact_Ordenes_Y_Contratos f
INNER JOIN oro.Dim_Tiempo               t   ON f.FK_Tiempo_Convocatoria = t.SK_Tiempo
INNER JOIN oro.Dim_Entidad_Compradora   e   ON f.FK_Entidad              = e.SK_Entidad
INNER JOIN oro.Dim_Ubigeo               ue  ON e.FK_Ubigeo               = ue.SK_Ubigeo
INNER JOIN oro.Dim_Ubigeo               ui  ON f.FK_Ubigeo_Item          = ui.SK_Ubigeo
INNER JOIN oro.Dim_Tipo_Proceso         tp  ON f.FK_Tipo_Proceso         = tp.SK_Tipo_Proceso
INNER JOIN oro.Dim_Medicamento          m   ON f.FK_Medicamento          = m.SK_Medicamento
WHERE t.SK_Tiempo > 0
GROUP BY
    t.Anio, t.Trimestre, t.Nombre_Trimestre, t.Mes, t.Nombre_Mes,
    e.Red_Asistencial,
    ue.Departamento, ue.Provincia, ue.Distrito, ue.Macroregion,
    ui.Departamento, ui.Region_Natural,
    tp.Tipo_Proceso_Seleccion, tp.Categoria_Proceso,
    tp.Es_Contratacion_Directa, tp.Causal_CD,
    m.Especialidad_Autorizada, m.Es_Uso_Critico;
GO

-- 8.2 Vista TÁCTICA
-- ¿Qué proveedores o comités dilatan más el Lead Time?
CREATE OR ALTER VIEW oro.vw_Lead_Time_Por_Proveedor AS
SELECT
    t.Anio,
    e.Red_Asistencial,
    ue.Departamento     AS Dpto_Entidad,
    ui.Departamento     AS Dpto_Entrega_Item,
    p.RUC_Proveedor, p.Nombre_Proveedor, p.Tipo_Proveedor, p.Es_Consorcio,
    m.Denominacion_DCI, m.Especialidad_Autorizada, m.Es_Uso_Critico,
    tp.Tipo_Proceso_Seleccion, tp.Es_Contratacion_Directa,
    f.Codigo_Convocatoria, f.N_Item,
    f.Fecha_Convocatoria, f.Fecha_Buena_Pro, f.Fecha_Suscripcion,
    f.Lead_Time_Total_Dias,
    f.Lead_Time_Comite_Dias,
    f.Lead_Time_Formalizacion_Dias,
    f.Monto_Adjudicado_Soles,
    f.Monto_Adicional,
    f.Ratio_Sobrecosto_Pct,
    f.Flag_Tiene_Adenda,
    f.Estado_Item
FROM       oro.Fact_Ordenes_Y_Contratos f
INNER JOIN oro.Dim_Tiempo               t   ON f.FK_Tiempo_Convocatoria = t.SK_Tiempo
INNER JOIN oro.Dim_Entidad_Compradora   e   ON f.FK_Entidad              = e.SK_Entidad
INNER JOIN oro.Dim_Ubigeo               ue  ON e.FK_Ubigeo               = ue.SK_Ubigeo
INNER JOIN oro.Dim_Ubigeo               ui  ON f.FK_Ubigeo_Item          = ui.SK_Ubigeo
INNER JOIN oro.Dim_Proveedor            p   ON f.FK_Proveedor            = p.SK_Proveedor
INNER JOIN oro.Dim_Medicamento          m   ON f.FK_Medicamento          = m.SK_Medicamento
INNER JOIN oro.Dim_Tipo_Proceso         tp  ON f.FK_Tipo_Proceso         = tp.SK_Tipo_Proceso
WHERE f.Lead_Time_Total_Dias IS NOT NULL AND t.SK_Tiempo > 0;
GO

-- 8.3 Vista OPERATIVA: Matriz de Riesgo HHI
-- ¿Qué redes concentran >80% de un fármaco crítico en un solo proveedor?
-- Columna Disparar_Alerta_PowerAutomate = 1 activa el botón del dashboard
--
-- CORRECCIÓN v4:
--   Error 8120: SK_Medicamento añadido al GROUP BY de HHI_Base.
--   Error 4109: funciones de ventana anidadas eliminadas.
--     Solución: CTE Dom con ROW_NUMBER identifica el proveedor dominante
--     sin necesidad de anidar MAX(...) OVER dentro de otro agregado.
CREATE OR ALTER VIEW oro.vw_Matriz_Riesgo_HHI AS
WITH
-- Paso 1: monto de cada proveedor por (año, entidad, medicamento)
Part AS (
    SELECT
        t.Anio,
        e.SK_Entidad,
        e.Red_Asistencial,
        ue.Departamento,
        ue.Macroregion,
        m.SK_Medicamento,
        m.Denominacion_DCI,
        m.Especialidad_Autorizada,
        m.Es_Uso_Critico,
        m.Es_Uso_Intrahospitalario,
        p.SK_Proveedor,
        p.RUC_Proveedor,
        p.Nombre_Proveedor,
        SUM(f.Monto_Adjudicado_Soles) AS Monto_Proveedor
    FROM       oro.Fact_Ordenes_Y_Contratos f
    INNER JOIN oro.Dim_Tiempo               t   ON f.FK_Tiempo_Convocatoria = t.SK_Tiempo
    INNER JOIN oro.Dim_Entidad_Compradora   e   ON f.FK_Entidad              = e.SK_Entidad
    INNER JOIN oro.Dim_Ubigeo               ue  ON e.FK_Ubigeo               = ue.SK_Ubigeo
    INNER JOIN oro.Dim_Medicamento          m   ON f.FK_Medicamento          = m.SK_Medicamento
    INNER JOIN oro.Dim_Proveedor            p   ON f.FK_Proveedor            = p.SK_Proveedor
    WHERE m.SK_Medicamento > 0
      AND t.SK_Tiempo > 0
      AND f.Monto_Adjudicado_Soles > 0
    GROUP BY
        t.Anio, e.SK_Entidad, e.Red_Asistencial,
        ue.Departamento, ue.Macroregion,
        m.SK_Medicamento, m.Denominacion_DCI,
        m.Especialidad_Autorizada, m.Es_Uso_Critico, m.Es_Uso_Intrahospitalario,
        p.SK_Proveedor, p.RUC_Proveedor, p.Nombre_Proveedor
),

-- Paso 2: totales de mercado por grupo (agregación sin proveedor)
MarketTotals AS (
    SELECT
        Anio, SK_Entidad, SK_Medicamento,
        SUM(Monto_Proveedor) AS Monto_Total_Mercado,
        COUNT(SK_Proveedor)  AS Num_Proveedores
    FROM Part
    GROUP BY Anio, SK_Entidad, SK_Medicamento
),

-- Paso 3: HHI por (año, entidad, medicamento) — sin columnas de proveedor
-- Usa MarketTotals pre-agregado para evitar window function dentro de aggregate
HHI_Base AS (
    SELECT
        p.Anio, p.SK_Entidad, p.Red_Asistencial, p.Departamento, p.Macroregion,
        p.SK_Medicamento, p.Denominacion_DCI, p.Especialidad_Autorizada,
        p.Es_Uso_Critico, p.Es_Uso_Intrahospitalario,
        mt.Monto_Total_Mercado,
        mt.Num_Proveedores,
        ROUND(SUM(
            POWER(
                CAST(p.Monto_Proveedor AS FLOAT)
                / NULLIF(mt.Monto_Total_Mercado, 0)
            , 2)
        ) * 10000, 0)         AS HHI
    FROM Part p
    INNER JOIN MarketTotals mt
        ON mt.Anio          = p.Anio
       AND mt.SK_Entidad    = p.SK_Entidad
       AND mt.SK_Medicamento = p.SK_Medicamento
    GROUP BY
        p.Anio, p.SK_Entidad, p.Red_Asistencial, p.Departamento, p.Macroregion,
        p.SK_Medicamento, p.Denominacion_DCI, p.Especialidad_Autorizada,
        p.Es_Uso_Critico, p.Es_Uso_Intrahospitalario,
        mt.Monto_Total_Mercado, mt.Num_Proveedores
),

-- Paso 4: proveedor dominante por grupo usando ROW_NUMBER
Dom AS (
    SELECT
        Anio, SK_Entidad, SK_Medicamento,
        RUC_Proveedor    AS RUC_Proveedor_Dominante,
        Nombre_Proveedor AS Proveedor_Dominante,
        ROUND(
            CAST(Monto_Proveedor AS FLOAT)
            / NULLIF(SUM(Monto_Proveedor) OVER
                (PARTITION BY Anio, SK_Entidad, SK_Medicamento), 0) * 100
        , 1)             AS Participacion_Pct_Dominante,
        ROW_NUMBER() OVER (
            PARTITION BY Anio, SK_Entidad, SK_Medicamento
            ORDER BY Monto_Proveedor DESC
        )                AS Rn
    FROM Part
)

-- Paso 5: unir HHI con proveedor dominante y calcular semáforo
SELECT
    h.Anio, h.Red_Asistencial, h.Departamento, h.Macroregion,
    h.Denominacion_DCI, h.Especialidad_Autorizada,
    h.Es_Uso_Critico, h.Es_Uso_Intrahospitalario,
    h.Monto_Total_Mercado, h.Num_Proveedores, h.HHI,
    d.RUC_Proveedor_Dominante,
    d.Proveedor_Dominante,
    d.Participacion_Pct_Dominante,
    CASE
        WHEN h.HHI >= 8000 AND h.Es_Uso_Critico = 1 THEN 'CRITICO'
        WHEN h.HHI >= 8000                           THEN 'ALTO'
        WHEN h.HHI >= 2500                           THEN 'MODERADO'
        ELSE                                              'BAJO'
    END AS Nivel_Alerta_HHI,
    CASE
        WHEN h.HHI >= 8000
         AND h.Es_Uso_Critico = 1
         AND d.Participacion_Pct_Dominante >= 80
        THEN 1 ELSE 0
    END AS Disparar_Alerta_PowerAutomate
FROM       HHI_Base h
INNER JOIN Dom      d  ON  d.Anio          = h.Anio
                       AND d.SK_Entidad     = h.SK_Entidad
                       AND d.SK_Medicamento = h.SK_Medicamento
                       AND d.Rn            = 1;
GO


-- ============================================================
-- 9. VALIDACIÓN POST-CARGA
-- ============================================================
-- 9.1 Row counts
SELECT 'Dim_Tiempo' AS Tabla, COUNT(*) AS Filas FROM oro.Dim_Tiempo
UNION ALL
SELECT 'Dim_Ubigeo', COUNT(*) FROM oro.Dim_Ubigeo
UNION ALL
SELECT 'Dim_Entidad_Compradora', COUNT(*) FROM oro.Dim_Entidad_Compradora
UNION ALL
SELECT 'Dim_Medicamento', COUNT(*) FROM oro.Dim_Medicamento
UNION ALL
SELECT 'Dim_Proveedor', COUNT(*) FROM oro.Dim_Proveedor
UNION ALL
SELECT 'Dim_Tipo_Proceso', COUNT(*) FROM oro.Dim_Tipo_Proceso
UNION ALL
SELECT 'Fact_Ordenes_Y_Contratos', COUNT(*) FROM oro.Fact_Ordenes_Y_Contratos
ORDER BY Tabla;
GO

-- 9.2 FK integrity: orphans por constraint
-- FK_Fact_TConvocatoria
SELECT 'FK_Fact_TConvocatoria' AS FK, COUNT(*) AS Orphans
FROM oro.Fact_Ordenes_Y_Contratos f
LEFT JOIN oro.Dim_Tiempo d ON f.FK_Tiempo_Convocatoria = d.SK_Tiempo
WHERE d.SK_Tiempo IS NULL;
GO

-- FK_Fact_TBuenaPro
SELECT 'FK_Fact_TBuenaPro' AS FK, COUNT(*) AS Orphans
FROM oro.Fact_Ordenes_Y_Contratos f
LEFT JOIN oro.Dim_Tiempo d ON f.FK_Tiempo_Buena_Pro = d.SK_Tiempo
WHERE d.SK_Tiempo IS NULL;
GO

-- FK_Fact_TSuscripcion
SELECT 'FK_Fact_TSuscripcion' AS FK, COUNT(*) AS Orphans
FROM oro.Fact_Ordenes_Y_Contratos f
LEFT JOIN oro.Dim_Tiempo d ON f.FK_Tiempo_Suscripcion = d.SK_Tiempo
WHERE d.SK_Tiempo IS NULL;
GO

-- FK_Fact_TEmisionOC
SELECT 'FK_Fact_TEmisionOC' AS FK, COUNT(*) AS Orphans
FROM oro.Fact_Ordenes_Y_Contratos f
LEFT JOIN oro.Dim_Tiempo d ON f.FK_Tiempo_Emision_OC = d.SK_Tiempo
WHERE d.SK_Tiempo IS NULL;
GO

-- FK_Fact_Entidad
SELECT 'FK_Fact_Entidad' AS FK, COUNT(*) AS Orphans
FROM oro.Fact_Ordenes_Y_Contratos f
LEFT JOIN oro.Dim_Entidad_Compradora d ON f.FK_Entidad = d.SK_Entidad
WHERE d.SK_Entidad IS NULL;
GO

-- FK_Fact_Medicamento
SELECT 'FK_Fact_Medicamento' AS FK, COUNT(*) AS Orphans
FROM oro.Fact_Ordenes_Y_Contratos f
LEFT JOIN oro.Dim_Medicamento d ON f.FK_Medicamento = d.SK_Medicamento
WHERE d.SK_Medicamento IS NULL;
GO

-- FK_Fact_Proveedor
SELECT 'FK_Fact_Proveedor' AS FK, COUNT(*) AS Orphans
FROM oro.Fact_Ordenes_Y_Contratos f
LEFT JOIN oro.Dim_Proveedor d ON f.FK_Proveedor = d.SK_Proveedor
WHERE d.SK_Proveedor IS NULL;
GO

-- FK_Fact_TipoProceso
SELECT 'FK_Fact_TipoProceso' AS FK, COUNT(*) AS Orphans
FROM oro.Fact_Ordenes_Y_Contratos f
LEFT JOIN oro.Dim_Tipo_Proceso d ON f.FK_Tipo_Proceso = d.SK_Tipo_Proceso
WHERE d.SK_Tipo_Proceso IS NULL;
GO

-- FK_Fact_UbigeoItem
SELECT 'FK_Fact_UbigeoItem' AS FK, COUNT(*) AS Orphans
FROM oro.Fact_Ordenes_Y_Contratos f
LEFT JOIN oro.Dim_Ubigeo d ON f.FK_Ubigeo_Item = d.SK_Ubigeo
WHERE d.SK_Ubigeo IS NULL;
GO

-- 9.3 KPIs resumen
SELECT
    COUNT(DISTINCT FK_Proveedor) AS Proveedores_Unicos,
    COUNT(DISTINCT FK_Entidad)   AS Entidades_Unicas,
    COUNT(DISTINCT FK_Medicamento) AS Medicamentos_Unicos,
    SUM(Monto_Referencial_Soles) AS Total_Referencial,
    SUM(Monto_Adjudicado_Soles)  AS Total_Adjudicado,
    SUM(Monto_Referencial_Soles - ISNULL(Monto_Adjudicado_Soles, 0)) AS Diferencia
FROM oro.Fact_Ordenes_Y_Contratos;
GO

-- 9.4 Validación vistas analíticas
SELECT COUNT(*) AS vw_Gasto_Por_Proceso_Y_Red FROM oro.vw_Gasto_Por_Proceso_Y_Red;
GO
SELECT COUNT(*) AS vw_Lead_Time_Por_Proveedor FROM oro.vw_Lead_Time_Por_Proveedor;
GO
SELECT COUNT(*) AS vw_Matriz_Riesgo_HHI FROM oro.vw_Matriz_Riesgo_HHI;
GO