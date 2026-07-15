const sql = require('mssql');

async function getAlerts() {
    // Las variables de entorno son inyectadas via dotenv o docker-compose
    const config = {
        user: process.env.DB_USER || 'sa',
        password: process.env.DB_PASSWORD || 'EsSalud2024!',
        server: process.env.DB_SERVER || 'localhost',
        database: process.env.DB_NAME || 'DW_EsSalud_Adquisiciones',
        port: parseInt(process.env.DB_PORT || '11433', 10),
        options: {
            encrypt: true,
            trustServerCertificate: true
        }
    };

    try {
        await sql.connect(config);
        const result = await sql.query`SELECT * FROM oro.Alertas ORDER BY Tipo_Alerta ASC, Valor DESC`;
        return result.recordset;
    } catch (err) {
        console.error('Error connecting to SQL Server:', err);
        throw err;
    }
}

module.exports = {
    getAlerts
};
