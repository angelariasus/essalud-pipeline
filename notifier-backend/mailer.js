const nodemailer = require('nodemailer');

function createTransporter() {
    return nodemailer.createTransport({
        host: process.env.SMTP_HOST || 'mailhog',
        port: parseInt(process.env.SMTP_PORT || '1025', 10),
        secure: process.env.SMTP_SECURE === 'true', 
        auth: process.env.SMTP_USER ? {
            user: process.env.SMTP_USER,
            pass: process.env.SMTP_PASS
        } : undefined,
        tls: {
            rejectUnauthorized: false
        }
    });
}

function generateHtmlTable(alerts) {
    if (alerts.length === 0) return '<p>No hay alertas detectadas.</p>';
    
    let rows = alerts.map(r => `
        <tr>
            <td>${r.Tipo_Alerta}</td>
            <td>${r.Anio}</td>
            <td>${r.Medicamento}</td>
            <td>${r.Red_Asistencial}</td>
            <td>${r.RUC_Proveedor}</td>
            <td>${r.Nombre_Proveedor}</td>
            <td align='right'>${r.Valor}</td>
            <td>${r.Detalle}</td>
        </tr>
    `).join('');

    return `
    <table border="1" cellpadding="4" cellspacing="0" style="border-collapse:collapse;font-size:13px">
        <tr style="background:#8B0000;color:#fff">
            <th>Tipo</th><th>Año</th><th>Medicamento</th><th>Red Asistencial</th>
            <th>RUC Proveedor</th><th>Proveedor</th><th>Valor</th><th>Detalle</th>
        </tr>
        ${rows}
    </table>
    `;
}

async function sendAlertEmail(alerts) {
    if (!alerts || alerts.length === 0) {
        console.log("No alerts to send.");
        return;
    }

    const n_hhi = alerts.filter(a => a.Tipo_Alerta === 'HHI_CRITICO').length;
    const n_lt = alerts.length - n_hhi;
    const fecha = new Date().toLocaleString('es-PE');

    const htmlContent = `
    <p>Estimados, Área de Abastecimiento:</p>
    <p>El monitoreo automático del <b>${fecha}</b> detectó <b>${alerts.length}</b> alertas activas 
    (${n_hhi} de concentración HHI crítica, ${n_lt} de lead time anómalo):</p>
    
    ${generateHtmlTable(alerts)}
    
    <p>Se solicita evaluar acciones de diversificación de proveedores y/o seguimiento 
    del proceso según corresponda.</p>
    <p>Atentamente,<br/>Sistema de Monitoreo BI — EsSalud Pipeline <i>(Microservicio Node.js)</i></p>
    `;

    const transporter = createTransporter();
    
    const to = process.env.SMTP_TO || 'abastecimiento@essalud-pipeline.local';
    const from = process.env.SMTP_FROM || 'alertas@essalud-pipeline.local';

    const mailOptions = {
        from: from,
        to: to,
        subject: `[ALERTA Node.js] ${alerts.length} alertas operativas detectadas`,
        html: htmlContent
    };

    console.log(`Enviando correo a ${to}...`);
    const info = await transporter.sendMail(mailOptions);
    console.log('Message sent: %s', info.messageId);
}

module.exports = {
    sendAlertEmail
};
