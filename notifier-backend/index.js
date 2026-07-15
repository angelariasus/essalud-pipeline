require('dotenv').config();
const express = require('express');
const { getAlerts } = require('./db');
const { sendAlertEmail } = require('./mailer');

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 3000;

app.post('/api/notify', async (req, res) => {
    try {
        console.log('Received request to send notifications...');
        
        // 1. Fetch alerts from DB
        const alerts = await getAlerts();
        console.log(`Fetched ${alerts.length} alerts from DB.`);
        
        // 2. Send Email
        await sendAlertEmail(alerts);
        
        res.status(200).json({
            message: 'Notifications processed successfully.',
            alertCount: alerts.length
        });
    } catch (error) {
        console.error('Error processing notifications:', error);
        res.status(500).json({ error: 'Internal Server Error', details: error.message });
    }
});

// Healthcheck
app.get('/health', (req, res) => {
    res.status(200).send('OK');
});

app.listen(PORT, () => {
    console.log(`Notifier backend listening on port ${PORT}`);
});
