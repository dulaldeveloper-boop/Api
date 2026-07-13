// ============================================
// WS-INCOME WhatsApp Campaign Engine v2.1
// Firebase Storage Session | Real-time | Anti-Duplicate
// ============================================

const express = require('express');
const cors = require('cors');
const path = require('path');
const fs = require('fs');
const { v4: uuidv4 } = require('uuid');
const admin = require('firebase-admin');
const cron = require('node-cron');

const {
    default: makeWASocket,
    useMultiFileAuthState,
    fetchLatestBaileysVersion,
    DisconnectReason,
    delay
} = require('@whiskeysockets/baileys');

require('dotenv').config();

const PORT = process.env.PORT || 10000;
const SESSIONS_DIR = process.env.RENDER ? '/tmp/sessions' : './sessions';

// ============================================
// FIREBASE INIT
// ============================================
let db, bucket;
try {
    const serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT);
    admin.initializeApp({
        credential: admin.credential.cert(serviceAccount),
        databaseURL: process.env.FIREBASE_DB_URL,
        storageBucket: "ws-income-official.firebasestorage.app"
    });
    db = admin.firestore();
    bucket = admin.storage().bucket();
    console.log('🔥 Firestore + Storage Connected');
} catch (err) {
    console.error('❌ Firebase init failed:', err.message);
    process.exit(1);
}

// ============================================
// GLOBALS
// ============================================
const sessions = new Map();          // sock instances
const sessionStates = new Map();     // current state per session
const todayCounts = new Map();       // todaySent per session
const dailyLimits = new Map();       // per session
const sessionUserMap = new Map();    // sessionId → userId

// ============================================
// EXPRESS SETUP
// ============================================
const app = express();
app.use(cors());
app.use(express.json({ limit: '50mb' }));
app.use(express.static(path.join(__dirname, 'public')));

if (!fs.existsSync(SESSIONS_DIR)) {
    fs.mkdirSync(SESSIONS_DIR, { recursive: true });
}

// ============================================
// HELPERS
// ============================================
function formatPhone(number) {
    return String(number).replace(/\D/g, '');
}

function jid(phone) {
    return `${phone}@s.whatsapp.net`;
}

// ============================================
// PHONE VALIDATION & CLEANING HELPER
// ============================================
function validateAndCleanPhone(phone) {
    // Remove all non-digit characters
    let cleanPhone = String(phone).replace(/\D/g, '');
    
    // Remove leading 0 if present (common in local BD formats)
    if (cleanPhone.startsWith('0')) {
        cleanPhone = cleanPhone.slice(1);
    }
    
    // Auto-add Bangladesh country code if missing
    if (!cleanPhone.startsWith('880')) {
        cleanPhone = '880' + cleanPhone;
    }
    
    // Validate: must be 13 digits for BD (880 + 10 digits)
    if (cleanPhone.length !== 13 || !cleanPhone.startsWith('880')) {
        return { valid: false, error: 'Invalid BD phone. Use format: 8801XXXXXXXXX or 01XXXXXXXXX' };
    }
    
    return { valid: true, phone: cleanPhone };
}

// ============================================
// FIREBASE STORAGE SESSION SAVE/LOAD
// ============================================
async function saveSessionToCloud(sessionId) {
    try {
        const sessionPath = `${SESSIONS_DIR}/${sessionId}`;
        if (!fs.existsSync(sessionPath)) return;

        const files = fs.readdirSync(sessionPath);
        for (const file of files) {
            const filePath = `${sessionPath}/${file}`;
            if (fs.statSync(filePath).isFile()) {
                await bucket.upload(filePath, {
                    destination: `sessions/${sessionId}/${file}`,
                    metadata: { contentType: 'application/octet-stream' }
                });
            }
        }
    } catch (err) {
        console.error('💾 Session save error:', err.message);
    }
}

async function loadSessionFromCloud(sessionId) {
    try {
        const sessionPath = `${SESSIONS_DIR}/${sessionId}`;
        if (!fs.existsSync(sessionPath)) {
            fs.mkdirSync(sessionPath, { recursive: true });
        }

        const [files] = await bucket.getFiles({ prefix: `sessions/${sessionId}/` });
        for (const file of files) {
            const fileName = path.basename(file.name);
            const destPath = `${sessionPath}/${fileName}`;
            if (!fs.existsSync(destPath)) {
                await file.download({ destination: destPath });
            }
        }
        return files.length > 0;
    } catch (err) {
        console.error('📥 Session load error:', err.message);
        return false;
    }
}

// ============================================
// WHATSAPP SESSION CREATE
// ============================================
async function createSession(sessionId, phoneNumber, userId) {
    // Load session from cloud first
    await loadSessionFromCloud(sessionId);

    const authPath = `${SESSIONS_DIR}/${sessionId}`;
    if (!fs.existsSync(authPath)) fs.mkdirSync(authPath, { recursive: true });

    const { state, saveCreds } = await useMultiFileAuthState(authPath);
    const { version } = await fetchLatestBaileysVersion();

    const sock = makeWASocket({
        version,
        auth: state,
        printQRInTerminal: false,
        syncFullHistory: false,
        logger: undefined
    });

    sock.ev.on('connection.update', async (update) => {
        const { connection, lastDisconnect } = update;

        if (connection === 'open') {
            const phone = sock.user?.id?.split(':')[0] || phoneNumber;
            const name = sock.user?.name || phone;

            sessionStates.set(sessionId, {
                status: 'connected',
                phone,
                name,
                userId: userId,
                connectedAt: new Date().toISOString()
            });
            sessionUserMap.set(sessionId, userId);

            const stored = (await db.collection('whatsapp_accounts').doc(sessionId).get()).data();
            const limit = stored?.dailyLimit || 5;
            dailyLimits.set(sessionId, limit);
            todayCounts.set(sessionId, stored?.todaySent || 0);

            await db.collection('whatsapp_accounts').doc(sessionId).set({
                phone,
                name,
                status: 'connected',
                userId: userId,
                dailyLimit: limit,
                todaySent: todayCounts.get(sessionId) || 0,
                updatedAt: admin.firestore.FieldValue.serverTimestamp()
            }, { merge: true });

            console.log(`✅ ${name} (${phone}) connected`);

            // Save session to cloud
            await saveSessionToCloud(sessionId);
        }

        if (connection === 'close') {
            const statusCode = lastDisconnect?.error?.output?.statusCode;
            const isLoggedOut = statusCode === DisconnectReason.loggedOut;

            if (isLoggedOut) {
                // 🔴 LOGOUT DETECTED - Send notification
                console.log(`🔴 ${phoneNumber} LOGGED OUT!`);

                // Update Firestore - status logged_out
                await db.collection('whatsapp_accounts').doc(sessionId).update({
                    status: 'logged_out',
                    updatedAt: admin.firestore.FieldValue.serverTimestamp()
                });

                // Send notification to user
                const userSnap = await db.collection('users').doc(userId).get();
                if (userSnap.exists) {
                    await db.collection('notifications').add({
                        userId: userId,
                        type: 'account_logged_out',
                        title: '⚠️ WhatsApp Account Logged Out',
                        message: `Account ${phoneNumber} has been logged out from WhatsApp. Please reconnect.`,
                        sessionId: sessionId,
                        phone: phoneNumber,
                        read: false,
                        createdAt: admin.firestore.FieldValue.serverTimestamp()
                    });
                }

                // Remove from memory
                sessions.delete(sessionId);
                sessionStates.delete(sessionId);
                sessionUserMap.delete(sessionId);

                // Delete session from cloud
                try {
                    const [files] = await bucket.getFiles({ prefix: `sessions/${sessionId}/` });
                    for (const file of files) {
                        await file.delete();
                    }
                } catch (e) {
                    // ignore
                }
            } else {
                // Disconnected but can reconnect
                sessionStates.set(sessionId, {
                    ...sessionStates.get(sessionId) || {},
                    status: 'disconnected'
                });

                await db.collection('whatsapp_accounts').doc(sessionId).update({
                    status: 'disconnected',
                    updatedAt: admin.firestore.FieldValue.serverTimestamp()
                });

                console.log(`🔄 ${phoneNumber} disconnected, attempting reconnect...`);
                setTimeout(() => createSession(sessionId, phoneNumber, userId), 5000);
            }
        }
    });

    // Save creds on update
    sock.ev.on('creds.update', async () => {
        await saveCreds();
        // Auto-save to cloud every creds update
        await saveSessionToCloud(sessionId);
    });

    sessions.set(sessionId, sock);
    return sock;
}

// ============================================
// CAMPAIGN ENGINE (with success/fail tracking)
// ============================================
async function runCampaign(campaignId) {
    const campaignRef = db.collection('campaigns').doc(campaignId);
    const campaignSnap = await campaignRef.get();
    if (!campaignSnap.exists) return;

    const campaign = campaignSnap.data();
    if (campaign.status !== 'running') return;

    const userId = campaign.userId;
    const messageTemplate = campaign.message;

    // Get pending targets from subcollection
    const targetsSnap = await campaignRef.collection('targets')
        .where('status', '==', 'pending')
        .limit(50)
        .get();

    if (targetsSnap.empty) {
        await campaignRef.update({ status: 'completed' });
        return;
    }

    // Get available accounts for this user
    const accountsSnap = await db.collection('whatsapp_accounts')
        .where('userId', '==', userId)
        .where('status', '==', 'connected')
        .get();

    const availableAccounts = [];
    accountsSnap.forEach(doc => {
        const data = doc.data();
        const todaySent = todayCounts.get(doc.id) || data.todaySent || 0;
        const dailyLimit = dailyLimits.get(doc.id) || data.dailyLimit || 5;
        if (todaySent < dailyLimit && sessions.has(doc.id)) {
            availableAccounts.push({ id: doc.id, todaySent, dailyLimit });
        }
    });

    if (availableAccounts.length === 0) {
        console.log(`⏸️ Campaign ${campaignId}: No available accounts`);
        return;
    }

    let accountIndex = 0;
    for (const targetDoc of targetsSnap.docs) {
        const currentSnap = await campaignRef.get();
        if (currentSnap.data()?.status !== 'running') break;

        const target = targetDoc.data();
        const account = availableAccounts[accountIndex % availableAccounts.length];
        const sock = sessions.get(account.id);

        if (!sock) {
            accountIndex++;
            continue;
        }

        try {
            const jidTarget = jid(target.phone);
            const msg = messageTemplate.replace(/\{name\}/g, target.name || '');
            
            // 📤 Send message
            const result = await sock.sendMessage(jidTarget, { text: msg });

            if (result?.key?.id) {
                // ✅ SUCCESS
                await targetDoc.ref.update({
                    status: 'sent',
                    sentAt: admin.firestore.FieldValue.serverTimestamp(),
                    messageId: result.key.id
                });

                await campaignRef.update({
                    sentCount: admin.firestore.FieldValue.increment(1)
                });

                // Update account count
                const newCount = (todayCounts.get(account.id) || 0) + 1;
                todayCounts.set(account.id, newCount);
                await db.collection('whatsapp_accounts').doc(account.id).update({
                    todaySent: newCount,
                    updatedAt: admin.firestore.FieldValue.serverTimestamp()
                });

                // Update user balance (if pricePerMessage set)
                if (campaign.pricePerMessage > 0) {
                    await db.collection('users').doc(userId).update({
                        balance: admin.firestore.FieldValue.increment(campaign.pricePerMessage),
                        totalEarned: admin.firestore.FieldValue.increment(campaign.pricePerMessage)
                    });
                }

                console.log(`✅ [${account.id}] Sent to ${target.phone}`);
            } else {
                // ⚠️ Message sent but no confirmation
                await targetDoc.ref.update({
                    status: 'failed',
                    error: 'No message ID returned'
                });
                await campaignRef.update({
                    failedCount: admin.firestore.FieldValue.increment(1)
                });
                console.log(`⚠️ [${account.id}] No confirmation for ${target.phone}`);
            }

        } catch (err) {
            // ❌ FAILED
            console.error(`❌ [${account.id}] Failed to ${target.phone}:`, err.message);

            await targetDoc.ref.update({
                status: 'failed',
                error: err.message
            });

            await campaignRef.update({
                failedCount: admin.firestore.FieldValue.increment(1)
            });

            // Check if account is banned/logged out
            if (err.message?.includes('logged out') || err.message?.includes('disconnected')) {
                await db.collection('whatsapp_accounts').doc(account.id).update({
                    status: 'logged_out',
                    updatedAt: admin.firestore.FieldValue.serverTimestamp()
                });
                sessions.delete(account.id);
                sessionStates.delete(account.id);
            }
        }

        accountIndex++;

        // Random delay 10-30 seconds
        const randomDelay = Math.floor(Math.random() * 20000) + 10000;
        await delay(randomDelay);
    }

    // Check if campaign completed
    const pendingSnap = await campaignRef.collection('targets')
        .where('status', '==', 'pending')
        .limit(1)
        .get();

    if (pendingSnap.empty) {
        await campaignRef.update({ status: 'completed', completedAt: admin.firestore.FieldValue.serverTimestamp() });
        console.log(`✅ Campaign ${campaignId} completed!`);
    }
}

// ============================================
// API ENDPOINTS
// ============================================

// 1. PAIR - get pairing code (with improved phone validation)
app.post('/api/pair', async (req, res) => {
    try {
        const { phone, userId } = req.body;
        if (!phone) return res.status(400).json({ success: false, error: 'Phone required' });

        // Validate and clean phone number
        const validation = validateAndCleanPhone(phone);
        
        if (!validation.valid) {
            return res.status(400).json({ 
                success: false, 
                error: validation.error 
            });
        }

        const cleanPhone = validation.phone;

        const sessionId = uuidv4();
        sessionStates.set(sessionId, { status: 'initializing', phone: cleanPhone, userId });

        // Baileys সেশন তৈরি
        const sock = await createSession(sessionId, cleanPhone, userId);
        
        // ৩ সেকেন্ড অপেক্ষা (সেশন রেডি হতে)
        await delay(3000);
        
        // Pairing code জেনারেট
        const code = await sock.requestPairingCode(cleanPhone);
        
        if (!code) {
            return res.status(400).json({ success: false, error: 'Failed to generate code. Try again.' });
        }

        sessionStates.set(sessionId, {
            status: 'pair_ready',
            pairCode: code,
            phone: cleanPhone,
            userId: userId
        });

        await db.collection('whatsapp_accounts').doc(sessionId).set({
            userId: userId,
            phone: cleanPhone,
            status: 'pair_ready',
            pairCode: code,
            dailyLimit: 5,
            todaySent: 0,
            createdAt: admin.firestore.FieldValue.serverTimestamp()
        });

        res.json({
            success: true,
            code,
            sessionId,
            message: 'Enter this code in WhatsApp > Linked Devices'
        });

    } catch (err) {
        console.error('Pair error:', err.message);
        
        // Error handle
        if (err.message?.includes('child')) {
            return res.status(400).json({ 
                success: false, 
                error: 'Connection failed. Please try again with correct phone number format (8801XXXXXXXXX).' 
            });
        }
        
        res.status(500).json({ success: false, error: err.message });
    }
});

// 2. LIST ACCOUNTS (by userId)
app.get('/api/accounts', async (req, res) => {
    try {
        const { userId } = req.query;
        let query = db.collection('whatsapp_accounts').orderBy('updatedAt', 'desc');
        if (userId) {
            query = query.where('userId', '==', userId);
        }
        const snap = await query.get();
        const accounts = [];
        snap.forEach(doc => accounts.push({ id: doc.id, ...doc.data() }));
        res.json(accounts);
    } catch (err) {
        res.status(500).json({ success: false, error: err.message });
    }
});

// 3. ACCOUNT STATUS
app.get('/api/status/:id', (req, res) => {
    const state = sessionStates.get(req.params.id);
    if (!state) return res.json({ status: 'not_found' });
    res.json({
        id: req.params.id,
        ...state,
        todaySent: todayCounts.get(req.params.id) || 0,
        dailyLimit: dailyLimits.get(req.params.id) || 5
    });
});

// 4. SEND SINGLE MESSAGE (with improved phone validation)
app.post('/api/send', async (req, res) => {
    try {
        const { accountId, to, text, userId } = req.body;
        const sock = sessions.get(accountId);

        if (!sock || sessionStates.get(accountId)?.status !== 'connected') {
            return res.status(400).json({
                success: false,
                error: 'Account not connected',
                status: sessionStates.get(accountId)?.status || 'unknown'
            });
        }

        // Validate recipient phone
        let cleanTo = String(to).replace(/\D/g, '');
        if (cleanTo.startsWith('0')) cleanTo = cleanTo.slice(1);
        if (!cleanTo.startsWith('880')) cleanTo = '880' + cleanTo;
        
        const jidTo = `${cleanTo}@s.whatsapp.net`;
        const result = await sock.sendMessage(jidTo, { text });

        if (!result?.key?.id) {
            return res.status(400).json({
                success: false,
                error: 'Message not confirmed - may not have been delivered'
            });
        }

        // Increment count
        const newCount = (todayCounts.get(accountId) || 0) + 1;
        todayCounts.set(accountId, newCount);
        await db.collection('whatsapp_accounts').doc(accountId).update({
            todaySent: newCount,
            updatedAt: admin.firestore.FieldValue.serverTimestamp()
        });

        // Save message log
        await db.collection('message_logs').add({
            accountId,
            userId: userId || '',
            to: jidTo,
            text,
            status: 'sent',
            messageId: result.key.id,
            createdAt: admin.firestore.FieldValue.serverTimestamp()
        });

        res.json({
            success: true,
            messageId: result.key.id,
            todaySent: newCount
        });
    } catch (err) {
        // Save failed log
        await db.collection('message_logs').add({
            accountId: req.body.accountId,
            userId: req.body.userId || '',
            to: req.body.to,
            text: req.body.text,
            status: 'failed',
            error: err.message,
            createdAt: admin.firestore.FieldValue.serverTimestamp()
        });

        res.status(400).json({
            success: false,
            error: err.message,
            isLoggedOut: err.message?.includes('logged out') || false
        });
    }
});

// 5. START CAMPAIGN
app.post('/api/campaign/start', async (req, res) => {
    try {
        const { userId, name, message, targets, pricePerMessage } = req.body;
        if (!userId || !targets || targets.length === 0) {
            return res.status(400).json({ success: false, error: 'Missing fields' });
        }

        // Check if user has connected accounts
        const accSnap = await db.collection('whatsapp_accounts')
            .where('userId', '==', userId)
            .where('status', '==', 'connected')
            .get();

        if (accSnap.empty) {
            return res.status(400).json({
                success: false,
                error: 'No connected WhatsApp accounts. Please connect an account first.'
            });
        }

        const campaignRef = db.collection('campaigns').doc();
        const campaignId = campaignRef.id;

        const campaignData = {
            userId,
            name,
            message,
            totalTargets: targets.length,
            sentCount: 0,
            failedCount: 0,
            status: 'running',
            pricePerMessage: pricePerMessage || 0,
            createdAt: admin.firestore.FieldValue.serverTimestamp()
        };

        await campaignRef.set(campaignData);

        // Store targets in subcollection with validated phone numbers
        const BATCH_SIZE = 400;
        for (let i = 0; i < targets.length; i += BATCH_SIZE) {
            const batch = db.batch();
            const chunk = targets.slice(i, i + BATCH_SIZE);
            chunk.forEach((t, idx) => {
                // Validate and clean each target phone
                let cleanPhone = String(t.phone).replace(/\D/g, '');
                if (cleanPhone.startsWith('0')) cleanPhone = cleanPhone.slice(1);
                if (!cleanPhone.startsWith('880')) cleanPhone = '880' + cleanPhone;
                
                const targetRef = campaignRef.collection('targets').doc(String(i + idx));
                batch.set(targetRef, {
                    phone: cleanPhone,
                    name: t.name || '',
                    status: 'pending'
                });
            });
            await batch.commit();
        }

        // Start campaign loop
        runCampaign(campaignId);

        // Firestore listener for pause/resume
        campaignRef.onSnapshot(snapshot => {
            const data = snapshot.data();
            if (!data) return;
            if (data.status === 'running') {
                console.log(`▶️ Campaign ${campaignId} resumed`);
                runCampaign(campaignId);
            }
        });

        res.json({ success: true, campaignId, totalTargets: targets.length });
    } catch (err) {
        res.status(500).json({ success: false, error: err.message });
    }
});

// 6. PAUSE CAMPAIGN
app.post('/api/campaign/pause', async (req, res) => {
    try {
        const { campaignId } = req.body;
        await db.collection('campaigns').doc(campaignId).update({ status: 'paused' });
        res.json({ success: true });
    } catch (err) {
        res.status(500).json({ success: false, error: err.message });
    }
});

// 7. RESUME CAMPAIGN
app.post('/api/campaign/resume', async (req, res) => {
    try {
        const { campaignId } = req.body;
        await db.collection('campaigns').doc(campaignId).update({ status: 'running' });
        res.json({ success: true });
    } catch (err) {
        res.status(500).json({ success: false, error: err.message });
    }
});

// 8. DISCONNECT ACCOUNT
app.delete('/api/disconnect/:id', async (req, res) => {
    try {
        const sock = sessions.get(req.params.id);
        if (sock) {
            await sock.logout();
            await sock.end();
        }
        sessions.delete(req.params.id);
        sessionStates.delete(req.params.id);
        sessionUserMap.delete(req.params.id);

        await db.collection('whatsapp_accounts').doc(req.params.id).update({
            status: 'disconnected',
            updatedAt: admin.firestore.FieldValue.serverTimestamp()
        });

        // Delete cloud session
        try {
            const [files] = await bucket.getFiles({ prefix: `sessions/${req.params.id}/` });
            for (const file of files) await file.delete();
        } catch (e) {
            // ignore
        }

        res.json({ success: true });
    } catch (err) {
        res.status(500).json({ success: false, error: err.message });
    }
});

// 8b. Additional disconnect endpoint for Replit frontend compatibility
app.delete('/api/accounts/:id', async (req, res) => {
    try {
        const sock = sessions.get(req.params.id);
        if (sock) {
            await sock.logout();
            await sock.end();
        }
        sessions.delete(req.params.id);
        sessionStates.delete(req.params.id);
        sessionUserMap.delete(req.params.id);

        await db.collection('whatsapp_accounts').doc(req.params.id).update({
            status: 'disconnected',
            updatedAt: admin.firestore.FieldValue.serverTimestamp()
        });

        // Delete cloud session
        try {
            const [files] = await bucket.getFiles({ prefix: `sessions/${req.params.id}/` });
            for (const file of files) await file.delete();
        } catch (e) {
            // ignore storage errors
        }

        res.json({ success: true, message: 'Device disconnected' });
    } catch (err) {
        res.status(500).json({ success: false, error: err.message });
    }
});

// 9. SET DAILY LIMIT
app.post('/api/limit/:id', async (req, res) => {
    const { limit } = req.body;
    dailyLimits.set(req.params.id, parseInt(limit) || 5);
    await db.collection('whatsapp_accounts').doc(req.params.id).update({ dailyLimit: parseInt(limit) || 5 });
    res.json({ success: true, dailyLimit: parseInt(limit) || 5 });
});

// 10. GET NOTIFICATIONS
app.get('/api/notifications/:userId', async (req, res) => {
    try {
        const snap = await db.collection('notifications')
            .where('userId', '==', req.params.userId)
            .orderBy('createdAt', 'desc')
            .limit(50)
            .get();

        const notifications = [];
        snap.forEach(doc => notifications.push({ id: doc.id, ...doc.data() }));
        res.json(notifications);
    } catch (err) {
        res.status(500).json({ success: false, error: err.message });
    }
});

// 11. MARK NOTIFICATION READ
app.post('/api/notifications/read', async (req, res) => {
    try {
        const { notificationId } = req.body;
        await db.collection('notifications').doc(notificationId).update({ read: true });
        res.json({ success: true });
    } catch (err) {
        res.status(500).json({ success: false, error: err.message });
    }
});

// ============================================
// MIDNIGHT CRON (reset todaySent)
// ============================================
cron.schedule('0 0 * * *', async () => {
    console.log('🕛 Midnight reset');
    todayCounts.clear();
    const batch = db.batch();
    const snap = await db.collection('whatsapp_accounts').get();
    snap.forEach(doc => batch.update(doc.ref, { todaySent: 0 }));
    await batch.commit();
}, {
    scheduled: true,
    timezone: "Asia/Dhaka"
});

// ============================================
// AUTO-SAVE SESSIONS EVERY 5 MINUTES
// ============================================
setInterval(async () => {
    for (const sessionId of sessions.keys()) {
        if (sessionStates.get(sessionId)?.status === 'connected') {
            await saveSessionToCloud(sessionId);
        }
    }
    console.log('💾 Sessions auto-saved to cloud');
}, 300000); // 5 minutes

// ============================================
// LOAD SESSIONS ON STARTUP
// ============================================
async function loadAllSessions() {
    try {
        const snap = await db.collection('whatsapp_accounts')
            .where('status', '==', 'connected')
            .get();

        for (const doc of snap.docs) {
            const data = doc.data();
            const sessionId = doc.id;
            const loaded = await loadSessionFromCloud(sessionId);

            if (loaded) {
                console.log(`📂 Restoring session: ${data.phone}`);
                sessionUserMap.set(sessionId, data.userId);
                await createSession(sessionId, data.phone, data.userId);
                await delay(2000);
            }
        }
        console.log('✅ All sessions loaded');
    } catch (err) {
        console.error('Session load error:', err.message);
    }
}

// ============================================
// SERVE DASHBOARD
// ============================================
app.get('*', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// ============================================
// START SERVER
// ============================================
app.listen(PORT, async () => {
    console.log(`🚀 WS-Income Engine v2.1 on port ${PORT}`);
    console.log(`📡 Firebase Storage Sessions | Anti-Duplicate | Message Detection`);
    await loadAllSessions();
});
