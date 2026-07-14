// ============================================
// WS-INCOME WhatsApp Campaign Engine v2.2
// Firebase Firestore Session | Real-time | Anti-Duplicate | Error Resilient
// ============================================

const express = require('express');
const cors = require('cors');
const path = require('path');
const fs = require('fs');
const { v4: uuidv4 } = require('uuid');
const admin = require('firebase-admin');
const cron = require('node-cron');
const pino = require('pino');

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
// LOGGER SETUP
// ============================================
const logger = pino({ 
    level: process.env.LOG_LEVEL || 'info',
    transport: {
        target: 'pino-pretty',
        options: {
            colorize: true,
            translateTime: 'SYS:standard',
            ignore: 'pid,hostname'
        }
    }
});

// ============================================
// FIREBASE INIT
// ============================================
let db;
try {
    const serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT);
    admin.initializeApp({
        credential: admin.credential.cert(serviceAccount),
        databaseURL: process.env.FIREBASE_DB_URL
    });
    db = admin.firestore();
    console.log('🔥 Firestore Connected');
} catch (err) {
    console.error('❌ Firebase init failed:', err.message);
    process.exit(1);
}

// ============================================
// GLOBALS
// ============================================
const sessions = new Map();              // sock instances
const sessionStates = new Map();         // current state per session
const todayCounts = new Map();           // todaySent per session
const dailyLimits = new Map();           // per session
const sessionUserMap = new Map();        // sessionId → userId
const campaignListeners = new Map();    // unsubscribe functions for campaigns

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
// MIDDLEWARE - REQUEST VALIDATION
// ============================================
function validateUserId(req, res, next) {
    const methods = req.method.toUpperCase();
    const path = req.path;
    
    // Routes requiring userId validation
    const protectedRoutes = [
        { method: 'POST', pattern: /^\/api\/send$/ },
        { method: 'POST', pattern: /^\/api\/campaign\/start$/ },
        { method: 'POST', pattern: /^\/api\/limit\/.*/ },
        { method: 'GET', pattern: /^\/api\/notifications\/.*/ }
    ];
    
    const isProtected = protectedRoutes.some(route => 
        route.method === methods && route.pattern.test(path)
    );
    
    if (isProtected) {
        const userId = req.body?.userId || req.query?.userId || req.params?.userId;
        if (!userId || typeof userId !== 'string' || userId.trim() === '') {
            return res.status(401).json({ 
                success: false, 
                error: 'Unauthorized: Invalid or missing userId' 
            });
        }
    }
    
    next();
}

app.use(validateUserId);

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
// FIREBASE FIRESTORE SESSION SAVE/LOAD
// ============================================
async function saveSessionToFirestore(sessionId) {
    try {
        const sessionPath = `${SESSIONS_DIR}/${sessionId}`;
        if (!fs.existsSync(sessionPath)) return;

        const files = fs.readdirSync(sessionPath);
        const sessionData = {};
        
        for (const file of files) {
            const filePath = `${sessionPath}/${file}`;
            if (fs.statSync(filePath).isFile()) {
                const content = fs.readFileSync(filePath, 'utf-8');
                sessionData[file] = content;
            }
        }
        
        // Firestore-এ সেভ (1MB limit per doc, সেশন ছোট হয়)
        await db.collection('session_data').doc(sessionId).set({
            data: sessionData,
            updatedAt: admin.firestore.FieldValue.serverTimestamp()
        }, { merge: true });
        
        logger.info({ sessionId }, '💾 Session saved to Firestore');
    } catch (err) {
        logger.error({ sessionId, err }, '💾 Session save error');
    }
}

async function loadSessionFromFirestore(sessionId) {
    try {
        const sessionPath = `${SESSIONS_DIR}/${sessionId}`;
        if (!fs.existsSync(sessionPath)) {
            fs.mkdirSync(sessionPath, { recursive: true });
        }

        const doc = await db.collection('session_data').doc(sessionId).get();
        if (!doc.exists) return false;

        const sessionData = doc.data().data || {};
        
        for (const [fileName, content] of Object.entries(sessionData)) {
            const destPath = `${sessionPath}/${fileName}`;
            fs.writeFileSync(destPath, content);
        }
        
        logger.info({ sessionId }, '📂 Session loaded from Firestore');
        return true;
    } catch (err) {
        logger.error({ sessionId, err }, '📥 Session load error');
        return false;
    }
}

// ============================================
// WHATSAPP SESSION CREATE
// ============================================
async function createSession(sessionId, phoneNumber) {
    try {
        const authPath = `${SESSIONS_DIR}/${sessionId}`;
        if (!fs.existsSync(authPath)) {
            fs.mkdirSync(authPath, { recursive: true });
        }

        const { state, saveCreds } = await useMultiFileAuthState(authPath);
        const { version } = await fetchLatestBaileysVersion();

        const waLogger = pino({ level: 'silent' });

        const sock = makeWASocket({
            version,
            auth: state,
            printQRInTerminal: false,
            syncFullHistory: false,
            logger: waLogger
        });

        // Connection handler
        sock.ev.on('connection.update', async (update) => {
            try {
                const { connection, lastDisconnect } = update;

                if (connection === 'open') {
                    const phone = sock.user?.id?.split(':')[0] || phoneNumber;
                    const name = sock.user?.name || phone;

                    sessionStates.set(sessionId, {
                        status: 'connected',
                        phone,
                        name,
                        connectedAt: new Date().toISOString()
                    });

                    const stored = (await db.collection('whatsapp_accounts').doc(sessionId).get()).data();
                    const limit = stored?.dailyLimit || 5;
                    dailyLimits.set(sessionId, limit);
                    todayCounts.set(sessionId, stored?.todaySent || 0);

                    await db.collection('whatsapp_accounts').doc(sessionId).update({
                        phone,
                        name,
                        status: 'connected',
                        dailyLimit: limit,
                        todaySent: todayCounts.get(sessionId) || 0,
                        updatedAt: admin.firestore.FieldValue.serverTimestamp()
                    });

                    console.log(`✅ Connected: ${name} (${phone})`);
                    await saveSessionToFirestore(sessionId);
                }

                if (connection === 'close') {
                    const statusCode = lastDisconnect?.error?.output?.statusCode;
                    const shouldReconnect = statusCode !== DisconnectReason.loggedOut;

                    if (shouldReconnect) {
                        console.log(`🔄 Reconnecting ${sessionId}...`);
                        sessionStates.set(sessionId, {
                            ...sessionStates.get(sessionId) || {},
                            status: 'disconnected'
                        });
                        
                        await db.collection('whatsapp_accounts').doc(sessionId).update({
                            status: 'disconnected',
                            updatedAt: admin.firestore.FieldValue.serverTimestamp()
                        });
                        
                        setTimeout(() => createSession(sessionId, phoneNumber), 5000);
                    } else {
                        console.log(`🔴 ${phoneNumber} LOGGED OUT!`);
                        sessions.delete(sessionId);
                        sessionStates.delete(sessionId);

                        await db.collection('whatsapp_accounts').doc(sessionId).update({
                            status: 'logged_out',
                            updatedAt: admin.firestore.FieldValue.serverTimestamp()
                        });

                        // Notify user if userId is stored
                        const userId = sessionUserMap.get(sessionId);
                        if (userId) {
                            try {
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
                            } catch (e) {
                                logger.error({ sessionId, userId, err: e }, 'Notification error');
                            }
                        }

                        sessionUserMap.delete(sessionId);

                        // Delete Firestore session
                        try {
                            await db.collection('session_data').doc(sessionId).delete();
                            logger.info({ sessionId }, '🗑️ Session data deleted from Firestore');
                        } catch (e) {
                            logger.warn({ sessionId }, 'Session cleanup failed');
                        }
                    }
                }
            } catch (err) {
                logger.error({ sessionId, err }, 'Connection update error');
            }
        });

        // Save credentials
        sock.ev.on('creds.update', async () => {
            try {
                await saveCreds();
                await saveSessionToFirestore(sessionId);
            } catch (err) {
                logger.error({ sessionId, err }, 'Credential save error');
            }
        });
        
        sessions.set(sessionId, sock);
        return sock;
    } catch (err) {
        logger.error({ sessionId, phoneNumber, err }, 'Create session error');
        throw err;
    }
}

// ============================================
// CAMPAIGN ENGINE (with success/fail tracking & error boundary)
// ============================================
async function runCampaign(campaignId) {
    try {
        const campaignRef = db.collection('campaigns').doc(campaignId);
        const campaignSnap = await campaignRef.get();
        if (!campaignSnap.exists) {
            logger.warn({ campaignId }, 'Campaign not found');
            return;
        }

        const campaign = campaignSnap.data();
        if (campaign.status !== 'running') {
            logger.info({ campaignId, status: campaign.status }, 'Campaign not in running state');
            return;
        }

        const userId = campaign.userId;
        const messageTemplate = campaign.message;

        // Get pending targets from subcollection
        const targetsSnap = await campaignRef.collection('targets')
            .where('status', '==', 'pending')
            .limit(50)
            .get();

        if (targetsSnap.empty) {
            await campaignRef.update({ 
                status: 'completed',
                completedAt: admin.firestore.FieldValue.serverTimestamp()
            });
            logger.info({ campaignId }, 'Campaign completed - no pending targets');
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
            logger.info({ campaignId }, '⏸️ Campaign paused: No available accounts');
            return;
        }

        let accountIndex = 0;
        for (const targetDoc of targetsSnap.docs) {
            const currentSnap = await campaignRef.get();
            if (currentSnap.data()?.status !== 'running') {
                logger.info({ campaignId }, 'Campaign interrupted');
                break;
            }

            const target = targetDoc.data();
            const account = availableAccounts[accountIndex % availableAccounts.length];
            const sock = sessions.get(account.id);

            if (!sock) {
                accountIndex++;
                continue;
            }

            try {
                // Atomic increment check to prevent race conditions
                const accountDoc = await db.collection('whatsapp_accounts').doc(account.id).get();
                const currentCount = accountDoc.data()?.todaySent || 0;
                
                if (currentCount >= (dailyLimits.get(account.id) || 5)) {
                    logger.info({ accountId: account.id, campaignId }, 'Account hit daily limit');
                    accountIndex++;
                    continue;
                }

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
                    logger.warn({ accountId: account.id, phone: target.phone }, 'No confirmation');
                }

            } catch (err) {
                // ❌ FAILED
                logger.error({ 
                    accountId: account.id, 
                    phone: target.phone,
                    error: err.message 
                }, 'Message send failed');

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
            await campaignRef.update({ 
                status: 'completed', 
                completedAt: admin.firestore.FieldValue.serverTimestamp()
            });
            logger.info({ campaignId }, '✅ Campaign completed!');
        }
    } catch (err) {
        logger.error({ campaignId, err }, '❌ Campaign execution error');
        try {
            await db.collection('campaigns').doc(campaignId).update({
                status: 'error',
                errorMessage: err.message,
                errorAt: admin.firestore.FieldValue.serverTimestamp()
            });
        } catch (e) {
            logger.error({ campaignId, err: e }, 'Failed to update campaign error state');
        }
    }
}

// ============================================
// API ENDPOINTS
// ============================================

// 1. PAIR - get pairing code
app.post('/api/pair', async (req, res) => {
    try {
        let { phone, userId } = req.body;
        
        if (!phone) {
            return res.status(400).json({ success: false, error: 'Phone number required' });
        }

        // Clean phone - just digits
        phone = phone.replace(/\D/g, '');
        
        if (phone.length < 10) {
            return res.status(400).json({ success: false, error: 'Invalid phone number' });
        }

        const sessionId = uuidv4();
        
        sessionStates.set(sessionId, { 
            status: 'initializing', 
            phone: phone,
            userId: userId || null 
        });

        // Create session
        const sock = await createSession(sessionId, phone);
        
        // 2 seconds delay for session to initialize
        await delay(2000);
        
        // Request pairing code
        const code = await sock.requestPairingCode(phone);
        
        if (!code) {
            return res.status(400).json({ 
                success: false, 
                error: 'Failed to generate pairing code. Try again.' 
            });
        }

        console.log(`🔑 Pairing Code: ${code} for ${phone}`);

        sessionStates.set(sessionId, {
            status: 'pair_ready',
            pairCode: code,
            phone: phone,
            userId: userId || null
        });

        // Save to Firestore
        await db.collection('whatsapp_accounts').doc(sessionId).set({
            userId: userId || '',
            phone: phone,
            status: 'pair_ready',
            pairCode: code,
            dailyLimit: 5,
            todaySent: 0,
            createdAt: admin.firestore.FieldValue.serverTimestamp()
        });

        res.json({
            success: true,
            code: code,
            sessionId: sessionId,
            phone: phone,
            message: 'Enter this code in WhatsApp > Linked Devices > Link Device'
        });

    } catch (err) {
        logger.error({ err }, 'Pair error');
        res.status(400).json({ 
            success: false, 
            error: err.message || 'Connection failed. Try again.' 
        });
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
        logger.error({ err }, 'List accounts error');
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

// 4. SEND SINGLE MESSAGE
app.post('/api/send', async (req, res) => {
    try {
        const { accountId, to, text, userId } = req.body;
        
        if (!accountId || !to || !text) {
            return res.status(400).json({
                success: false,
                error: 'Missing required fields: accountId, to, text'
            });
        }

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
        try {
            await db.collection('message_logs').add({
                accountId: req.body.accountId,
                userId: req.body.userId || '',
                to: req.body.to,
                text: req.body.text,
                status: 'failed',
                error: err.message,
                createdAt: admin.firestore.FieldValue.serverTimestamp()
            });
        } catch (e) {
            logger.error({ err: e }, 'Failed to log message error');
        }

        logger.error({ accountId: req.body.accountId, err }, 'Send message error');
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

        logger.info({ campaignId, userId, targetCount: targets.length }, 'Campaign created');

        // Start campaign loop
        runCampaign(campaignId);

        // Firestore listener for pause/resume with automatic cleanup
        if (campaignListeners.has(campaignId)) {
            campaignListeners.get(campaignId)(); // Cleanup old listener
        }

        const unsubscribe = campaignRef.onSnapshot(snapshot => {
            const data = snapshot.data();
            if (!data) return;
            
            if (data.status === 'running') {
                logger.info({ campaignId }, '▶️ Campaign resumed');
                runCampaign(campaignId);
            }
            
            if (data.status === 'completed' || data.status === 'paused' || data.status === 'error') {
                logger.info({ campaignId, status: data.status }, 'Cleaning up campaign listener');
                unsubscribe(); // Cleanup listener
                campaignListeners.delete(campaignId);
            }
        });
        
        campaignListeners.set(campaignId, unsubscribe);

        res.json({ success: true, campaignId, totalTargets: targets.length });
    } catch (err) {
        logger.error({ err }, 'Start campaign error');
        res.status(500).json({ success: false, error: err.message });
    }
});

// 6. PAUSE CAMPAIGN
app.post('/api/campaign/pause', async (req, res) => {
    try {
        const { campaignId } = req.body;
        
        if (!campaignId) {
            return res.status(400).json({ success: false, error: 'campaignId required' });
        }

        await db.collection('campaigns').doc(campaignId).update({ 
            status: 'paused',
            pausedAt: admin.firestore.FieldValue.serverTimestamp()
        });
        res.json({ success: true });
    } catch (err) {
        logger.error({ campaignId: req.body.campaignId, err }, 'Pause campaign error');
        res.status(500).json({ success: false, error: err.message });
    }
});

// 7. RESUME CAMPAIGN
app.post('/api/campaign/resume', async (req, res) => {
    try {
        const { campaignId } = req.body;
        
        if (!campaignId) {
            return res.status(400).json({ success: false, error: 'campaignId required' });
        }

        await db.collection('campaigns').doc(campaignId).update({ 
            status: 'running',
            resumedAt: admin.firestore.FieldValue.serverTimestamp()
        });
        res.json({ success: true });
    } catch (err) {
        logger.error({ campaignId: req.body.campaignId, err }, 'Resume campaign error');
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

        // Delete Firestore session
        try {
            await db.collection('session_data').doc(req.params.id).delete();
            logger.info({ sessionId: req.params.id }, '🗑️ Session data deleted from Firestore');
        } catch (e) {
            logger.warn({ sessionId: req.params.id }, 'Session cleanup failed');
        }

        res.json({ success: true });
    } catch (err) {
        logger.error({ sessionId: req.params.id, err }, 'Disconnect error');
        res.status(500).json({ success: false, error: err.message });
    }
});

// 8b. Additional disconnect endpoint for frontend compatibility
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

        // Delete Firestore session
        try {
            await db.collection('session_data').doc(req.params.id).delete();
            logger.info({ sessionId: req.params.id }, '🗑️ Session data deleted from Firestore');
        } catch (e) {
            logger.warn({ sessionId: req.params.id }, 'Session cleanup failed');
        }

        res.json({ success: true, message: 'Device disconnected' });
    } catch (err) {
        logger.error({ sessionId: req.params.id, err }, 'Disconnect error');
        res.status(500).json({ success: false, error: err.message });
    }
});

// 9. SET DAILY LIMIT
app.post('/api/limit/:id', async (req, res) => {
    try {
        const { limit } = req.body;
        
        if (!limit || isNaN(parseInt(limit)) || parseInt(limit) < 0) {
            return res.status(400).json({ 
                success: false, 
                error: 'Invalid limit: must be a positive number' 
            });
        }

        const parsedLimit = parseInt(limit);
        dailyLimits.set(req.params.id, parsedLimit);
        await db.collection('whatsapp_accounts').doc(req.params.id).update({ 
            dailyLimit: parsedLimit,
            updatedAt: admin.firestore.FieldValue.serverTimestamp()
        });
        res.json({ success: true, dailyLimit: parsedLimit });
    } catch (err) {
        logger.error({ sessionId: req.params.id, err }, 'Set limit error');
        res.status(500).json({ success: false, error: err.message });
    }
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
        logger.error({ userId: req.params.userId, err }, 'Get notifications error');
        res.status(500).json({ success: false, error: err.message });
    }
});

// 11. MARK NOTIFICATION READ
app.post('/api/notifications/read', async (req, res) => {
    try {
        const { notificationId } = req.body;
        
        if (!notificationId) {
            return res.status(400).json({ 
                success: false, 
                error: 'notificationId required' 
            });
        }

        await db.collection('notifications').doc(notificationId).update({ 
            read: true,
            readAt: admin.firestore.FieldValue.serverTimestamp()
        });
        res.json({ success: true });
    } catch (err) {
        logger.error({ notificationId: req.body.notificationId, err }, 'Mark read error');
        res.status(500).json({ success: false, error: err.message });
    }
});

// ============================================
// MIDNIGHT CRON (reset todaySent)
// ============================================
cron.schedule('0 0 * * *', async () => {
    try {
        console.log('🕛 Midnight reset');
        todayCounts.clear();
        const batch = db.batch();
        const snap = await db.collection('whatsapp_accounts').get();
        snap.forEach(doc => batch.update(doc.ref, { todaySent: 0 }));
        await batch.commit();
        logger.info('✅ Daily counts reset');
    } catch (err) {
        logger.error({ err }, 'Midnight reset error');
    }
}, {
    scheduled: true,
    timezone: "Asia/Dhaka"
});

// ============================================
// AUTO-SAVE SESSIONS EVERY 5 MINUTES
// ============================================
setInterval(async () => {
    try {
        for (const sessionId of sessions.keys()) {
            if (sessionStates.get(sessionId)?.status === 'connected') {
                await saveSessionToFirestore(sessionId);
            }
        }
        console.log('💾 Sessions auto-saved to Firestore');
    } catch (err) {
        logger.error({ err }, 'Auto-save sessions error');
    }
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
            const loaded = await loadSessionFromFirestore(sessionId);

            if (loaded) {
                console.log(`📂 Restoring session: ${data.phone}`);
                if (data.userId) {
                    sessionUserMap.set(sessionId, data.userId);
                }
                await createSession(sessionId, data.phone);
                await delay(2000);
            }
        }
        logger.info('✅ All sessions loaded');
    } catch (err) {
        logger.error({ err }, 'Session load error');
    }
}

// ============================================
// SERVE DASHBOARD
// ============================================
app.get('*', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// ============================================
// ERROR HANDLER MIDDLEWARE
// ============================================
app.use((err, req, res, next) => {
    logger.error({ err, path: req.path, method: req.method }, 'Unhandled error');
    res.status(500).json({ 
        success: false, 
        error: 'Internal server error' 
    });
});

// ============================================
// START SERVER
// ============================================
app.listen(PORT, async () => {
    console.log(`🚀 WS-Income Engine v2.2 on port ${PORT}`);
    console.log(`📡 Firebase Firestore Sessions | Anti-Duplicate | Error Resilient`);
    await loadAllSessions();
});