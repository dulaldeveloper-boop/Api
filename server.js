// ============================================
// WS-INCOME WhatsApp Campaign Engine v3.0
// Firebase Realtime Database | Real-time | Anti-Duplicate | Error Resilient
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
// ✅ FIREBASE REALTIME DATABASE INIT
// ============================================
let db;
try {
    const serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT);
    admin.initializeApp({
        credential: admin.credential.cert(serviceAccount),
        databaseURL: process.env.FIREBASE_DB_URL || "https://ws-income-official-default-rtdb.asia-southeast1.firebasedatabase.app"
    });
    db = admin.database();
    console.log('🔥 Realtime Database Connected');
} catch (err) {
    console.error('❌ Firebase init failed:', err.message);
    process.exit(1);
}

// ============================================
// GLOBALS
// ============================================
const sessions = new Map();
const sessionStates = new Map();
const todayCounts = new Map();
const dailyLimits = new Map();
const sessionUserMap = new Map();
const campaignListeners = new Map();
const campaignQueues = new Map(); // Campaign message queues
const activeCampaigns = new Map(); // Currently running campaigns

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
// MIDDLEWARE
// ============================================
function validateUserId(req, res, next) {
    const methods = req.method.toUpperCase();
    const path = req.path;
    
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

function generateId() {
    return uuidv4().replace(/-/g, '').substring(0, 20);
}

// ============================================
// ✅ REALTIME DB SESSION SAVE/LOAD
// ============================================
async function saveSessionToDB(sessionId) {
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
        
        await db.ref('session_data/' + sessionId).set({
            data: sessionData,
            updatedAt: admin.database.ServerValue.TIMESTAMP
        });
        
        logger.info({ sessionId }, '💾 Session saved to Realtime DB');
    } catch (err) {
        logger.error({ sessionId, err }, '💾 Session save error');
    }
}

async function loadSessionFromDB(sessionId) {
    try {
        const sessionPath = `${SESSIONS_DIR}/${sessionId}`;
        if (!fs.existsSync(sessionPath)) {
            fs.mkdirSync(sessionPath, { recursive: true });
        }

        const snap = await db.ref('session_data/' + sessionId).once('value');
        if (!snap.exists()) return false;

        const sessionData = snap.val().data || {};
        
        for (const [fileName, content] of Object.entries(sessionData)) {
            const destPath = `${sessionPath}/${fileName}`;
            fs.writeFileSync(destPath, content);
        }
        
        logger.info({ sessionId }, '📂 Session loaded from Realtime DB');
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

                    const storedSnap = await db.ref('whatsapp_accounts/' + sessionId).once('value');
                    const stored = storedSnap.val() || {};
                    const limit = stored.dailyLimit || 5;
                    dailyLimits.set(sessionId, limit);
                    todayCounts.set(sessionId, stored.todaySent || 0);

                    await db.ref('whatsapp_accounts/' + sessionId).update({
                        phone,
                        name,
                        status: 'connected',
                        dailyLimit: limit,
                        todaySent: todayCounts.get(sessionId) || 0,
                        updatedAt: admin.database.ServerValue.TIMESTAMP
                    });

                    // Update user stats
                    const userId = sessionUserMap.get(sessionId) || stored.userId;
                    if (userId) {
                        await updateUserDeviceStats(userId);
                    }

                    console.log(`✅ Connected: ${name} (${phone})`);
                    await saveSessionToDB(sessionId);
                    
                    // Check for pending campaigns
                    if (userId) {
                        checkAndRunCampaigns(userId);
                    }
                }

                if (connection === 'close') {
                    const statusCode = lastDisconnect?.error?.output?.statusCode;
                    const shouldReconnect = statusCode !== DisconnectReason.loggedOut;

                    if (shouldReconnect) {
                        console.log(`🔄 Reconnecting ${sessionId}...`);
                        sessionStates.set(sessionId, {
                            ...sessionStates.get(sessionId) || {},
                            status: 'reconnecting'
                        });
                        
                        await db.ref('whatsapp_accounts/' + sessionId).update({
                            status: 'reconnecting',
                            updatedAt: admin.database.ServerValue.TIMESTAMP
                        });
                        
                        setTimeout(() => createSession(sessionId, phoneNumber), 5000);
                    } else {
                        console.log(`🔴 ${phoneNumber} LOGGED OUT!`);
                        sessions.delete(sessionId);
                        sessionStates.delete(sessionId);

                        await db.ref('whatsapp_accounts/' + sessionId).update({
                            status: 'logged_out',
                            updatedAt: admin.database.ServerValue.TIMESTAMP
                        });

                        const userId = sessionUserMap.get(sessionId);
                        if (userId) {
                            await updateUserDeviceStats(userId);
                            
                            const notifRef = db.ref('notifications').push();
                            await notifRef.set({
                                userId: userId,
                                type: 'account_logged_out',
                                title: '⚠️ WhatsApp Account Logged Out',
                                message: `Account ${phoneNumber} has been logged out. Please reconnect.`,
                                sessionId: sessionId,
                                phone: phoneNumber,
                                read: false,
                                createdAt: admin.database.ServerValue.TIMESTAMP
                            });
                        }

                        sessionUserMap.delete(sessionId);

                        try {
                            await db.ref('session_data/' + sessionId).remove();
                            logger.info({ sessionId }, '🗑️ Session data deleted');
                        } catch (e) {
                            logger.warn({ sessionId }, 'Session cleanup failed');
                        }
                    }
                }
            } catch (err) {
                logger.error({ sessionId, err }, 'Connection update error');
            }
        });

        sock.ev.on('creds.update', async () => {
            try {
                await saveCreds();
                await saveSessionToDB(sessionId);
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
// ✅ UPDATE USER DEVICE STATS
// ============================================
async function updateUserDeviceStats(userId) {
    try {
        const accountsSnap = await db.ref('whatsapp_accounts')
            .orderByChild('userId')
            .equalTo(userId)
            .once('value');
        
        let total = 0, online = 0, offline = 0;
        accountsSnap.forEach((child) => {
            total++;
            const acc = child.val();
            if (acc.status === 'connected') online++;
            else if (acc.status === 'logged_out' || acc.status === 'disconnected') offline++;
        });
        
        await db.ref('users/' + userId).update({
            totalAccounts: total,
            onlineAccounts: online,
            offlineAccounts: offline,
            updatedAt: admin.database.ServerValue.TIMESTAMP
        });
    } catch (err) {
        logger.error({ userId, err }, 'Update user stats error');
    }
}

// ============================================
// ✅ CHECK AND RUN CAMPAIGNS FOR USER
// ============================================
async function checkAndRunCampaigns(userId) {
    try {
        const campaignsSnap = await db.ref('campaigns')
            .orderByChild('userId')
            .equalTo(userId)
            .once('value');
        
        campaignsSnap.forEach((child) => {
            const campaign = child.val();
            if (campaign.status === 'running' && !activeCampaigns.has(child.key)) {
                logger.info({ campaignId: child.key }, '▶️ Auto-starting campaign');
                runCampaign(child.key);
            }
        });
    } catch (err) {
        logger.error({ userId, err }, 'Check campaigns error');
    }
}

// ============================================
// ✅ CAMPAIGN ENGINE v3.0 (Realtime DB)
// ============================================
// ============================================
// ✅ CAMPAIGN ENGINE v3.1 (Join Check + Anti-Ban Delay + Progress)
// ============================================
async function runCampaign(campaignId) {
    if (activeCampaigns.has(campaignId)) {
        logger.info({ campaignId }, 'Campaign already running');
        return;
    }
    
    activeCampaigns.set(campaignId, true);
    
    try {
        const campaignSnap = await db.ref('campaigns/' + campaignId).once('value');
        if (!campaignSnap.exists()) {
            activeCampaigns.delete(campaignId);
            return;
        }

        const campaign = campaignSnap.val();
        if (campaign.status !== 'running') {
            logger.info({ campaignId, status: campaign.status }, 'Campaign not running');
            activeCampaigns.delete(campaignId);
            return;
        }

        const userId = campaign.userId;
        const messageTemplate = campaign.message;
        
        // Get targets from campaign_targets
        const targetsSnap = await db.ref('campaign_targets/' + campaignId)
            .orderByChild('status')
            .equalTo('pending')
            .limitToFirst(50)
            .once('value');

        if (!targetsSnap.exists()) {
            await db.ref('campaigns/' + campaignId).update({ 
                status: 'completed',
                completedAt: admin.database.ServerValue.TIMESTAMP
            });
            activeCampaigns.delete(campaignId);
            logger.info({ campaignId }, '✅ Campaign completed');
            return;
        }

        // ✅ JOIN CHECK: শুধু Join করা users-দের account নাও
        const progressSnap = await db.ref('campaign_progress')
            .orderByChild('campaignId')
            .equalTo(campaignId)
            .once('value');

        const joinedAccounts = [];
        if (progressSnap.exists()) {
            progressSnap.forEach((child) => {
                const prog = child.val();
                if (prog.status === 'active' && prog.accountId) {
                    joinedAccounts.push({ 
                        id: prog.accountId, 
                        progressKey: child.key,
                        todaySent: 0, 
                        dailyLimit: 5 
                    });
                }
            });
        }

        if (joinedAccounts.length === 0) {
            logger.info({ campaignId }, '⏸️ No joined accounts available, waiting...');
    // ✅ শুধু return, paused সেট করবে না!
            activeCampaigns.delete(campaignId);
            return;
        }
        // Get full account details for joined accounts
        const availableAccounts = [];
        for (const acc of joinedAccounts) {
            const deviceSnap = await db.ref('whatsapp_accounts/' + acc.id).once('value');
            const device = deviceSnap.val();
            if (device && device.status === 'connected' && sessions.has(acc.id)) {
                const todaySent = todayCounts.get(acc.id) || device.todaySent || 0;
                const dailyLimit = dailyLimits.get(acc.id) || device.dailyLimit || 5;
                if (todaySent < dailyLimit) {
                    availableAccounts.push({ 
                        id: acc.id, 
                        progressKey: acc.progressKey, 
                        todaySent, 
                        dailyLimit 
                    });
                }
            }
        }

        if (availableAccounts.length === 0) {
            logger.info({ campaignId }, '⏸️ No available devices (all offline or limit reached)');
            activeCampaigns.delete(campaignId);
            return;
        }

        logger.info({ campaignId, joinedCount: joinedAccounts.length, availableCount: availableAccounts.length }, '▶️ Campaign running');

        let accountIndex = 0;
        const targets = [];
        targetsSnap.forEach((child) => {
            targets.push({ id: child.key, ...child.val() });
        });

        for (const target of targets) {
            // Check campaign status
            const currentSnap = await db.ref('campaigns/' + campaignId).once('value');
            if (!currentSnap.exists() || currentSnap.val().status !== 'running') {
                logger.info({ campaignId }, 'Campaign stopped');
                break;
            }

            const account = availableAccounts[accountIndex % availableAccounts.length];
            const sock = sessions.get(account.id);

            if (!sock) {
                accountIndex++;
                continue;
            }

            // Check daily limit
            const currentCount = todayCounts.get(account.id) || 0;
            if (currentCount >= (dailyLimits.get(account.id) || 5)) {
                accountIndex++;
                continue;
            }

            try {
                const jidTarget = jid(target.phone);
                const msg = messageTemplate
                    .replace(/\{name\}/g, target.name || '')
                    .replace(/\{phone\}/g, target.phone || '');
                
                const result = await sock.sendMessage(jidTarget, { text: msg });

                if (result?.key?.id) {
                    // ✅ SUCCESS
                    await db.ref('campaign_targets/' + campaignId + '/' + target.id).update({
                        status: 'sent',
                        sentAt: admin.database.ServerValue.TIMESTAMP,
                        messageId: result.key.id,
                        sentBy: account.id
                    });

                    const newSentCount = (campaign.sentCount || 0) + 1;
                    await db.ref('campaigns/' + campaignId).update({
                        sentCount: newSentCount
                    });
                    campaign.sentCount = newSentCount;

                    // Update account count
                    const newCount = currentCount + 1;
                    todayCounts.set(account.id, newCount);
                    await db.ref('whatsapp_accounts/' + account.id).update({
                        todaySent: newCount,
                        updatedAt: admin.database.ServerValue.TIMESTAMP
                    });

                    // Update user balance
                    if (campaign.pricePerMessage > 0 && userId) {
                        const userSnap = await db.ref('users/' + userId).once('value');
                        const userData = userSnap.val() || {};
                        await db.ref('users/' + userId).update({
                            balance: (userData.balance || 0) + campaign.pricePerMessage,
                            totalEarned: (userData.totalEarned || 0) + campaign.pricePerMessage,
                            todayTotalSent: (userData.todayTotalSent || 0) + 1
                        });
                    }

                    // ✅ Update campaign_progress for this user
                    if (account.progressKey) {
                        await db.ref('campaign_progress/' + account.progressKey).update({
                            sentCount: admin.database.ServerValue.increment(1),
                            earned: admin.database.ServerValue.increment(campaign.pricePerMessage || 0),
                            updatedAt: admin.database.ServerValue.TIMESTAMP
                        });
                    }

                    console.log(`✅ [${account.id}] Sent to ${target.phone} (${newSentCount}/${campaign.totalTargets})`);
                } else {
                    await db.ref('campaign_targets/' + campaignId + '/' + target.id).update({
                        status: 'failed',
                        error: 'No message ID returned'
                    });
                    
                    await db.ref('campaigns/' + campaignId).update({
                        failedCount: (campaign.failedCount || 0) + 1
                    });
                }

            } catch (err) {
                logger.error({ accountId: account.id, phone: target.phone, error: err.message }, 'Send failed');

                await db.ref('campaign_targets/' + campaignId + '/' + target.id).update({
                    status: 'failed',
                    error: err.message
                });

                await db.ref('campaigns/' + campaignId).update({
                    failedCount: (campaign.failedCount || 0) + 1
                });

                if (err.message?.includes('logged out') || err.message?.includes('disconnected')) {
                    await db.ref('whatsapp_accounts/' + account.id).update({
                        status: 'logged_out',
                        updatedAt: admin.database.ServerValue.TIMESTAMP
                    });
                    sessions.delete(account.id);
                    sessionStates.delete(account.id);
                    
                    if (userId) await updateUserDeviceStats(userId);
                }
            }

            accountIndex++;
            
            // ✅ RANDOM DELAY 20-60 seconds (Anti-Ban)
            const randomDelay = Math.floor(Math.random() * 40000) + 20000;
            logger.info({ campaignId, delay: (randomDelay/1000).toFixed(1) }, '⏳ Waiting before next message');
            await delay(randomDelay);
        }

        // Check if completed
        const pendingSnap = await db.ref('campaign_targets/' + campaignId)
            .orderByChild('status')
            .equalTo('pending')
            .limitToFirst(1)
            .once('value');

        if (!pendingSnap.exists()) {
            await db.ref('campaigns/' + campaignId).update({ 
                status: 'completed', 
                completedAt: admin.database.ServerValue.TIMESTAMP
            });
            logger.info({ campaignId }, '🎉 Campaign completed!');
            
            // Notify user
            if (userId) {
                await db.ref('notifications').push().set({
                    userId: userId,
                    type: 'campaign_completed',
                    title: '🎉 Campaign Completed!',
                    message: `Campaign "${campaign.name}" has been completed. ${campaign.sentCount} messages sent.`,
                    read: false,
                    createdAt: admin.database.ServerValue.TIMESTAMP
                });
            }
        }
    } catch (err) {
        logger.error({ campaignId, err }, '❌ Campaign error');
        try {
            await db.ref('campaigns/' + campaignId).update({
                status: 'error',
                errorMessage: err.message,
                errorAt: admin.database.ServerValue.TIMESTAMP
            });
        } catch (e) {}
    } finally {
        activeCampaigns.delete(campaignId);
    }
}

// ============================================
// ✅ LISTEN FOR CAMPAIGN CHANGES (REAL-TIME)
// ============================================
db.ref('campaigns').on('child_changed', (snapshot) => {
    const campaign = snapshot.val();
    const campaignId = snapshot.key;
    
    // ✅ Only run if status changed TO running (not already running)
    if (campaign.status === 'running' && !activeCampaigns.has(campaignId)) {
        logger.info({ campaignId }, '▶️ Campaign status changed to running');
        runCampaign(campaignId);
    }
    
    // ✅ If paused/completed/error - just log, don't re-run
    if (campaign.status === 'paused' || campaign.status === 'completed' || campaign.status === 'error') {
        logger.info({ campaignId, status: campaign.status }, 'ℹ️ Campaign status updated');
    }
});

db.ref('campaigns').on('child_added', (snapshot) => {
    const campaign = snapshot.val();
    const campaignId = snapshot.key;
    
    if (campaign.status === 'running' && !activeCampaigns.has(campaignId)) {
        logger.info({ campaignId }, '🆕 New campaign detected');
        runCampaign(campaignId);
    }
});

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

        phone = phone.replace(/\D/g, '');
        
        if (phone.length < 10) {
            return res.status(400).json({ success: false, error: 'Invalid phone number' });
        }

        // Check if phone already connected to another user
        const existingSnap = await db.ref('whatsapp_accounts')
            .orderByChild('phone')
            .equalTo(phone)
            .once('value');
        
        if (existingSnap.exists()) {
            let existingUserId = null;
            existingSnap.forEach((child) => {
                existingUserId = child.val().userId;
            });
            
            if (existingUserId && existingUserId !== userId) {
                return res.status(400).json({
                    success: false,
                    error: 'This number is already connected to another account!'
                });
            }
        }

        const sessionId = generateId();
        
        if (userId) {
            sessionUserMap.set(sessionId, userId);
        }
        
        sessionStates.set(sessionId, { 
            status: 'initializing', 
            phone: phone,
            userId: userId || null 
        });

        const sock = await createSession(sessionId, phone);
        
        await delay(2000);
        
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

        await db.ref('whatsapp_accounts/' + sessionId).set({
            userId: userId || '',
            phone: phone,
            status: 'pair_ready',
            pairCode: code,
            dailyLimit: 5,
            todaySent: 0,
            createdAt: admin.database.ServerValue.TIMESTAMP
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

// 2. ACCOUNT STATUS
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

// 3. DISCONNECT
app.delete('/api/disconnect/:id', async (req, res) => {
    try {
        const sock = sessions.get(req.params.id);
        if (sock) {
            await sock.logout();
            await sock.end();
        }
        sessions.delete(req.params.id);
        sessionStates.delete(req.params.id);
        const userId = sessionUserMap.get(req.params.id);
        sessionUserMap.delete(req.params.id);

        await db.ref('whatsapp_accounts/' + req.params.id).update({
            status: 'disconnected',
            updatedAt: admin.database.ServerValue.TIMESTAMP
        });

        if (userId) await updateUserDeviceStats(userId);

        try {
            await db.ref('session_data/' + req.params.id).remove();
        } catch (e) {}

        res.json({ success: true });
    } catch (err) {
        logger.error({ sessionId: req.params.id, err }, 'Disconnect error');
        res.status(500).json({ success: false, error: err.message });
    }
});

// 4. SET DAILY LIMIT
app.post('/api/limit/:id', async (req, res) => {
    try {
        const { limit } = req.body;
        const parsedLimit = parseInt(limit);
        
        if (!limit || parsedLimit < 0) {
            return res.status(400).json({ success: false, error: 'Invalid limit' });
        }

        dailyLimits.set(req.params.id, parsedLimit);
        await db.ref('whatsapp_accounts/' + req.params.id).update({ 
            dailyLimit: parsedLimit,
            updatedAt: admin.database.ServerValue.TIMESTAMP
        });
        res.json({ success: true, dailyLimit: parsedLimit });
    } catch (err) {
        res.status(500).json({ success: false, error: err.message });
    }
});

// 5. START CAMPAIGN
app.post('/api/campaign/start', async (req, res) => {
    try {
        const { userId, name, message, targets, pricePerMessage } = req.body;
        if (!userId || !targets || targets.length === 0) {
            return res.status(400).json({ success: false, error: 'Missing fields' });
        }

        const campaignId = generateId();
        const now = new Date().toISOString();

        await db.ref('campaigns/' + campaignId).set({
            userId,
            name,
            message,
            totalTargets: targets.length,
            sentCount: 0,
            failedCount: 0,
            status: 'running',
            pricePerMessage: pricePerMessage || 0,
            createdAt: now
        });

        // Save targets
        const targetUpdates = {};
        targets.forEach((t, idx) => {
            let cleanPhone = String(t.phone).replace(/\D/g, '');
            if (cleanPhone.startsWith('0')) cleanPhone = cleanPhone.slice(1);
            if (!cleanPhone.startsWith('880')) cleanPhone = '880' + cleanPhone;
            
            targetUpdates['campaign_targets/' + campaignId + '/' + idx] = {
                phone: cleanPhone,
                name: t.name || '',
                status: 'pending',
                createdAt: now
            };
        });
        
        await db.ref().update(targetUpdates);

        logger.info({ campaignId, userId, targetCount: targets.length }, 'Campaign created');

        // Start campaign
        runCampaign(campaignId);

        res.json({ success: true, campaignId, totalTargets: targets.length });
    } catch (err) {
        logger.error({ err }, 'Start campaign error');
        res.status(500).json({ success: false, error: err.message });
    }
});

// 6. SEND SINGLE MESSAGE
app.post('/api/send', async (req, res) => {
    try {
        const { accountId, to, text, userId } = req.body;
        
        if (!accountId || !to || !text) {
            return res.status(400).json({ success: false, error: 'Missing fields' });
        }

        const sock = sessions.get(accountId);
        if (!sock || sessionStates.get(accountId)?.status !== 'connected') {
            return res.status(400).json({ success: false, error: 'Account not connected' });
        }

        let cleanTo = String(to).replace(/\D/g, '');
        if (cleanTo.startsWith('0')) cleanTo = cleanTo.slice(1);
        if (!cleanTo.startsWith('880')) cleanTo = '880' + cleanTo;
        
        const jidTo = `${cleanTo}@s.whatsapp.net`;
        const result = await sock.sendMessage(jidTo, { text });

        if (!result?.key?.id) {
            return res.status(400).json({ success: false, error: 'Message not confirmed' });
        }

        const newCount = (todayCounts.get(accountId) || 0) + 1;
        todayCounts.set(accountId, newCount);
        await db.ref('whatsapp_accounts/' + accountId).update({
            todaySent: newCount,
            updatedAt: admin.database.ServerValue.TIMESTAMP
        });

        await db.ref('message_logs').push().set({
            accountId, userId: userId || '', to: jidTo, text,
            status: 'sent', messageId: result.key.id,
            createdAt: admin.database.ServerValue.TIMESTAMP
        });

        res.json({ success: true, messageId: result.key.id, todaySent: newCount });
    } catch (err) {
        res.status(400).json({ success: false, error: err.message });
    }
});

// ============================================
// MIDNIGHT CRON (reset todaySent)
// ============================================
cron.schedule('0 0 * * *', async () => {
    try {
        console.log('🕛 Midnight reset');
        todayCounts.clear();
        
        const snap = await db.ref('whatsapp_accounts').once('value');
        const updates = {};
        snap.forEach((child) => {
            updates['whatsapp_accounts/' + child.key + '/todaySent'] = 0;
        });
        await db.ref().update(updates);
        
        logger.info('✅ Daily counts reset');
    } catch (err) {
        logger.error({ err }, 'Midnight reset error');
    }
}, {
    scheduled: true,
    timezone: "Asia/Dhaka"
});

// ============================================
// AUTO-SAVE SESSIONS
// ============================================
setInterval(async () => {
    try {
        for (const sessionId of sessions.keys()) {
            if (sessionStates.get(sessionId)?.status === 'connected') {
                await saveSessionToDB(sessionId);
            }
        }
        console.log('💾 Sessions auto-saved');
    } catch (err) {
        logger.error({ err }, 'Auto-save error');
    }
}, 300000);

// ============================================
// LOAD SESSIONS ON STARTUP
// ============================================
async function loadAllSessions() {
    try {
        const snap = await db.ref('whatsapp_accounts')
            .orderByChild('status')
            .equalTo('connected')
            .once('value');

        const sessions = [];
        snap.forEach((child) => {
            sessions.push({ id: child.key, ...child.val() });
        });

        for (const data of sessions) {
            const sessionId = data.id;
            const loaded = await loadSessionFromDB(sessionId);

            if (loaded) {
                console.log(`📂 Restoring session: ${data.phone}`);
                if (data.userId) {
                    sessionUserMap.set(sessionId, data.userId);
                }
                await createSession(sessionId, data.phone);
                await delay(3000);
            }
        }
        logger.info(`✅ ${sessions.length} sessions loaded`);
        
        // Check for running campaigns
        const campaignsSnap = await db.ref('campaigns')
            .orderByChild('status')
            .equalTo('running')
            .once('value');
        
        campaignsSnap.forEach((child) => {
            logger.info({ campaignId: child.key }, '▶️ Resuming campaign');
            runCampaign(child.key);
        });
        
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
// START SERVER
// ============================================
app.listen(PORT, async () => {
    console.log(`🚀 WS-Income Engine v3.0 on port ${PORT}`);
    console.log(`📡 Firebase Realtime Database | Real-time Campaign Engine`);
    await loadAllSessions();
});
