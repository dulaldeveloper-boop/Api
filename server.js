// ============================================
// WS-INCOME WhatsApp Campaign Engine v3.2
// Firebase Realtime Database | Real-time | Anti-Duplicate | Error Resilient
// Fixed: campaign_progress query & update paths
// Added: Better logging, error recovery, campaign resume
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
const activeCampaigns = new Map();
const campaignRetryTimers = new Map(); // For retry logic

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
        { method: 'GET', pattern: /^\/api\/notifications\/.*/ },
        { method: 'POST', pattern: /^\/api\/campaign\/.*/ }
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

function cleanPhoneNumber(phone) {
    let cleaned = String(phone).replace(/\D/g, '');
    if (cleaned.startsWith('0')) cleaned = cleaned.slice(1);
    if (!cleaned.startsWith('880')) cleaned = '880' + cleaned;
    return cleaned;
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
// ✅ CAMPAIGN ENGINE v3.2 (FIXED + IMPROVED)
// ============================================
async function runCampaign(campaignId) {
    // Prevent duplicate runs
    if (activeCampaigns.has(campaignId)) {
        logger.info({ campaignId }, '⚠️ Campaign already running, skipping');
        return;
    }
    
    activeCampaigns.set(campaignId, true);
    logger.info({ campaignId }, '🚀 Starting campaign engine');
    
    try {
        // Get campaign data
        const campaignSnap = await db.ref('campaigns/' + campaignId).once('value');
        if (!campaignSnap.exists()) {
            logger.warn({ campaignId }, '❌ Campaign not found in database');
            activeCampaigns.delete(campaignId);
            return;
        }

        const campaign = campaignSnap.val();
        if (campaign.status !== 'running') {
            logger.info({ campaignId, status: campaign.status }, 'ℹ️ Campaign not in running state');
            activeCampaigns.delete(campaignId);
            return;
        }

        const userId = campaign.userId;
        const messageTemplate = campaign.message;
        
        // ✅ FIX: Get targets first to check if there's work to do
        const targetsSnap = await db.ref('campaign_targets/' + campaignId)
            .orderByChild('status')
            .equalTo('pending')
            .limitToFirst(50)
            .once('value');

        if (!targetsSnap.exists()) {
            logger.info({ campaignId }, '✅ No pending targets - campaign completed');
            await db.ref('campaigns/' + campaignId).update({ 
                status: 'completed',
                completedAt: admin.database.ServerValue.TIMESTAMP
            });
            activeCampaigns.delete(campaignId);
            
            // Notify user
            if (userId) {
                await db.ref('notifications').push().set({
                    userId: userId,
                    type: 'campaign_completed',
                    title: '🎉 Campaign Completed!',
                    message: `Campaign "${campaign.name}" has been completed. ${campaign.sentCount || 0} messages sent.`,
                    campaignId: campaignId,
                    read: false,
                    createdAt: admin.database.ServerValue.TIMESTAMP
                });
            }
            return;
        }

        // ✅ FIX: Correct campaign_progress query
        logger.info({ campaignId }, '🔍 Checking campaign_progress...');
        const progressSnap = await db.ref('campaign_progress/' + campaignId).once('value');
        
        if (!progressSnap.exists()) {
            logger.warn({ campaignId }, '❌ No campaign_progress found! No accounts have joined.');
            
            // Pause campaign since no accounts joined
            await db.ref('campaigns/' + campaignId).update({
                status: 'paused',
                pausedReason: 'No accounts joined the campaign',
                pausedAt: admin.database.ServerValue.TIMESTAMP
            });
            
            if (userId) {
                await db.ref('notifications').push().set({
                    userId: userId,
                    type: 'campaign_paused',
                    title: '⏸️ Campaign Paused',
                    message: `Campaign "${campaign.name}" paused: No accounts have joined yet.`,
                    campaignId: campaignId,
                    read: false,
                    createdAt: admin.database.ServerValue.TIMESTAMP
                });
            }
            
            activeCampaigns.delete(campaignId);
            
            // ✅ NEW: Schedule retry after 60 seconds
            logger.info({ campaignId }, '⏰ Scheduling retry in 60 seconds...');
            if (campaignRetryTimers.has(campaignId)) {
                clearTimeout(campaignRetryTimers.get(campaignId));
            }
            campaignRetryTimers.set(campaignId, setTimeout(() => {
                campaignRetryTimers.delete(campaignId);
                runCampaign(campaignId);
            }, 60000));
            
            return;
        }

        // ✅ Parse progress data correctly
        const progressData = progressSnap.val();
        logger.info({ campaignId, progressKeys: Object.keys(progressData).length }, '📊 Campaign progress found');
        
        // Collect joined accounts with active status
        const joinedAccounts = [];
        for (const [progressKey, progress] of Object.entries(progressData)) {
            logger.info({ 
                campaignId, 
                progressKey, 
                accountId: progress.accountId,
                status: progress.status,
                userId: progress.userId 
            }, '📋 Progress entry');
            
            if (progress.status === 'active' && progress.accountId) {
                joinedAccounts.push({
                    accountId: progress.accountId,
                    progressKey: progressKey,
                    userId: progress.userId
                });
            }
        }

        if (joinedAccounts.length === 0) {
            logger.warn({ campaignId }, '⏸️ No active accounts found in progress');
            
            // Check if all progress entries are completed/stopped
            const allDone = Object.values(progressData).every(p => p.status === 'completed' || p.status === 'stopped');
            
            if (allDone) {
                await db.ref('campaigns/' + campaignId).update({
                    status: 'completed',
                    completedAt: admin.database.ServerValue.TIMESTAMP
                });
            } else {
                // Schedule retry
                if (campaignRetryTimers.has(campaignId)) {
                    clearTimeout(campaignRetryTimers.get(campaignId));
                }
                campaignRetryTimers.set(campaignId, setTimeout(() => {
                    campaignRetryTimers.delete(campaignId);
                    runCampaign(campaignId);
                }, 30000));
            }
            
            activeCampaigns.delete(campaignId);
            return;
        }

        // ✅ Get full account details for joined accounts
        logger.info({ campaignId, joinedCount: joinedAccounts.length }, '🔍 Checking account availability...');
        const availableAccounts = [];
        
        for (const acc of joinedAccounts) {
            try {
                const deviceSnap = await db.ref('whatsapp_accounts/' + acc.accountId).once('value');
                
                if (!deviceSnap.exists()) {
                    logger.warn({ accountId: acc.accountId }, 'Device not found');
                    continue;
                }
                
                const device = deviceSnap.val();
                const sessionState = sessionStates.get(acc.accountId);
                
                // Check if session exists and is connected
                if (!sessions.has(acc.accountId) || !sessionState || sessionState.status !== 'connected') {
                    logger.warn({ 
                        accountId: acc.accountId, 
                        phone: device.phone,
                        hasSession: sessions.has(acc.accountId),
                        sessionStatus: sessionState?.status 
                    }, '⚠️ Device not connected');
                    
                    // Update device status in DB
                    if (device.status !== 'logged_out') {
                        await db.ref('whatsapp_accounts/' + acc.accountId).update({
                            status: 'disconnected',
                            updatedAt: admin.database.ServerValue.TIMESTAMP
                        });
                    }
                    continue;
                }
                
                const todaySent = todayCounts.get(acc.accountId) || device.todaySent || 0;
                const dailyLimit = dailyLimits.get(acc.accountId) || device.dailyLimit || 5;
                
                if (todaySent >= dailyLimit) {
                    logger.info({ 
                        accountId: acc.accountId, 
                        todaySent, 
                        dailyLimit 
                    }, '📊 Daily limit reached');
                    continue;
                }
                
                availableAccounts.push({
                    id: acc.accountId,
                    progressKey: acc.progressKey,
                    todaySent,
                    dailyLimit,
                    phone: device.phone
                });
                
            } catch (err) {
                logger.error({ accountId: acc.accountId, error: err.message }, 'Error checking account');
            }
        }

        if (availableAccounts.length === 0) {
            logger.info({ campaignId }, '⏸️ No available devices (all offline or limits reached)');
            
            // Schedule retry in 30 seconds
            if (campaignRetryTimers.has(campaignId)) {
                clearTimeout(campaignRetryTimers.get(campaignId));
            }
            campaignRetryTimers.set(campaignId, setTimeout(() => {
                campaignRetryTimers.delete(campaignId);
                runCampaign(campaignId);
            }, 30000));
            
            activeCampaigns.delete(campaignId);
            return;
        }

        logger.info({ 
            campaignId, 
            joinedCount: joinedAccounts.length, 
            availableCount: availableAccounts.length,
            availablePhones: availableAccounts.map(a => a.phone)
        }, '▶️ Campaign running with available accounts');

        // ✅ Process targets
        let accountIndex = 0;
        const targets = [];
        targetsSnap.forEach((child) => {
            targets.push({ id: child.key, ...child.val() });
        });

        logger.info({ campaignId, targetCount: targets.length }, '🎯 Processing targets');

        for (const target of targets) {
            // Check campaign status before each message
            const currentSnap = await db.ref('campaigns/' + campaignId).once('value');
            if (!currentSnap.exists()) {
                logger.warn({ campaignId }, 'Campaign deleted, stopping');
                break;
            }
            if (currentSnap.val().status !== 'running') {
                logger.info({ campaignId, status: currentSnap.val().status }, 'Campaign status changed, stopping');
                break;
            }

            // Round-robin through available accounts
            const account = availableAccounts[accountIndex % availableAccounts.length];
            const sock = sessions.get(account.id);

            if (!sock) {
                logger.warn({ accountId: account.id }, 'Session lost, skipping account');
                accountIndex++;
                continue;
            }

            // Double-check daily limit
            const currentCount = todayCounts.get(account.id) || 0;
            if (currentCount >= (dailyLimits.get(account.id) || 5)) {
                logger.info({ accountId: account.id, currentCount }, 'Limit reached during processing');
                accountIndex++;
                continue;
            }

            try {
                const targetPhone = cleanPhoneNumber(target.phone);
                const jidTarget = jid(targetPhone);
                
                // Personalize message
                const msg = messageTemplate
                    .replace(/\{name\}/g, target.name || 'Friend')
                    .replace(/\{phone\}/g, targetPhone || '');
                
                logger.info({ 
                    accountId: account.id, 
                    target: targetPhone,
                    name: target.name 
                }, '📤 Sending message');
                
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
                        sentCount: newSentCount,
                        lastSentAt: admin.database.ServerValue.TIMESTAMP
                    });
                    campaign.sentCount = newSentCount;

                    // Update account count
                    const newCount = currentCount + 1;
                    todayCounts.set(account.id, newCount);
                    await db.ref('whatsapp_accounts/' + account.id).update({
                        todaySent: newCount,
                        lastUsedAt: admin.database.ServerValue.TIMESTAMP,
                        updatedAt: admin.database.ServerValue.TIMESTAMP
                    });

                    // Update user balance
                    if (campaign.pricePerMessage > 0 && userId) {
                        const userSnap = await db.ref('users/' + userId).once('value');
                        const userData = userSnap.val() || {};
                        await db.ref('users/' + userId).update({
                            balance: (userData.balance || 0) + campaign.pricePerMessage,
                            totalEarned: (userData.totalEarned || 0) + campaign.pricePerMessage,
                            todayTotalSent: (userData.todayTotalSent || 0) + 1,
                            updatedAt: admin.database.ServerValue.TIMESTAMP
                        });
                    }

                    // ✅ FIXED: Update campaign_progress with CORRECT path
                    const progressPath = 'campaign_progress/' + campaignId + '/' + account.progressKey;
                    await db.ref(progressPath).update({
                        sentCount: admin.database.ServerValue.increment(1),
                        earned: admin.database.ServerValue.increment(campaign.pricePerMessage || 0),
                        lastSentAt: admin.database.ServerValue.TIMESTAMP,
                        updatedAt: admin.database.ServerValue.TIMESTAMP
                    });
                    
                    logger.info({ progressPath }, '✅ Progress updated');

                    console.log(`✅ [${account.phone}] Sent to ${targetPhone} (${newSentCount}/${campaign.totalTargets})`);
                } else {
                    logger.warn({ accountId: account.id, target: targetPhone }, 'No message ID returned');
                    await db.ref('campaign_targets/' + campaignId + '/' + target.id).update({
                        status: 'failed',
                        error: 'No message ID returned'
                    });
                    
                    await db.ref('campaigns/' + campaignId).update({
                        failedCount: (campaign.failedCount || 0) + 1
                    });
                }

            } catch (err) {
                logger.error({ 
                    accountId: account.id, 
                    phone: target.phone, 
                    error: err.message 
                }, '❌ Send failed');

                await db.ref('campaign_targets/' + campaignId + '/' + target.id).update({
                    status: 'failed',
                    error: err.message
                });

                await db.ref('campaigns/' + campaignId).update({
                    failedCount: (campaign.failedCount || 0) + 1
                });

                // Handle logged out accounts
                if (err.message?.includes('logged out') || err.message?.includes('disconnected')) {
                    logger.warn({ accountId: account.id }, 'Account logged out, cleaning up');
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
            
            // ✅ RANDOM DELAY 30-60 seconds (Anti-Ban)
            const randomDelay = Math.floor(Math.random() * 30000) + 30000;
            logger.info({ 
                campaignId, 
                delay: (randomDelay/1000).toFixed(1) + 's',
                nextAccountIndex: accountIndex % availableAccounts.length
            }, '⏳ Anti-ban delay');
            await delay(randomDelay);
        }

        // ✅ Check if completed after processing batch
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
                    campaignId: campaignId,
                    read: false,
                    createdAt: admin.database.ServerValue.TIMESTAMP
                });
            }
        } else {
            // More targets remain, continue after delay
            logger.info({ campaignId }, '🔄 More targets remaining, continuing...');
            activeCampaigns.delete(campaignId);
            await delay(5000);
            runCampaign(campaignId);
            return;
        }
        
    } catch (err) {
        logger.error({ campaignId, err }, '❌ Campaign engine error');
        try {
            await db.ref('campaigns/' + campaignId).update({
                status: 'error',
                errorMessage: err.message,
                errorAt: admin.database.ServerValue.TIMESTAMP
            });
        } catch (e) {
            logger.error({ campaignId, error: e.message }, 'Failed to update campaign error status');
        }
    } finally {
        activeCampaigns.delete(campaignId);
        logger.info({ campaignId }, '🏁 Campaign engine finished cycle');
    }
}

// ============================================
// ✅ LISTEN FOR CAMPAIGN CHANGES (REAL-TIME)
// ============================================
db.ref('campaigns').on('child_changed', (snapshot) => {
    const campaign = snapshot.val();
    const campaignId = snapshot.key;
    
    // Only run if status changed TO running (not already running)
    if (campaign.status === 'running' && !activeCampaigns.has(campaignId)) {
        logger.info({ campaignId }, '▶️ Campaign status changed to running');
        
        // Clear any existing retry timer
        if (campaignRetryTimers.has(campaignId)) {
            clearTimeout(campaignRetryTimers.get(campaignId));
            campaignRetryTimers.delete(campaignId);
        }
        
        runCampaign(campaignId);
    }
    
    // If paused/completed/error - log and clean up retry timers
    if (campaign.status === 'paused' || campaign.status === 'completed' || campaign.status === 'error') {
        logger.info({ campaignId, status: campaign.status }, 'ℹ️ Campaign status updated');
        if (campaignRetryTimers.has(campaignId)) {
            clearTimeout(campaignRetryTimers.get(campaignId));
            campaignRetryTimers.delete(campaignId);
        }
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
            const cleanPhone = cleanPhoneNumber(t.phone);
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

        const cleanTo = cleanPhoneNumber(to);
        const jidTo = jid(cleanTo);
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

// 7. ✅ NEW: Get campaign status
app.get('/api/campaign/:id/status', async (req, res) => {
    try {
        const campaignSnap = await db.ref('campaigns/' + req.params.id).once('value');
        if (!campaignSnap.exists()) {
            return res.status(404).json({ success: false, error: 'Campaign not found' });
        }
        
        const campaign = campaignSnap.val();
        
        // Get progress
        const progressSnap = await db.ref('campaign_progress/' + req.params.id).once('value');
        const progress = progressSnap.val() || {};
        
        const progressSummary = {};
        Object.entries(progress).forEach(([key, val]) => {
            if (val.accountId) {
                progressSummary[val.accountId] = {
                    sentCount: val.sentCount || 0,
                    earned: val.earned || 0,
                    status: val.status
                };
            }
        });
        
        res.json({
            success: true,
            campaign: {
                ...campaign,
                id: req.params.id,
                isRunning: activeCampaigns.has(req.params.id),
                progress: progressSummary
            }
        });
    } catch (err) {
        res.status(500).json({ success: false, error: err.message });
    }
});

// 8. ✅ NEW: Pause campaign
app.post('/api/campaign/:id/pause', async (req, res) => {
    try {
        await db.ref('campaigns/' + req.params.id).update({
            status: 'paused',
            pausedAt: admin.database.ServerValue.TIMESTAMP
        });
        
        // Clear retry timer if exists
        if (campaignRetryTimers.has(req.params.id)) {
            clearTimeout(campaignRetryTimers.get(req.params.id));
            campaignRetryTimers.delete(req.params.id);
        }
        
        res.json({ success: true, message: 'Campaign paused' });
    } catch (err) {
        res.status(500).json({ success: false, error: err.message });
    }
});

// 9. ✅ NEW: Resume campaign
app.post('/api/campaign/:id/resume', async (req, res) => {
    try {
        await db.ref('campaigns/' + req.params.id).update({
            status: 'running',
            resumedAt: admin.database.ServerValue.TIMESTAMP
        });
        
        // Clear any existing retry timer
        if (campaignRetryTimers.has(req.params.id)) {
            clearTimeout(campaignRetryTimers.get(req.params.id));
            campaignRetryTimers.delete(req.params.id);
        }
        
        // Start campaign if not already running
        if (!activeCampaigns.has(req.params.id)) {
            runCampaign(req.params.id);
        }
        
        res.json({ success: true, message: 'Campaign resumed' });
    } catch (err) {
        res.status(500).json({ success: false, error: err.message });
    }
});

// ============================================
// MIDNIGHT CRON (reset todaySent)
// ============================================
cron.schedule('0 0 * * *', async () => {
    try {
        console.log('🕛 Midnight reset - clearing daily counts');
        todayCounts.clear();
        
        const snap = await db.ref('whatsapp_accounts').once('value');
        const updates = {};
        snap.forEach((child) => {
            updates['whatsapp_accounts/' + child.key + '/todaySent'] = 0;
        });
        await db.ref().update(updates);
        
        // Also reset user daily stats
        const usersSnap = await db.ref('users').once('value');
        const userUpdates = {};
        usersSnap.forEach((child) => {
            userUpdates['users/' + child.key + '/todayTotalSent'] = 0;
        });
        await db.ref().update(userUpdates);
        
        logger.info('✅ Daily counts reset successfully');
    } catch (err) {
        logger.error({ err }, 'Midnight reset error');
    }
}, {
    scheduled: true,
    timezone: "Asia/Dhaka"
});

// ============================================
// AUTO-SAVE SESSIONS (every 5 minutes)
// ============================================
setInterval(async () => {
    try {
        let savedCount = 0;
        for (const sessionId of sessions.keys()) {
            if (sessionStates.get(sessionId)?.status === 'connected') {
                await saveSessionToDB(sessionId);
                savedCount++;
            }
        }
        if (savedCount > 0) {
            logger.info({ savedCount }, '💾 Sessions auto-saved');
        }
    } catch (err) {
        logger.error({ err }, 'Auto-save error');
    }
}, 300000);

// ============================================
// HEALTH CHECK - Keep alive for Render
// ============================================
setInterval(() => {
    const connectedSessions = Array.from(sessionStates.values())
        .filter(s => s.status === 'connected').length;
    const runningCampaigns = activeCampaigns.size;
    logger.info({ 
        connectedSessions, 
        runningCampaigns,
        uptime: process.uptime().toFixed(0) + 's'
    }, '❤️ Health check');
}, 60000);

// ============================================
// LOAD SESSIONS ON STARTUP
// ============================================
async function loadAllSessions() {
    try {
        const snap = await db.ref('whatsapp_accounts')
            .orderByChild('status')
            .equalTo('connected')
            .once('value');

        const sessionsToLoad = [];
        snap.forEach((child) => {
            sessionsToLoad.push({ id: child.key, ...child.val() });
        });

        logger.info({ count: sessionsToLoad.length }, '📂 Loading sessions...');

        for (const data of sessionsToLoad) {
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
        logger.info(`✅ ${sessionsToLoad.length} sessions loaded`);
        
        // Check for running campaigns after sessions are loaded
        await delay(5000);
        const campaignsSnap = await db.ref('campaigns')
            .orderByChild('status')
            .equalTo('running')
            .once('value');
        
        let resumedCount = 0;
        campaignsSnap.forEach((child) => {
            logger.info({ campaignId: child.key }, '▶️ Resuming campaign');
            runCampaign(child.key);
            resumedCount++;
        });
        
        if (resumedCount > 0) {
            logger.info({ resumedCount }, '✅ Campaigns resumed');
        }
        
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
// GRACEFUL SHUTDOWN
// ============================================
process.on('SIGTERM', async () => {
    console.log('🛑 SIGTERM received - shutting down gracefully');
    
    // Clear all retry timers
    for (const [campaignId, timer] of campaignRetryTimers) {
        clearTimeout(timer);
    }
    campaignRetryTimers.clear();
    
    // Save all sessions
    for (const sessionId of sessions.keys()) {
        if (sessionStates.get(sessionId)?.status === 'connected') {
            await saveSessionToDB(sessionId);
        }
    }
    
    console.log('✅ Graceful shutdown complete');
    process.exit(0);
});

// ============================================
// START SERVER
// ============================================
app.listen(PORT, async () => {
    console.log('═══════════════════════════════════════════');
    console.log(`🚀 WS-Income Engine v3.2 on port ${PORT}`);
    console.log('📡 Firebase Realtime Database');
    console.log('🔧 Campaign Engine with Auto-Retry');
    console.log('🌍 Timezone: Asia/Dhaka');
    console.log('═══════════════════════════════════════════');
    await loadAllSessions();
});
