// ============================================
// WS-INCOME WhatsApp Campaign Engine v5.0
// Firebase Realtime Database | Event-Driven Architecture
// Optimized: Minimal API Endpoints, Firebase Triggers for Automation
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
// FIREBASE REALTIME DATABASE INIT
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
const activeCampaigns = new Map();
const campaignRetryTimers = new Map();
const scheduledTimers = new Map();
const messageDeliveryQueue = new Map();

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
        { method: 'POST', pattern: /^\/api\/campaign\/start$/ }
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

function generateReferralCode() {
    const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
    let code = 'REF';
    for (let i = 0; i < 8; i++) {
        code += chars.charAt(Math.floor(Math.random() * chars.length));
    }
    return code;
}

// ============================================
// REFERRAL SYSTEM LOGIC (Triggered by Firebase Events)
// ============================================

async function initializeReferralSystem() {
    try {
        const settingsSnap = await db.ref('app_settings/referral_settings').once('value');
        if (!settingsSnap.exists()) {
            await db.ref('app_settings/referral_settings').set({
                conditionType: "withdrawal",
                conditionValue: 1,
                conditionMessage: "First withdrawal must be successful",
                conditionActive: true,
                level1Commission: 10,
                level2Commission: 5,
                level3Commission: 2,
                levelBonus: {
                    "5": 100,
                    "10": 250,
                    "20": 500
                },
                topReferrerBonus: 1000,
                updatedAt: admin.database.ServerValue.TIMESTAMP
            });
            logger.info('✅ Default referral settings initialized');
        }
    } catch (err) {
        logger.error({ err }, '❌ Referral initialization error');
    }
}

async function generateUserReferralCode(userId) {
    try {
        const userSnap = await db.ref('users/' + userId).once('value');
        if (!userSnap.exists()) return null;
        
        const userData = userSnap.val();
        if (userData.referralCode) return userData.referralCode;
        
        let referralCode;
        let isUnique = false;
        let attempts = 0;
        
        while (!isUnique && attempts < 10) {
            referralCode = generateReferralCode();
            const existingSnap = await db.ref('users')
                .orderByChild('referralCode')
                .equalTo(referralCode)
                .once('value');
            
            if (!existingSnap.exists()) {
                isUnique = true;
            }
            attempts++;
        }
        
        if (!isUnique) {
            throw new Error('Failed to generate unique referral code');
        }
        
        await db.ref('users/' + userId).update({
            referralCode: referralCode,
            updatedAt: admin.database.ServerValue.TIMESTAMP
        });
        
        if (!userData.referralStats) {
            await db.ref('users/' + userId + '/referralStats').set({
                totalReferred: 0,
                activeReferred: 0,
                successfulReferred: 0,
                totalBonusEarned: 0,
                pendingBonus: 0,
                referralLevel: 0,
                monthlyReferrals: 0,
                monthlyBonus: 0,
                updatedAt: admin.database.ServerValue.TIMESTAMP
            });
        }
        
        return referralCode;
    } catch (err) {
        logger.error({ userId, err }, '❌ Generate referral code error');
        return null;
    }
}

async function processReferral(newUserId, referralCode) {
    try {
        const referrerSnap = await db.ref('users')
            .orderByChild('referralCode')
            .equalTo(referralCode)
            .once('value');
        
        if (!referrerSnap.exists()) {
            logger.warn({ referralCode }, '⚠️ Invalid referral code');
            return false;
        }
        
        let referrerId;
        referrerSnap.forEach(child => {
            referrerId = child.key;
        });
        
        if (referrerId === newUserId) {
            logger.warn({ newUserId }, '⚠️ Self-referral prevented');
            return false;
        }
        
        const existingRefSnap = await db.ref('referrals/' + newUserId).once('value');
        if (existingRefSnap.exists()) {
            logger.warn({ newUserId }, '⚠️ User already referred');
            return false;
        }
        
        const referralData = {
            referrerId: referrerId,
            referredUserId: newUserId,
            referralCode: referralCode,
            status: 'active',
            bonusAmount: 0,
            commissionLevel: 1,
            createdAt: admin.database.ServerValue.TIMESTAMP,
            activatedAt: null,
            successfulAt: null
        };
        
        await db.ref('referrals/' + newUserId).set(referralData);
        
        const statsSnap = await db.ref('users/' + referrerId + '/referralStats').once('value');
        const stats = statsSnap.val() || {};
        
        const updates = {};
        updates['users/' + referrerId + '/referralStats/totalReferred'] = (stats.totalReferred || 0) + 1;
        updates['users/' + referrerId + '/referralStats/activeReferred'] = (stats.activeReferred || 0) + 1;
        updates['users/' + referrerId + '/referralStats/monthlyReferrals'] = (stats.monthlyReferrals || 0) + 1;
        updates['users/' + referrerId + '/referralStats/updatedAt'] = admin.database.ServerValue.TIMESTAMP;
        await db.ref().update(updates);
        
        await db.ref('users/' + newUserId).update({
            referredBy: referrerId,
            referralCode: referralCode,
            updatedAt: admin.database.ServerValue.TIMESTAMP
        });
        
        logger.info({ newUserId, referrerId, referralCode }, '✅ Referral processed successfully');
        return true;
        
    } catch (err) {
        logger.error({ newUserId, referralCode, err }, '❌ Process referral error');
        return false;
    }
}

async function calculateReferralBonus(referredUserId, earnedAmount) {
    try {
        const refSnap = await db.ref('referrals/' + referredUserId).once('value');
        const ref = refSnap.val();
        if (!ref || ref.status !== 'successful') return;

        const settingsSnap = await db.ref('app_settings/referral_settings').once('value');
        const settings = settingsSnap.val() || {};

        // Level 1 Commission
        const l1Bonus = earnedAmount * ((settings.level1Commission || 10) / 100);
        await db.ref('users/' + ref.referrerId + '/balance').transaction(bal => (bal || 0) + l1Bonus);
        await db.ref('users/' + ref.referrerId + '/totalEarned').transaction(bal => (bal || 0) + l1Bonus);
        await db.ref('users/' + ref.referrerId + '/referralStats/totalBonusEarned').transaction(bal => (bal || 0) + l1Bonus);
        
        logger.info({ referrerId: ref.referrerId, referredUserId, bonus: l1Bonus, level: 1 }, '💰 Level 1 commission paid');

        // Level 2 Commission
        const ref2Snap = await db.ref('referrals/' + ref.referrerId).once('value');
        const ref2 = ref2Snap.val();
        if (ref2 && ref2.status === 'successful') {
            const l2Bonus = earnedAmount * ((settings.level2Commission || 5) / 100);
            await db.ref('users/' + ref2.referrerId + '/balance').transaction(bal => (bal || 0) + l2Bonus);
            await db.ref('users/' + ref2.referrerId + '/totalEarned').transaction(bal => (bal || 0) + l2Bonus);
            await db.ref('users/' + ref2.referrerId + '/referralStats/totalBonusEarned').transaction(bal => (bal || 0) + l2Bonus);
            
            logger.info({ referrerId: ref2.referrerId, referredUserId, bonus: l2Bonus, level: 2 }, '💰 Level 2 commission paid');

            // Level 3 Commission
            const ref3Snap = await db.ref('referrals/' + ref2.referrerId).once('value');
            const ref3 = ref3Snap.val();
            if (ref3 && ref3.status === 'successful') {
                const l3Bonus = earnedAmount * ((settings.level3Commission || 2) / 100);
                await db.ref('users/' + ref3.referrerId + '/balance').transaction(bal => (bal || 0) + l3Bonus);
                await db.ref('users/' + ref3.referrerId + '/totalEarned').transaction(bal => (bal || 0) + l3Bonus);
                await db.ref('users/' + ref3.referrerId + '/referralStats/totalBonusEarned').transaction(bal => (bal || 0) + l3Bonus);
                
                logger.info({ referrerId: ref3.referrerId, referredUserId, bonus: l3Bonus, level: 3 }, '💰 Level 3 commission paid');
            }
        }

        await checkReferralMilestones(ref.referrerId, settings);
        
    } catch (err) {
        logger.error({ referredUserId, earnedAmount, err }, '❌ Calculate referral bonus error');
    }
}

async function checkReferralMilestones(userId, settings) {
    try {
        const statsSnap = await db.ref('users/' + userId + '/referralStats').once('value');
        const stats = statsSnap.val() || {};
        const totalRefs = stats.successfulReferred || 0;
        const levelBonuses = settings.levelBonus || {};
        
        const milestonesSnap = await db.ref('users/' + userId + '/referralStats/achievedMilestones').once('value');
        const achievedMilestones = milestonesSnap.val() || {};
        
        for (const [count, bonus] of Object.entries(levelBonuses)) {
            const requiredCount = parseInt(count);
            if (totalRefs >= requiredCount && !achievedMilestones[count]) {
                await db.ref('users/' + userId + '/balance').transaction(bal => (bal || 0) + bonus);
                await db.ref('users/' + userId + '/totalEarned').transaction(bal => (bal || 0) + bonus);
                await db.ref('users/' + userId + '/referralStats/totalBonusEarned').transaction(bal => (bal || 0) + bonus);
                await db.ref('users/' + userId + '/referralStats/achievedMilestones/' + count).set(true);
                
                await db.ref('notifications').push().set({
                    userId: userId,
                    type: 'referral_milestone',
                    title: '🎉 Referral Milestone Achieved!',
                    message: `Congratulations! You've reached ${requiredCount} successful referrals and earned a bonus of ৳${bonus}!`,
                    bonus: bonus,
                    milestone: requiredCount,
                    read: false,
                    createdAt: admin.database.ServerValue.TIMESTAMP
                });
                
                logger.info({ userId, milestone: requiredCount, bonus }, '🏆 Referral milestone achieved');
            }
        }
    } catch (err) {
        logger.error({ userId, err }, '❌ Check milestones error');
    }
}

async function updateReferralStatus(userId) {
    try {
        const settingsSnap = await db.ref('app_settings/referral_settings').once('value');
        const settings = settingsSnap.val() || {};
        
        if (!settings.conditionActive) return;
        
        const refSnap = await db.ref('referrals/' + userId).once('value');
        if (!refSnap.exists()) return;
        
        const ref = refSnap.val();
        if (ref.status === 'successful') return;
        
        let conditionMet = false;
        
        if (settings.conditionType === 'withdrawal') {
            const withdrawalsSnap = await db.ref('withdrawals')
                .orderByChild('userId')
                .equalTo(userId)
                .once('value');
            
            let successCount = 0;
            withdrawalsSnap.forEach(child => {
                if (child.val().status === 'completed' || child.val().status === 'successful') {
                    successCount++;
                }
            });
            
            conditionMet = successCount >= settings.conditionValue;
        } else if (settings.conditionType === 'messageCount') {
            const userSnap = await db.ref('users/' + userId).once('value');
            const userData = userSnap.val() || {};
            conditionMet = (userData.totalSent || 0) >= settings.conditionValue;
        }
        
        if (conditionMet && ref.status === 'active') {
            await db.ref('referrals/' + userId).update({
                status: 'successful',
                successfulAt: admin.database.ServerValue.TIMESTAMP
            });
            
            const updates = {};
            updates['users/' + ref.referrerId + '/referralStats/successfulReferred'] = admin.database.ServerValue.increment(1);
            updates['users/' + ref.referrerId + '/referralStats/activeReferred'] = admin.database.ServerValue.increment(-1);
            await db.ref().update(updates);
            
            const userSnap = await db.ref('users/' + userId).once('value');
            const userData = userSnap.val() || {};
            if (userData.totalEarned > 0) {
                await calculateReferralBonus(userId, userData.totalEarned);
            }
            
            logger.info({ userId, referrerId: ref.referrerId }, '✅ Referral status updated to successful');
        }
    } catch (err) {
        logger.error({ userId, err }, '❌ Update referral status error');
    }
}

// ============================================
// MESSAGE DELIVERY & PAYMENT LOGIC
// ============================================
async function processMessageDeliveryPayment(msgId) {
    try {
        if (messageDeliveryQueue.has(msgId)) {
            const queueData = messageDeliveryQueue.get(msgId);
            if (queueData.processed) {
                logger.info({ msgId }, '💰 Payment already processed, skipping');
                return false;
            }
        }
        
        messageDeliveryQueue.set(msgId, { processed: true, timestamp: Date.now() });
        
        const logSnap = await db.ref('message_logs')
            .orderByChild('messageId')
            .equalTo(msgId)
            .once('value');
        
        if (!logSnap.exists()) {
            logger.warn({ msgId }, '⚠️ No log found for message');
            return false;
        }
        
        let logData = null;
        let logKey = null;
        
        logSnap.forEach((child) => {
            logData = child.val();
            logKey = child.key;
        });
        
        if (logData.paymentAdded) {
            logger.info({ msgId }, '💰 Payment already added in log');
            return false;
        }
        
        if (logData.status !== 'sent') {
            logger.warn({ msgId, status: logData.status }, '⚠️ Message not in sent status');
            return false;
        }
        
        const { userId, campaignId, accountId, progressKey, pricePerMessage } = logData;
        
        let finalPrice = pricePerMessage || 0;
        if (!finalPrice && campaignId) {
            const campSnap = await db.ref('campaigns/' + campaignId).once('value');
            const campaign = campSnap.val();
            finalPrice = campaign?.pricePerMessage || 0;
        }
        
        if (finalPrice > 0 && userId) {
            const updates = {};
            updates['users/' + userId + '/balance'] = admin.database.ServerValue.increment(finalPrice);
            updates['users/' + userId + '/totalEarned'] = admin.database.ServerValue.increment(finalPrice);
            updates['users/' + userId + '/todayTotalSent'] = admin.database.ServerValue.increment(1);
            updates['users/' + userId + '/totalSent'] = admin.database.ServerValue.increment(1);
            
            await db.ref().update(updates);
            
            if (campaignId && progressKey) {
                await db.ref('campaign_progress/' + campaignId + '/' + progressKey).update({
                    earned: admin.database.ServerValue.increment(finalPrice),
                    deliveredCount: admin.database.ServerValue.increment(1),
                    updatedAt: admin.database.ServerValue.TIMESTAMP
                });
            }
            
            if (campaignId) {
                const targetSnap = await db.ref('campaign_targets/' + campaignId)
                    .orderByChild('messageId')
                    .equalTo(msgId)
                    .once('value');
                
                targetSnap.forEach(async (targetChild) => {
                    await db.ref('campaign_targets/' + campaignId + '/' + targetChild.key).update({
                        verified: true,
                        verifiedAt: admin.database.ServerValue.TIMESTAMP
                    });
                });
                
                await db.ref('campaigns/' + campaignId).update({
                    deliveredCount: admin.database.ServerValue.increment(1)
                });
            }
            
            await updateReferralStatus(userId);
            await calculateReferralBonus(userId, finalPrice);
            
            await db.ref('notifications').push().set({
                userId: userId,
                type: 'message_delivered',
                title: '✅ Message Delivered!',
                message: `Your message has been delivered successfully. Earned ৳${finalPrice}!`,
                amount: finalPrice,
                messageId: msgId,
                read: false,
                createdAt: admin.database.ServerValue.TIMESTAMP
            });
            
            logger.info({ userId, amount: finalPrice, msgId, campaignId }, '💰 Payment processed for delivered message');
        } else {
            logger.info({ msgId, userId, finalPrice }, 'ℹ️ No payment needed (price is 0 or no userId)');
        }
        
        await db.ref('message_logs/' + logKey).update({
            deliveryStatus: 'DELIVERED',
            statusUpdatedAt: admin.database.ServerValue.TIMESTAMP,
            paymentAdded: true,
            paymentAmount: finalPrice,
            paymentProcessedAt: admin.database.ServerValue.TIMESTAMP
        });
        
        setTimeout(() => {
            messageDeliveryQueue.delete(msgId);
        }, 3600000);
        
        return true;
        
    } catch (err) {
        logger.error({ msgId, err }, '❌ Process delivery payment error');
        messageDeliveryQueue.delete(msgId);
        return false;
    }
}

// ============================================
// CAMPAIGN SCHEDULER
// ============================================
function scheduleCampaignStart(campaignId, startTime) {
    const delay = startTime - Date.now();
    
    if (scheduledTimers.has(campaignId)) {
        clearTimeout(scheduledTimers.get(campaignId));
        scheduledTimers.delete(campaignId);
    }
    
    if (delay > 0) {
        const timer = setTimeout(async () => {
            try {
                const campaignSnap = await db.ref('campaigns/' + campaignId).once('value');
                if (campaignSnap.exists()) {
                    const campaign = campaignSnap.val();
                    if (campaign.status === 'paused') {
                        await db.ref('campaigns/' + campaignId).update({
                            status: 'running',
                            scheduledStartTime: null,
                            startedAt: admin.database.ServerValue.TIMESTAMP
                        });
                        
                        logger.info({ campaignId }, '⏰ Auto-started campaign via scheduler');
                        
                        if (campaign.userId) {
                            await db.ref('notifications').push().set({
                                userId: campaign.userId,
                                type: 'campaign_started',
                                title: '▶️ Campaign Auto-Started',
                                message: `Campaign "${campaign.name}" has automatically started.`,
                                campaignId: campaignId,
                                read: false,
                                createdAt: admin.database.ServerValue.TIMESTAMP
                            });
                        }
                    }
                }
                scheduledTimers.delete(campaignId);
            } catch (err) {
                logger.error({ campaignId, err }, '❌ Auto-start timer error');
                scheduledTimers.delete(campaignId);
            }
        }, delay);
        
        scheduledTimers.set(campaignId, timer);
        
        const minutes = Math.floor(delay / 60000);
        const hours = Math.floor(minutes / 60);
        logger.info({ campaignId, delay: `${hours}h ${minutes % 60}m` }, '🕒 Scheduled campaign auto-start');
    } else {
        logger.info({ campaignId }, '⏰ Scheduled time passed, starting immediately');
        db.ref('campaigns/' + campaignId).update({ 
            status: 'running', 
            scheduledStartTime: null,
            startedAt: admin.database.ServerValue.TIMESTAMP
        });
    }
}

async function rescheduleCampaigns() {
    try {
        const snap = await db.ref('campaigns').once('value');
        let scheduledCount = 0;
        
        snap.forEach(child => {
            const c = child.val();
            if (c.status === 'paused' && c.scheduledStartTime && c.scheduledStartTime > Date.now()) {
                scheduleCampaignStart(child.key, c.scheduledStartTime);
                scheduledCount++;
            }
        });
        
        if (scheduledCount > 0) {
            logger.info({ scheduledCount }, '📅 Rescheduled pending campaign starts');
        }
    } catch (err) {
        logger.error({ err }, '❌ Reschedule campaigns error');
    }
}

// ============================================
// FIREBASE SESSION PERSISTENCE
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
                const safeKey = file.replace(/[.$#[\]/]/g, '_');
                sessionData[safeKey] = content;
            }
        }
        
        await db.ref('session_data/' + sessionId).update({
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

                    const userId = sessionUserMap.get(sessionId) || stored.userId;
                    if (userId) {
                        await updateUserDeviceStats(userId);
                    }

                    console.log(`✅ Connected: ${name} (${phone})`);
                    await saveSessionToDB(sessionId);
                    
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

        // ✅ CRITICAL: Message Delivery Tracking & Auto Payment
        sock.ev.on('messages.update', async (updates) => {
            for (const update of updates) {
                try {
                    const msgId = update.key.id;
                    const status = update.update?.status;
                    
                    if (!msgId) continue;
                    
                    const statusMap = {
                        0: 'ERROR',
                        1: 'PENDING',
                        2: 'SERVER_ACK',
                        3: 'DELIVERY_ACK',
                        4: 'READ'
                    };
                    
                    const statusText = statusMap[status] || 'UNKNOWN';
                    logger.info({ msgId, status: statusText }, '📊 Message status update');
                    
                    // Payment on delivery or read receipt
                    if (status >= 3) {
                        await processMessageDeliveryPayment(msgId);
                    } else if (status === 2) {
                        const logSnap = await db.ref('message_logs')
                            .orderByChild('messageId')
                            .equalTo(msgId)
                            .once('value');
                        
                        logSnap.forEach(async (child) => {
                            await db.ref('message_logs/' + child.key).update({
                                deliveryStatus: statusText,
                                statusUpdatedAt: admin.database.ServerValue.TIMESTAMP
                            });
                        });
                    }
                } catch (err) {
                    logger.error({ err }, '❌ Delivery tracker error');
                }
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
// UPDATE USER DEVICE STATS (For Dashboard)
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
// CAMPAIGN ENGINE (Core Logic)
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

async function runCampaign(campaignId) {
    if (activeCampaigns.has(campaignId)) {
        logger.info({ campaignId }, '⚠️ Campaign already running, skipping');
        return;
    }
    
    activeCampaigns.set(campaignId, true);
    logger.info({ campaignId }, '🚀 Starting campaign engine');
    
    try {
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
            
            if (campaign.status === 'paused' && campaign.scheduledStartTime && campaign.scheduledStartTime > Date.now()) {
                scheduleCampaignStart(campaignId, campaign.scheduledStartTime);
            }
            return;
        }

        const userId = campaign.userId;
        const messageTemplate = campaign.message;
        
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
            
            if (scheduledTimers.has(campaignId)) {
                clearTimeout(scheduledTimers.get(campaignId));
                scheduledTimers.delete(campaignId);
            }
            
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

        const progressSnap = await db.ref('campaign_progress/' + campaignId).once('value');
        
        if (!progressSnap.exists()) {
            logger.warn({ campaignId }, '❌ No campaign_progress found! No accounts have joined.');
            
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
            
            if (campaignRetryTimers.has(campaignId)) {
                clearTimeout(campaignRetryTimers.get(campaignId));
            }
            campaignRetryTimers.set(campaignId, setTimeout(() => {
                campaignRetryTimers.delete(campaignId);
                runCampaign(campaignId);
            }, 60000));
            
            return;
        }

        const progressData = progressSnap.val();
        logger.info({ campaignId, progressKeys: Object.keys(progressData).length }, '📊 Campaign progress found');
        
        const joinedAccounts = [];
        for (const [progressKey, progress] of Object.entries(progressData)) {
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
            
            const allDone = Object.values(progressData).every(p => p.status === 'completed' || p.status === 'stopped');
            
            if (allDone) {
                await db.ref('campaigns/' + campaignId).update({
                    status: 'completed',
                    completedAt: admin.database.ServerValue.TIMESTAMP
                });
            } else {
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
                
                if (!sessions.has(acc.accountId) || !sessionState || sessionState.status !== 'connected') {
                    logger.warn({ accountId: acc.accountId, phone: device.phone }, '⚠️ Device not connected');
                    
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
                    logger.info({ accountId: acc.accountId, todaySent, dailyLimit }, '📊 Daily limit reached');
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

        let accountIndex = 0;
        const targets = [];
        targetsSnap.forEach((child) => {
            targets.push({ id: child.key, ...child.val() });
        });

        logger.info({ campaignId, targetCount: targets.length }, '🎯 Processing targets');

        for (const target of targets) {
            const currentSnap = await db.ref('campaigns/' + campaignId).once('value');
            if (!currentSnap.exists()) {
                logger.warn({ campaignId }, 'Campaign deleted, stopping');
                break;
            }
            if (currentSnap.val().status !== 'running') {
                logger.info({ campaignId, status: currentSnap.val().status }, 'Campaign status changed, stopping');
                break;
            }

            const account = availableAccounts[accountIndex % availableAccounts.length];
            const sock = sessions.get(account.id);

            if (!sock) {
                logger.warn({ accountId: account.id }, 'Session lost, skipping account');
                accountIndex++;
                continue;
            }

            const currentCount = todayCounts.get(account.id) || 0;
            if (currentCount >= (dailyLimits.get(account.id) || 5)) {
                logger.info({ accountId: account.id, currentCount }, 'Limit reached during processing');
                accountIndex++;
                continue;
            }

            try {
                const targetPhone = cleanPhoneNumber(target.phone);
                const jidTarget = jid(targetPhone);
                
                const msg = messageTemplate
                    .replace(/\{name\}/g, target.name || 'Friend')
                    .replace(/\{phone\}/g, targetPhone || '');
                
                logger.info({ accountId: account.id, target: targetPhone }, '🔍 Checking if number is on WhatsApp');
                
                const [waResult] = await sock.onWhatsApp(jidTarget);
                
                if (!waResult || !waResult.exists) {
                    logger.warn(`❌ Number not on WA: ${targetPhone}`);
                    
                    await db.ref('campaign_targets/' + campaignId + '/' + target.id).update({
                        status: 'failed',
                        error: 'Not a WhatsApp account',
                        failedAt: admin.database.ServerValue.TIMESTAMP
                    });
                    
                    await db.ref('campaigns/' + campaignId).update({
                        failedCount: admin.database.ServerValue.increment(1)
                    });
                    
                    accountIndex++;
                    continue;
                }

                const verifiedJid = waResult.jid;

                logger.info({ target: targetPhone }, '✍️ Simulating human typing...');
                
                await sock.presenceSubscribe(verifiedJid);
                await sock.sendPresenceUpdate('composing', verifiedJid);
                
                const typingDuration = Math.max(2000, Math.min(msg.length * 50, 5000));
                await delay(typingDuration);
                
                await sock.sendPresenceUpdate('paused', verifiedJid);

                logger.info({ accountId: account.id, target: targetPhone, name: target.name }, '📤 Sending verified message');
                
                const result = await sock.sendMessage(verifiedJid, { text: msg });

                if (result?.key?.id) {
                    const msgId = result.key.id;
                    
                    await db.ref('campaign_targets/' + campaignId + '/' + target.id).update({
                        status: 'sent',
                        sentAt: admin.database.ServerValue.TIMESTAMP,
                        messageId: msgId,
                        sentBy: account.id,
                        verified: false
                    });

                    const newSentCount = (campaign.sentCount || 0) + 1;
                    await db.ref('campaigns/' + campaignId).update({
                        sentCount: newSentCount,
                        lastSentAt: admin.database.ServerValue.TIMESTAMP
                    });
                    campaign.sentCount = newSentCount;

                    const newCount = currentCount + 1;
                    todayCounts.set(account.id, newCount);
                    await db.ref('whatsapp_accounts/' + account.id).update({
                        todaySent: newCount,
                        lastUsedAt: admin.database.ServerValue.TIMESTAMP,
                        updatedAt: admin.database.ServerValue.TIMESTAMP
                    });

                    const progressPath = 'campaign_progress/' + campaignId + '/' + account.progressKey;
                    await db.ref(progressPath).update({
                        sentCount: admin.database.ServerValue.increment(1),
                        lastSentAt: admin.database.ServerValue.TIMESTAMP,
                        updatedAt: admin.database.ServerValue.TIMESTAMP
                    });

                    const logRef = db.ref('message_logs').push();
                    await logRef.set({
                        campaignId: campaignId,
                        userId: userId,
                        accountId: account.id,
                        targetPhone: targetPhone,
                        targetName: target.name || '',
                        messageId: msgId,
                        messageText: msg,
                        status: 'sent',
                        deliveryStatus: 'PENDING',
                        paymentAdded: false,
                        progressKey: account.progressKey,
                        pricePerMessage: campaign.pricePerMessage || 0,
                        sentAt: admin.database.ServerValue.TIMESTAMP,
                        statusUpdatedAt: null
                    });

                    console.log(`📤 [${account.phone}] Sent to ${targetPhone} - MsgID: ${msgId} (Pending verification)`);

                    // Wait for potential delivery confirmation
                    await delay(30000);
                    
                    const verifySnap = await db.ref('message_logs')
                        .orderByChild('messageId')
                        .equalTo(msgId)
                        .once('value');
                    
                    let isDelivered = false;
                    verifySnap.forEach((child) => {
                        if (child.val().paymentAdded) {
                            isDelivered = true;
                        }
                    });
                    
                    if (isDelivered) {
                        console.log(`✅ Verified delivered: ${targetPhone}`);
                        await db.ref('campaign_targets/' + campaignId + '/' + target.id).update({
                            verified: true,
                            verifiedAt: admin.database.ServerValue.TIMESTAMP
                        });
                    } else {
                        console.log(`⏳ Delivery pending: ${targetPhone} (will be tracked by listener)`);
                    }

                } else {
                    logger.warn({ accountId: account.id, target: targetPhone }, 'No message ID returned');
                    await db.ref('campaign_targets/' + campaignId + '/' + target.id).update({
                        status: 'failed',
                        error: 'No message ID returned'
                    });
                    
                    await db.ref('campaigns/' + campaignId).update({
                        failedCount: admin.database.ServerValue.increment(1)
                    });
                }

            } catch (err) {
                logger.error({ accountId: account.id, phone: target.phone, error: err.message }, '❌ Send failed');

                await db.ref('campaign_targets/' + campaignId + '/' + target.id).update({
                    status: 'failed',
                    error: err.message
                });

                await db.ref('campaigns/' + campaignId).update({
                    failedCount: admin.database.ServerValue.increment(1)
                });

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
            
            const randomDelay = Math.floor(Math.random() * 30000) + 30000;
            logger.info({ campaignId, delay: (randomDelay/1000).toFixed(1) + 's' }, '⏳ Anti-ban delay');
            await delay(randomDelay);
        }

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
            
            if (scheduledTimers.has(campaignId)) {
                clearTimeout(scheduledTimers.get(campaignId));
                scheduledTimers.delete(campaignId);
            }
            
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
// 🔥 FIREBASE EVENT LISTENERS (The Core Automation)
// ============================================

// 1. Campaign Status Changes (Pause/Resume/Start Detection)
db.ref('campaigns').on('child_changed', (snapshot) => {
    const campaign = snapshot.val();
    const campaignId = snapshot.key;
    
    if (campaign.status === 'running' && !activeCampaigns.has(campaignId)) {
        logger.info({ campaignId }, '▶️ Campaign status changed to running');
        
        if (campaignRetryTimers.has(campaignId)) {
            clearTimeout(campaignRetryTimers.get(campaignId));
            campaignRetryTimers.delete(campaignId);
        }
        
        if (scheduledTimers.has(campaignId)) {
            clearTimeout(scheduledTimers.get(campaignId));
            scheduledTimers.delete(campaignId);
        }
        
        runCampaign(campaignId);
    }
    
    if (campaign.status === 'paused' && campaign.scheduledStartTime && campaign.scheduledStartTime > Date.now()) {
        logger.info({ campaignId, scheduledStartTime: campaign.scheduledStartTime }, '🕒 Scheduling auto-start for paused campaign');
        scheduleCampaignStart(campaignId, campaign.scheduledStartTime);
    }
    
    if (campaign.status === 'paused' || campaign.status === 'completed' || campaign.status === 'error') {
        if (campaignRetryTimers.has(campaignId)) {
            clearTimeout(campaignRetryTimers.get(campaignId));
            campaignRetryTimers.delete(campaignId);
        }
    }
});

// 2. New Campaign Added (Immediate Start)
db.ref('campaigns').on('child_added', (snapshot) => {
    const campaign = snapshot.val();
    const campaignId = snapshot.key;
    
    if (campaign.status === 'running' && !activeCampaigns.has(campaignId)) {
        logger.info({ campaignId }, '🆕 New running campaign detected');
        runCampaign(campaignId);
    } else if (campaign.status === 'paused' && campaign.scheduledStartTime && campaign.scheduledStartTime > Date.now()) {
        logger.info({ campaignId }, '🆕 New scheduled campaign detected');
        scheduleCampaignStart(campaignId, campaign.scheduledStartTime);
    }
});

// 3. Withdrawal Completion triggers Referral Status Update
db.ref('withdrawals').on('child_changed', async (snapshot) => {
    const withdrawal = snapshot.val();
    if (withdrawal.status === 'completed' || withdrawal.status === 'successful') {
        await updateReferralStatus(withdrawal.userId);
    }
});

// 4. New User Signup with Referral Code
db.ref('users').on('child_added', async (snapshot) => {
    const userData = snapshot.val();
    const userId = snapshot.key;
    
    // Auto-process referral if user has a referral code that hasn't been processed yet
    if (userData.referralCode && !userData.referralProcessed) {
        const success = await processReferral(userId, userData.referralCode);
        if (success) {
            await db.ref('users/' + userId).update({ referralProcessed: true });
        }
    }
    
    // Auto-generate referral code for new users
    if (!userData.referralCode) {
        await generateUserReferralCode(userId);
    }
});

// 5. User Balance Change triggers Referral Bonus (Real-time Commission)
db.ref('users').on('child_changed', async (snapshot) => {
    const userData = snapshot.val();
    const userId = snapshot.key;
    const beforeData = snapshot.previous.val();
    
    // Check if totalEarned has increased
    if (userData && beforeData && userData.totalEarned > (beforeData?.totalEarned || 0)) {
        const earnedAmount = userData.totalEarned - (beforeData?.totalEarned || 0);
        await calculateReferralBonus(userId, earnedAmount);
    }
});

// ============================================
// 🚀 MINIMAL API ENDPOINTS (Only 5!)
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

// 2. ACCOUNT STATUS (Read Only - Mostly handled by Firebase Client SDK)
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

// 4. SEND SINGLE MESSAGE (with delivery tracking)
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
        const msgId = result?.key?.id;

        if (!msgId) {
            return res.status(400).json({ success: false, error: 'Message not confirmed' });
        }

        const newCount = (todayCounts.get(accountId) || 0) + 1;
        todayCounts.set(accountId, newCount);
        await db.ref('whatsapp_accounts/' + accountId).update({
            todaySent: newCount,
            updatedAt: admin.database.ServerValue.TIMESTAMP
        });

        await db.ref('message_logs').push().set({
            accountId, 
            userId: userId || '', 
            targetPhone: cleanTo,
            messageId: msgId,
            messageText: text,
            status: 'sent',
            deliveryStatus: 'PENDING',
            paymentAdded: false,
            sentAt: admin.database.ServerValue.TIMESTAMP
        });

        res.json({ 
            success: true, 
            messageId: msgId, 
            todaySent: newCount,
            message: 'Message sent. Delivery verification in progress.'
        });
    } catch (err) {
        res.status(400).json({ success: false, error: err.message });
    }
});

// 5. START CAMPAIGN (Creation only, execution is automatic via listener)
app.post('/api/campaign/start', async (req, res) => {
    try {
        const { userId, name, message, targets, pricePerMessage, autoStartDelay } = req.body;
        
        if (!userId || !targets || targets.length === 0) {
            return res.status(400).json({ success: false, error: 'Missing fields' });
        }

        const campaignId = generateId();
        const now = new Date().toISOString();
        
        const scheduledTime = autoStartDelay ? Date.now() + autoStartDelay * 3600000 : null;

        await db.ref('campaigns/' + campaignId).set({
            userId,
            name,
            message,
            totalTargets: targets.length,
            sentCount: 0,
            failedCount: 0,
            deliveredCount: 0,
            status: scheduledTime ? 'paused' : 'running',
            pricePerMessage: pricePerMessage || 0,
            scheduledStartTime: scheduledTime,
            createdAt: now,
            updatedAt: admin.database.ServerValue.TIMESTAMP
        });

        const targetUpdates = {};
        targets.forEach((t, idx) => {
            const cleanPhone = cleanPhoneNumber(t.phone);
            targetUpdates['campaign_targets/' + campaignId + '/' + idx] = {
                phone: cleanPhone,
                name: t.name || '',
                status: 'pending',
                verified: false,
                createdAt: now
            };
        });
        
        await db.ref().update(targetUpdates);

        logger.info({ campaignId, userId, targetCount: targets.length, scheduledTime, status: scheduledTime ? 'paused (scheduled)' : 'running' }, 'Campaign created');

        // No need to call runCampaign directly if status is 'running'
        // The Firebase listener 'child_added' will detect it and start automatically!
        if (scheduledTime) {
            scheduleCampaignStart(campaignId, scheduledTime);
            if (userId) {
                const startTime = new Date(scheduledTime).toLocaleString('en-US', { 
                    timeZone: 'Asia/Dhaka',
                    hour: '2-digit',
                    minute: '2-digit',
                    hour12: true
                });
                
                await db.ref('notifications').push().set({
                    userId: userId,
                    type: 'campaign_scheduled',
                    title: '📅 Campaign Scheduled',
                    message: `Campaign "${name}" will auto-start in ${autoStartDelay} hour(s) at ${startTime}.`,
                    campaignId: campaignId,
                    read: false,
                    createdAt: admin.database.ServerValue.TIMESTAMP
                });
            }
        }

        res.json({ 
            success: true, 
            campaignId, 
            totalTargets: targets.length,
            status: scheduledTime ? 'paused' : 'running',
            scheduledStartTime: scheduledTime
        });
    } catch (err) {
        logger.error({ err }, 'Start campaign error');
        res.status(500).json({ success: false, error: err.message });
    }
});

// ============================================
// CRON JOBS
// ============================================

// Midnight reset (Daily)
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

// Monthly Referral Reset & Bonus Distribution
cron.schedule('0 0 1 * *', async () => {
    try {
        console.log('📅 Monthly reset - processing top referrer bonuses');
        
        const settingsSnap = await db.ref('app_settings/referral_settings').once('value');
        const settings = settingsSnap.val() || {};
        const topBonus = settings.topReferrerBonus || 0;
        
        if (topBonus > 0) {
            const usersSnap = await db.ref('users').once('value');
            const topReferrers = [];
            
            usersSnap.forEach(child => {
                const user = child.val();
                if (user.referralStats?.monthlyReferrals > 0) {
                    topReferrers.push({
                        userId: child.key,
                        name: user.name || 'Anonymous',
                        monthlyReferrals: user.referralStats.monthlyReferrals || 0
                    });
                }
            });
            
            topReferrers.sort((a, b) => b.monthlyReferrals - a.monthlyReferrals);
            
            for (let i = 0; i < Math.min(3, topReferrers.length); i++) {
                const bonus = i === 0 ? topBonus : Math.floor(topBonus / 2);
                await db.ref('users/' + topReferrers[i].userId + '/balance').transaction(bal => (bal || 0) + bonus);
                
                await db.ref('notifications').push().set({
                    userId: topReferrers[i].userId,
                    type: 'top_referrer',
                    title: '🏆 Top Referrer Award!',
                    message: `Congratulations! You ranked #${i + 1} in monthly referrals and earned a bonus of ৳${bonus}!`,
                    rank: i + 1,
                    bonus: bonus,
                    read: false,
                    createdAt: admin.database.ServerValue.TIMESTAMP
                });
                
                logger.info({ userId: topReferrers[i].userId, rank: i + 1, bonus }, '🏆 Monthly top referrer bonus paid');
            }
        }
        
        // Reset monthly stats
        const usersSnap = await db.ref('users').once('value');
        const updates = {};
        usersSnap.forEach((child) => {
            updates['users/' + child.key + '/referralStats/monthlyReferrals'] = 0;
            updates['users/' + child.key + '/referralStats/monthlyBonus'] = 0;
        });
        await db.ref().update(updates);
        
        logger.info('✅ Monthly referral stats reset');
    } catch (err) {
        logger.error({ err }, 'Monthly reset error');
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
// HEALTH CHECK
// ============================================
setInterval(() => {
    const connectedSessions = Array.from(sessionStates.values())
        .filter(s => s.status === 'connected').length;
    const runningCampaigns = activeCampaigns.size;
    const scheduledCampaigns = scheduledTimers.size;
    
    logger.info({ 
        connectedSessions, 
        runningCampaigns,
        scheduledCampaigns,
        uptime: process.uptime().toFixed(0) + 's'
    }, '❤️ Health check');
}, 60000);

// ============================================
// LOAD SESSIONS ON STARTUP
// ============================================
async function loadAllSessions() {
    try {
        await initializeReferralSystem();
        
        const snap = await db.ref('whatsapp_accounts').once('value');

        const sessionsToLoad = [];
        snap.forEach((child) => {
            const account = child.val();
            if (account.status !== 'logged_out') {
                sessionsToLoad.push({ id: child.key, ...account });
            }
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
            } else {
                console.log(`⚠️ No saved session for ${data.phone}, will need re-pairing`);
                await db.ref('whatsapp_accounts/' + sessionId).update({
                    status: 'disconnected',
                    updatedAt: admin.database.ServerValue.TIMESTAMP
                });
            }
        }
        logger.info(`✅ ${sessionsToLoad.length} sessions processed`);
        
        await delay(2000);
        await rescheduleCampaigns();
        
        // No need to explicitly resume campaigns here
        // Firebase listeners will detect running campaigns and start them
        
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
    
    for (const [campaignId, timer] of campaignRetryTimers) {
        clearTimeout(timer);
    }
    campaignRetryTimers.clear();
    
    for (const [campaignId, timer] of scheduledTimers) {
        clearTimeout(timer);
    }
    scheduledTimers.clear();
    
    messageDeliveryQueue.clear();
    
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
    console.log(`🚀 WS-Income Engine v5.0 on port ${PORT}`);
    console.log('📡 Firebase Realtime Database');
    console.log('⚡ Event-Driven Architecture');
    console.log('🎯 Only 5 Core API Endpoints');
    console.log('🤖 Automated via Firebase Listeners & Cron');
    console.log('👥 Multi-Level Referral System (Auto)');
    console.log('📬 Message Delivery Tracking with Auto-Payment');
    console.log('🌍 Timezone: Asia/Dhaka');
    console.log('═══════════════════════════════════════════');
    await loadAllSessions();
});
