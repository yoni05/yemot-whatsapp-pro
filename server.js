const express = require('express');
const pino = require('pino');
const QRCode = require('qrcode');
const { MongoClient } = require('mongodb');
const {
    default: makeWASocket,
    DisconnectReason,
    fetchLatestBaileysVersion,
    BufferJSON,
    initAuthCreds,
    proto,
    downloadMediaMessage
} = require('@whiskeysockets/baileys');

const app = express();
app.use(express.urlencoded({ extended: true }));
app.use(express.json());
app.use((req, res, next) => {
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, DELETE, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
    if (req.method === 'OPTIONS') return res.sendStatus(200);
    next();
});

const TOKEN = (process.env.YEMOT_TOKEN || '').trim();
const GEMINI_KEY = (process.env.GEMINI_API_KEY || '').trim();
const MONGODB_URI = (process.env.MONGODB_URI || '').trim();
let tzintukPhone = (process.env.TZINTUK_PHONE || '').replace(/[^0-9]/g, '');
const YM = 'https://www.call2all.co.il/ym/api/';

let sock = null;
let latestQrDataUrl = null;
let waStatus = 'מאתחל חיבור...';
let lastTzintukStatus = 'טרם נשלח צינתוק';
let lastTzintukTime = 0;
let cachedModelName = null;
let mongoDb = null;

// מאגרים בזיכרון שמסונכרנים ל-MongoDB
const privateInbox = [];   // שלוחה 2: הודעות פרטיות נכנסות
const groupInbox = [];     // שלוחה 3: הודעות קבוצות נכנסות
const sentOutbox = [];     // שלוחה 5: הודעות שאני שלחתי
const contactsList = [];   // שלוחה 6: אנשי קשר [{ name, phone, jid }]
const groupsListCache = [];
const chatHistoryMap = new Map(); // key: normalizedPhoneOrJid -> [{ sender, text, time, ts, fromMe }]
const lidToPhoneMap = new Map();  // מיפוי @lid למספר טלפון רגיל
const rawMsgCache = new Map();    // עבור getMessage של Baileys
const processedMsgIds = new Set();
const userCallSession = new Map();

async function fetchWithTimeout(url, options = {}, timeout = 14000) {
    const controller = new AbortController();
    const id = setTimeout(() => controller.abort(), timeout);
    try {
        const res = await fetch(url, { ...options, signal: controller.signal });
        clearTimeout(id);
        return res;
    } catch (err) {
        clearTimeout(id);
        throw err;
    }
}

function lastVal(param) {
    if (Array.isArray(param)) return String(param[param.length - 1] || '').trim();
    return String(param || '').trim();
}

// קריטי לימות המשיח: אסור שיופיעו נקודות (.) או נקודתיים (:) או (=) בתוך פקודת t-
function cleanTts(str, maxLen = 220) {
    return String(str || '')
        .replace(/https?:\/\/\S+/gi, ' קישור ')
        .replace(/[:.]/g, ' ')
        .replace(/[^א-תa-zA-Z0-9\s,?!-]/g, ' ')
        .replace(/\s+/g, ' ')
        .trim()
        .slice(0, maxLen);
}

function yemotRead(ttsPrompt, varName, opts = 'no,1,1,7,Digits,no') {
    return `read=t-${cleanTts(ttsPrompt, 360)}=${varName},${opts}`;
}

function yemotSayAndRead(sayTts, readTts, varName, opts = 'no,1,1,7,Digits,no') {
    return `id_list_message=t-${cleanTts(sayTts, 260)}&read=t-${cleanTts(readTts, 360)}=${varName},${opts}`;
}

function normalizePhone(phoneStr) {
    let d = String(phoneStr || '').replace(/[^0-9]/g, '');
    if (d.startsWith('972') && d.length >= 11) d = '0' + d.slice(3);
    return d;
}

function phoneToJid(phoneStr) {
    let digits = String(phoneStr || '').replace(/[^0-9]/g, '');
    if (digits.startsWith('0')) digits = '972' + digits.slice(1);
    return `${digits}@s.whatsapp.net`;
}

function resolveContactName(phoneOrJid, fallbackName = '') {
    const norm = normalizePhone(String(phoneOrJid || '').split('@')[0]);
    const found = contactsList.find(c => normalizePhone(c.phone) === norm || c.jid === phoneOrJid);
    if (found) return found.name;
    return cleanTts(fallbackName || norm || 'לא ידוע', 35);
}

function addChatHistory(chatKey, sender, text, time, fromMe) {
    if (!chatKey) return;
    if (!chatHistoryMap.has(chatKey)) chatHistoryMap.set(chatKey, []);
    const arr = chatHistoryMap.get(chatKey);
    arr.push({ sender: cleanTts(sender, 35), text: cleanTts(text, 180), time: cleanTts(time, 20), ts: Date.now(), fromMe: !!fromMe });
    if (arr.length > 30) arr.shift();
}

// --- 1. מסד נתונים MongoDB: שמירת חיבור, הודעות, היסטוריה, אנשי קשר וצינתוק ---
async function initMongoAndLoadState() {
    if (!MONGODB_URI) return;
    try {
        const client = new MongoClient(MONGODB_URI);
        await client.connect();
        mongoDb = client.db('whatsapp_yemot');

        const savedContacts = await mongoDb.collection('contacts').find({}).toArray();
        contactsList.length = 0;
        for (const c of savedContacts) {
            if (c.name && c.phone) {
                contactsList.push({ name: cleanTts(c.name, 35), phone: normalizePhone(c.phone), jid: phoneToJid(c.phone) });
            }
        }

        const savedPrivate = await mongoDb.collection('inbox_private').find({}).sort({ timestamp: -1 }).limit(60).toArray();
        privateInbox.length = 0;
        for (const m of savedPrivate) {
            privateInbox.push(m);
            const key = normalizePhone(m.phone || m.jid);
            addChatHistory(key, m.senderName, m.text, m.time, false);
        }

        const savedGroups = await mongoDb.collection('inbox_groups').find({}).sort({ timestamp: -1 }).limit(60).toArray();
        groupInbox.length = 0;
        for (const g of savedGroups) groupInbox.push(g);

        const savedSent = await mongoDb.collection('outbox_sent').find({}).sort({ timestamp: -1 }).limit(60).toArray();
        sentOutbox.length = 0;
        for (const s of savedSent) {
            sentOutbox.push(s);
            const key = normalizePhone(s.targetPhone || s.jid);
            addChatHistory(key, 'אני', s.text, s.time, true);
        }

        const cfg = await mongoDb.collection('settings').findOne({ _id: 'config' });
        if (cfg && cfg.tzintukPhone) tzintukPhone = normalizePhone(cfg.tzintukPhone);

        console.log(`[MongoDB] Loaded ${contactsList.length} contacts, ${privateInbox.length} private msgs, ${sentOutbox.length} sent msgs.`);
    } catch (e) {
        console.warn('[MongoDB Load Warning]:', e.message);
    }
}

async function saveDocToMongo(collName, doc) {
    if (!mongoDb) return;
    try {
        await mongoDb.collection(collName).insertOne({ ...doc });
    } catch {}
}

async function upsertContact(name, phone) {
    const cleanName = cleanTts(name, 35);
    const normPhone = normalizePhone(phone);
    if (!cleanName || normPhone.length < 9) return null;
    const jid = phoneToJid(normPhone);

    const existingIdx = contactsList.findIndex(c => normalizePhone(c.phone) === normPhone);
    const entry = { name: cleanName, phone: normPhone, jid };
    if (existingIdx >= 0) contactsList[existingIdx] = entry;
    else contactsList.push(entry);

    if (mongoDb) {
        try {
            await mongoDb.collection('contacts').updateOne(
                { phone: normPhone },
                { $set: entry },
                { upsert: true }
            );
        } catch {}
    }
    return entry;
}

async function deleteContactByPhone(phone) {
    const normPhone = normalizePhone(phone);
    const idx = contactsList.findIndex(c => normalizePhone(c.phone) === normPhone);
    if (idx >= 0) contactsList.splice(idx, 1);
    if (mongoDb) {
        try { await mongoDb.collection('contacts').deleteOne({ phone: normPhone }); } catch {}
    }
}

async function recordOutgoingMessage(targetJid, targetLabel, textContent) {
    const cleanText = cleanTts(textContent, 180);
    if (!cleanText) return;
    const timeStr = new Date().toLocaleTimeString('he-IL', { timeZone: 'Asia/Jerusalem', hour: '2-digit', minute: '2-digit' }).replace(':', ' ו ');
    const normPhone = normalizePhone(String(targetJid || '').split('@')[0]);
    const resolvedName = resolveContactName(targetJid, targetLabel || normPhone);

    const entry = {
        jid: targetJid,
        targetPhone: normPhone,
        targetName: resolvedName,
        text: cleanText,
        time: timeStr,
        timestamp: Date.now()
    };

    sentOutbox.unshift(entry);
    if (sentOutbox.length > 80) sentOutbox.pop();
    addChatHistory(normPhone || targetJid, 'אני', cleanText, timeStr, true);
    await saveDocToMongo('outbox_sent', entry);
}

// --- 2. שליחת צינתוק בימות המשיח כשמתקבלת הודעה פרטית חדשה ---
async function triggerYemotTzintuk(forceTest = false) {
    if (!TOKEN || !tzintukPhone) {
        lastTzintukStatus = 'חסר טוקן ימות המשיח או מספר טלפון לצינתוק';
        return { ok: false, reason: lastTzintukStatus };
    }
    const now = Date.now();
    if (!forceTest && (now - lastTzintukTime < 25000)) {
        return { ok: true, skipped: true };
    }
    lastTzintukTime = now;

    try {
        // ניסיון 1: הפעלת RunTzintuk ישירות למספר
        const url1 = `${YM}RunTzintuk?token=${encodeURIComponent(TOKEN)}&phones=${encodeURIComponent(tzintukPhone)}`;
        const r1 = await fetchWithTimeout(url1, {}, 10000);
        const j1 = await r1.json().catch(() => ({}));
        if (j1 && j1.responseStatus === 'OK') {
            lastTzintukStatus = `צינתוק נשלח בהצלחה ל-${tzintukPhone} (${new Date().toLocaleTimeString('he-IL')})`;
            return { ok: true, data: j1 };
        }

        // ניסיון 2: עדכון רשימת צינתוקים 1 (tzl:1) והפעלת RunTzintuk על tzl:1
        const uploadUrl = `${YM}UploadPhoneList?token=${encodeURIComponent(TOKEN)}&name=1&type=tzl&data=${encodeURIComponent(tzintukPhone)}`;
        await fetchWithTimeout(uploadUrl, {}, 8000).catch(() => null);

        const url2 = `${YM}RunTzintuk?token=${encodeURIComponent(TOKEN)}&phones=tzl:1`;
        const r2 = await fetchWithTimeout(url2, {}, 10000);
        const j2 = await r2.json().catch(() => ({}));
        if (j2 && j2.responseStatus === 'OK') {
            lastTzintukStatus = `צינתוק (רשימה 1) נשלח בהצלחה ל-${tzintukPhone}`;
            return { ok: true, data: j2 };
        }

        lastTzintukStatus = `תגובת ימות המשיח לצינתוק: ${JSON.stringify(j2 || j1).slice(0, 120)}`;
        return { ok: false, data: j2 || j1 };
    } catch (e) {
        lastTzintukStatus = `שגיאה בשליחת צינתוק: ${e.message}`;
        return { ok: false, error: e.message };
    }
}

// --- 3. ניהול Auth State ב-MongoDB ---
async function useMongoDBAuthState(mongoUri) {
    if (!mongoUri) {
        const path = require('path');
        const os = require('os');
        const { useMultiFileAuthState } = require('@whiskeysockets/baileys');
        return await useMultiFileAuthState(path.join(os.tmpdir(), 'wa_auth_local'));
    }
    const client = new MongoClient(mongoUri);
    await client.connect();
    const collection = client.db('whatsapp_yemot').collection('auth_state');

    const writeData = async (data, id) => {
        const serialized = JSON.stringify(data, BufferJSON.replacer);
        await collection.updateOne({ _id: id }, { $set: { data: serialized } }, { upsert: true });
    };
    const readData = async (id) => {
        try {
            const doc = await collection.findOne({ _id: id });
            return doc?.data ? JSON.parse(doc.data, BufferJSON.reviver) : null;
        } catch { return null; }
    };
    const removeData = async (id) => {
        try { await collection.deleteOne({ _id: id }); } catch {}
    };

    const creds = (await readData('creds')) || initAuthCreds();
    return {
        state: {
            creds,
            keys: {
                get: async (type, ids) => {
                    const data = {};
                    await Promise.all(ids.map(async (id) => {
                        let value = await readData(`${type}-${id}`);
                        if (type === 'app-state-sync-key' && value) {
                            value = proto.Message.AppStateSyncKeyData.fromObject(value);
                        }
                        data[id] = value;
                    }));
                    return data;
                },
                set: async (data) => {
                    const tasks = [];
                    for (const category in data) {
                        for (const id in data[category]) {
                            const value = data[category][id];
                            const key = `${category}-${id}`;
                            tasks.push(value ? writeData(value, key) : removeData(key));
                        }
                    }
                    await Promise.all(tasks);
                }
            }
        },
        saveCreds: () => writeData(creds, 'creds')
    };
}

// --- 4. תמלול הודעות קוליות עם Gemini ---
async function getFastGeminiModels() {
    const defaults = ['gemini-2.5-flash', 'gemini-2.0-flash', 'gemini-flash-latest'];
    if (cachedModelName) return [cachedModelName, ...defaults.filter(m => m !== cachedModelName)];
    try {
        const res = await fetchWithTimeout(`https://generativelanguage.googleapis.com/v1beta/models?key=${GEMINI_KEY}`, {}, 5000);
        if (res.ok) {
            const data = await res.json();
            const avail = (data.models || [])
                .filter(m => (m.supportedGenerationMethods || []).includes('generateContent'))
                .map(m => m.name.replace('models/', ''))
                .filter(m => m.includes('flash') && !m.includes('tts') && !m.includes('image'));
            if (avail.length > 0) {
                cachedModelName = avail[0];
                return avail;
            }
        }
    } catch {}
    return defaults;
}

async function transcribeAudioBuffer(audioBuf, mimeType = 'audio/wav') {
    const models = await getFastGeminiModels();
    const audioBase64 = audioBuf.toString('base64');
    const prompt = 'תמלל את ההקלטה לעברית בצורה המדויקת ביותר. החזר אך ורק את המילים שנאמרו ללא שום תוספת וללא סימני פיסוק.';

    for (const model of models.slice(0, 3)) {
        try {
            const url = `https://generativelanguage.googleapis.com/v1beta/models/${model}:generateContent?key=${GEMINI_KEY}`;
            const res = await fetchWithTimeout(url, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    contents: [{ parts: [{ text: prompt }, { inlineData: { mimeType, data: audioBase64 } }] }],
                    generationConfig: { temperature: 0.1, maxOutputTokens: 250 }
                })
            }, 11000);
            if (res.ok) {
                const data = await res.json();
                const text = (data?.candidates?.[0]?.content?.parts || []).map(p => p.text || '').join(' ').trim();
                if (text) return text;
            }
        } catch {}
    }
    return '';
}

async function downloadRecordingFromYemot(recordPath, apiDir) {
    const cleanPath = String(recordPath).replace(/^ivr2:\/*/, '').replace(/^\/*/, '');
    const cleanDir = String(apiDir || '').replace(/^ivr2:\/*/, '').replace(/^\/*/, '').replace(/\/*$/, '');
    const candidates = [`ivr2:/${cleanPath}`, `ivr2:${cleanPath}`];
    if (cleanDir && !cleanPath.startsWith(cleanDir + '/')) candidates.unshift(`ivr2:/${cleanDir}/${cleanPath}`);
    if (!cleanPath.toLowerCase().endsWith('.wav')) {
        candidates.unshift(`ivr2:/${cleanPath}.wav`);
        if (cleanDir) candidates.unshift(`ivr2:/${cleanDir}/${cleanPath}.wav`);
    }
    for (const p of [...new Set(candidates)]) {
        try {
            const url = `${YM}DownloadFile?token=${encodeURIComponent(TOKEN)}&path=${encodeURIComponent(p)}`;
            const res = await fetchWithTimeout(url, {}, 9000);
            if (!res.ok) continue;
            const buf = Buffer.from(await res.arrayBuffer());
            const head = buf.toString('utf8', 0, Math.min(buf.length, 120));
            if (buf.length > 200 && !head.includes('responseStatus') && !head.includes('EXCEPTION')) {
                return buf;
            }
        } catch {}
    }
    throw new Error('Failed to download recording from Yemot');
}

// --- 5. חילוץ תוכן הודעת ווצאפ מכל סוגי העטיפות (כולל הודעות נעלמות) ---
function unwrapWaMessage(msgObj) {
    if (!msgObj) return null;
    let m = msgObj;
    for (let i = 0; i < 4; i++) {
        if (m.ephemeralMessage?.message) m = m.ephemeralMessage.message;
        else if (m.viewOnceMessage?.message) m = m.viewOnceMessage.message;
        else if (m.viewOnceMessageV2?.message) m = m.viewOnceMessageV2.message;
        else if (m.viewOnceMessageV2Extension?.message) m = m.viewOnceMessageV2Extension.message;
        else if (m.documentWithCaptionMessage?.message) m = m.documentWithCaptionMessage.message;
        else if (m.editedMessage?.message?.protocolMessage?.editedMessage) m = m.editedMessage.message.protocolMessage.editedMessage;
        else break;
    }
    return m;
}

async function refreshGroupsCache() {
    if (!sock) return;
    try {
        const groupsObj = await sock.groupFetchAllParticipating();
        groupsListCache.length = 0;
        for (const [jid, meta] of Object.entries(groupsObj || {})) {
            groupsListCache.push({ jid, name: cleanTts(meta.subject || 'קבוצה', 45) });
        }
    } catch {}
}

async function startWhatsAppConnection() {
    try {
        const { state, saveCreds } = await useMongoDBAuthState(MONGODB_URI);
        const { version } = await fetchLatestBaileysVersion();

        sock = makeWASocket({
            version,
            logger: pino({ level: 'silent' }),
            printQRInTerminal: false,
            auth: state,
            browser: ['Yemot WhatsApp Pro', 'Chrome', '122.0.0'],
            syncFullHistory: false,
            getMessage: async (key) => {
                return rawMsgCache.get(key.id) || { conversation: '' };
            }
        });

        sock.ev.on('creds.update', saveCreds);

        sock.ev.on('connection.update', async (update) => {
            const { connection, lastDisconnect, qr } = update;
            if (qr) {
                latestQrDataUrl = await QRCode.toDataURL(qr);
                waStatus = 'ממתין לסריקת קוד QR';
            }
            if (connection === 'open') {
                latestQrDataUrl = null;
                waStatus = 'מחובר ופעיל 100% (שמור ב-MongoDB)';
                setTimeout(refreshGroupsCache, 3000);
            }
            if (connection === 'close') {
                const statusCode = lastDisconnect?.error?.output?.statusCode;
                const shouldReconnect = statusCode !== DisconnectReason.loggedOut;
                waStatus = `החיבור נסגר (קוד ${statusCode}). מתחבר מחדש...`;
                if (shouldReconnect) setTimeout(startWhatsAppConnection, 4000);
                else waStatus = 'נותק מהמכשיר. יש לסרוק QR מחדש.';
            }
        });

        sock.ev.on('contacts.upsert', (contacts) => {
            for (const c of (contacts || [])) {
                if (c.id && c.lid) {
                    lidToPhoneMap.set(c.lid, c.id);
                }
            }
        });

        sock.ev.on('messages.upsert', async ({ messages }) => {
            for (const msg of (messages || [])) {
                try {
                    if (!msg.key || !msg.message) continue;
                    const msgId = msg.key.id;
                    if (msgId) {
                        if (processedMsgIds.has(msgId)) continue;
                        processedMsgIds.add(msgId);
                        rawMsgCache.set(msgId, msg.message);
                        if (processedMsgIds.size > 500) {
                            const first = processedMsgIds.values().next().value;
                            processedMsgIds.delete(first);
                            rawMsgCache.delete(first);
                        }
                    }

                    const rawJid = msg.key.remoteJid || '';
                    if (!rawJid || rawJid === 'status@broadcast') continue;

                    const altJid = msg.key.senderPn || msg.key.remoteJidAlt || msg.key.participantPn || lidToPhoneMap.get(rawJid) || '';
                    if (rawJid.endsWith('@lid') && altJid.endsWith('@s.whatsapp.net')) {
                        lidToPhoneMap.set(rawJid, altJid);
                    }
                    const effectiveJid = rawJid.endsWith('@s.whatsapp.net') ? rawJid : (altJid.endsWith('@s.whatsapp.net') ? altJid : rawJid);
                    const isGroup = rawJid.endsWith('@g.us');

                    const inner = unwrapWaMessage(msg.message);
                    if (!inner) continue;

                    let textContent =
                        inner.conversation ||
                        inner.extendedTextMessage?.text ||
                        inner.imageMessage?.caption ||
                        inner.videoMessage?.caption ||
                        inner.documentMessage?.caption || '';

                    const audioMsg = inner.audioMessage;
                    if (!textContent && audioMsg) {
                        try {
                            const audioBuffer = await downloadMediaMessage(msg, 'buffer', {});
                            const transcribed = await transcribeAudioBuffer(audioBuffer, audioMsg.mimetype || 'audio/ogg');
                            textContent = transcribed ? `הודעה קולית ${transcribed}` : 'הודעה קולית';
                        } catch {
                            textContent = 'הודעה קולית';
                        }
                    }

                    const cleanText = cleanTts(textContent, 180);
                    if (!cleanText) continue;

                    // אם זו הודעה שאני שלחתי מהמכשיר עצמו - נשמור לשלוחה 5 רק אם לא נשמרה כבר
                    if (msg.key.fromMe) {
                        const recentSame = sentOutbox[0] && sentOutbox[0].text === cleanText && (Date.now() - sentOutbox[0].timestamp < 8000);
                        if (!recentSame && !isGroup) {
                            await recordOutgoingMessage(effectiveJid, msg.pushName || '', cleanText);
                        }
                        continue;
                    }

                    const timeStr = new Date().toLocaleTimeString('he-IL', { timeZone: 'Asia/Jerusalem', hour: '2-digit', minute: '2-digit' }).replace(':', ' ו ');

                    if (isGroup) {
                        const grpObj = groupsListCache.find(g => g.jid === rawJid);
                        const groupTitle = grpObj ? grpObj.name : 'קבוצה';
                        const participantJid = msg.key.participant || altJid || '';
                        const senderName = resolveContactName(participantJid, msg.pushName || participantJid.split('@')[0]);
                        const entry = {
                            jid: rawJid,
                            groupName: groupTitle,
                            senderName,
                            text: cleanText,
                            time: timeStr,
                            timestamp: Date.now()
                        };
                        groupInbox.unshift(entry);
                        if (groupInbox.length > 80) groupInbox.pop();
                        addChatHistory(rawJid, senderName, cleanText, timeStr, false);
                        await saveDocToMongo('inbox_groups', entry);
                    } else {
                        const phoneDigits = normalizePhone(effectiveJid.split('@')[0]);
                        const senderName = resolveContactName(effectiveJid, msg.pushName || phoneDigits);
                        const entry = {
                            jid: effectiveJid,
                            rawJid,
                            phone: phoneDigits,
                            senderName,
                            text: cleanText,
                            time: timeStr,
                            timestamp: Date.now()
                        };
                        privateInbox.unshift(entry);
                        if (privateInbox.length > 80) privateInbox.pop();
                        addChatHistory(phoneDigits || effectiveJid, senderName, cleanText, timeStr, false);
                        await saveDocToMongo('inbox_private', entry);

                        // שליחת צינתוק אוטומטי על הודעה פרטית חדשה!
                        triggerYemotTzintuk(false).catch(() => null);
                    }
                } catch (err) {
                    console.error('[Incoming Handler Error]:', err.message);
                }
            }
        });
    } catch (err) {
        waStatus = 'שגיאה באתחול: ' + err.message;
        setTimeout(startWhatsAppConnection, 6000);
    }
}

initMongoAndLoadState().then(startWhatsAppConnection);

// --- 6. ממשק API ולוח בקרה ב-Web לניהול אנשי קשר, צינתוקים וניטור ---
app.get('/api/contacts', (req, res) => {
    res.json({ contacts: contactsList, tzintukPhone, lastTzintukStatus, privateCount: privateInbox.length, sentCount: sentOutbox.length });
});

app.post('/api/contacts', async (req, res) => {
    const { name, phone } = req.body || {};
    const added = await upsertContact(name, phone);
    if (!added) return res.status(400).json({ error: 'שם או מספר טלפון לא תקינים' });
    if (req.headers.accept && req.headers.accept.includes('text/html')) return res.redirect('/');
    res.json({ ok: true, contact: added, contacts: contactsList });
});

app.post('/api/contacts/delete', async (req, res) => {
    const { phone } = req.body || {};
    await deleteContactByPhone(phone);
    if (req.headers.accept && req.headers.accept.includes('text/html')) return res.redirect('/');
    res.json({ ok: true, contacts: contactsList });
});

app.post('/api/tzintuk', async (req, res) => {
    const { phone, test } = req.body || {};
    if (phone !== undefined) {
        tzintukPhone = normalizePhone(phone);
        if (mongoDb) {
            try { await mongoDb.collection('settings').updateOne({ _id: 'config' }, { $set: { tzintukPhone } }, { upsert: true }); } catch {}
        }
    }
    let testResult = null;
    if (test) testResult = await triggerYemotTzintuk(true);
    if (req.headers.accept && req.headers.accept.includes('text/html')) return res.redirect('/');
    res.json({ ok: true, tzintukPhone, lastTzintukStatus, testResult });
});

app.get('/', (req, res) => {
    const contactsRows = contactsList.map(c => `
        <tr>
            <td><b>${c.name}</b></td>
            <td dir="ltr">${c.phone}</td>
            <td>
                <form method="POST" action="/api/contacts/delete" style="margin:0">
                    <input type="hidden" name="phone" value="${c.phone}">
                    <button type="submit" style="background:#b91c1c;color:#fff;border:0;padding:4px 10px;border-radius:6px;cursor:pointer">מחק</button>
                </form>
            </td>
        </tr>`).join('');

    res.type('text/html; charset=utf-8').send(`<!DOCTYPE html>
<html lang="he" dir="rtl">
<head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>לוח בקרה ווצאפ קולי - ימות המשיח</title>
<style>
body{font-family:Arial,sans-serif;background:#f3f5f4;color:#16221d;max-width:740px;margin:24px auto;padding:16px}
.card{background:#fff;padding:20px;border-radius:12px;border:1px solid #d5ddd9;margin-bottom:18px;box-shadow:0 2px 6px rgba(0,0,0,0.04)}
.status{font-size:17px;font-weight:bold;color:#0f6b4f;margin:8px 0}
img.qr{max-width:260px;border:6px solid #0f6b4f;border-radius:12px;margin:12px auto;display:block}
input{padding:8px 10px;border:1px solid #d5ddd9;border-radius:6px;font-size:14px}
button{background:#0f6b4f;color:#fff;border:0;padding:9px 14px;border-radius:6px;font-weight:bold;cursor:pointer}
table{width:100%;border-collapse:collapse;margin-top:12px;font-size:14px}
th,td{border-bottom:1px solid #e5ece8;padding:8px;text-align:right}
</style>
</head>
<body>
<div class="card" style="text-align:center">
<h2 style="margin:0 0 6px">מערכת ווצאפ קולית לימות המשיח - לוח בקרה</h2>
<div class="status">${waStatus}</div>
${latestQrDataUrl ? `<p>סרוק את קוד ה-QR מהווצאפ שלך:</p><img class="qr" src="${latestQrDataUrl}">` : `<p>הודעות נכנסות (שלוחה 2): <b>${privateInbox.length}</b> | הודעות קבוצות (שלוחה 3): <b>${groupInbox.length}</b> | הודעות ששלחתי (שלוחה 5): <b>${sentOutbox.length}</b> | אנשי קשר (שלוחה 6): <b>${contactsList.length}</b></p>`}
</div>

<div class="card">
<h3 style="margin-top:0">הגדרת צינתוק בקבלת הודעה פרטית חדשה</h3>
<form method="POST" action="/api/tzintuk" style="display:flex;gap:8px;flex-wrap:wrap;align-items:center">
<input type="text" name="phone" value="${tzintukPhone}" placeholder="מספר טלפון לצינתוק (050...)" dir="ltr">
<button type="submit">שמור מספר</button>
<button type="submit" name="test" value="1" style="background:#2b5b84">שלח צינתוק בדיקה</button>
</form>
<p style="font-size:13px;color:#5b6b64;margin:8px 0 0">סטטוס צינתוק אחרון: ${lastTzintukStatus}</p>
</div>

<div class="card">
<h3 style="margin-top:0">ניהול אנשי קשר (מסונכרן לקו הטלפוני ול-MongoDB)</h3>
<form method="POST" action="/api/contacts" style="display:flex;gap:8px;flex-wrap:wrap">
<input type="text" name="name" placeholder="שם איש הקשר (בעברית)" required>
<input type="text" name="phone" placeholder="מספר טלפון (050...)" dir="ltr" required>
<button type="submit">הוסף איש קשר</button>
</form>
<table>
<thead><tr><th>שם</th><th>טלפון</th><th>פעולה</th></tr></thead>
<tbody>${contactsRows || '<tr><td colspan="3">עדיין לא נוספו אנשי קשר</td></tr>'}</tbody>
</table>
</div>
</body></html>`);
});

// --- 7. נתיב ה-API המרכזי לימות המשיח (שלוחות 1, 2, 3, 4, 5, 6) ---
const MAIN_MENU_PROMPT = 'ברוכים הבאים למערכת הווצאפ לשליחת הודעה הקישו 1, לשמיעת הודעות שנשלחו אליכם הקישו 2, לקבוצות הקישו 3, לשיחה חיה בזמן אמת הקישו 4, לשמיעת הודעות שאתם שלחתם הקישו 5, לאנשי קשר וצינתוקים הקישו 6, ליציאה הקישו 0';

async function sendWhatsAppTextAndRecord(targetJid, targetLabel, text) {
    let sendJid = targetJid;
    // אם יש מיפוי או מספר רגיל, נוודא JID תקין
    if (!sendJid.includes('@')) sendJid = phoneToJid(sendJid);
    await sock.sendMessage(sendJid, { text });
    await recordOutgoingMessage(sendJid, targetLabel, text);
}

app.all(['/yemot', '/yemot/:mode'], async (req, res) => {
    const q = { ...req.query, ...req.body };
    res.type('text/plain; charset=utf-8');

    try {
        const callerPhone = normalizePhone(lastVal(q.ApiPhone)) || 'guest';
        const callId = lastVal(q.ApiCallId) || callerPhone;
        const routeMode = req.params.mode || lastVal(q.mode) || '';

        // אם טרם הוגדר מספר לצינתוק, נגדיר אוטומטית את המספר של המתקשר הראשון לקו
        if (!tzintukPhone && callerPhone.startsWith('05')) {
            tzintukPhone = callerPhone;
        }

        if (q.hangup === 'yes') {
            userCallSession.delete(callId);
            return res.send('noop');
        }

        if (!sock) {
            return res.send('id_list_message=t-שרת הווצאפ מתאתחל כעת אנא נסו שוב בעוד מספר שניות&go_to_folder=..');
        }

        let session = userCallSession.get(callId);
        const tempRecFolder = `/users/${callerPhone}/temp`;

        // פונקציית עזר למעבר לשלוחה (תומכת גם בכניסה ישירה לשלוחה 1-6)
        const initStepForMode = (m) => {
            if (m === '1') return 'EXT1_MENU';
            if (m === '2') return 'EXT2_ENTER';
            if (m === '3') return 'EXT3_MENU_INIT';
            if (m === '4') return 'EXT4_MENU_INIT';
            if (m === '5') return 'EXT5_ENTER';
            if (m === '6') return 'EXT6_MENU_INIT';
            return 'MAIN_MENU_PROMPT';
        };

        if (!session) {
            session = { step: initStepForMode(routeMode), lastRouteMode: routeMode, msgIdx: 0, grpIdx: 0, ctIdx: 0, sentIdx: 0 };
            userCallSession.set(callId, session);
        } else if (routeMode && routeMode !== session.lastRouteMode) {
            session.lastRouteMode = routeMode;
            session.step = initStepForMode(routeMode);
        }

        // תפריט ראשי
        if (session.step === 'MAIN_MENU_PROMPT') {
            session.step = 'MAIN_MENU_WAIT';
            return res.send(yemotRead(MAIN_MENU_PROMPT, 'MenuChoice'));
        }

        if (session.step === 'MAIN_MENU_WAIT') {
            const menuChoice = lastVal(q.MenuChoice);
            if (menuChoice === '1') session.step = 'EXT1_MENU';
            else if (menuChoice === '2') session.step = 'EXT2_ENTER';
            else if (menuChoice === '3') session.step = 'EXT3_MENU_INIT';
            else if (menuChoice === '4') session.step = 'EXT4_MENU_INIT';
            else if (menuChoice === '5') session.step = 'EXT5_ENTER';
            else if (menuChoice === '6') session.step = 'EXT6_MENU_INIT';
            else if (menuChoice === '0' || menuChoice === '*') {
                userCallSession.delete(callId);
                return res.send('go_to_folder=..');
            } else {
                return res.send(yemotRead(MAIN_MENU_PROMPT, 'MenuChoice'));
            }
        }

        // =========================================================================
        // שלוחה 1: שליחת הודעת ווצאפ (מאנשי קשר / הקשת מספר / הוספת איש קשר)
        // =========================================================================
        if (session.step === 'EXT1_MENU') {
            session.step = 'EXT1_MENU_WAIT';
            return res.send(yemotRead('שלוחה 1 שליחת הודעת ווצאפ לבחירת איש קשר מהרשימה הקישו 1, להקשת מספר טלפון הקישו 2, להוספת איש קשר חדש הקישו 3, לחזרה לתפריט הקישו 0', 'Ext1Mode'));
        }

        if (session.step === 'EXT1_MENU_WAIT') {
            const m = lastVal(q.Ext1Mode);
            if (m === '1') {
                session.ctIdx = 0;
                session.afterContactPick = 'SEND_MSG';
                session.step = 'PICK_CONTACT_PROMPT';
            } else if (m === '2') {
                session.step = 'EXT1_WAIT_PHONE';
                return res.send(yemotRead('אנא הקישו את מספר הטלפון של הנמען ובסיום הקישו סולמית', 'TargetPhone', 'no,15,9,7,Digits,no'));
            } else if (m === '3') {
                session.step = 'EXT6_ADD_WAIT_PHONE';
                return res.send(yemotRead('הוספת איש קשר חדש אנא הקישו את מספר הטלפון של איש הקשר ובסיום הקישו סולמית', 'NewCtPhone', 'no,15,9,7,Digits,no'));
            } else {
                session.step = 'MAIN_MENU_WAIT';
                return res.send(yemotRead(MAIN_MENU_PROMPT, 'MenuChoice'));
            }
        }

        if (session.step === 'EXT1_WAIT_PHONE') {
            const targetPhone = lastVal(q.TargetPhone);
            if (!targetPhone) return res.send(yemotRead('אנא הקישו את מספר הטלפון ובסיום סולמית', 'TargetPhone', 'no,15,9,7,Digits,no'));
            session.targetJid = phoneToJid(targetPhone);
            session.targetLabel = resolveContactName(targetPhone, targetPhone);
            session.step = 'EXT1_WAIT_RECORD';
            return res.send(`read=t-אנא הקליטו את ההודעה עבור ${cleanTts(session.targetLabel, 40)} ובסיום הקישו סולמית=RecPath,no,record,${tempRecFolder},wa_send,no,7,1`);
        }

        if (session.step === 'EXT1_WAIT_RECORD') {
            const recPath = lastVal(q.RecPath);
            const audioBuf = await downloadRecordingFromYemot(recPath, lastVal(q.ApiExtension));
            const transcribed = await transcribeAudioBuffer(audioBuf, 'audio/wav');
            const cleanMsg = cleanTts(transcribed, 200);

            if (!cleanMsg) {
                session.step = 'MAIN_MENU_WAIT';
                return res.send(yemotSayAndRead('לא הצלחתי לזהות דיבור בהקלטה', MAIN_MENU_PROMPT, 'MenuChoice'));
            }

            await sendWhatsAppTextAndRecord(session.targetJid, session.targetLabel, cleanMsg);
            session.step = 'MAIN_MENU_WAIT';
            return res.send(yemotSayAndRead(`ההודעה ${cleanMsg} נשלחה בהצלחה אל ${session.targetLabel}`, MAIN_MENU_PROMPT, 'MenuChoice'));
        }

        // =========================================================================
        // מנגנון בחירת איש קשר מהרשימה (משותף לשלוחה 1, שלוחה 4 ושלוחה 6)
        // =========================================================================
        if (session.step === 'PICK_CONTACT_PROMPT') {
            if (contactsList.length === 0) {
                session.step = 'EXT6_ADD_WAIT_PHONE';
                return res.send(yemotSayAndRead('רשימת אנשי הקשר שלכם עדיין ריקה בואו נוסיף איש קשר ראשון', 'הקישו את מספר הטלפון של איש הקשר ובסיום סולמית', 'NewCtPhone', 'no,15,9,7,Digits,no'));
            }
            const page = contactsList.slice(session.ctIdx, session.ctIdx + 7);
            const listStr = page.map((c, i) => `ל${c.name} הקישו ${i + 1}`).join(', ');
            const moreStr = contactsList.length > 7 ? ', לאנשי קשר נוספים הקישו 9' : '';
            session.step = 'PICK_CONTACT_WAIT';
            return res.send(yemotRead(`בחירת איש קשר ${listStr}${moreStr}, לחזרה הקישו 0`, 'ContactPick'));
        }

        if (session.step === 'PICK_CONTACT_WAIT') {
            const pickStr = lastVal(q.ContactPick);
            if (pickStr === '0' || pickStr === '*') {
                session.step = 'MAIN_MENU_WAIT';
                return res.send(yemotRead(MAIN_MENU_PROMPT, 'MenuChoice'));
            }
            if (pickStr === '9') {
                session.ctIdx = (session.ctIdx + 7 < contactsList.length) ? (session.ctIdx + 7) : 0;
                session.step = 'PICK_CONTACT_PROMPT';
                const page = contactsList.slice(session.ctIdx, session.ctIdx + 7);
                const listStr = page.map((c, i) => `ל${c.name} הקישו ${i + 1}`).join(', ');
                session.step = 'PICK_CONTACT_WAIT';
                return res.send(yemotRead(`בחירת איש קשר ${listStr}, לאנשי קשר נוספים הקישו 9, לחזרה הקישו 0`, 'ContactPick'));
            }
            const pick = parseInt(pickStr, 10);
            const page = contactsList.slice(session.ctIdx, session.ctIdx + 7);
            if (!isNaN(pick) && pick >= 1 && pick <= page.length) {
                const chosen = page[pick - 1];
                if (session.afterContactPick === 'LIVE_CHAT') {
                    session.liveJid = chosen.jid;
                    session.livePhone = normalizePhone(chosen.phone);
                    session.liveLabel = chosen.name;
                    session.lastHeardTs = 0;
                    session.step = 'EXT4_LIVE_PROMPT';
                } else if (session.afterContactPick === 'DELETE_CONTACT') {
                    await deleteContactByPhone(chosen.phone);
                    session.step = 'MAIN_MENU_WAIT';
                    return res.send(yemotSayAndRead(`איש הקשר ${chosen.name} נמחק בהצלחה`, MAIN_MENU_PROMPT, 'MenuChoice'));
                } else {
                    session.targetJid = chosen.jid;
                    session.targetLabel = chosen.name;
                    session.step = 'EXT1_WAIT_RECORD';
                    return res.send(`read=t-נבחר ${cleanTts(chosen.name, 35)} אנא הקליטו את ההודעה ובסיום הקישו סולמית=RecPath,no,record,${tempRecFolder},wa_send,no,7,1`);
                }
            } else {
                session.step = 'MAIN_MENU_WAIT';
                return res.send(yemotRead(MAIN_MENU_PROMPT, 'MenuChoice'));
            }
        }

        // =========================================================================
        // שלוחה 2: שמיעת הודעות פרטיות שנשלחו אליך (ללא באג הנקודה!)
        // =========================================================================
        const renderInboxCurrent = (prefixNotice = '') => {
            if (privateInbox.length === 0) {
                session.step = 'MAIN_MENU_WAIT';
                return res.send(yemotSayAndRead('אין הודעות פרטיות חדשות שהתקבלו', MAIN_MENU_PROMPT, 'MenuChoice'));
            }
            if (session.msgIdx >= privateInbox.length) session.msgIdx = 0;
            if (session.msgIdx < 0) session.msgIdx = privateInbox.length - 1;
            const item = privateInbox[session.msgIdx];
            const senderDisplay = resolveContactName(item.phone || item.jid, item.senderName);
            const promptText = `הודעה ${session.msgIdx + 1} מתוך ${privateInbox.length} מאת ${senderDisplay} בשעה ${item.time}, תוכן ההודעה, ${item.text}, למענה הקישו 1, להודעה הבאה הקישו 2, להודעה הקודמת הקישו 3, לשמירת השולח באנשי הקשר הקישו 4, לתפריט הראשי הקישו 0`;
            session.step = 'EXT2_WAIT_ACT';
            if (prefixNotice) return res.send(yemotSayAndRead(prefixNotice, promptText, 'InboxAct'));
            return res.send(yemotRead(promptText, 'InboxAct'));
        };

        if (session.step === 'EXT2_ENTER') {
            session.msgIdx = 0;
            return renderInboxCurrent();
        }

        if (session.step === 'EXT2_WAIT_ACT') {
            const act = lastVal(q.InboxAct);
            const cur = privateInbox[session.msgIdx];
            if (act === '1' && cur) {
                session.targetJid = cur.jid;
                session.targetLabel = resolveContactName(cur.phone || cur.jid, cur.senderName);
                session.step = 'EXT1_WAIT_RECORD';
                return res.send(`read=t-הקליטו כעת את תשובתכם עבור ${cleanTts(session.targetLabel, 35)} ובסיום הקישו סולמית=RecPath,no,record,${tempRecFolder},wa_reply,no,7,1`);
            } else if (act === '2') {
                session.msgIdx++;
                return renderInboxCurrent();
            } else if (act === '3') {
                session.msgIdx--;
                return renderInboxCurrent();
            } else if (act === '4' && cur) {
                session.newCtPhone = cur.phone || normalizePhone(cur.jid);
                session.step = 'EXT6_ADD_WAIT_NAME_REC';
                return res.send(`read=t-הקליטו בקול את השם שתרצו לשמור עבור מספר ${cleanTts(session.newCtPhone, 20)} ובסיום הקישו סולמית=RecPath,no,record,${tempRecFolder},wa_ctname,no,5,1`);
            } else {
                session.step = 'MAIN_MENU_WAIT';
                return res.send(yemotRead(MAIN_MENU_PROMPT, 'MenuChoice'));
            }
        }

        // =========================================================================
        // שלוחה 3: קבוצות ווצאפ
        // =========================================================================
        if (session.step === 'EXT3_MENU_INIT') {
            if (groupsListCache.length === 0) await refreshGroupsCache();
            session.step = 'EXT3_MENU_WAIT';
            return res.send(yemotRead('שלוחה 3 קבוצות ווצאפ לשמיעת ההודעות האחרונות מהקבוצות הקישו 1, לבחירת קבוצה ושליחת הודעה הקישו 2, לתפריט הראשי הקישו 0', 'GroupAct'));
        }

        const renderGroupMsgCurrent = () => {
            if (groupInbox.length === 0) {
                session.step = 'MAIN_MENU_WAIT';
                return res.send(yemotSayAndRead('עדיין לא התקבלו הודעות חדשות בקבוצות', MAIN_MENU_PROMPT, 'MenuChoice'));
            }
            if (session.msgIdx >= groupInbox.length) session.msgIdx = 0;
            const gItem = groupInbox[session.msgIdx];
            const gText = `הודעה ${session.msgIdx + 1} מתוך ${groupInbox.length} בקבוצת ${gItem.groupName} כתב ${gItem.senderName}, ${gItem.text}, לשליחת תגובה לקבוצה הקישו 1, להודעה הבאה הקישו 2, לתפריט הקישו 0`;
            session.step = 'EXT3_LISTEN_WAIT';
            return res.send(yemotRead(gText, 'GroupMsgAct'));
        };

        if (session.step === 'EXT3_MENU_WAIT') {
            const act = lastVal(q.GroupAct);
            if (act === '1') {
                session.msgIdx = 0;
                return renderGroupMsgCurrent();
            } else if (act === '2') {
                session.grpIdx = 0;
                session.step = 'EXT3_PICK_GROUP_PROMPT';
            } else {
                session.step = 'MAIN_MENU_WAIT';
                return res.send(yemotRead(MAIN_MENU_PROMPT, 'MenuChoice'));
            }
        }

        if (session.step === 'EXT3_LISTEN_WAIT') {
            const act = lastVal(q.GroupMsgAct);
            const cur = groupInbox[session.msgIdx];
            if (act === '1' && cur) {
                session.targetJid = cur.jid;
                session.targetLabel = cur.groupName;
                session.step = 'EXT1_WAIT_RECORD';
                return res.send(`read=t-הקליטו את ההודעה לקבוצת ${cleanTts(cur.groupName, 35)} ובסיום הקישו סולמית=RecPath,no,record,${tempRecFolder},wa_grp_reply,no,7,1`);
            } else if (act === '2') {
                session.msgIdx++;
                return renderGroupMsgCurrent();
            } else {
                session.step = 'MAIN_MENU_WAIT';
                return res.send(yemotRead(MAIN_MENU_PROMPT, 'MenuChoice'));
            }
        }

        if (session.step === 'EXT3_PICK_GROUP_PROMPT') {
            if (groupsListCache.length === 0) {
                session.step = 'MAIN_MENU_WAIT';
                return res.send(yemotSayAndRead('לא נמצאו קבוצות בחשבון הווצאפ', MAIN_MENU_PROMPT, 'MenuChoice'));
            }
            const page = groupsListCache.slice(session.grpIdx, session.grpIdx + 7);
            const parts = page.map((g, i) => `לקבוצת ${g.name} הקישו ${i + 1}`).join(', ');
            const more = groupsListCache.length > 7 ? ', לקבוצות נוספות הקישו 9' : '';
            session.step = 'EXT3_PICK_GROUP_WAIT';
            return res.send(yemotRead(`בחירת קבוצה ${parts}${more}, לחזרה הקישו 0`, 'GroupPick'));
        }

        if (session.step === 'EXT3_PICK_GROUP_WAIT') {
            const pickStr = lastVal(q.GroupPick);
            if (pickStr === '9') {
                session.grpIdx = (session.grpIdx + 7 < groupsListCache.length) ? (session.grpIdx + 7) : 0;
                const page = groupsListCache.slice(session.grpIdx, session.grpIdx + 7);
                const parts = page.map((g, i) => `לקבוצת ${g.name} הקישו ${i + 1}`).join(', ');
                return res.send(yemotRead(`בחירת קבוצה ${parts}, לקבוצות נוספות הקישו 9, לחזרה הקישו 0`, 'GroupPick'));
            }
            const pick = parseInt(pickStr, 10);
            const page = groupsListCache.slice(session.grpIdx, session.grpIdx + 7);
            if (!isNaN(pick) && pick >= 1 && pick <= page.length) {
                const chosen = page[pick - 1];
                session.targetJid = chosen.jid;
                session.targetLabel = chosen.name;
                session.step = 'EXT1_WAIT_RECORD';
                return res.send(`read=t-נבחרה קבוצת ${cleanTts(chosen.name, 35)} אנא הקליטו את ההודעה ובסיום הקישו סולמית=RecPath,no,record,${tempRecFolder},wa_grp_send,no,7,1`);
            }
            session.step = 'MAIN_MENU_WAIT';
            return res.send(yemotRead(MAIN_MENU_PROMPT, 'MenuChoice'));
        }

        // =========================================================================
        // שלוחה 4: שיחה חיה בזמן אמת (כולל המתנה חכמה לתגובה חדשה + אנשי קשר)
        // =========================================================================
        if (session.step === 'EXT4_MENU_INIT') {
            session.step = 'EXT4_MENU_WAIT';
            return res.send(yemotRead('שלוחה 4 שיחה חיה בזמן אמת לבחירת איש קשר לשיחה הקישו 1, להקשת מספר טלפון הקישו 2, לשיחה עם הפונה האחרון הקישו 3, לתפריט הקישו 0', 'LiveChoice'));
        }

        if (session.step === 'EXT4_MENU_WAIT') {
            const lc = lastVal(q.LiveChoice);
            if (lc === '1') {
                session.ctIdx = 0;
                session.afterContactPick = 'LIVE_CHAT';
                session.step = 'PICK_CONTACT_PROMPT';
                // מפעיל מיד את תפריט בחירת איש הקשר
                if (contactsList.length === 0) {
                    session.step = 'EXT4_WAIT_PHONE';
                    return res.send(yemotSayAndRead('רשימת אנשי הקשר ריקה', 'הקישו מספר טלפון לשיחה חיה ובסיום סולמית', 'LivePhone', 'no,15,9,7,Digits,no'));
                }
                const page = contactsList.slice(0, 7);
                const listStr = page.map((c, i) => `ל${c.name} הקישו ${i + 1}`).join(', ');
                session.step = 'PICK_CONTACT_WAIT';
                return res.send(yemotRead(`בחירת איש קשר לשיחה חיה ${listStr}, לחזרה הקישו 0`, 'ContactPick'));
            } else if (lc === '2') {
                session.step = 'EXT4_WAIT_PHONE';
                return res.send(yemotRead('הקישו את מספר הטלפון לשיחה חיה ובסיום הקישו סולמית', 'LivePhone', 'no,15,9,7,Digits,no'));
            } else if (lc === '3' && privateInbox.length > 0) {
                const lastIn = privateInbox[0];
                session.liveJid = lastIn.jid;
                session.livePhone = normalizePhone(lastIn.phone || lastIn.jid);
                session.liveLabel = resolveContactName(session.livePhone, lastIn.senderName);
                session.lastHeardTs = 0;
                session.step = 'EXT4_LIVE_PROMPT';
            } else {
                session.step = 'MAIN_MENU_WAIT';
                return res.send(yemotRead(MAIN_MENU_PROMPT, 'MenuChoice'));
            }
        }

        if (session.step === 'EXT4_WAIT_PHONE') {
            const lp = lastVal(q.LivePhone);
            session.livePhone = normalizePhone(lp);
            session.liveJid = phoneToJid(lp);
            session.liveLabel = resolveContactName(session.livePhone, session.livePhone);
            session.lastHeardTs = 0;
            session.step = 'EXT4_LIVE_PROMPT';
        }

        const buildLivePrompt = async (waitForIncomingMs = 0) => {
            const key = session.livePhone || session.liveJid;
            if (waitForIncomingMs > 0) {
                const start = Date.now();
                while (Date.now() - start < waitForIncomingMs) {
                    const hist = chatHistoryMap.get(key) || [];
                    const unreadIncoming = hist.filter(m => !m.fromMe && m.ts > (session.lastHeardTs || 0));
                    if (unreadIncoming.length > 0) break;
                    await new Promise(r => setTimeout(r, 700));
                }
            }
            const hist = chatHistoryMap.get(key) || [];
            const unreadIncoming = hist.filter(m => !m.fromMe && m.ts > (session.lastHeardTs || 0));
            let statusStr = '';
            if (unreadIncoming.length > 0) {
                const latest = unreadIncoming[unreadIncoming.length - 1];
                session.lastHeardTs = latest.ts;
                const combined = unreadIncoming.slice(-2).map(m => `${m.sender} כתב ${m.text}`).join(', ');
                statusStr = `הודעה חדשה בשיחה עם ${session.liveLabel}, ${combined}`;
            } else if (hist.length > 0) {
                const lastMsg = hist[hist.length - 1];
                session.lastHeardTs = Math.max(session.lastHeardTs || 0, lastMsg.ts);
                statusStr = `שיחה חיה עם ${session.liveLabel}, הודעה אחרונה מאת ${lastMsg.sender} בשעה ${lastMsg.time}, ${lastMsg.text}`;
            } else {
                statusStr = `שיחה חיה עם ${session.liveLabel}, עדיין אין הודעות חדשות בשיחה זו`;
            }
            session.step = 'EXT4_LIVE_WAIT_ACT';
            return res.send(yemotRead(`${statusStr}, להקלטת הודעה בשיחה הקישו 1, להמתנה ובדיקת תגובה חדשה בזמן אמת הקישו 2, לתפריט הראשי הקישו 0`, 'LiveAct', 'no,1,1,6,Digits,no'));
        };

        if (session.step === 'EXT4_LIVE_PROMPT') {
            return await buildLivePrompt(0);
        }

        if (session.step === 'EXT4_LIVE_WAIT_ACT') {
            const act = lastVal(q.LiveAct);
            if (act === '1') {
                session.step = 'EXT4_LIVE_WAIT_REC';
                return res.send(`read=t-הקליטו את דבריכם עבור ${cleanTts(session.liveLabel, 35)} ובסיום הקישו סולמית=RecPath,no,record,${tempRecFolder},wa_live,no,7,1`);
            } else if (act === '2') {
                // המתנה חכמה בזמן אמת (עד 5.5 שניות) לתגובה נכנסת מהנמען
                return await buildLivePrompt(5500);
            } else {
                session.step = 'MAIN_MENU_WAIT';
                return res.send(yemotRead(MAIN_MENU_PROMPT, 'MenuChoice'));
            }
        }

        if (session.step === 'EXT4_LIVE_WAIT_REC') {
            const recPath = lastVal(q.RecPath);
            const audioBuf = await downloadRecordingFromYemot(recPath, lastVal(q.ApiExtension));
            const transcribed = await transcribeAudioBuffer(audioBuf, 'audio/wav');
            const cleanMsg = cleanTts(transcribed, 200);
            if (cleanMsg) {
                await sendWhatsAppTextAndRecord(session.liveJid, session.liveLabel, cleanMsg);
            }
            // לאחר שליחת ההודעה בשיחה חיה, ממתינים 3 שניות לראות אם הצד השני כבר ענה מיד
            return await buildLivePrompt(3000);
        }

        // =========================================================================
        // שלוחה 5: שמיעת ההודעות שאני שלחתי (תמיד בשלוחה 5)
        // =========================================================================
        const renderSentCurrent = () => {
            if (sentOutbox.length === 0) {
                session.step = 'MAIN_MENU_WAIT';
                return res.send(yemotSayAndRead('עדיין לא נשלחו הודעות ממערכת זו', MAIN_MENU_PROMPT, 'MenuChoice'));
            }
            if (session.sentIdx >= sentOutbox.length) session.sentIdx = 0;
            if (session.sentIdx < 0) session.sentIdx = sentOutbox.length - 1;
            const item = sentOutbox[session.sentIdx];
            const targetDisp = resolveContactName(item.targetPhone || item.jid, item.targetName);
            const promptText = `הודעה יוצאת ${session.sentIdx + 1} מתוך ${sentOutbox.length} שנשלחה אל ${targetDisp} בשעה ${item.time}, תוכן ההודעה ששלחתם, ${item.text}, לשליחת הודעה נוספת אל ${targetDisp} הקישו 1, להודעה הבאה ששלחתם הקישו 2, להודעה הקודמת הקישו 3, לתפריט הראשי הקישו 0`;
            session.step = 'EXT5_WAIT_ACT';
            return res.send(yemotRead(promptText, 'SentAct'));
        };

        if (session.step === 'EXT5_ENTER') {
            session.sentIdx = 0;
            return renderSentCurrent();
        }

        if (session.step === 'EXT5_WAIT_ACT') {
            const act = lastVal(q.SentAct);
            const cur = sentOutbox[session.sentIdx];
            if (act === '1' && cur) {
                session.targetJid = cur.jid;
                session.targetLabel = resolveContactName(cur.targetPhone || cur.jid, cur.targetName);
                session.step = 'EXT1_WAIT_RECORD';
                return res.send(`read=t-הקליטו הודעה נוספת עבור ${cleanTts(session.targetLabel, 35)} ובסיום הקישו סולמית=RecPath,no,record,${tempRecFolder},wa_sent_again,no,7,1`);
            } else if (act === '2') {
                session.sentIdx++;
                return renderSentCurrent();
            } else if (act === '3') {
                session.sentIdx--;
                return renderSentCurrent();
            } else {
                session.step = 'MAIN_MENU_WAIT';
                return res.send(yemotRead(MAIN_MENU_PROMPT, 'MenuChoice'));
            }
        }

        // =========================================================================
        // שלוחה 6: ניהול אנשי קשר והגדרת צינתוק דרך הקו
        // =========================================================================
        if (session.step === 'EXT6_MENU_INIT') {
            session.step = 'EXT6_MENU_WAIT';
            return res.send(yemotRead(`שלוחה 6 אנשי קשר וצינתוקים ברשימה יש ${contactsList.length} אנשי קשר, לשמיעת הרשימה ושליחת הודעה הקישו 1, להוספת איש קשר חדש דרך הקו הקישו 2, למחיקת איש קשר הקישו 3, להגדרת המספר שלכם לקבלת צינתוקים הקישו 4, לתפריט הראשי הקישו 0`, 'Ext6Act'));
        }

        if (session.step === 'EXT6_MENU_WAIT') {
            const act = lastVal(q.Ext6Act);
            if (act === '1') {
                session.ctIdx = 0;
                session.afterContactPick = 'SEND_MSG';
                session.step = 'PICK_CONTACT_PROMPT';
                if (contactsList.length === 0) {
                    session.step = 'EXT6_ADD_WAIT_PHONE';
                    return res.send(yemotSayAndRead('רשימת אנשי הקשר ריקה', 'הקישו את מספר הטלפון של איש הקשר החדש ובסיום סולמית', 'NewCtPhone', 'no,15,9,7,Digits,no'));
                }
                const page = contactsList.slice(0, 7);
                const listStr = page.map((c, i) => `ל${c.name} הקישו ${i + 1}`).join(', ');
                session.step = 'PICK_CONTACT_WAIT';
                return res.send(yemotRead(`בחירת איש קשר לשליחת הודעה ${listStr}, לחזרה הקישו 0`, 'ContactPick'));
            } else if (act === '2') {
                session.step = 'EXT6_ADD_WAIT_PHONE';
                return res.send(yemotRead('הקישו את מספר הטלפון של איש הקשר החדש ובסיום הקישו סולמית', 'NewCtPhone', 'no,15,9,7,Digits,no'));
            } else if (act === '3') {
                if (contactsList.length === 0) {
                    session.step = 'MAIN_MENU_WAIT';
                    return res.send(yemotSayAndRead('אין אנשי קשר למחיקה', MAIN_MENU_PROMPT, 'MenuChoice'));
                }
                session.ctIdx = 0;
                session.afterContactPick = 'DELETE_CONTACT';
                const page = contactsList.slice(0, 7);
                const listStr = page.map((c, i) => `למחיקת ${c.name} הקישו ${i + 1}`).join(', ');
                session.step = 'PICK_CONTACT_WAIT';
                return res.send(yemotRead(`בחירת איש קשר למחיקה ${listStr}, לביטול הקישו 0`, 'ContactPick'));
            } else if (act === '4') {
                session.step = 'EXT6_WAIT_TZINTUK_PHONE';
                return res.send(yemotRead('הקישו את מספר הטלפון שבו תרצו לקבל צינתוק כשמתקבלת הודעה פרטית חדשה ובסיום סולמית', 'TzPhoneIn', 'no,15,9,7,Digits,no'));
            } else {
                session.step = 'MAIN_MENU_WAIT';
                return res.send(yemotRead(MAIN_MENU_PROMPT, 'MenuChoice'));
            }
        }

        if (session.step === 'EXT6_ADD_WAIT_PHONE') {
            const p = normalizePhone(lastVal(q.NewCtPhone));
            if (!p || p.length < 9) {
                session.step = 'MAIN_MENU_WAIT';
                return res.send(yemotSayAndRead('מספר הטלפון שהוקש אינו תקין', MAIN_MENU_PROMPT, 'MenuChoice'));
            }
            session.newCtPhone = p;
            session.step = 'EXT6_ADD_WAIT_NAME_REC';
            return res.send(`read=t-כעת הקליטו בקול ברור את שם איש הקשר ובסיום הקישו סולמית=RecPath,no,record,${tempRecFolder},wa_ctname,no,5,1`);
        }

        if (session.step === 'EXT6_ADD_WAIT_NAME_REC') {
            const recPath = lastVal(q.RecPath);
            const audioBuf = await downloadRecordingFromYemot(recPath, lastVal(q.ApiExtension));
            const transcribed = await transcribeAudioBuffer(audioBuf, 'audio/wav');
            const cleanName = cleanTts(transcribed, 35);
            if (!cleanName) {
                session.step = 'MAIN_MENU_WAIT';
                return res.send(yemotSayAndRead('לא הצלחתי לזהות את השם בהקלטה', MAIN_MENU_PROMPT, 'MenuChoice'));
            }
            await upsertContact(cleanName, session.newCtPhone);
            session.step = 'MAIN_MENU_WAIT';
            return res.send(yemotSayAndRead(`איש הקשר ${cleanName} עם מספר ${session.newCtPhone} נשמר בהצלחה במערכת`, MAIN_MENU_PROMPT, 'MenuChoice'));
        }

        if (session.step === 'EXT6_WAIT_TZINTUK_PHONE') {
            const tp = normalizePhone(lastVal(q.TzPhoneIn));
            tzintukPhone = tp;
            if (mongoDb) {
                try { await mongoDb.collection('settings').updateOne({ _id: 'config' }, { $set: { tzintukPhone } }, { upsert: true }); } catch {}
            }
            session.step = 'MAIN_MENU_WAIT';
            return res.send(yemotSayAndRead(`מספר הטלפון לצינתוקים עודכן בהצלחה ל ${tp}`, MAIN_MENU_PROMPT, 'MenuChoice'));
        }

        session.step = 'MAIN_MENU_WAIT';
        return res.send(yemotRead(MAIN_MENU_PROMPT, 'MenuChoice'));

    } catch (e) {
        console.error('[Yemot WhatsApp Error]:', e.message);
        return res.send(yemotSayAndRead('אירעה שגיאה זמנית בשרת', MAIN_MENU_PROMPT, 'MenuChoice'));
    }
});

const port = process.env.PORT || 3000;
app.listen(port, () => console.log(`=== Yemot WhatsApp Pro Server Ready on port ${port} ===`));
