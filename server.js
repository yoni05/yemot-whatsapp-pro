const express = require('express');
const path = require('path');
const os = require('os');
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
    res.header('Access-Control-Allow-Origin', '*');
    res.header('Access-Control-Allow-Headers', 'Content-Type');
    res.header('Access-Control-Allow-Methods', 'GET, POST, DELETE, OPTIONS');
    if (req.method === 'OPTIONS') return res.sendStatus(200);
    next();
});

const TOKEN = (process.env.YEMOT_TOKEN || '').trim();
const GEMINI_KEY = (process.env.GEMINI_API_KEY || '').trim();
const MONGODB_URI = (process.env.MONGODB_URI || '').trim();
const YM = 'https://www.call2all.co.il/ym/api/';

let sock = null;
let latestQrDataUrl = null;
let waStatus = 'מאתחל חיבור...';
let cachedModelName = null;
let db = null;

const privateInbox = [];
const sentOutbox = [];
const groupInbox = [];
const contactsList = [];
const lidToPhoneMap = new Map();
const lastUnreadKeyByJid = new Map();
const groupsListCache = [];
const userCallSession = new Map();
const folderCounters = {};

const wait = ms => new Promise(resolve => setTimeout(resolve, ms));

function getRandomDelay(minMs = 3000, maxMs = 9000) {
    return Math.floor(Math.random() * (maxMs - minMs + 1)) + minMs;
}

// --- סימון "וי כחול" (Read Receipts) להודעות שנקראו ---
async function markMessageAsRead(targetJid, explicitKey = null) {
    if (!sock) return;
    try {
        const keyToRead = explicitKey || lastUnreadKeyByJid.get(targetJid);
        if (keyToRead && keyToRead.id) {
            await sock.readMessages([keyToRead]);
            lastUnreadKeyByJid.delete(targetJid);
            console.log(`[Blue Tick] Marked message ${keyToRead.id} from ${targetJid} as read.`);
        }
    } catch (e) {
        console.warn('[Blue Tick Warning]:', e.message);
    }
}

// --- מנגנון השהיה אנושי חכם (עד 9 שניות) + וי כחול + חיווי הקלדה ---
async function sendWithHumanDelay(targetJid, textMsg, explicitKey = null) {
    if (!sock) throw new Error('WhatsApp socket not connected');
    const totalDelayMs = getRandomDelay(3000, 9000);
    const readPauseMs = Math.min(1800, Math.floor(totalDelayMs * 0.3));
    const typingDelayMs = Math.max(1500, totalDelayMs - readPauseMs);

    try {
        await sock.sendPresenceUpdate('available', targetJid);
        await markMessageAsRead(targetJid, explicitKey);
    } catch {}

    await wait(readPauseMs);

    console.log(`[Human Delay] Simulating typing to ${targetJid} for ${typingDelayMs}ms (total ${totalDelayMs}ms)...`);
    try {
        await sock.sendPresenceUpdate('composing', targetJid);
    } catch {}

    await wait(typingDelayMs);

    try {
        await sock.sendPresenceUpdate('paused', targetJid);
    } catch {}

    return await sock.sendMessage(targetJid, { text: textMsg });
}

async function fetchWithTimeout(url, options = {}, timeout = 15000) {
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

function sanitizeForYemotTts(str, maxLen = 180) {
    return String(str || '')
        .replace(/https?:\/\/\S+/gi, ' קישור ')
        .replace(/[^א-תa-zA-Z0-9\s.,?!-]/g, ' ')
        .replace(/\./g, ', ')
        .replace(/\s+/g, ' ')
        .trim()
        .slice(0, maxLen);
}

function normalizeIsraeliPhone(phoneStr) {
    let digits = String(phoneStr || '').replace(/[^0-9]/g, '');
    if (digits.startsWith('972')) digits = '0' + digits.slice(3);
    return digits;
}

function phoneToJid(phoneStr) {
    let digits = String(phoneStr || '').replace(/[^0-9]/g, '');
    if (digits.startsWith('0')) digits = '972' + digits.slice(1);
    return `${digits}@s.whatsapp.net`;
}

function findContactName(phoneOrJid) {
    const cleanPhone = normalizeIsraeliPhone(String(phoneOrJid).split('@')[0]);
    const found = contactsList.find(c => normalizeIsraeliPhone(c.phone) === cleanPhone || c.jid === phoneOrJid);
    return found ? found.name : '';
}

async function initDatabaseAndAuth(mongoUri) {
    if (!mongoUri) {
        const { useMultiFileAuthState } = require('@whiskeysockets/baileys');
        const localAuth = await useMultiFileAuthState(path.join(os.tmpdir(), 'wa_auth_local'));
        return { ...localAuth, clearState: async () => {} };
    }

    const client = new MongoClient(mongoUri);
    await client.connect();
    db = client.db('whatsapp_yemot');

    try {
        const savedContacts = await db.collection('contacts').find({}).toArray();
        contactsList.length = 0;
        contactsList.push(...savedContacts);

        const savedPrivate = await db.collection('private_inbox').find({}).sort({ timestamp: -1 }).limit(50).toArray();
        privateInbox.length = 0;
        privateInbox.push(...savedPrivate);

        const savedSent = await db.collection('sent_outbox').find({}).sort({ timestamp: -1 }).limit(50).toArray();
        sentOutbox.length = 0;
        sentOutbox.push(...savedSent);

        const savedGroup = await db.collection('group_inbox').find({}).sort({ timestamp: -1 }).limit(50).toArray();
        groupInbox.length = 0;
        groupInbox.push(...savedGroup);

        console.log(`[MongoDB] Loaded ${contactsList.length} contacts, ${privateInbox.length} incoming, ${sentOutbox.length} sent.`);
    } catch (e) {
        console.warn('[MongoDB Load Warning]:', e.message);
    }

    const collection = db.collection('auth_state');
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
    const clearState = async () => {
        try { await collection.drop(); } catch {}
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
        saveCreds: () => writeData(creds, 'creds'),
        clearState
    };
}

async function saveContact(name, phone) {
    const cleanName = sanitizeForYemotTts(name, 40);
    const cleanPhone = normalizeIsraeliPhone(phone);
    if (!cleanName || !cleanPhone) return null;
    const jid = phoneToJid(cleanPhone);
    const existingIdx = contactsList.findIndex(c => normalizeIsraeliPhone(c.phone) === cleanPhone);
    const contactObj = { name: cleanName, phone: cleanPhone, jid, updatedAt: Date.now() };
    if (existingIdx >= 0) contactsList[existingIdx] = contactObj;
    else contactsList.push(contactObj);

    if (db) {
        await db.collection('contacts').updateOne({ phone: cleanPhone }, { $set: contactObj }, { upsert: true }).catch(() => null);
    }
    return contactObj;
}

async function recordSentMessage(targetJid, targetLabel, textContent) {
    const cleanText = sanitizeForYemotTts(textContent, 180);
    const timeStr = new Date().toLocaleTimeString('he-IL', { timeZone: 'Asia/Jerusalem', hour: '2-digit', minute: '2-digit' });
    const contactName = findContactName(targetJid) || targetLabel || normalizeIsraeliPhone(targetJid.split('@')[0]);
    const entry = {
        jid: targetJid,
        targetName: sanitizeForYemotTts(contactName, 40),
        text: cleanText,
        time: timeStr,
        timestamp: Date.now()
    };
    sentOutbox.unshift(entry);
    if (sentOutbox.length > 50) sentOutbox.pop();

    if (db) {
        db.collection('sent_outbox').insertOne(entry).catch(() => null);
    }
    uploadTtsToYemotFolder('5', `שלחתם אל ${entry.targetName} בשעה ${timeStr} את ההודעה: ${cleanText}`);
}

async function getNextFileNumberStr(folder) {
    const cleanFolder = String(folder || '').replace(/^ivr2:\/*/, '').replace(/^\/*/, '').replace(/\/*$/, '');
    if (folderCounters[cleanFolder] !== undefined) {
        folderCounters[cleanFolder]++;
        return String(folderCounters[cleanFolder]).padStart(3, '0');
    }
    let maxExisting = 0;
    try {
        const url = `${YM}GetIVR2Dir?token=${encodeURIComponent(TOKEN)}&path=${encodeURIComponent('ivr2:/' + cleanFolder)}`;
        const res = await fetchWithTimeout(url, {}, 5000);
        if (res.ok) {
            const data = await res.json();
            for (const f of (data.files || [])) {
                const fname = (typeof f === 'string') ? f : (f && (f.name || f.fileName || ''));
                const m = String(fname).match(/^(\d+)\.(wav|tts|txt)$/i);
                if (m) {
                    const num = parseInt(m[1], 10);
                    if (!isNaN(num) && num < 900 && num > maxExisting) maxExisting = num;
                }
            }
        }
    } catch {}
    folderCounters[cleanFolder] = maxExisting + 1;
    return String(folderCounters[cleanFolder]).padStart(3, '0');
}

async function uploadTtsToYemotFolder(folder, ttsText) {
    if (!TOKEN) return;
    try {
        const nextNum = await getNextFileNumberStr(folder);
        const targetPath = `ivr2:/${folder}/${nextNum}.tts`;
        const form = new FormData();
        form.append('token', TOKEN);
        form.append('path', targetPath);
        form.append('convertAudio', '0');
        form.append('file', new Blob([Buffer.from(ttsText, 'utf8')], { type: 'text/plain; charset=utf-8' }), `${nextNum}.tts`);
        await fetchWithTimeout(`${YM}UploadFile`, { method: 'POST', body: form }, 10000);
    } catch (e) {
        console.warn('[Yemot Upload TTS Warning]:', e.message);
    }
}

async function getFastGeminiModels() {
    const defaults = ['gemini-2.5-flash', 'gemini-flash-latest', 'gemini-2.0-flash', 'gemini-flash-lite-latest'];
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
    const prompt = 'תמלל את ההקלטה לעברית בצורה המדויקת ביותר. החזר אך ורק את המילים שנאמרו ללא שום תוספת וללא סימני פיסוק מיותרים.';

    for (const model of models.slice(0, 3)) {
        try {
            const url = `https://generativelanguage.googleapis.com/v1beta/models/${model}:generateContent?key=${GEMINI_KEY}`;
            const res = await fetchWithTimeout(url, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    contents: [{
                        parts: [
                            { text: prompt },
                            { inlineData: { mimeType, data: audioBase64 } }
                        ]
                    }],
                    generationConfig: { temperature: 0.1, maxOutputTokens: 300 }
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

function unwrapWhatsAppMessage(messageObj) {
    if (!messageObj) return null;
    if (messageObj.ephemeralMessage?.message) return unwrapWhatsAppMessage(messageObj.ephemeralMessage.message);
    if (messageObj.viewOnceMessage?.message) return unwrapWhatsAppMessage(messageObj.viewOnceMessage.message);
    if (messageObj.viewOnceMessageV2?.message) return unwrapWhatsAppMessage(messageObj.viewOnceMessageV2.message);
    if (messageObj.documentWithCaptionMessage?.message) return unwrapWhatsAppMessage(messageObj.documentWithCaptionMessage.message);
    return messageObj;
}

async function refreshGroupsCache() {
    if (!sock) return;
    try {
        const groupsObj = await sock.groupFetchAllParticipating();
        groupsListCache.length = 0;
        for (const [jid, meta] of Object.entries(groupsObj || {})) {
            groupsListCache.push({
                jid,
                name: sanitizeForYemotTts(meta.subject || 'קבוצה', 45)
            });
        }
    } catch (e) {
        console.warn('[Groups Fetch Warning]:', e.message);
    }
}

async function startWhatsAppConnection() {
    try {
        const { state, saveCreds, clearState } = await initDatabaseAndAuth(MONGODB_URI);
        const { version } = await fetchLatestBaileysVersion();

        sock = makeWASocket({
            version,
            logger: pino({ level: 'silent' }),
            printQRInTerminal: false,
            auth: state,
            browser: ['Yemot WhatsApp Pro', 'Chrome', '122.0.0'],
            syncFullHistory: false,
            markOnlineOnConnect: false
        });

        sock.ev.on('creds.update', saveCreds);

        sock.ev.on('contacts.upsert', (contacts) => {
            for (const c of contacts) {
                if (c.id && c.lid) lidToPhoneMap.set(c.lid, c.id);
                if (c.id && c.id.endsWith('@s.whatsapp.net') && (c.name || c.notify)) {
                    const ph = normalizeIsraeliPhone(c.id.split('@')[0]);
                    if (!findContactName(ph)) {
                        saveContact(c.name || c.notify, ph).catch(() => null);
                    }
                }
            }
        });

        sock.ev.on('connection.update', async (update) => {
            const { connection, lastDisconnect, qr } = update;
            if (qr) {
                latestQrDataUrl = await QRCode.toDataURL(qr);
                waStatus = 'ממתין לסריקת קוד QR';
            }
            if (connection === 'open') {
                latestQrDataUrl = null;
                waStatus = 'מחובר ופעיל 100% (כולל אנשי קשר, וי כחול והשהיה עד 9 שניות)';
                setTimeout(refreshGroupsCache, 3000);
            }
            if (connection === 'close') {
                const statusCode = lastDisconnect?.error?.output?.statusCode;
                const shouldReconnect = statusCode !== DisconnectReason.loggedOut;
                if (shouldReconnect) {
                    waStatus = `החיבור נסגר (קוד ${statusCode}). מתחבר מחדש אוטומטית...`;
                    setTimeout(startWhatsAppConnection, 4000);
                } else {
                    waStatus = 'נותק מהמכשיר! מנקה נתונים ומייצר ברקוד חדש...';
                    await clearState();
                    setTimeout(startWhatsAppConnection, 3000);
                }
            }
        });

        sock.ev.on('messages.upsert', async ({ messages, type }) => {
            if (type !== 'notify' && type !== 'append') return;

            for (const rawMsg of messages) {
                try {
                    if (!rawMsg.message) continue;
                    let remoteJid = rawMsg.key.remoteJid || '';
                    if (!remoteJid || remoteJid === 'status@broadcast') continue;

                    if (remoteJid.endsWith('@lid') && rawMsg.key.senderPn) {
                        remoteJid = rawMsg.key.senderPn;
                    } else if (remoteJid.endsWith('@lid') && lidToPhoneMap.has(remoteJid)) {
                        remoteJid = lidToPhoneMap.get(remoteJid);
                    }

                    const isGroup = remoteJid.endsWith('@g.us');
                    const innerMsg = unwrapWhatsAppMessage(rawMsg.message);
                    if (!innerMsg) continue;

                    let textContent =
                        innerMsg.conversation ||
                        innerMsg.extendedTextMessage?.text ||
                        innerMsg.imageMessage?.caption ||
                        innerMsg.videoMessage?.caption ||
                        innerMsg.buttonsResponseMessage?.selectedDisplayText ||
                        innerMsg.listResponseMessage?.title || '';

                    const audioMsg = innerMsg.audioMessage;
                    if (!textContent && audioMsg) {
                        try {
                            const audioBuffer = await downloadMediaMessage(rawMsg, 'buffer', {});
                            const transcribed = await transcribeAudioBuffer(audioBuffer, audioMsg.mimetype || 'audio/ogg');
                            textContent = transcribed ? `הודעה קולית: ${transcribed}` : 'הודעה קולית';
                        } catch {
                            textContent = 'הודעה קולית';
                        }
                    }

                    const cleanText = sanitizeForYemotTts(textContent, 180);
                    if (!cleanText) continue;

                    if (rawMsg.key.fromMe) {
                        if (type === 'notify') {
                            const targetLabel = findContactName(remoteJid) || normalizeIsraeliPhone(remoteJid.split('@')[0]);
                            await recordSentMessage(remoteJid, targetLabel, cleanText);
                        }
                        continue;
                    }

                    const phoneDigits = normalizeIsraeliPhone(remoteJid.split('@')[0]);
                    const knownName = findContactName(remoteJid);
                    const senderName = sanitizeForYemotTts(knownName || rawMsg.pushName || phoneDigits, 40);
                    const timeStr = new Date().toLocaleTimeString('he-IL', { timeZone: 'Asia/Jerusalem', hour: '2-digit', minute: '2-digit' });

                    const msgId = rawMsg.key.id;
                    if (privateInbox.some(m => m.id === msgId) || groupInbox.some(m => m.id === msgId)) continue;

                    const msgKeyObj = {
                        remoteJid: rawMsg.key.remoteJid,
                        id: rawMsg.key.id,
                        fromMe: false,
                        participant: rawMsg.key.participant || undefined
                    };
                    lastUnreadKeyByJid.set(remoteJid, msgKeyObj);

                    if (isGroup) {
                        const grpObj = groupsListCache.find(g => g.jid === remoteJid);
                        const groupTitle = grpObj ? grpObj.name : 'קבוצה';
                        const entry = {
                            id: msgId,
                            jid: remoteJid,
                            rawKey: msgKeyObj,
                            groupName: groupTitle,
                            senderName,
                            text: cleanText,
                            time: timeStr,
                            timestamp: Date.now()
                        };
                        groupInbox.unshift(entry);
                        if (groupInbox.length > 50) groupInbox.pop();
                        if (db) db.collection('group_inbox').insertOne(entry).catch(() => null);
                    } else {
                        const entry = {
                            id: msgId,
                            jid: remoteJid,
                            rawKey: msgKeyObj,
                            phone: phoneDigits,
                            senderName,
                            text: cleanText,
                            time: timeStr,
                            timestamp: Date.now()
                        };
                        privateInbox.unshift(entry);
                        if (privateInbox.length > 50) privateInbox.pop();
                        if (db) db.collection('private_inbox').insertOne(entry).catch(() => null);

                        uploadTtsToYemotFolder('2', `הודעה מאת ${senderName} בשעה ${timeStr}: ${cleanText}`);
                    }
                } catch (err) {
                    console.error('[Incoming Message Error]:', err.message);
                }
            }
        });
    } catch (err) {
        waStatus = 'שגיאה באתחול: ' + err.message;
        setTimeout(startWhatsAppConnection, 6000);
    }
}

startWhatsAppConnection();

app.get('/api/contacts', (req, res) => {
    res.json({ ok: true, contacts: contactsList });
});

app.post('/api/contacts', async (req, res) => {
    try {
        const { name, phone } = req.body;
        const saved = await saveContact(name, phone);
        const isJsonReq = (req.headers['content-type'] || '').includes('application/json');
        if (!saved) {
            if (isJsonReq) return res.status(400).json({ ok: false, error: 'נא להזין שם ומספר תקינים' });
            return res.redirect('/');
        }
        if (isJsonReq) return res.json({ ok: true, contact: saved, contacts: contactsList });
        return res.redirect('/');
    } catch (e) {
        res.status(500).json({ ok: false, error: e.message });
    }
});

app.get('/', (req, res) => {
    const contactsHtml = contactsList.map(c => `<li><b>${c.name}</b> - ${c.phone}</li>`).join('') || '<li>עדיין לא הוגדרו אנשי קשר</li>';
    const recentPrivateHtml = privateInbox.slice(0, 5).map(m => `<li><b>${m.senderName}</b> (${m.time}): ${m.text}</li>`).join('') || '<li>אין הודעות נכנסות</li>';
    const autoRefreshMeta = latestQrDataUrl ? '<meta http-equiv="refresh" content="7">' : '';

    res.type('text/html; charset=utf-8').send(`<!DOCTYPE html>
<html lang="he" dir="rtl">
<head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
${autoRefreshMeta}
<title>לוח בקרה ואנשי קשר - ווצאפ ימות המשיח v2.1</title>
<style>
body{font-family:Arial,sans-serif;background:#f4f7f6;color:#1e2925;max-width:680px;margin:24px auto;padding:16px}
.card{background:#fff;padding:20px;border-radius:12px;border:1px solid #dce5e1;margin-bottom:16px}
.status{font-size:17px;font-weight:bold;color:#107c5c;margin:10px 0}
input,button{padding:9px 12px;border-radius:6px;border:1px solid #ccc;font:inherit;margin:4px 0}
button{background:#107c5c;color:#fff;border:0;font-weight:bold;cursor:pointer}
img.qr{max-width:260px;border:5px solid #107c5c;border-radius:12px;display:block;margin:12px auto}
ul{padding-right:20px;line-height:1.7}
</style>
</head>
<body>
<div class="card" style="text-align:center">
<h2>מערכת ווצאפ קולית לימות המשיח (גרסה 2.1 Pro)</h2>
<div class="status">${waStatus}</div>
${latestQrDataUrl ? `<p>סרוק את קוד ה-QR מהווצאפ שלך (הדף מתרענן אוטומטית):</p><img class="qr" src="${latestQrDataUrl}">` : `<p>נכנסות (שלוחה 2): <b>${privateInbox.length}</b> | קבוצות (שלוחה 3): <b>${groupInbox.length}</b> | נשלחו (שלוחה 5): <b>${sentOutbox.length}</b> | אנשי קשר (שלוחה 6): <b>${contactsList.length}</b></p>`}
</div>

<div class="card">
<h3>הוספת איש קשר חדש (זמין מיד בשלוחות 1, 4, 6)</h3>
<form method="POST" action="/api/contacts">
<input name="name" placeholder="שם איש הקשר (למשל: אבא / משה)" style="width:45%" required>
<input name="phone" placeholder="מספר טלפון (050...)" style="width:35%" required>
<button type="submit">הוסף</button>
</form>
<h4>רשימת אנשי הקשר השמורים:</h4>
<ul>${contactsHtml}</ul>
</div>

<div class="card">
<h3>5 הודעות פרטיות אחרונות שהתקבלו (שלוחה 2):</h3>
<ul>${recentPrivateHtml}</ul>
</div>
</body></html>`);
});

const MAIN_MENU_PROMPT = 'ברוכים הבאים לוואטסאפ הקולי! לשליחת הודעה הקישו 1, לשמיעת הודעות שנשלחו אליכם הקישו 2, לקבוצות הקישו 3, לשיחה בזמן אמת הקישו 4, לשמיעת ההודעות שאתם שלחתם הקישו 5, לאנשי קשר הקישו 6, ליציאה הקישו 0';

app.all(['/yemot', '/yemot/:mode'], async (req, res) => {
    const q = { ...req.query, ...req.body };
    res.type('text/plain; charset=utf-8');

    try {
        const callerPhone = lastVal(q.ApiPhone).replace(/[^0-9]/g, '') || 'guest';
        const callId = lastVal(q.ApiCallId) || callerPhone;
        const routeMode = req.params.mode || lastVal(q.mode) || '';

        if (q.hangup === 'yes') {
            userCallSession.delete(callId);
            return res.send('noop');
        }

        if (!sock) {
            return res.send('id_list_message=t-שרת הווצאפ ממתין לסריקת ברקוד&go_to_folder=..');
        }

        let session = userCallSession.get(callId);
        const menuChoice = lastVal(q.MenuChoice);
        const sendMode = lastVal(q.SendMode);
        const contactPick = lastVal(q.ContactPick);
        const targetPhone = lastVal(q.TargetPhone);
        const recPath = lastVal(q.RecPath);
        const inboxAct = lastVal(q.InboxAct);
        const groupChoice = lastVal(q.GroupChoice);
        const groupAct = lastVal(q.GroupAct);
        const liveChoice = lastVal(q.LiveChoice);
        const liveAct = lastVal(q.LiveAct);
        const sentAct = lastVal(q.SentAct);
        const contactAct = lastVal(q.ContactAct);
        const newContactPhone = lastVal(q.NewContactPhone);
        const newContactNameRec = lastVal(q.NewContactNameRec);

        const tempRecFolder = `/users/${callerPhone}/temp`;

        if (!session || (routeMode && session.lastRouteMode !== routeMode && !menuChoice && !sendMode && !contactPick && !targetPhone && !recPath && !inboxAct && !groupChoice && !groupAct && !liveChoice && !liveAct && !sentAct && !contactAct && !newContactPhone && !newContactNameRec)) {
            session = { step: 'MAIN_MENU', msgIdx: 0, grpIdx: 0, cntIdx: 0, lastRouteMode: routeMode };
            if (routeMode === '1') session.step = 'EXT1_START';
            else if (routeMode === '2') session.step = 'EXT2_START';
            else if (routeMode === '3') session.step = 'EXT3_START';
            else if (routeMode === '4') session.step = 'EXT4_START';
            else if (routeMode === '5') session.step = 'EXT5_START';
            else if (routeMode === '6') session.step = 'EXT6_START';
            userCallSession.set(callId, session);
        }

        if (session.step === 'MAIN_MENU' && menuChoice) {
            if (menuChoice === '1') session.step = 'EXT1_START';
            else if (menuChoice === '2') session.step = 'EXT2_START';
            else if (menuChoice === '3') session.step = 'EXT3_START';
            else if (menuChoice === '4') session.step = 'EXT4_START';
            else if (menuChoice === '5') session.step = 'EXT5_START';
            else if (menuChoice === '6') session.step = 'EXT6_START';
            else if (menuChoice === '0' || menuChoice === '*') {
                userCallSession.delete(callId);
                return res.send('go_to_folder=..');
            }
        }

        // שלוחה 1: שליחת הודעה
        if (session.step === 'EXT1_START') {
            if (contactsList.length > 0) {
                session.step = 'EXT1_CHOOSE_METHOD';
                return res.send('read=t-שלוחה 1 לבחירת איש קשר מהרשימה הקישו 1, להקשת מספר טלפון הקישו 2, לחזרה הקישו 0=SendMode,no,1,1,7,Digits,no');
            } else {
                session.step = 'EXT1_WAIT_PHONE';
                return res.send('read=t-שלוחה 1 אנא הקישו את מספר הטלפון של הנמען ובסיום הקישו סולמית=TargetPhone,no,15,9,7,Digits,no');
            }
        }

        if (session.step === 'EXT1_CHOOSE_METHOD' && sendMode) {
            if (sendMode === '1') {
                session.cntIdx = 0;
                session.afterContactStep = 'EXT1_WAIT_RECORD';
                session.step = 'SELECT_CONTACT_LIST';
            } else if (sendMode === '2') {
                session.step = 'EXT1_WAIT_PHONE';
                return res.send('read=t-אנא הקישו את מספר הטלפון של הנמען ובסיום הקישו סולמית=TargetPhone,no,15,9,7,Digits,no');
            } else {
                session.step = 'MAIN_MENU';
                return res.send(`read=t-${MAIN_MENU_PROMPT}=MenuChoice,no,1,1,7,Digits,no`);
            }
        }

        if (session.step === 'SELECT_CONTACT_LIST') {
            if (contactPick === '9') {
                session.cntIdx = ((session.cntIdx + 7) < contactsList.length) ? (session.cntIdx + 7) : 0;
            } else if (contactPick === '0') {
                session.step = 'MAIN_MENU';
                return res.send(`read=t-${MAIN_MENU_PROMPT}=MenuChoice,no,1,1,7,Digits,no`);
            } else if (contactPick) {
                const idx = parseInt(contactPick, 10);
                const pageItems = contactsList.slice(session.cntIdx, session.cntIdx + 7);
                if (!isNaN(idx) && idx >= 1 && idx <= pageItems.length) {
                    const chosen = pageItems[idx - 1];
                    if (session.afterContactStep === 'EXT4_LIVE_LOOP') {
                        session.liveJid = chosen.jid;
                        session.livePhone = chosen.phone;
                        session.liveLabel = chosen.name;
                        session.step = 'EXT4_LIVE_LOOP';
                    } else {
                        session.targetJid = chosen.jid;
                        session.targetLabel = chosen.name;
                        session.replyKey = null;
                        session.step = 'EXT1_WAIT_RECORD';
                        return res.send(`read=t-נבחר ${chosen.name} אנא הקליטו את ההודעה ובסיום הקישו סולמית=RecPath,no,record,${tempRecFolder},wa_send,no,7,1`);
                    }
                }
            }

            if (session.step === 'SELECT_CONTACT_LIST') {
                const pageItems = contactsList.slice(session.cntIdx, session.cntIdx + 7);
                const txtOpts = pageItems.map((c, i) => `עבור ${c.name} הקישו ${i + 1}`).join(', ');
                const moreTxt = contactsList.length > 7 ? 'לעוד אנשי קשר הקישו 9,' : '';
                return res.send(`read=t-בחירת איש קשר: ${txtOpts}, ${moreTxt} לחזרה הקישו 0=ContactPick,no,1,1,7,Digits,no`);
            }
        }

        if (session.step === 'EXT1_WAIT_PHONE' && targetPhone) {
            session.targetJid = phoneToJid(targetPhone);
            session.targetLabel = findContactName(targetPhone) || targetPhone;
            session.replyKey = null;
            session.step = 'EXT1_WAIT_RECORD';
            return res.send(`read=t-אנא הקליטו את תוכן ההודעה עבור ${session.targetLabel} ובסיום הקישו סולמית=RecPath,no,record,${tempRecFolder},wa_send,no,7,1`);
        }

        if (session.step === 'EXT1_WAIT_RECORD' && recPath) {
            const audioBuf = await downloadRecordingFromYemot(recPath, lastVal(q.ApiExtension));
            const transcribed = await transcribeAudioBuffer(audioBuf, 'audio/wav');
            const cleanMsg = sanitizeForYemotTts(transcribed, 200);
            if (!cleanMsg) {
                session.step = 'MAIN_MENU';
                return res.send(`id_list_message=t-לא הצלחתי לשמוע את ההקלטה אנא נסו שנית&read=t-${MAIN_MENU_PROMPT}=MenuChoice,no,1,1,7,Digits,no`);
            }
            await sendWithHumanDelay(session.targetJid, cleanMsg, session.replyKey);
            await recordSentMessage(session.targetJid, session.targetLabel, cleanMsg);

            session.step = 'MAIN_MENU';
            return res.send(`id_list_message=t-ההודעה ${cleanMsg} נשלחה בהצלחה אל ${session.targetLabel}&read=t-${MAIN_MENU_PROMPT}=MenuChoice,no,1,1,7,Digits,no`);
        }

        // שלוחה 2: הודעות נכנסות (מסמן וי כחול אוטומטי בעת ההאזנה)
        if (session.step === 'EXT2_START') {
            session.msgIdx = 0;
            session.step = 'EXT2_BROWSE';
        }

        if (session.step === 'EXT2_BROWSE') {
            if (inboxAct === '1') {
                const currentMsg = privateInbox[session.msgIdx];
                if (currentMsg) {
                    session.targetJid = currentMsg.jid;
                    session.targetLabel = currentMsg.senderName;
                    session.replyKey = currentMsg.rawKey || null;
                    session.step = 'EXT1_WAIT_RECORD';
                    return res.send(`read=t-הקליטו כעת את תשובתכם עבור ${currentMsg.senderName} ובסיום הקישו סולמית=RecPath,no,record,${tempRecFolder},wa_reply,no,7,1`);
                }
            } else if (inboxAct === '2') {
                session.msgIdx++;
            } else if (inboxAct === '3' && session.msgIdx > 0) {
                session.msgIdx--;
            } else if (inboxAct === '4') {
                const currentMsg = privateInbox[session.msgIdx];
                if (currentMsg) {
                    session.liveJid = currentMsg.jid;
                    session.livePhone = currentMsg.phone;
                    session.liveLabel = currentMsg.senderName;
                    session.step = 'EXT4_LIVE_LOOP';
                }
            } else if (inboxAct === '0' || inboxAct === '*') {
                session.step = 'MAIN_MENU';
                return res.send(`read=t-${MAIN_MENU_PROMPT}=MenuChoice,no,1,1,7,Digits,no`);
            }

            if (session.step === 'EXT2_BROWSE') {
                if (privateInbox.length === 0) {
                    session.step = 'MAIN_MENU';
                    return res.send(`id_list_message=t-עדיין לא התקבלו הודעות פרטיות חדשות&read=t-${MAIN_MENU_PROMPT}=MenuChoice,no,1,1,7,Digits,no`);
                }
                if (session.msgIdx >= privateInbox.length) session.msgIdx = 0;
                const item = privateInbox[session.msgIdx];
                markMessageAsRead(item.jid, item.rawKey);
                const cName = findContactName(item.phone || item.jid) || item.senderName;
                const promptText = `הודעה ${session.msgIdx + 1} מתוך ${privateInbox.length}, מאת ${cName} בשעה ${item.time}: ${item.text}. למענה מהיר הקישו 1, להודעה הבאה הקישו 2, להודעה הקודמת הקישו 3, למעבר לשיחה חיה עם שולח זה הקישו 4, לתפריט הקישו 0`;
                return res.send(`read=t-${promptText}=InboxAct,no,1,1,7,Digits,no`);
            }
        }

        // שלוחה 3: קבוצות
        if (session.step === 'EXT3_START') {
            if (groupsListCache.length === 0) await refreshGroupsCache();
            session.grpIdx = 0;
            session.step = 'EXT3_MENU';
            return res.send('read=t-שלוחה 3 קבוצות ווצאפ לשמיעת ההודעות מהקבוצות הקישו 1, לבחירת קבוצה ושליחת הודעה הקישו 2, לתפריט הקישו 0=GroupAct,no,1,1,7,Digits,no');
        }

        if (session.step === 'EXT3_MENU' && groupAct) {
            if (groupAct === '1') {
                session.msgIdx = 0;
                session.step = 'EXT3_LISTEN_MSGS';
            } else if (groupAct === '2') {
                session.grpIdx = 0;
                session.step = 'EXT3_SELECT_GROUP';
            } else {
                session.step = 'MAIN_MENU';
                return res.send(`read=t-${MAIN_MENU_PROMPT}=MenuChoice,no,1,1,7,Digits,no`);
            }
        }

        if (session.step === 'EXT3_LISTEN_MSGS') {
            if (groupChoice === '1') {
                const cur = groupInbox[session.msgIdx];
                if (cur) {
                    session.targetJid = cur.jid;
                    session.targetLabel = cur.groupName;
                    session.replyKey = cur.rawKey || null;
                    session.step = 'EXT1_WAIT_RECORD';
                    return res.send(`read=t-הקליטו את ההודעה לקבוצת ${cur.groupName} ובסיום הקישו סולמית=RecPath,no,record,${tempRecFolder},wa_grp_reply,no,7,1`);
                }
            } else if (groupChoice === '2') {
                session.msgIdx++;
            } else if (groupChoice === '0') {
                session.step = 'MAIN_MENU';
                return res.send(`read=t-${MAIN_MENU_PROMPT}=MenuChoice,no,1,1,7,Digits,no`);
            }
            if (groupInbox.length === 0) {
                session.step = 'MAIN_MENU';
                return res.send(`id_list_message=t-אין הודעות חדשות בקבוצות&read=t-${MAIN_MENU_PROMPT}=MenuChoice,no,1,1,7,Digits,no`);
            }
            if (session.msgIdx >= groupInbox.length) session.msgIdx = 0;
            const gItem = groupInbox[session.msgIdx];
            const gText = `בקבוצת ${gItem.groupName} בשעה ${gItem.time} כתב ${gItem.senderName}: ${gItem.text}. לתגובה לקבוצה הקישו 1, להודעה הבאה הקישו 2, לתפריט הקישו 0`;
            return res.send(`read=t-${gText}=GroupChoice,no,1,1,7,Digits,no`);
        }

        if (session.step === 'EXT3_SELECT_GROUP') {
            if (groupsListCache.length === 0) {
                session.step = 'MAIN_MENU';
                return res.send(`id_list_message=t-לא נמצאו קבוצות&read=t-${MAIN_MENU_PROMPT}=MenuChoice,no,1,1,7,Digits,no`);
            }
            if (groupChoice === '9') {
                session.grpIdx = ((session.grpIdx + 7) < groupsListCache.length) ? (session.grpIdx + 7) : 0;
            } else if (groupChoice === '0') {
                session.step = 'MAIN_MENU';
                return res.send(`read=t-${MAIN_MENU_PROMPT}=MenuChoice,no,1,1,7,Digits,no`);
            } else if (groupChoice) {
                const pick = parseInt(groupChoice, 10);
                const pageSlice = groupsListCache.slice(session.grpIdx, session.grpIdx + 7);
                if (!isNaN(pick) && pick >= 1 && pick <= pageSlice.length) {
                    const chosenGrp = pageSlice[pick - 1];
                    session.targetJid = chosenGrp.jid;
                    session.targetLabel = chosenGrp.name;
                    session.replyKey = null;
                    session.step = 'EXT1_WAIT_RECORD';
                    return res.send(`read=t-נבחרה קבוצת ${chosenGrp.name} הקליטו את ההודעה ובסיום הקישו סולמית=RecPath,no,record,${tempRecFolder},wa_grp_send,no,7,1`);
                }
            }
            const pageGroups = groupsListCache.slice(session.grpIdx, session.grpIdx + 7);
            const parts = pageGroups.map((g, i) => `לקבוצת ${g.name} הקישו ${i + 1}`);
            const moreTxt = (groupsListCache.length > 7) ? 'לקבוצות נוספות הקישו 9' : '';
            return res.send(`read=t-בחירת קבוצה: ${parts.join(', ')} ${moreTxt} לחזרה הקישו 0=GroupChoice,no,1,1,7,Digits,no`);
        }

        // שלוחה 4: שיחה חיה
        if (session.step === 'EXT4_START') {
            session.step = 'EXT4_CHOOSE_TARGET';
            return res.send('read=t-שלוחה 4 שיחה חיה בזמן אמת! לשיחה עם הפונה האחרון ששלח לכם הודעה הקישו 1, לבחירה מרשימת אנשי הקשר הקישו 2, להקשת מספר טלפון הקישו 3, לתפריט הקישו 0=LiveChoice,no,1,1,7,Digits,no');
        }

        if (session.step === 'EXT4_CHOOSE_TARGET' && liveChoice) {
            if (liveChoice === '1' && privateInbox.length > 0) {
                session.liveJid = privateInbox[0].jid;
                session.livePhone = privateInbox[0].phone;
                session.liveLabel = findContactName(privateInbox[0].phone) || privateInbox[0].senderName;
                session.step = 'EXT4_LIVE_LOOP';
            } else if (liveChoice === '2' && contactsList.length > 0) {
                session.cntIdx = 0;
                session.afterContactStep = 'EXT4_LIVE_LOOP';
                session.step = 'SELECT_CONTACT_LIST';
                const pageItems = contactsList.slice(0, 7);
                const txtOpts = pageItems.map((c, i) => `עבור ${c.name} הקישו ${i + 1}`).join(', ');
                return res.send(`read=t-בחירת איש קשר לשיחה חיה: ${txtOpts}, לחזרה הקישו 0=ContactPick,no,1,1,7,Digits,no`);
            } else if (liveChoice === '3' || (liveChoice === '1' && privateInbox.length === 0) || (liveChoice === '2' && contactsList.length === 0)) {
                session.step = 'EXT4_WAIT_PHONE';
                return res.send('read=t-הקישו את מספר הטלפון לשיחה חיה ובסיום הקישו סולמית=TargetPhone,no,15,9,7,Digits,no');
            } else {
                session.step = 'MAIN_MENU';
                return res.send(`read=t-${MAIN_MENU_PROMPT}=MenuChoice,no,1,1,7,Digits,no`);
            }
        }

        if (session.step === 'EXT4_WAIT_PHONE' && targetPhone) {
            session.livePhone = normalizeIsraeliPhone(targetPhone);
            session.liveJid = phoneToJid(targetPhone);
            session.liveLabel = findContactName(session.livePhone) || targetPhone;
            session.step = 'EXT4_LIVE_LOOP';
        }

        if (session.step === 'EXT4_LIVE_LOOP') {
            if (liveAct === '1') {
                session.step = 'EXT4_WAIT_REC';
                return res.send(`read=t-הקליטו כעת את הודעתכם עבור ${session.liveLabel} ובסיום הקישו סולמית=RecPath,no,record,${tempRecFolder},wa_live,no,7,1`);
            } else if (liveAct === '0' || liveAct === '*') {
                session.step = 'MAIN_MENU';
                return res.send(`read=t-${MAIN_MENU_PROMPT}=MenuChoice,no,1,1,7,Digits,no`);
            }

            const targetPh = normalizeIsraeliPhone(session.livePhone || session.liveJid.split('@')[0]);
            const incomingFromTarget = privateInbox.filter(m => normalizeIsraeliPhone(m.phone || m.jid.split('@')[0]) === targetPh);
            const sentToTarget = sentOutbox.filter(m => normalizeIsraeliPhone(m.jid.split('@')[0]) === targetPh);

            let statusConversation = '';
            if (incomingFromTarget.length > 0) {
                const lastIn = incomingFromTarget[0];
                markMessageAsRead(lastIn.jid, lastIn.rawKey);
                statusConversation += `הודעה אחרונה מאת ${session.liveLabel} בשעה ${lastIn.time}: ${lastIn.text}. `;
            } else {
                statusConversation += `אין הודעות נכנסות חדשות מאת ${session.liveLabel}. `;
            }
            if (sentToTarget.length > 0) {
                statusConversation += `הודעה אחרונה שאתם שלחתם בשעה ${sentToTarget[0].time}: ${sentToTarget[0].text}. `;
            }

            return res.send(`read=t-שיחה חיה עם ${session.liveLabel}. ${statusConversation} להקלטת תגובה הקישו 1, לרענון ובדיקה אם התקבלה תשובה חדשה כעת הקישו 2, ליציאה הקישו 0=LiveAct,no,1,1,7,Digits,no`);
        }

        if (session.step === 'EXT4_WAIT_REC' && recPath) {
            const audioBuf = await downloadRecordingFromYemot(recPath, lastVal(q.ApiExtension));
            const transcribed = await transcribeAudioBuffer(audioBuf, 'audio/wav');
            const cleanMsg = sanitizeForYemotTts(transcribed, 200);
            if (cleanMsg) {
                await sendWithHumanDelay(session.liveJid, cleanMsg);
                await recordSentMessage(session.liveJid, session.liveLabel, cleanMsg);
            }
            session.step = 'EXT4_LIVE_LOOP';

            const targetPh = normalizeIsraeliPhone(session.livePhone || session.liveJid.split('@')[0]);
            const latestIn = privateInbox.find(m => normalizeIsraeliPhone(m.phone || m.jid.split('@')[0]) === targetPh);
            const replyInfo = (latestIn && (Date.now() - latestIn.timestamp < 15000))
                ? `התקבלה תשובה חדשה כעת מאת ${session.liveLabel}: ${latestIn.text}.`
                : `נשלח: ${cleanMsg || 'לא זוהה דיבור'}.`;

            return res.send(`id_list_message=t-${replyInfo}&read=t-להקלטת הודעה נוספת הקישו 1, לבדיקת תגובה חדשה הקישו 2, ליציאה הקישו 0=LiveAct,no,1,1,7,Digits,no`);
        }

        // שלוחה 5: הודעות יוצאות ששלחתי
        if (session.step === 'EXT5_START') {
            session.msgIdx = 0;
            session.step = 'EXT5_BROWSE';
        }

        if (session.step === 'EXT5_BROWSE') {
            if (sentAct === '1') {
                const cur = sentOutbox[session.msgIdx];
                if (cur) {
                    session.targetJid = cur.jid;
                    session.targetLabel = cur.targetName;
                    session.replyKey = null;
                    session.step = 'EXT1_WAIT_RECORD';
                    return res.send(`read=t-הקליטו הודעה נוספת עבור ${cur.targetName} ובסיום הקישו סולמית=RecPath,no,record,${tempRecFolder},wa_send,no,7,1`);
                }
            } else if (sentAct === '2') {
                session.msgIdx++;
            } else if (sentAct === '3' && session.msgIdx > 0) {
                session.msgIdx--;
            } else if (sentAct === '0' || sentAct === '*') {
                session.step = 'MAIN_MENU';
                return res.send(`read=t-${MAIN_MENU_PROMPT}=MenuChoice,no,1,1,7,Digits,no`);
            }

            if (sentOutbox.length === 0) {
                session.step = 'MAIN_MENU';
                return res.send(`id_list_message=t-עדיין לא נשלחו הודעות מהמערכת&read=t-${MAIN_MENU_PROMPT}=MenuChoice,no,1,1,7,Digits,no`);
            }
            if (session.msgIdx >= sentOutbox.length) session.msgIdx = 0;
            const sItem = sentOutbox[session.msgIdx];
            const sPrompt = `הודעה יוצאת ${session.msgIdx + 1} מתוך ${sentOutbox.length}. שלחתם אל ${sItem.targetName} בשעה ${sItem.time}: ${sItem.text}. לשליחת הודעה נוספת אליו הקישו 1, להודעה הבאה ששלחתם הקישו 2, להודעה הקודמת הקישו 3, לתפריט הקישו 0`;
            return res.send(`read=t-${sPrompt}=SentAct,no,1,1,7,Digits,no`);
        }

        // שלוחה 6: ניהול אנשי קשר
        if (session.step === 'EXT6_START') {
            session.step = 'EXT6_MENU';
            return res.send('read=t-שלוחה 6 אנשי קשר! לשמיעת רשימת אנשי הקשר ובחירה לשליחה הקישו 1, להוספת איש קשר חדש דרך הטלפון הקישו 2, לתפריט הראשי הקישו 0=ContactAct,no,1,1,7,Digits,no');
        }

        if (session.step === 'EXT6_MENU' && contactAct) {
            if (contactAct === '1') {
                if (contactsList.length === 0) {
                    session.step = 'EXT6_WAIT_PHONE';
                    return res.send('id_list_message=t-רשימת אנשי הקשר ריקה כעת&read=t-להוספת איש קשר ראשון הקישו את מספר הטלפון שלו ובסיום סולמית=NewContactPhone,no,15,9,7,Digits,no');
                }
                session.cntIdx = 0;
                session.afterContactStep = 'EXT1_WAIT_RECORD';
                session.step = 'SELECT_CONTACT_LIST';
                const pageItems = contactsList.slice(0, 7);
                const txtOpts = pageItems.map((c, i) => `עבור ${c.name} הקישו ${i + 1}`).join(', ');
                return res.send(`read=t-רשימת אנשי הקשר: ${txtOpts}, לחזרה הקישו 0=ContactPick,no,1,1,7,Digits,no`);
            } else if (contactAct === '2') {
                session.step = 'EXT6_WAIT_PHONE';
                return res.send('read=t-הוספת איש קשר חדש אנא הקישו את מספר הטלפון של איש הקשר ובסיום הקישו סולמית=NewContactPhone,no,15,9,7,Digits,no');
            } else {
                session.step = 'MAIN_MENU';
                return res.send(`read=t-${MAIN_MENU_PROMPT}=MenuChoice,no,1,1,7,Digits,no`);
            }
        }

        if (session.step === 'EXT6_WAIT_PHONE' && newContactPhone) {
            session.newContactPhone = normalizeIsraeliPhone(newContactPhone);
            session.step = 'EXT6_WAIT_NAME_REC';
            return res.send(`read=t-נקלט מספר ${session.newContactPhone}, כעת אמרו בקולכם את שם איש הקשר ובסיום הקישו סולמית=NewContactNameRec,no,record,${tempRecFolder},contact_name,no,7,1`);
        }

        if (session.step === 'EXT6_WAIT_NAME_REC' && newContactNameRec) {
            const audioBuf = await downloadRecordingFromYemot(newContactNameRec, lastVal(q.ApiExtension));
            const transcribedName = await transcribeAudioBuffer(audioBuf, 'audio/wav');
            const cleanName = sanitizeForYemotTts(transcribedName, 35) || `איש קשר ${contactsList.length + 1}`;
            await saveContact(cleanName, session.newContactPhone);
            session.step = 'MAIN_MENU';
            return res.send(`id_list_message=t-איש הקשר ${cleanName} עם מספר ${session.newContactPhone} נשמר בהצלחה במערכת&read=t-${MAIN_MENU_PROMPT}=MenuChoice,no,1,1,7,Digits,no`);
        }

        return res.send(`read=t-${MAIN_MENU_PROMPT}=MenuChoice,no,1,1,7,Digits,no`);

    } catch (e) {
        console.error('[Yemot WhatsApp Error]:', e.message);
        return res.send(`id_list_message=t-אירעה שגיאה זמנית&read=t-${MAIN_MENU_PROMPT}=MenuChoice,no,1,1,7,Digits,no`);
    }
});

const port = process.env.PORT || 3000;
app.listen(port, () => console.log(`=== Yemot WhatsApp Pro v2.1 Server Ready on port ${port} ===`));
