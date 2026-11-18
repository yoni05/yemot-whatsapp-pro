const express = require('express');
const fs = require('fs');
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
let TZINTUK_PHONE = (process.env.TZINTUK_PHONE || '').trim();
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
const groupsListCache = [];
const userCallSession = new Map();
const folderCounters = {};

// --- מנגנון השהיה אנושי חכם (3 עד 9 שניות לכל היותר) + חיווי הקלדה ---
const wait = ms => new Promise(resolve => setTimeout(resolve, ms));

function getRandomDelay(minMs = 3000, maxMs = 9000) {
    return Math.floor(Math.random() * (maxMs - minMs + 1)) + minMs;
}

async function sendWithHumanDelay(targetJid, textMsg) {
    if (!sock) throw new Error('WhatsApp socket not connected');
    const delayMs = getRandomDelay(3000, 9000);
    console.log(`[Human Delay] Simulating typing to ${targetJid} for ${delayMs}ms (max 9s)...`);
    try {
        await sock.sendPresenceUpdate('composing', targetJid);
    } catch {}
    await wait(delayMs);
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

        const cfg = await db.collection('settings').findOne({ _id: 'tzintuk' });
        if (cfg && cfg.phone && !TZINTUK_PHONE) TZINTUK_PHONE = cfg.phone;

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

async function triggerYemotTzintuk(senderName) {
    if (!TOKEN || !TZINTUK_PHONE) return;
    const cleanTarget = normalizeIsraeliPhone(TZINTUK_PHONE);
    if (!cleanTarget) return;

    try {
        const url1 = `${YM}SendTzintuk?token=${encodeURIComponent(TOKEN)}&phones=${encodeURIComponent(cleanTarget)}`;
        const r1 = await fetchWithTimeout(url1, {}, 8000);
        const t1 = await r1.text();
        if (t1.includes('ERROR') || t1.includes('Exception')) {
            const url2 = `${YM}RunTzintuk?token=${encodeURIComponent(TOKEN)}&phones=tzl:${encodeURIComponent(cleanTarget)}`;
            await fetchWithTimeout(url2, {}, 8000);
        }
    } catch (e) {
        console.warn('[Tzintuk Warning]:', e.message);
    }
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
                waStatus = 'מחובר ופעיל 100% (כולל אנשי קשר, השהיה עד 9 שניות וצינתוקים)';
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

                    if (isGroup) {
                        const grpObj = groupsListCache.find(g => g.jid === remoteJid);
                        const groupTitle = grpObj ? grpObj.name : 'קבוצה';
                        const entry = {
                            id: msgId,
                            jid: remoteJid,
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
                        if (type === 'notify') {
                            triggerYemotTzintuk(senderName);
                        }
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
    res.json({ ok: true, contacts: contactsList, tzintukPhone: TZINTUK_PHONE });
});

app.post('/api/contacts', async (req, res) => {
    try {
        const { name, phone } = req.body;
        const saved = await saveContact(name, phone);
        if (!saved) return res.status(400).json({ ok: false, error: 'נא להזין שם ומספר תקינים' });
        res.json({ ok: true, contact: saved, contacts: contactsList });
    } catch (e) {
        res.status(500).json({ ok: false, error: e.message });
    }
});

app.post('/api/tzintuk', async (req, res) => {
    try {
        TZINTUK_PHONE = normalizeIsraeliPhone(req.body.phone || '');
        if (db) {
            await db.collection('settings').updateOne({ _id: 'tzintuk' }, { $set: { phone: TZINTUK_PHONE } }, { upsert: true });
        }
        res.json({ ok: true, tzintukPhone: TZINTUK_PHONE });
    } catch (e) {
        res.status(500).json({ ok: false, error: e.message });
    }
});

app.get('/', (req, res) => {
    const contactsHtml = contactsList.map(c => `<li><b>${c.name}</b> - ${c.phone}</li>`).join('') || '<li>עדיין לא הוגדרו אנשי קשר</li>';
    const recentPrivateHtml = privateInbox.slice(0, 5).map(m => `<li><b>${m.senderName}</b> (${m.time}): ${m.text}</li>`).join('') || '<li>אין הודעות נכנסות</li>';

    res.type('text/html; charset=utf-8').send(`<!DOCTYPE html>
<html lang="he" dir="rtl">
<head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
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
${latestQrDataUrl ? `<p>סרוק את קוד ה-QR מהווצאפ שלך:</p><img class="qr" src="${latestQrDataUrl}"><script>setTimeout(()=>location.reload(),7000);
