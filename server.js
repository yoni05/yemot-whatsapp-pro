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

const TOKEN = (process.env.YEMOT_TOKEN || '').trim();
const GEMINI_KEY = (process.env.GEMINI_API_KEY || '').trim();
const MONGODB_URI = (process.env.MONGODB_URI || '').trim();
const YM = 'https://www.call2all.co.il/ym/api/';

let sock = null;
let latestQrDataUrl = null;
let waStatus = 'מאתחל חיבור...';
let cachedModelName = null;

const privateInbox = []; 
const groupInbox = [];   
const activeChatsMap = new Map(); 
const groupsListCache = []; 
const userCallSession = new Map(); 
const folderCounters = {};

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

function phoneToJid(phoneStr) {
    let digits = String(phoneStr || '').replace(/[^0-9]/g, '');
    if (digits.startsWith('0')) digits = '972' + digits.slice(1);
    return `${digits}@s.whatsapp.net`;
}

// --- 1. מנגנון שמירת החיבור ב-MongoDB כולל פונקציית מחיקה עצמית! ---
async function useMongoDBAuthState(mongoUri) {
    if (!mongoUri) {
        console.warn('[Auth] No MONGODB_URI provided, falling back to local folder');
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
        } catch {
            return null;
        }
    };

    const removeData = async (id) => {
        try { await collection.deleteOne({ _id: id }); } catch {}
    };

    // פונקציה חכמה למחיקת כל התיקייה במידה והמשתמש מנתק את המכשיר
    const clearState = async () => {
        try { await collection.drop(); } catch (e) {}
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
        clearState // חשיפת הפונקציה לשימוש בעת ניתוק
    };
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
        console.log(`[WhatsApp] Loaded ${groupsListCache.length} groups`);
    } catch (e) {
        console.warn('[Groups Fetch Warning]:', e.message);
    }
}

// --- 4. מנגנון ההתחברות החכם לווצאפ ---
async function startWhatsAppConnection() {
    try {
        const { state, saveCreds, clearState } = await useMongoDBAuthState(MONGODB_URI);
        const { version } = await fetchLatestBaileysVersion();

        sock = makeWASocket({
            version,
            logger: pino({ level: 'silent' }),
            printQRInTerminal: false,
            auth: state,
            browser: ['Yemot WhatsApp Line', 'Chrome', '120.0.0'],
            syncFullHistory: false
        });

        sock.ev.on('creds.update', saveCreds);

        sock.ev.on('connection.update', async (update) => {
            const { connection, lastDisconnect, qr } = update;
            if (qr) {
                latestQrDataUrl = await QRCode.toDataURL(qr);
                waStatus = 'ממתין לסריקת קוד QR (סרוק פעם אחת והחיבור יישמר ב-MongoDB)';
            }
            if (connection === 'open') {
                latestQrDataUrl = null;
                waStatus = 'מחובר ופעיל 100% (שמור ב-MongoDB ללא איפוסים)';
                console.log('[WhatsApp] Connected successfully!');
                setTimeout(refreshGroupsCache, 3000);
            }
            if (connection === 'close') {
                const statusCode = lastDisconnect?.error?.output?.statusCode;
                const shouldReconnect = statusCode !== DisconnectReason.loggedOut;
                
                if (shouldReconnect) {
                    waStatus = `החיבור נסגר (קוד ${statusCode}). מתחבר מחדש אוטומטית...`;
                    setTimeout(startWhatsAppConnection, 4000);
                } else {
                    // כאן הקסם: זיהוי ניתוק יזום -> מחיקת המסד והפעלת ברקוד מחדש אוטומטית!
                    waStatus = 'נותק מהמכשיר! השרת מנקה נתונים ישנים ומייצר ברקוד חדש...';
                    console.log('[WhatsApp] Logged out! Wiping old auth state from MongoDB...');
                    await clearState(); // מוחק את התיקייה ב-MongoDB
                    setTimeout(startWhatsAppConnection, 3000); // מפעיל מחדש כדי לקבל QR נקי
                }
            }
        });

        sock.ev.on('messages.upsert', async ({ messages, type }) => {
            if (type !== 'notify') return;
            for (const msg of messages) {
                try {
                    if (!msg.message || msg.key.fromMe) continue;
                    const remoteJid = msg.key.remoteJid || '';
                    if (!remoteJid || remoteJid === 'status@broadcast') continue;

                    const isGroup = remoteJid.endsWith('@g.us');
                    const senderName = sanitizeForYemotTts(msg.pushName || remoteJid.split('@')[0], 35);

                    let textContent =
                        msg.message.conversation ||
                        msg.message.extendedTextMessage?.text ||
                        msg.message.imageMessage?.caption ||
                        msg.message.videoMessage?.caption || '';

                    const audioMsg = msg.message.audioMessage;
                    if (!textContent && audioMsg) {
                        try {
                            const audioBuffer = await downloadMediaMessage(msg, 'buffer', {});
                            const transcribed = await transcribeAudioBuffer(audioBuffer, audioMsg.mimetype || 'audio/ogg');
                            textContent = transcribed ? `(הודעה קולית) ${transcribed}` : 'הודעה קולית';
                        } catch {
                            textContent = 'הודעה קולית';
                        }
                    }

                    const cleanText = sanitizeForYemotTts(textContent, 160);
                    if (!cleanText) continue;

                    const timeStr = new Date().toLocaleTimeString('he-IL', { timeZone: 'Asia/Jerusalem', hour: '2-digit', minute: '2-digit' });

                    if (isGroup) {
                        const grpObj = groupsListCache.find(g => g.jid === remoteJid);
                        const groupTitle = grpObj ? grpObj.name : 'קבוצה';
                        const entry = { jid: remoteJid, groupName: groupTitle, senderName, text: cleanText, time: timeStr, timestamp: Date.now() };
                        groupInbox.unshift(entry);
                        if (groupInbox.length > 50) groupInbox.pop();
                        activeChatsMap.set(remoteJid, { title: groupTitle, lastSender: senderName, lastText: cleanText, time: timeStr });
                    } else {
                        const phoneDigits = remoteJid.split('@')[0].replace(/^972/, '0');
                        const entry = { jid: remoteJid, phone: phoneDigits, senderName, text: cleanText, time: timeStr, timestamp: Date.now() };
                        privateInbox.unshift(entry);
                        if (privateInbox.length > 50) privateInbox.pop();
                        activeChatsMap.set(remoteJid, { title: senderName, lastSender: senderName, lastText: cleanText, time: timeStr });
                        uploadTtsToYemotFolder('2', `הודעה חדשה מאת ${senderName} טלפון ${phoneDigits} בשעה ${timeStr}: ${cleanText}`);
                    }
                } catch (err) {
                    console.error('[Incoming Message Handler Error]:', err.message);
                }
            }
        });
    } catch (err) {
        console.error('[WhatsApp Init Error]:', err.message);
        waStatus = 'שגיאה באתחול: ' + err.message;
        setTimeout(startWhatsAppConnection, 6000);
    }
}

startWhatsAppConnection();

app.get('/', (req, res) => {
    res.type('text/html; charset=utf-8').send(`<!DOCTYPE html>
<html lang="he" dir="rtl">
<head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<meta http-equiv="refresh" content="8">
<title>מצב חיבור ווצאפ - ימות המשיח</title>
<style>
body{font-family:Arial,sans-serif;background:#f3f5f4;color:#16221d;max-width:620px;margin:30px auto;padding:20px;text-align:center}
.card{background:#fff;padding:24px;border-radius:12px;border:1px solid #d5ddd9;box-shadow:0 2px 8px rgba(0,0,0,0.05)}
.status{font-size:18px;font-weight:bold;color:#0f6b4f;margin:14px 0}
img.qr{max-width:280px;border:6px solid #0f6b4f;border-radius:12px;margin:15px auto;display:block}
</style>
</head>
<body>
<div class="card">
<h2>מערכת ווצאפ קולית לימות המשיח</h2>
<div class="status">${waStatus}</div>
${latestQrDataUrl ? `<p>סרוק את קוד ה-QR מהווצאפ שלך (מכשירים מקושרים):</p><img class="qr" src="${latestQrDataUrl}">` : `<p>הודעות פרטיות בזיכרון (שלוחה 2): <b>${privateInbox.length}</b> | הודעות קבוצות (שלוחה 3): <b>${groupInbox.length}</b></p>`}
<p style="font-size:13px;color:#5b6b64">הדף מתרענן אוטומטית כל 8 שניות.</p>
</div>
</body></html>`);
});

const MAIN_MENU_PROMPT = 'ברוכים הבאים למערכת הווצאפ! לשליחת הודעת ווצאפ הקישו 1, לשמיעת ההודעות שנשלחו אליכם הקישו 2, לקבוצות ווצאפ הקישו 3, לשיחה חיה דרך ווצאפ הקישו 4, ליציאה הקישו 0';

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
            return res.send('id_list_message=t-שרת הווצאפ עדיין מתאתחל או ממתין לסריקת קוד קיו אר&go_to_folder=..');
        }

        let session = userCallSession.get(callId);
        const menuChoice = lastVal(q.MenuChoice);
        const targetPhone = lastVal(q.TargetPhone);
        const recPath = lastVal(q.RecPath);
        const inboxAct = lastVal(q.InboxAct);
        const groupChoice = lastVal(q.GroupChoice);
        const groupAct = lastVal(q.GroupAct);
        const liveChoice = lastVal(q.LiveChoice);
        const liveAct = lastVal(q.LiveAct);

        const tempRecFolder = `/users/${callerPhone}/temp`;

        if (!session) {
            session = { step: 'MAIN_MENU', msgIdx: 0, grpIdx: 0 };
            if (routeMode === '1') session.step = 'EXT1_START';
            else if (routeMode === '2') session.step = 'EXT2_START';
            else if (routeMode === '3') session.step = 'EXT3_START';
            else if (routeMode === '4') session.step = 'EXT4_START';
            userCallSession.set(callId, session);
        }

        if (session.step === 'MAIN_MENU' && menuChoice) {
            if (menuChoice === '1') session.step = 'EXT1_START';
            else if (menuChoice === '2') session.step = 'EXT2_START';
            else if (menuChoice === '3') session.step = 'EXT3_START';
            else if (menuChoice === '4') session.step = 'EXT4_START';
            else if (menuChoice === '0' || menuChoice === '*') {
                userCallSession.delete(callId);
                return res.send('go_to_folder=..');
            }
        }

        if (session.step === 'EXT1_START') {
            session.step = 'EXT1_WAIT_PHONE';
            return res.send('read=t-שלוחה 1 שליחת הודעת ווצאפ אנא הקישו את מספר הטלפון של הנמען ובסיום הקישו סולמית=TargetPhone,no,15,9,7,Digits,no');
        }

        if (session.step === 'EXT1_WAIT_PHONE' && targetPhone) {
            session.targetJid = phoneToJid(targetPhone);
            session.targetLabel = targetPhone;
            session.step = 'EXT1_WAIT_RECORD';
            return res.send(`read=t-אנא הקליטו את תוכן ההודעה לשליחה ובסיום הקישו סולמית=RecPath,no,record,${tempRecFolder},wa_send,no,7,1`);
        }

        if (session.step === 'EXT1_WAIT_RECORD' && recPath) {
            const audioBuf = await downloadRecordingFromYemot(recPath, lastVal(q.ApiExtension));
            const transcribed = await transcribeAudioBuffer(audioBuf, 'audio/wav');
            const cleanMsg = sanitizeForYemotTts(transcribed, 200);
            if (!cleanMsg) {
                session.step = 'MAIN_MENU';
                return res.send(`id_list_message=t-לא הצלחתי לזהות את הדיבור בהקלטה&read=t-${MAIN_MENU_PROMPT}=MenuChoice,no,1,1,7,Digits,no`);
            }
            await sock.sendMessage(session.targetJid, { text: cleanMsg });
            session.step = 'MAIN_MENU';
            return res.send(`id_list_message=t-ההודעה ${cleanMsg} נשלחה בהצלחה למספר ${session.targetLabel}&read=t-${MAIN_MENU_PROMPT}=MenuChoice,no,1,1,7,Digits,no`);
        }

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
                    session.step = 'EXT1_WAIT_RECORD';
                    return res.send(`read=t-הקליטו כעת את תשובתכם עבור ${currentMsg.senderName} ובסיום הקישו סולמית=RecPath,no,record,${tempRecFolder},wa_reply,no,7,1`);
                }
            } else if (inboxAct === '2') {
                session.msgIdx++;
            } else if (inboxAct === '0' || inboxAct === '*') {
                session.step = 'MAIN_MENU';
                return res.send(`read=t-${MAIN_MENU_PROMPT}=MenuChoice,no,1,1,7,Digits,no`);
            }

            if (privateInbox.length === 0) {
                session.step = 'MAIN_MENU';
                return res.send(`id_list_message=t-אין הודעות פרטיות חדשות שהתקבלו&read=t-${MAIN_MENU_PROMPT}=MenuChoice,no,1,1,7,Digits,no`);
            }
            if (session.msgIdx >= privateInbox.length) session.msgIdx = 0;
            const item = privateInbox[session.msgIdx];
            const promptText = `הודעה ${session.msgIdx + 1} מתוך ${privateInbox.length} מאת ${item.senderName} בשעה ${item.time}: ${item.text}. למענה הקישו 1, להודעה הבאה הקישו 2, לתפריט הראשי הקישו 0`;
            return res.send(`read=t-${promptText}=InboxAct,no,1,1,7,Digits,no`);
        }

        if (session.step === 'EXT3_START') {
            if (groupsListCache.length === 0) await refreshGroupsCache();
            session.grpIdx = 0;
            session.step = 'EXT3_MENU';
            return res.send('read=t-שלוחה 3 קבוצות ווצאפ לשמיעת ההודעות האחרונות מהקבוצות הקישו 1, לבחירת קבוצה ושליחת הודעה הקישו 2, לתפריט הראשי הקישו 0=GroupAct,no,1,1,7,Digits,no');
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
                    session.step = 'EXT1_WAIT_RECORD';
                    return res.send(`read=t-הקליטו את ההודעה לשליחה לקבוצת ${cur.groupName} ובסיום הקישו סולמית=RecPath,no,record,${tempRecFolder},wa_grp_reply,no,7,1`);
                }
            } else if (groupChoice === '2') {
                session.msgIdx++;
            } else if (groupChoice === '0') {
                session.step = 'MAIN_MENU';
                return res.send(`read=t-${MAIN_MENU_PROMPT}=MenuChoice,no,1,1,7,Digits,no`);
            }
            if (groupInbox.length === 0) {
                session.step = 'MAIN_MENU';
                return res.send(`id_list_message=t-עדיין לא התקבלו הודעות חדשות בקבוצות&read=t-${MAIN_MENU_PROMPT}=MenuChoice,no,1,1,7,Digits,no`);
            }
            if (session.msgIdx >= groupInbox.length) session.msgIdx = 0;
            const gItem = groupInbox[session.msgIdx];
            const gText = `בקבוצת ${gItem.groupName} כתב ${gItem.senderName}: ${gItem.text}. לשליחת תגובה לקבוצה הקישו 1, להודעה הבאה הקישו 2, לתפריט הקישו 0`;
            return res.send(`read=t-${gText}=GroupChoice,no,1,1,7,Digits,no`);
        }

        if (session.step === 'EXT3_SELECT_GROUP') {
            if (groupsListCache.length === 0) {
                session.step = 'MAIN_MENU';
                return res.send(`id_list_message=t-לא נמצאו קבוצות בחשבון הווצאפ&read=t-${MAIN_MENU_PROMPT}=MenuChoice,no,1,1,7,Digits,no`);
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
                    session.step = 'EXT1_WAIT_RECORD';
                    return res.send(`read=t-נבחרה קבוצת ${chosenGrp.name} אנא הקליטו את ההודעה ובסיום הקישו סולמית=RecPath,no,record,${tempRecFolder},wa_grp_send,no,7,1`);
                }
            }
            const pageGroups = groupsListCache.slice(session.grpIdx, session.grpIdx + 7);
            const parts = pageGroups.map((g, i) => `לקבוצת ${g.name} הקישו ${i + 1}`);
            const moreTxt = (groupsListCache.length > 7) ? 'לקבוצות נוספות הקישו 9' : '';
            return res.send(`read=t-בחירת קבוצה: ${parts.join(', ')} ${moreTxt} לחזרה הקישו 0=GroupChoice,no,1,1,7,Digits,no`);
        }

        if (session.step === 'EXT4_START') {
            session.step = 'EXT4_CHOOSE_TARGET';
            return res.send('read=t-שלוחה 4 שיחה חיה בווצאפ להקשת מספר טלפון לשיחה הקישו 1, להמשך שיחה עם הפונה האחרון הקישו 2, לתפריט הראשי הקישו 0=LiveChoice,no,1,1,7,Digits,no');
        }

        if (session.step === 'EXT4_CHOOSE_TARGET' && liveChoice) {
            if (liveChoice === '1') {
                session.step = 'EXT4_WAIT_PHONE';
                return res.send('read=t-הקישו את מספר הטלפון לשיחה חיה ובסיום הקישו סולמית=TargetPhone,no,15,9,7,Digits,no');
            } else if (liveChoice === '2' && privateInbox.length > 0) {
                session.liveJid = privateInbox[0].jid;
                session.liveLabel = privateInbox[0].senderName;
                session.step = 'EXT4_LIVE_LOOP';
            } else {
                session.step = 'MAIN_MENU';
                return res.send(`read=t-${MAIN_MENU_PROMPT}=MenuChoice,no,1,1,7,Digits,no`);
            }
        }

        if (session.step === 'EXT4_WAIT_PHONE' && targetPhone) {
            session.liveJid = phoneToJid(targetPhone);
            session.liveLabel = targetPhone;
            session.step = 'EXT4_LIVE_LOOP';
        }

        if (session.step === 'EXT4_LIVE_LOOP') {
            if (liveAct === '1') {
                session.step = 'EXT4_WAIT_REC';
                return res.send(`read=t-הקליטו את דבריכם בשיחה עם ${session.liveLabel} ובסיום הקישו סולמית=RecPath,no,record,${tempRecFolder},wa_live,no,7,1`);
            } else if (liveAct === '0' || liveAct === '*') {
                session.step = 'MAIN_MENU';
                return res.send(`read=t-${MAIN_MENU_PROMPT}=MenuChoice,no,1,1,7,Digits,no`);
            }
            const chatState = activeChatsMap.get(session.liveJid);
            const lastMsgStr = chatState
                ? `הודעה אחרונה מאת ${chatState.lastSender} בשעה ${chatState.time}: ${chatState.lastText}`
                : `שיחה חיה מול ${session.liveLabel} עדיין לא התקבלה תגובה חדשה`;
            return res.send(`read=t-${lastMsgStr}. להקלטת הודעה בשיחה הקישו 1, לבדיקה אם התקבלה תשובה חדשה הקישו 2, ליציאה מהשיחה הקישו 0=LiveAct,no,1,1,7,Digits,no`);
        }

        if (session.step === 'EXT4_WAIT_REC' && recPath) {
            const audioBuf = await downloadRecordingFromYemot(recPath, lastVal(q.ApiExtension));
            const transcribed = await transcribeAudioBuffer(audioBuf, 'audio/wav');
            const cleanMsg = sanitizeForYemotTts(transcribed, 200);
            if (cleanMsg) await sock.sendMessage(session.liveJid, { text: cleanMsg });
            session.step = 'EXT4_LIVE_LOOP';
            return res.send(`id_list_message=t-נשלח: ${cleanMsg || 'לא זוהה דיבור'}&read=t-להקלטת הודעה נוספת הקישו 1, לשמיעת התגובה שהתקבלה הקישו 2, ליציאה הקישו 0=LiveAct,no,1,1,7,Digits,no`);
        }

        return res.send(`read=t-${MAIN_MENU_PROMPT}=MenuChoice,no,1,1,7,Digits,no`);

    } catch (e) {
        console.error('[Yemot WhatsApp Error]:', e.message);
        return res.send(`id_list_message=t-אירעה שגיאה זמנית&read=t-${MAIN_MENU_PROMPT}=MenuChoice,no,1,1,7,Digits,no`);
    }
});

const port = process.env.PORT || 3000;
app.listen(port, () => console.log(`=== Yemot WhatsApp Pro Server Ready on port ${port} ===`));
