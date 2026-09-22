/* ========================================================
   botEngine.js - CHẠY NỀN BACKEND (NODE.JS)
   ======================================================== */
const axios = require('axios');
const crypto = require('crypto');
const WebSocket = require('ws');

/* ================== CONFIG & STATE ================== */
const OKX_API_BASE = 'https://www.okx.com/api/v5';
const EXCLUDE_PREFIXES = ['BTC-USDT', 'ETH-USDT', 'BNB-USDT', 'USDT', 'USDC', 'FDUSD'];
const OKX_TICKERS = 'https://www.okx.com/api/v5/market/tickers?instType=SWAP';

const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN || '8799491154:AAFvQ1DnFK_UT8sNkEkw6Cizbg5SpAA7e9o';
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID || '2002638809';

const apiKey = process.env.OKX_API_KEY || '7ffea234-8094-4f4c-91f6-1773d2370b5c';
const secretKey = process.env.OKX_SECRET_KEY || '55D97BC2B8E2457EAA62F6152BEE9C03';
const passphrase = process.env.OKX_PASSPHRASE || 'Minhtantruong@1688';

let sentSignals = {};
let isTrading = false;
let isScanning = false;
let isClosingAll = false;
const CONCURRENCY_LIMIT = 5;
const TOP_N = 5;

let ws = null;
let wsSubscribed = new Set();
let isWsReconnecting = false;

// Trạng thái lưu trên RAM (thay cho localStorage)
let activeOrders = {};
let tradeHistory = [];
let fillerCooldown = {};
let positionsData = {};
let instrumentCache = {};
let atrCache = {};
const ATR_CACHE_TTL = 5 * 60 * 1000;

let topPump = [];
let topDump = [];

/* ================== UTILS ================== */
const log = msg => {
    console.log(`[${new Date().toLocaleTimeString()}] ${msg}`);
};

const sleep = ms => new Promise(r => setTimeout(r, ms));

async function sendTelegram(message) {
    if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID) return;
    const url = `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`;
    try {
        await axios.post(url, {
            chat_id: TELEGRAM_CHAT_ID,
            text: message,
            parse_mode: 'HTML'
        });
    } catch (e) {
        // Bỏ qua lỗi gửi tin nhắn
    }
}

/* ================== API CORE (NODE.JS CRYPTO) ================== */
async function okxPublic(endpoint) {
    try {
        const res = await axios.get(OKX_API_BASE + endpoint);
        return res.data;
    } catch (e) {
        return null;
    }
}

async function okxApiRequest(endpoint, method = 'GET', body = null) {
    if (!apiKey || !secretKey || !passphrase) {
        console.warn("Chưa cấu hình API Key OKX!");
        return null;
    }

    const ts = new Date().toISOString();
    const methodUpper = method.toUpperCase();
    const fullEndpoint = endpoint.startsWith('/api/v5') ? endpoint : '/api/v5' + endpoint;
    const bodyStr = (methodUpper === 'GET' || !body) ? '' : JSON.stringify(body);

    const msg = ts + methodUpper + fullEndpoint + bodyStr;
    const sign = crypto.createHmac('sha256', secretKey).update(msg).digest('base64');

    try {
        const config = {
            method: methodUpper,
            url: 'https://www.okx.com' + fullEndpoint,
            headers: {
                'OK-ACCESS-KEY': apiKey,
                'OK-ACCESS-SIGN': sign,
                'OK-ACCESS-TIMESTAMP': ts,
                'OK-ACCESS-PASSPHRASE': passphrase,
                'Content-Type': 'application/json'
            }
        };

        if (bodyStr) config.data = body;

        const res = await axios(config);
        if (res.data && res.data.code !== "0") {
            console.warn('OKX API ERROR:', res.data.code, res.data.msg, 'Path:', fullEndpoint);
        }
        return res.data;
    } catch (e) {
        console.error('okxApiRequest error:', e.response ? e.response.data : e.message);
        return null;
    }
}

/* ================== WEBSOCKET (NODE.JS) ================== */
function initWebSocket() {
    if (ws && (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING)) {
        return;
    }

    ws = new WebSocket('wss://ws.okx.com:8443/ws/v5/public');

    ws.on('open', () => {
        log("✅ WebSocket Connected");
        isWsReconnecting = false;
        if (wsSubscribed.size > 0) {
            const list = Array.from(wsSubscribed);
            wsSubscribed.clear();
            subscribeWS(list);
        }
    });

    ws.on('message', (data) => {
        try {
            const msg = JSON.parse(data.toString());
            if (!msg?.data) return;

            for (const d of msg.data) {
                const inst = d.instId;
                const last = Number(d.last);
                if (!isFinite(last)) continue;

                const pump = topPump.find(x => x.instId === inst);
                if (pump) {
                    pump.last = last;
                    if (pump.price1h) pump.delta = ((last - pump.price1h) / pump.price1h) * 100;
                }

                const dump = topDump.find(x => x.instId === inst);
                if (dump) {
                    dump.last = last;
                    if (dump.price1h) dump.delta = ((last - dump.price1h) / dump.price1h) * 100;
                }

                if (activeOrders[inst]) {
                    activeOrders[inst].last = last;
                }
            }
        } catch (err) {
            console.error("WS Message Error:", err.message);
        }
    });

    ws.on('close', () => {
        log("❌ WebSocket Closed. Reconnecting in 3s...");
        autoReconnectWS();
    });

    ws.on('error', (err) => {
        console.error("WS Error:", err.message);
    });
}

function autoReconnectWS() {
    if (isWsReconnecting) return;
    isWsReconnecting = true;
    setTimeout(() => {
        isWsReconnecting = false;
        initWebSocket();
    }, 3000);
}

function subscribeWS(instIds) {
    if (ws && ws.readyState === WebSocket.OPEN) {
        for (const instId of instIds) {
            if (wsSubscribed.has(instId)) continue;
            ws.send(JSON.stringify({ op: 'subscribe', args: [{ channel: 'tickers', instId }] }));
            wsSubscribed.add(instId);
        }
    }
}

/* ================== DATA FETCHING ================== */
async function fetchATR_Price1h(instId) {
    try {
        const now = Date.now();
        const cached = atrCache[instId];
        if (cached && now - cached.ts < ATR_CACHE_TTL) return cached.data;

        const resCandles = await axios.get(`${OKX_API_BASE}/market/history-candles?instId=${instId}&bar=1H&limit=30`);
        const j = resCandles.data;

        if (!Array.isArray(j.data) || j.data.length < 20) return null;

        const candlesRaw = j.data.map(c => ({
            high: +c[2], low: +c[3], close: +c[4], vol: +c[5]
        }));

        const price1h = candlesRaw[1].close;
        let trSum = 0;
        for (let i = 1; i <= 14; i++) {
            const h = candlesRaw[i].high;
            const l = candlesRaw[i].low;
            const pc = candlesRaw[i + 1].close;
            trSum += Math.max(h - l, Math.abs(h - pc), Math.abs(l - pc));
        }
        const atr = trSum / 14;
        const volAvg1h = candlesRaw.slice(1, 11).reduce((sum, c) => sum + c.vol, 0) / 10;

        const data = { instId, atr, price1h, volAvg1h };
        atrCache[instId] = { ts: now, data };
        return data;
    } catch (err) {
        return atrCache[instId]?.data || null;
    }
}

function calcTP_SL(last, atr, isLong) {
    const tp = isLong ? last + (atr * 2.5) : last - (atr * 2.5);
    const sl = isLong ? last - (atr * 1.5) : last + (atr * 1.5);
    return { tp, sl };
}

/* ================== CORE SCAN & TRADE ================== */
async function scanOnce() {
    if (isScanning) return;
    isScanning = true;

    try {
        initWebSocket();
        const res = await axios.get(OKX_TICKERS);
        const j = res.data;
        if (!j?.data) return;

        const rawTickers = j.data
            .filter(t => t.instId.endsWith('-SWAP') && t.instId.includes('USDT'))
            .filter(t => !EXCLUDE_PREFIXES.some(ex => t.instId.startsWith(ex)))
            .map(t => {
                const last = parseFloat(t.last || 0);
                const open24h = parseFloat(t.open24h || t.sodUtc0 || last);
                const delta24h = open24h > 0 ? ((last - open24h) / open24h) * 100 : 0;
                return { ...t, last, delta24h };
            });

        const top24hPump = [...rawTickers].sort((a, b) => b.delta24h - a.delta24h).slice(0, 60);
        const top24hDump = [...rawTickers].sort((a, b) => a.delta24h - b.delta24h).slice(0, 60);
        const top120Tickers = [...top24hPump, ...top24hDump];

        const atrPriceMap = {};
        for (let i = 0; i < top120Tickers.length; i += CONCURRENCY_LIMIT) {
            const chunk = top120Tickers.slice(i, i + CONCURRENCY_LIMIT);
            const rs = await Promise.all(chunk.map(t => fetchATR_Price1h(t.instId)));
            rs.forEach(r => { if (r?.instId) atrPriceMap[r.instId] = r; });
            if (i + CONCURRENCY_LIMIT < top120Tickers.length) await sleep(800);
        }

        const candidates = [];
        for (const t of top120Tickers) {
            const inst = t.instId;
            const last = Number(t.last);
            const dataInfo = atrPriceMap[inst] || {};
            const { atr, price1h, volAvg1h } = dataInfo;

            if (!last || !price1h) continue;

            const delta = (last - price1h) / price1h * 100;
            const side = delta > 0 ? 'buy' : 'sell';
            const { tp, sl } = calcTP_SL(last, atr, side === 'buy');

            candidates.push({
                instId: inst, last, price1h, delta, delta24h: t.delta24h,
                atr, tp, sl, side, tradeCase: side === 'buy' ? "Uptrend 📈" : "Downtrend 📉"
            });
        }

        topPump = candidates.filter(x => x.delta > 0).sort((a, b) => b.delta - a.delta).slice(0, TOP_N);
        topDump = candidates.filter(x => x.delta < 0).sort((a, b) => a.delta - b.delta).slice(0, TOP_N);

        subscribeWS([...topPump.map(x => x.instId), ...topDump.map(x => x.instId)]);

        if (isTrading) {
            const qualityCandidates = [...topPump, ...topDump].filter(c => !activeOrders[c.instId]);
            await masterFlow(qualityCandidates);
        }
    } catch (err) {
        log('Lỗi scan: ' + err.message);
    } finally {
        isScanning = false;
    }
}

async function masterFlow(qualityCandidates) {
    if (!isTrading) return;

    try {
        const posRes = await okxApiRequest('/account/positions?instType=SWAP');
        if (!posRes || posRes.code !== '0') return;

        const openPositions = (posRes.data || []).filter(p => Math.abs(+p.pos) > 0);
        const openIds = new Set(openPositions.map(p => p.instId));

        if (openIds.size >= 10) return;

        const currentHour = new Date().getHours();
        if (currentHour >= 22 || currentHour < 5) {
            log(`💤 Khung giờ rủi ro rạng sáng (${currentHour}h). Dừng mở vị thế mới.`);
            return;
        }

        for (const p of qualityCandidates) {
            if (openIds.size >= 10) break;
            if (openIds.has(p.instId)) continue;

            const sideLower = String(p.side).toLowerCase();
            const posSide = sideLower === 'buy' ? 'long' : 'short';

            const res = await okxApiRequest('/trade/order', 'POST', {
                instId: p.instId,
                tdMode: 'cross',
                side: sideLower,
                posSide: posSide,
                ordType: 'market',
                sz: '1', // Có thể tùy chỉnh số lượng dựa vào vốn
                attachAlgoOrds: [
                    { algoOrdType: 'take_profit', tpTriggerPx: p.tp.toFixed(4), tpOrdPx: '-1' },
                    { algoOrdType: 'stop_loss', slTriggerPx: p.sl.toFixed(4), slOrdPx: '-1' }
                ]
            });

            if (res?.code === '0') {
                activeOrders[p.instId] = {
                    instId: p.instId, side: sideLower.toUpperCase(), posSide, entry: p.last, tp: p.tp, sl: p.sl
                };
                log(`✅ Đã mở lệnh ${p.instId} (${posSide.toUpperCase()})`);
                sendTelegram(`🚀 **BOT VÀO LỆNH:** ${p.instId} | Side: ${posSide.toUpperCase()} | Entry: ${p.last}`);
            }
        }
    } catch (e) {
        console.error('Lỗi masterFlow:', e.message);
    }
}

async function monitorOrders() {
    try {
        const posRes = await okxApiRequest('/account/positions?instType=SWAP');
        if (!posRes || posRes.code !== '0') return;

        const positions = (posRes.data || []).filter(p => Math.abs(+p.pos) > 0);
        const runningInstIds = positions.map(p => p.instId);

        for (const id in activeOrders) {
            if (!runningInstIds.includes(id)) {
                delete activeOrders[id];
            }
        }
    } catch (e) {
        console.error("Lỗi Monitor:", e.message);
    }
}

/* ================== QUẢN LÝ TRẠNG THÁI RUN/STOP ================== */
function setTradingState(state) {
    isTrading = Boolean(state);
    log(`Trạng thái Auto Trade: ${isTrading ? 'ON 🟢' : 'OFF 🔴'}`);
    return isTrading;
}

function getTradingState() {
    return isTrading;
}

/* ================== BOT CYCLE METHOD FOR SERVER.JS ================== */
async function runBotCycle() {
    await monitorOrders();
    await scanOnce();
}

/* ================== EXPORTS ================== */
module.exports = {
    runBotCycle,
    setTradingState,
    getTradingState,
    activeOrders,
    tradeHistory
};
