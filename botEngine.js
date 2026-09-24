/* ========================================================
   botEngine.js - CHẠY NỀN BACKEND (NODE.JS) - FIX LEVERAGE API
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

let isTrading = false;
let isScanning = false;
const CONCURRENCY_LIMIT = 5;
const TOP_N = 5;

let capitalPerTrade = 10; 
let defaultLeverage = 20;  

let ws = null;
let wsSubscribed = new Set();
let isWsReconnecting = false;

let activeOrders = {};
let tradeHistory = [];
let atrCache = {};
const ATR_CACHE_TTL = 5 * 60 * 1000;

let topPump = [];
let topDump = [];

const log = msg => {
    const formattedMsg = `[${new Date().toLocaleTimeString()}] ${msg}`;
    console.log(formattedMsg);
    if (global.broadcastLog && typeof global.broadcastLog === 'function') {
        global.broadcastLog(formattedMsg);
    }
};

const sleep = ms => new Promise(r => setTimeout(r, ms));

async function sendTelegram(message) {
    if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID) return;
    const url = `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`;
    try {
        await axios.post(url, { chat_id: TELEGRAM_CHAT_ID, text: message, parse_mode: 'HTML' });
    } catch (e) {}
}

async function okxPublic(endpoint) {
    try {
        const res = await axios.get(OKX_API_BASE + endpoint);
        return res.data;
    } catch (e) {
        return null;
    }
}

// HÀM KÝ VÀ GỌI API CHUẨN XÁC VỚI OKX
async function okxApiRequest(endpoint, method = 'GET', body = null) {
    const apiKey = '9eec71cf-b692-4c5c-9869-27e6ece48e0b';
    const secretKey = '8C07B300FE8DEA411762AB34C232AD6F';
    const passphrase = 'Hongnguyen@1987';

    if (!apiKey || !secretKey || !passphrase) {
        log("❌ Chưa cấu hình API Keys cho trading");
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
        return res.data;
    } catch (e) {
        log(`❌ okxApiRequest Exception (${endpoint}): ${e.response ? JSON.stringify(e.response.data) : e.message}`);
        return null;
    }
}

function initWebSocket() {
    if (ws && (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING)) return;
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
            if (data.toString() === 'pong') return;
            const msg = JSON.parse(data.toString());
            if (!msg?.data) return;

            for (const d of msg.data) {
                const inst = d.instId;
                const last = Number(d.last);
                if (!isFinite(last)) continue;

                const pump = topPump.find(x => x.instId === inst);
                if (pump) { pump.last = last; if (pump.price1h) pump.delta = ((last - pump.price1h) / pump.price1h) * 100; }

                const dump = topDump.find(x => x.instId === inst);
                if (dump) { dump.last = last; if (dump.price1h) dump.delta = ((last - dump.price1h) / dump.price1h) * 100; }

                if (activeOrders[inst]) activeOrders[inst].last = last;
            }
        } catch (err) {}
    });

    ws.on('close', () => {
        isWsReconnecting = true;
        setTimeout(() => { isWsReconnecting = false; initWebSocket(); }, 3000);
    });
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

async function getAccountBalance() {
    try {
        const res = await okxApiRequest('/account/balance?ccy=USDT', 'GET');
        if (res && res.code === '0' && res.data?.[0]?.details?.[0]) {
            const availBal = parseFloat(res.data[0].details[0].availBal || 0);
            const eq = parseFloat(res.data[0].details[0].eq || 0);
            return { availBal, eq };
        }
    } catch (err) {}
    return { availBal: 0, eq: 0 };
}

async function fetchATR_Price1h(instId) {
    try {
        const now = Date.now();
        const cached = atrCache[instId];
        if (cached && now - cached.ts < ATR_CACHE_TTL) return cached.data;

        const resCandles = await axios.get(`${OKX_API_BASE}/market/history-candles?instId=${instId}&bar=1H&limit=30`);
        const j = resCandles.data;
        if (!Array.isArray(j.data) || j.data.length < 20) return null;

        const candlesRaw = j.data.map(c => ({ high: +c[2], low: +c[3], close: +c[4], vol: +c[5] }));
        const price1h = candlesRaw[1].close;
        let trSum = 0;
        for (let i = 1; i <= 14; i++) {
            const h = candlesRaw[i].high;
            const l = candlesRaw[i].low;
            const pc = candlesRaw[i + 1].close;
            trSum += Math.max(h - l, Math.abs(h - pc), Math.abs(l - pc));
        }
        const atr = trSum / 14;
        const data = { instId, atr, price1h };
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

async function placeOrder(instId, side, price, slPrice, tpPrice) {
    try {
        const instRes = await okxPublic(`/public/instruments?instType=SWAP&instId=${instId}`);
        const info = instRes?.data?.[0];
        if (!info) return null;

        const ctVal = parseFloat(info.ctVal);
        const lotSz = parseFloat(info.lotSz);
        const minSz = parseFloat(info.minSz || lotSz);
        const tickSz = parseFloat(info.tickSz || "0.0001");
        
        const pPrec = tickSz.toString().includes('.') ? tickSz.toString().split('.')[1].length : 0;
        const qPrec = info.lotSz.includes('.') ? info.lotSz.split('.')[1].length : 0;

        let currentLeverage = defaultLeverage;
        let qtyStr = "";
        let leverageFixed = false;

        while (currentLeverage >= 10) {
            // SỬ DỤNG HÀM OKX API REQUEST ĐỂ CÓ CHỮ KÝ HMAC HỢP LỆ CHO MỌI REQUEST TRADING
            const levRes = await okxApiRequest('/account/set-leverage', 'POST', {
                instId,
                lever: currentLeverage.toString(),
                mgnMode: 'cross'
            });

            if (levRes?.code === '0' || levRes?.code === '32115') {
                let rawQty = (capitalPerTrade * currentLeverage) / (price * ctVal);
                let qty = Math.floor(rawQty / lotSz) * lotSz;

                if (qty < minSz) return null;

                qtyStr = qPrec > 0 ? qty.toFixed(qPrec) : String(Math.round(qty));
                leverageFixed = true;
                break;
            } else if (levRes?.code === '59102') {
                if (currentLeverage > 20) currentLeverage = 20;
                else if (currentLeverage > 10) currentLeverage = 10;
                else break;
            } else {
                break;
            }
        }

        if (!leverageFixed) return null;

        const tpStr = tpPrice.toFixed(pPrec);
        const slStr = slPrice.toFixed(pPrec);

        const attachAlgoOrds = [
            { "algoOrdType": "take_profit", "sz": qtyStr, "tpTriggerPx": tpStr, "tpOrdPx": "-1" },
            { "algoOrdType": "stop_loss", "sz": qtyStr, "slTriggerPx": slStr, "slOrdPx": "-1" }
        ];

        const sideLower = side.toLowerCase();
        const posSide = sideLower === 'buy' ? 'long' : 'short';

        const orderResult = await okxApiRequest('/trade/order', 'POST', {
            instId,
            tdMode: 'cross',
            side: sideLower,
            ordType: 'market',
            sz: qtyStr,
            posSide: posSide,
            attachAlgoOrds: attachAlgoOrds
        });

        if (orderResult && orderResult.code === '0') {
            const orderId = orderResult.data[0].ordId;
            log(`✅ ĐÃ MỞ LỆNH THÀNH CÔNG ${side.toUpperCase()} ${qtyStr} Lot ${instId}`);
            sendTelegram(`🚀 <b>ĐÃ MỞ LỆNH THÀNH CÔNG:</b> ${side.toUpperCase()} ${instId}`);
            
            activeOrders[instId] = { id: orderId, instId, side: sideLower, price, status: 'open' };
            return true;
        } else {
            log(`❌ Lỗi đặt lệnh ${instId}: ${orderResult?.msg || 'Lỗi không xác định'}`);
            return null;
        }
    } catch (err) {
        log(`❌ Lỗi ngoại lệ placeOrder ${instId}: ${err.message}`);
        return null;
    }
}

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

        const top24hPump = [...rawTickers].sort((a, b) => b.delta24h - a.delta24h).slice(0, 40);
        const top24hDump = [...rawTickers].sort((a, b) => a.delta24h - b.delta24h).slice(0, 40);
        const topTickers = [...top24hPump, ...top24hDump];

        const atrPriceMap = {};
        for (let i = 0; i < topTickers.length; i += CONCURRENCY_LIMIT) {
            const chunk = topTickers.slice(i, i + CONCURRENCY_LIMIT);
            const rs = await Promise.all(chunk.map(t => fetchATR_Price1h(t.instId)));
            rs.forEach(r => { if (r?.instId) atrPriceMap[r.instId] = r; });
            if (i + CONCURRENCY_LIMIT < topTickers.length) await sleep(500);
        }

        const candidates = [];
        for (const t of topTickers) {
            const inst = t.instId;
            const last = Number(t.last);
            const dataInfo = atrPriceMap[inst] || {};
            const { atr, price1h } = dataInfo;
            if (!last || !price1h || !atr) continue;

            const delta = (last - price1h) / price1h * 100;
            const side = delta > 0 ? 'buy' : 'sell';
            const { tp, sl } = calcTP_SL(last, atr, side === 'buy');

            candidates.push({ instId: inst, last, delta, atr, tp, sl, side });
        }

        topPump = candidates.filter(x => x.delta > 0).sort((a, b) => b.delta - a.delta).slice(0, TOP_N);
        topDump = candidates.filter(x => x.delta < 0).sort((a, b) => a.delta - b.delta).slice(0, TOP_N);

        subscribeWS([...topPump.map(x => x.instId), ...topDump.map(x => x.instId)]);

        if (isTrading) {
            const qualityCandidates = [...topPump, ...topDump].filter(c => !activeOrders[c.instId]);
            for (const p of qualityCandidates) {
                await placeOrder(p.instId, p.side, p.last, p.sl, p.tp);
            }
        }
    } catch (err) {
        log('Lỗi scan: ' + err.message);
    } finally {
        isScanning = false;
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
                log(`🔔 Vị thế ${id} đã đóng.`);
                delete activeOrders[id];
            }
        }
    } catch (e) {}
}

function setTradingState(state) {
    isTrading = Boolean(state);
    log(`Trạng thái Auto Trade: ${isTrading ? 'BẬT 🟢' : 'TẮT 🔴'}`);
    return isTrading;
}

function getTradingState() {
    return isTrading;
}

async function runBotCycle() {
    await monitorOrders();
    await scanOnce();
}

module.exports = {
    runBotCycle,
    setTradingState,
    getTradingState,
    activeOrders,
    tradeHistory,
    topPump,  // <--- Bổ sung dòng này nếu thiếu
    topDump   // <--- Bổ sung dòng này nếu thiếu
};
