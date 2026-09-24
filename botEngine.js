/* ========================================================
   botEngine.js - CHẠY NỀN BACKEND (NODE.JS) - FIX ĐỦ 120+ CẶP & REALTIME
   ======================================================== */
const axios = require('axios');
const crypto = require('crypto');
const WebSocket = require('ws');

/* ================== CONFIG & STATE ================== */
const OKX_API_BASE = 'https://www.okx.com/api/v5';
// Chỉ loại bỏ các đồng stablecoin thuần túy hoặc index lớn nếu cần, giữ lại toàn bộ các cặp altcoin SWAP
const EXCLUDE_PREFIXES = ['USDT-USDT', 'USDC-USDC']; 
const OKX_TICKERS = 'https://www.okx.com/api/v5/market/tickers?instType=SWAP';

const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN || '8799491154:AAFvQ1DnFK_UT8sNkEkw6Cizbg5SpAA7e9o';
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID || '2002638809';

const apiKey = process.env.OKX_API_KEY || '9eec71cf-b692-4c5c-9869-27e6ece48e0b';
const secretKey = process.env.OKX_SECRET_KEY || '8C07B300FE8DEA411762AB34C232AD6F';
const passphrase = process.env.OKX_PASSPHRASE || 'Hongnguyen@1987';

let isTrading = false;
let isScanning = false;
const CONCURRENCY_LIMIT = 15; // Tăng tốc độ quét đồng thời để quét qua 120+ cặp cực nhanh
const TOP_N = 5;

let capitalPerTrade = 10;
let defaultLeverage = 20;

let ws = null;
let wsSubscribed = new Set();
let isWsReconnecting = false;

let activeOrders = {};
let tradeHistory = [];
let atrCache = {};
const ATR_CACHE_TTL = 10 * 60 * 1000;

// Biến lưu trữ Top 5 cố định
let topPump = [];
let topDump = [];

/* ================== UTILS & LOGGING ================== */
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

/* ================== API CORE ================== */
async function okxApiRequest(endpoint, method = 'GET', body = null) {
    if (!apiKey || !secretKey || !passphrase) return null;

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
        return null;
    }
}

/* ================== WEBSOCKET (REALTIME FIX) ================== */
function initWebSocket() {
    if (ws && (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING)) return;

    ws = new WebSocket('wss://ws.okx.com:8443/ws/v5/public');

    ws.on('open', () => {
        log("✅ WebSocket Connected (Realtime Stream)");
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

                // Cập nhật liên tục cho Top Pump
                const pump = topPump.find(x => x.instId === inst);
                if (pump) {
                    pump.last = last;
                    if (pump.price1h > 0) {
                        pump.delta = ((last - pump.price1h) / pump.price1h) * 100;
                    }
                }

                // Cập nhật liên tục cho Top Dump
                const dump = topDump.find(x => x.instId === inst);
                if (dump) {
                    dump.last = last;
                    if (dump.price1h > 0) {
                        dump.delta = ((last - dump.price1h) / dump.price1h) * 100;
                    }
                }

                if (activeOrders[inst]) {
                    activeOrders[inst].last = last;
                }
            }
        } catch (err) {}
    });

    ws.on('close', () => { autoReconnectWS(); });
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
        const args = instIds.map(instId => ({ channel: 'tickers', instId }));
        if (args.length > 0) {
            ws.send(JSON.stringify({ op: 'subscribe', args }));
            instIds.forEach(id => wsSubscribed.add(id));
        }
    }
}

/* ================== DATA FETCHING ================== */
async function getAccountBalance() {
    try {
        const res = await okxApiRequest('/account/balance?ccy=USDT', 'GET');
        if (res && res.code === '0' && res.data?.[0]?.details?.[0]) {
            return {
                availBal: parseFloat(res.data[0].details[0].availBal || 0),
                eq: parseFloat(res.data[0].details[0].eq || 0)
            };
        }
    } catch (err) {}
    return { availBal: 0, eq: 0 };
}

async function fetchATR_Price1h(instId) {
    try {
        const now = Date.now();
        const cached = atrCache[instId];
        if (cached && now - cached.ts < ATR_CACHE_TTL) return cached.data;

        const resCandles = await axios.get(`${OKX_API_BASE}/market/history-candles?instId=${instId}&bar=1H&limit=30`, { timeout: 5000 });
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

/* ================== ORDER EXECUTION ================== */
async function placeOrder(instId, side, price, slPrice, tpPrice) {
    try {
        const res = await axios.get(`https://www.okx.com/api/v5/public/instruments?instType=SWAP&instId=${instId}`, { timeout: 4000 });
        const info = res.data?.data?.[0];
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
            const levRes = await okxApiRequest('/account/set-leverage', 'POST', {
                instId, lever: currentLeverage.toString(), mgnMode: 'cross'
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
                else currentLeverage = 10;
            } else {
                break;
            }
        }

        if (!leverageFixed) return null;

        const tpStr = tpPrice.toFixed(pPrec);
        const slStr = slPrice.toFixed(pPrec);

        const orderResult = await okxApiRequest('/trade/order', 'POST', {
            instId,
            tdMode: 'cross',
            side: side.toLowerCase(),
            ordType: 'market',
            sz: qtyStr,
            posSide: side.toLowerCase() === 'buy' ? 'long' : 'short',
            attachAlgoOrds: [
                { "algoOrdType": "take_profit", "sz": qtyStr, "tpTriggerPx": tpStr, "tpOrdPx": "-1" },
                { "algoOrdType": "stop_loss", "sz": qtyStr, "slTriggerPx": slStr, "slOrdPx": "-1" }
            ]
        });

        if (orderResult && orderResult.code === '0') {
            const orderId = orderResult.data[0].ordId;
            const quantity = parseFloat(qtyStr);
            const notional = quantity * price * ctVal;
            const successLog = `✅ ĐÃ MỞ LỆNH ${side.toUpperCase()} ${qtyStr} Lot ${instId} | SL: ${slStr} | TP: ${tpStr} (~${notional.toFixed(2)} USDT)`;
            log(successLog);
            sendTelegram(`🚀 <b>BOT AUTO TRADE:</b>\n${successLog}`);

            const orderInfo = {
                id: orderId, instId, side: side.toLowerCase(), price, quantity,
                slPrice, tpPrice, capital: capitalPerTrade, leverage: currentLeverage,
                timestamp: Date.now(), status: 'open'
            };
            activeOrders[instId] = orderInfo;
            return orderInfo;
        }
        return null;
    } catch (err) {
        return null;
    }
}

/* ================== CORE SCAN (QUÉT ĐỦ 120+ CẶP & CỐ ĐỊNH TOP 5) ================== */
async function scanOnce() {
    if (isScanning) return;
    isScanning = true;

    try {
        initWebSocket();

        if (isTrading) {
            const { availBal } = await getAccountBalance();
            if (availBal < capitalPerTrade) {
                isScanning = false;
                return;
            }
        }

        const res = await axios.get(OKX_TICKERS, { timeout: 6000 });
        const j = res.data;
        if (!j?.data) {
            isScanning = false;
            return;
        }

        // Lọc chuẩn xác toàn bộ các cặp đuôi -USDT-SWAP (hơn 120+ cặp)
        const rawTickers = j.data
            .filter(t => t.instId.endsWith('-USDT-SWAP') || (t.instId.endsWith('-SWAP') && t.instId.includes('USDT')))
            .filter(t => !EXCLUDE_PREFIXES.some(ex => t.instId.startsWith(ex)))
            .map(t => ({ instId: t.instId, last: parseFloat(t.last || 0), vol24h: parseFloat(t.vol24h || 0) }))
            .filter(t => t.last > 0);

        const totalPairs = rawTickers.length;
        log(`🔍 Đang quét toàn bộ ${totalPairs} cặp giao dịch Futures...`);

        const atrPriceMap = {};
        for (let i = 0; i < totalPairs; i += CONCURRENCY_LIMIT) {
            const chunk = rawTickers.slice(i, i + CONCURRENCY_LIMIT);
            const rs = await Promise.all(chunk.map(t => fetchATR_Price1h(t.instId)));
            rs.forEach(r => { if (r?.instId) atrPriceMap[r.instId] = r; });
            
            const scannedCount = Math.min(i + CONCURRENCY_LIMIT, totalPairs);
            log(`progress: ${scannedCount}/${totalPairs} cặp...`);
            
            if (i + CONCURRENCY_LIMIT < totalPairs) await sleep(20);
        }

        const allCandidates = [];
        for (const t of rawTickers) {
            const dataInfo = atrPriceMap[t.instId];
            if (!dataInfo || !dataInfo.price1h || !dataInfo.atr) continue;

            const last = t.last;
            const { price1h, atr } = dataInfo;
            const delta1h = ((last - price1h) / price1h) * 100;
            const side = delta1h > 0 ? 'buy' : 'sell';
            const { tp, sl } = calcTP_SL(last, atr, side === 'buy');

            allCandidates.push({ instId: t.instId, last, price1h, delta: delta1h, atr, tp, sl, side, vol24h: t.vol24h });
        }

        const sortedByPump = [...allCandidates].filter(x => x.delta > 0).sort((a, b) => b.delta - a.delta);
        const sortedByDump = [...allCandidates].filter(x => x.delta < 0).sort((a, b) => b.delta - a.delta);

        // CỐ ĐỊNH TOP 5: Giữ nguyên danh sách hiện tại, chỉ cập nhật số liệu mới hoặc điền thêm khi thiếu slot
        if (!topPump || topPump.length === 0) {
            topPump = sortedByPump.slice(0, TOP_N);
        } else {
            topPump = topPump.map(existing => {
                const updated = sortedByPump.find(x => x.instId === existing.instId);
                return updated || existing;
            });
            if (topPump.length < TOP_N) {
                for (const p of sortedByPump) {
                    if (topPump.length >= TOP_N) break;
                    if (!topPump.some(e => e.instId === p.instId)) topPump.push(p);
                }
            }
        }

        if (!topDump || topDump.length === 0) {
            topDump = sortedByDump.slice(0, TOP_N);
        } else {
            topDump = topDump.map(existing => {
                const updated = sortedByDump.find(x => x.instId === existing.instId);
                return updated || existing;
            });
            if (topDump.length < TOP_N) {
                for (const d of sortedByDump) {
                    if (topDump.length >= TOP_N) break;
                    if (!topDump.some(e => e.instId === d.instId)) topDump.push(d);
                }
            }
        }

        subscribeWS([...topPump.map(x => x.instId), ...topDump.map(x => x.instId)]);

        if (isTrading) {
            const qualityCandidates = [...topPump, ...topDump].filter(c => !activeOrders[c.instId]);
            for (const p of qualityCandidates) {
                if (Object.keys(activeOrders).length >= 10) break;
                const order = await placeOrder(p.instId, p.side, p.last, p.sl, p.tp);
                if (order) activeOrders[p.instId] = order;
            }
        }
    } catch (err) {
        log('Lỗi scan: ' + err.message);
    } finally {
        isScanning = false;
    }
}

/* ================== MONITOR ORDERS ================== */
async function monitorOrders() {
    try {
        const posRes = await okxApiRequest('/account/positions?instType=SWAP');
        if (!posRes || posRes.code !== '0') return;

        const positions = (posRes.data || []).filter(p => Math.abs(+p.pos) > 0);
        const runningInstIds = positions.map(p => p.instId);

        for (const id in activeOrders) {
            if (!runningInstIds.includes(id)) {
                log(`🔔 Vị thế ${id} đã chạm TP/SL hoặc đóng. Đã giải phóng khỏi Top để quét coin mới.`);
                delete activeOrders[id];
                topPump = topPump.filter(x => x.instId !== id);
                topDump = topDump.filter(x => x.instId !== id);
            }
        }
    } catch (e) {}
}

/* ================== EXPORTS ================== */
function setTradingState(state) {
    isTrading = Boolean(state);
    log(`Trạng thái Auto Trade: ${isTrading ? 'BẬT 🟢' : 'TẮT 🔴'}`);
    return isTrading;
}

function getTradingState() { return isTrading; }

function setTradingConfig(config) {
    if (config) {
        if (config.capital !== undefined) capitalPerTrade = parseFloat(config.capital) || 10;
        if (config.leverage !== undefined) defaultLeverage = parseInt(config.leverage) || 20;
        log(`⚙️ Cấu hình mới -> Vốn: ${capitalPerTrade} USDT | Lev: ${defaultLeverage}x`);
    }
}

async function runBotCycle() {
    await monitorOrders();
    await scanOnce();
}

module.exports = {
    runBotCycle,
    setTradingState,
    getTradingState,
    setTradingConfig,
    get activeOrders() { return activeOrders; },
    get tradeHistory() { return tradeHistory; },
    get topPump() { return topPump; },
    get topDump() { return topDump; }
};
