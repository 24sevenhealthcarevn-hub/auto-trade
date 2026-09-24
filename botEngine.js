/* ========================================================
   botEngine.js - CHẠY NỀN BACKEND (NODE.JS) - FULL FIX
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

// Dán trực tiếp thông tin API OKX vào đây:
const apiKey = process.env.OKX_API_KEY || '9eec71cf-b692-4c5c-9869-27e6ece48e0b';
const secretKey = process.env.OKX_SECRET_KEY || '8C07B300FE8DEA411762AB34C232AD6F';
const passphrase = process.env.OKX_PASSPHRASE || 'Hongnguyen@1987';

let isTrading = false;
let isScanning = false;
const CONCURRENCY_LIMIT = 5;
const TOP_N = 5;

// Cấu hình giao dịch (Sẽ nhận trực tiếp từ giao diện Web thông qua server.js)
let capitalPerTrade = 10; // Vốn mỗi lệnh (USDT)
let defaultLeverage = 20;  // Đòn bẩy mặc định

let ws = null;
let wsSubscribed = new Set();
let isWsReconnecting = false;

// Trạng thái lưu trên RAM
let activeOrders = {};
let tradeHistory = [];
let atrCache = {};
const ATR_CACHE_TTL = 5 * 60 * 1000;

let topPump = [];
let topDump = [];

/* ================== UTILS & LOGGING ================== */
const log = msg => {
    const formattedMsg = `[${new Date().toLocaleTimeString()}] ${msg}`;
    console.log(formattedMsg);
    // Phát log tới server.js để gửi về giao diện Web nếu có callback
    if (global.broadcastLog && typeof global.broadcastLog === 'function') {
        global.broadcastLog(formattedMsg);
    }
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
        // Bỏ qua lỗi gửi Telegram
    }
}

/* ================== API CORE ================== */
async function okxPublic(endpoint) {
    try {
        const res = await axios.get(OKX_API_BASE + endpoint);
        return res.data;
    } catch (e) {
        return null;
    }
}

async function okxApiRequest(endpoint, method = 'GET', body = null) {
    const apiKey = process.env.OKX_API_KEY || '9eec71cf-b692-4c5c-9869-27e6ece48e0b';
    const secretKey = process.env.OKX_SECRET_KEY || '8C07B300FE8DEA411762AB34C232AD6F';
    const passphrase = process.env.OKX_PASSPHRASE || 'Hongnguyen@1987';

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
        if (res.data && res.data.code !== "0") {
            log(`⚠️ OKX API Error (${fullEndpoint}): Code ${res.data.code} - ${res.data.msg}`);
        }
        return res.data;
    } catch (e) {
        log(`❌ okxApiRequest Exception: ${e.response ? JSON.stringify(e.response.data) : e.message}`);
        return null;
    }
}

/* ================== WEBSOCKET ================== */
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
            if (data.toString() === 'pong') return;
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
            // Bỏ qua lỗi parse
        }
    });

    ws.on('close', () => {
        log("⚠️ WebSocket Closed. Reconnecting in 3s...");
        autoReconnectWS();
    });

    ws.on('error', (err) => {
        // WS error
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

/* ================== DATA FETCHING & ACCOUNT ================== */
async function getAccountBalance() {
    try {
        const res = await okxApiRequest('/account/balance?ccy=USDT', 'GET');
        if (res && res.code === '0' && res.data?.[0]?.details?.[0]) {
            const availBal = parseFloat(res.data[0].details[0].availBal || 0);
            const eq = parseFloat(res.data[0].details[0].eq || 0);
            return { availBal, eq };
        }
    } catch (err) {
        log(`❌ Lỗi kiểm tra số dư: ${err.message}`);
    }
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

/* ================== ORDER EXECUTION (PLACE ORDER) ================== */
async function placeOrder(instId, side, price, slPrice, tpPrice) {
    try {
        const res = await fetch(`https://www.okx.com/api/v5/public/instruments?instType=SWAP&instId=${instId}`);
        const instRes = await res.json();
        const info = instRes?.data?.[0];
        if (!info) {
            log(`❌ Không lấy được thông tin instrument cho ${instId}`);
            return null;
        }

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
                instId,
                lever: currentLeverage.toString(),
                mgnMode: 'cross'
            });

            if (levRes?.code === '0' || levRes?.code === '32115') {
                let rawQty = (capitalPerTrade * currentLeverage) / (price * ctVal);
                let qty = Math.floor(rawQty / lotSz) * lotSz;

                if (qty < minSz) {
                    log(`⚠️ ${instId}: Vốn ${capitalPerTrade} USDT không đủ để mở vị thế tối thiểu (${minSz} lot).`);
                    return null;
                }

                qtyStr = qPrec > 0 ? qty.toFixed(qPrec) : String(Math.round(qty));
                leverageFixed = true;
                break;
            } else if (levRes?.code === '59102') {
                if (currentLeverage > 50) currentLeverage = 50;
                else if (currentLeverage > 30) currentLeverage = 30;
                else if (currentLeverage > 20) currentLeverage = 20;
                else if (currentLeverage > 10) currentLeverage = 10;
                else break;

                log(`🔄 ${instId}: Hạ đòn bẩy xuống ${currentLeverage}x do giới hạn OKX`);
            } else {
                log(`❌ Set leverage lỗi cho ${instId}: ${levRes?.code} ${levRes?.msg || ''}`);
                return null;
            }
        }

        if (!leverageFixed) return null;

        const tpStr = tpPrice.toFixed(pPrec);
        const slStr = slPrice.toFixed(pPrec);

        const attachAlgoOrds = [
            {
                "algoOrdType": "take_profit",
                "sz": qtyStr,
                "tpTriggerPx": tpStr,
                "tpOrdPx": "-1"
            },
            {
                "algoOrdType": "stop_loss",
                "sz": qtyStr,
                "slTriggerPx": slStr,
                "slOrdPx": "-1"
            }
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
            const quantity = parseFloat(qtyStr);
            const notional = quantity * price * ctVal;
            const margin = notional / currentLeverage;

            const successLog = `✅ ĐÃ MỞ LỆNH ${sideUpper(side)} ${qtyStr} Lot ${instId} | SL: ${slStr} | TP: ${tpStr} (Vị thế ~${notional.toFixed(2)} USDT, Lev ${currentLeverage}x, Vốn ${capitalPerTrade} USDT)`;
            log(successLog);
            sendTelegram(`🚀 <b>BOT AUTO TRADE:</b>\n${successLog}`);

            const orderInfo = {
                id: orderId,
                instId,
                side: sideLower,
                price,
                quantity,
                slPrice,
                tpPrice,
                capital: capitalPerTrade,
                leverage: currentLeverage,
                margin,
                notional,
                timestamp: Date.now(),
                status: 'open'
            };

            activeOrders[instId] = orderInfo;
            return orderInfo;
        } else {
            log(`❌ Lỗi đặt lệnh ${instId}: ${orderResult?.msg || 'Lỗi không xác định'}`);
            return null;
        }
    } catch (err) {
        log(`❌ Ngoại lệ khi đặt lệnh ${instId}: ${err.message}`);
        return null;
    }
}

function sideUpper(s) {
    return String(s).toUpperCase();
}

/* ================== CORE SCAN ================== */
async function scanOnce() {
    if (isScanning) return;
    isScanning = true;

    try {
        initWebSocket();

        if (isTrading) {
            const { availBal, eq } = await getAccountBalance();
            log(`💰 [OKX ACCOUNT] Số dư khả dụng: ${availBal.toFixed(2)} USDT | Tổng tài sản: ${eq.toFixed(2)} USDT`);

            if (availBal < capitalPerTrade) {
                log(`⚠️ Số dư khả dụng (${availBal.toFixed(2)} USDT) nhỏ hơn vốn cài đặt (${capitalPerTrade} USDT). Tạm dừng mở vị thế mới!`);
                isScanning = false;
                return;
            }
        }

        const res = await axios.get(OKX_TICKERS);
        const j = res.data;
        if (!j?.data) {
            isScanning = false;
            return;
        }

        // Bước 1 & 2: Scan toàn sàn và chuẩn bị danh sách lấy dữ liệu nến 1H để tính Delta 1H
        const rawTickers = j.data
            .filter(t => t.instId.endsWith('-SWAP') && t.instId.includes('USDT'))
            .filter(t => !EXCLUDE_PREFIXES.some(ex => t.instId.startsWith(ex)))
            .map(t => ({
                instId: t.instId,
                last: parseFloat(t.last || 0)
            }))
            .filter(t => t.last > 0);

        // Lấy ATR và giá 1H trước đó cho toàn sàn (hoặc batch lớn) để tính Delta 1H chuẩn xác
        const atrPriceMap = {};
        for (let i = 0; i < rawTickers.length; i += CONCURRENCY_LIMIT) {
            const chunk = rawTickers.slice(i, i + CONCURRENCY_LIMIT);
            const rs = await Promise.all(chunk.map(t => fetchATR_Price1h(t.instId)));
            rs.forEach(r => { if (r?.instId) atrPriceMap[r.instId] = r; });
            if (i + CONCURRENCY_LIMIT < rawTickers.length) await sleep(300);
        }

        // Tính Delta 1H cho tất cả coin trên sàn
        const allCandidates = [];
        for (const t of rawTickers) {
            const dataInfo = atrPriceMap[t.instId];
            if (!dataInfo || !dataInfo.price1h || !dataInfo.atr) continue;

            const last = t.last;
            const { price1h, atr } = dataInfo;
            const delta1h = ((last - price1h) / price1h) * 100;
            const side = delta1h > 0 ? 'buy' : 'sell';
            const { tp, sl } = calcTP_SL(last, atr, side === 'buy');

            allCandidates.push({
                instId: t.instId,
                last,
                price1h,
                delta: delta1h,
                atr,
                tp,
                sl,
                side
            });
        }

        // Bước 3: Giữ cố định Top 5 đang chạy lệnh/theo dõi, chỉ thay thế coin mới khi hoàn thành TP/SL
        const sortedByPump = [...allCandidates].filter(x => x.delta > 0).sort((a, b) => b.delta - a.delta);
        const sortedByDump = [...allCandidates].filter(x => x.delta < 0).sort((a, b) => b.delta - a.delta);

        let keptPump = topPump.filter(p => activeOrders[p.instId] || p.status === 'open');
        let keptDump = topDump.filter(d => activeOrders[d.instId] || d.status === 'open');

        for (const p of sortedByPump) {
            if (keptPump.length >= TOP_N) break;
            if (!keptPump.some(existing => existing.instId === p.instId)) {
                keptPump.push(p);
            }
        }

        for (const d of sortedByDump) {
            if (keptDump.length >= TOP_N) break;
            if (!keptDump.some(existing => existing.instId === d.instId)) {
                keptDump.push(d);
            }
        }

        topPump = keptPump.slice(0, TOP_N);
        topDump = keptDump.slice(0, TOP_N);

        subscribeWS([...topPump.map(x => x.instId), ...topDump.map(x => x.instId)]);

        // Bước 4: Kiểm tra điều kiện và thực hiện đặt lệnh (Tối đa 10 vị thế - chỉ chạy khi bật Auto Trade)
        if (isTrading) {
            const qualityCandidates = [...topPump, ...topDump]
                .filter(c => !activeOrders[c.instId]);

            for (const p of qualityCandidates) {
                if (Object.keys(activeOrders).length >= 10) break;

                const order = await placeOrder(
                    p.instId,
                    p.side,
                    p.last,
                    p.sl,
                    p.tp
                );

                if (order) {
                    activeOrders[p.instId] = order;
                }
            }
        }
    } catch (err) {
        log('Lỗi scan: ' + err.message);
    } finally {
        isScanning = false;
    }
}

/* ================== MONITOR ORDERS (ĐỒNG BỘ VỊ THẾ) ================== */
async function monitorOrders() {
    try {
        const posRes = await okxApiRequest('/account/positions?instType=SWAP');
        if (!posRes || posRes.code !== '0') return;

        const positions = (posRes.data || []).filter(p => Math.abs(+p.pos) > 0);
        const runningInstIds = positions.map(p => p.instId);

        for (const id in activeOrders) {
            if (!runningInstIds.includes(id)) {
                log(`🔔 Vị thế ${id} đã được đóng (dính TP/SL hoặc đóng trên sàn). Xóa activeOrders để nhường chỗ cho vòng scan sau.`);
                delete activeOrders[id];
            }
        }
    } catch (e) {
        // Monitor error
    }
}

/* ================== QUẢN LÝ TRẠNG THÁI & CẤU HÌNH ================== */
function setTradingState(state) {
    isTrading = Boolean(state);
    log(`Trạng thái Auto Trade: ${isTrading ? 'BẬT 🟢 (Đang chạy ngầm)' : 'TẮT 🔴'}`);
    return isTrading;
}

function getTradingState() {
    return isTrading;
}

// Nhận cấu hình Vốn và Đòn bẩy từ giao diện Web (thông qua server.js)
function setTradingConfig(config) {
    if (config) {
        if (config.capital !== undefined) {
            capitalPerTrade = parseFloat(config.capital) || 10;
        }
        if (config.leverage !== undefined) {
            defaultLeverage = parseInt(config.leverage) || 20;
        }
        log(`⚙️ Đã cập nhật cấu hình mới -> Vốn mỗi lệnh: ${capitalPerTrade} USDT | Đòn bẩy: ${defaultLeverage}x`);
    }
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
    setTradingConfig,
    get activeOrders() { return activeOrders; },
    get tradeHistory() { return tradeHistory; },
    get topPump() { return topPump; },
    get topDump() { return topDump; }
};
