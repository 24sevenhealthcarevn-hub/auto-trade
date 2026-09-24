/* ========================================================
   botEngine.js - CHẠY NỀN BACKEND (NODE.JS) - FULL FIX SAFE
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

const apiKey = process.env.OKX_API_KEY || '9eec71cf-b692-4c5c-9869-27e6ece48e0b';
const secretKey = process.env.OKX_SECRET_KEY || '8C07B300FE8DEA411762AB34C232AD6F';
const passphrase = process.env.OKX_PASSPHRASE || 'Hongnguyen@1987';

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
        await axios.post(url, {
            chat_id: TELEGRAM_CHAT_ID,
            text: message,
            parse_mode: 'HTML'
        });
    } catch (e) {}
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
    if (!apiKey || !secretKey || !passphrase) {
        log("❌ Chưa cấu hình API Keys cho trading");
        return null;
    }

    const ts = new Date().toISOString();
    const methodUpper = method.toUpperCase();
    const fullEndpoint = endpoint.startsWith('/api/v5')
        ? endpoint
        : '/api/v5' + endpoint;

    const bodyStr = (methodUpper === 'GET' || !body)
        ? ''
        : JSON.stringify(body);

    const msg = ts + methodUpper + fullEndpoint + bodyStr;

    const sign = crypto
        .createHmac('sha256', secretKey)
        .update(msg)
        .digest('base64');

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
            log(
                `⚠️ OKX API Error (${fullEndpoint}): ` +
                `Code ${res.data.code} - ${res.data.msg}`
            );
        }

        return res.data;

    } catch (e) {
        log(
            `❌ okxApiRequest Exception: ` +
            `${e.response ? JSON.stringify(e.response.data) : e.message}`
        );

        return null;
    }
}

/* ================== WEBSOCKET ================== */
function initWebSocket() {
    if (
        ws &&
        (
            ws.readyState === WebSocket.OPEN ||
            ws.readyState === WebSocket.CONNECTING
        )
    ) {
        return;
    }

    ws = new WebSocket(
        'wss://ws.okx.com:8443/ws/v5/public'
    );

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

                const inst = String(d.instId || '');
                const last = Number(d.last);

                if (!isFinite(last)) continue;

                /* ===== REALTIME PUMP ===== */
                const pump = topPump.find(
                    x => x.instId === inst
                );

                if (pump) {

                    pump.last = last;

                    if (pump.price1h) {
                        pump.delta =
                            ((last - pump.price1h) /
                                pump.price1h) * 100;
                    }

                    if (d.volCcy24h !== undefined) {
                        pump.vol24h =
                            Number(d.volCcy24h);
                    }
                }

                /* ===== REALTIME DUMP ===== */
                const dump = topDump.find(
                    x => x.instId === inst
                );

                if (dump) {

                    dump.last = last;

                    if (dump.price1h) {
                        dump.delta =
                            ((last - dump.price1h) /
                                dump.price1h) * 100;
                    }

                    if (d.volCcy24h !== undefined) {
                        dump.vol24h =
                            Number(d.volCcy24h);
                    }
                }

                /* ===== REALTIME ACTIVE ORDER ===== */
                if (activeOrders[inst]) {
                    activeOrders[inst].last = last;
                }
            }

        } catch (err) {}
    });

    ws.on('close', () => {

        log(
            "⚠️ WebSocket Closed. Reconnecting in 3s..."
        );

        autoReconnectWS();
    });

    ws.on('error', (err) => {});
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

    if (
        ws &&
        ws.readyState === WebSocket.OPEN
    ) {

        const desired = new Set(
            instIds
                .map(instId =>
                    String(instId || '')
                )
                .filter(instId => instId)
        );

        /* ===== UNSUBSCRIBE COIN KHÔNG CÒN TRONG TOP 10 ===== */

        for (
            const instId of Array.from(wsSubscribed)
        ) {

            if (!desired.has(instId)) {

                ws.send(JSON.stringify({
                    op: 'unsubscribe',
                    args: [{
                        channel: 'tickers',
                        instId
                    }]
                }));

                wsSubscribed.delete(instId);
            }
        }

        /* ===== SUBSCRIBE TOP 5 PUMP + TOP 5 DUMP ===== */

        for (const instId of desired) {

            if (wsSubscribed.has(instId)) continue;

            ws.send(JSON.stringify({
                op: 'subscribe',
                args: [{
                    channel: 'tickers',
                    instId
                }]
            }));

            wsSubscribed.add(instId);
        }
    }
}

/* ================== DATA FETCHING & ACCOUNT ================== */
async function getAccountBalance() {

    try {

        const res = await okxApiRequest(
            '/account/balance?ccy=USDT',
            'GET'
        );

        if (
            res &&
            res.code === '0' &&
            res.data?.[0]?.details?.[0]
        ) {

            const availBal =
                parseFloat(
                    res.data[0].details[0].availBal || 0
                );

            const eq =
                parseFloat(
                    res.data[0].details[0].eq || 0
                );

            return {
                availBal,
                eq
            };
        }

    } catch (err) {

        log(
            `❌ Lỗi kiểm tra số dư: ${err.message}`
        );
    }

    return {
        availBal: 0,
        eq: 0
    };
}

async function fetchATR_Price1h(instId) {

    try {

        const strInst = String(instId);

        const now = Date.now();

        const cached = atrCache[strInst];

        if (
            cached &&
            now - cached.ts < ATR_CACHE_TTL
        ) {
            return cached.data;
        }

        const resCandles = await axios.get(
            `${OKX_API_BASE}/market/candles` +
            `?instId=${strInst}` +
            `&bar=1H` +
            `&limit=30`
        );

        const j = resCandles.data;

        if (
            !Array.isArray(j.data) ||
            j.data.length < 16
        ) {
            return null;
        }

        const candlesRaw = j.data

            .map(c => ({
                ts: Number(c[0]),
                open: Number(c[1]),
                high: Number(c[2]),
                low: Number(c[3]),
                close: Number(c[4]),
                vol: Number(c[5]),
                confirm: String(
                    c[8] ??
                    c[6] ??
                    '0'
                )
            }))

            .filter(c =>
                c.ts &&
                c.open > 0 &&
                c.high > 0 &&
                c.low > 0 &&
                c.close > 0
            )

            .sort(
                (a, b) => a.ts - b.ts
            );

        if (candlesRaw.length < 16) {
            return null;
        }

        /* ===== CURRENT 1H CANDLE OPEN ===== */

        const current =
            candlesRaw[
                candlesRaw.length - 1
            ];

        const price1h =
            current.open;

        /* ===== ATR 14 ===== */

        const closedCandles =
            candlesRaw
                .filter(
                    c => c.confirm === '1'
                )
                .slice(-15);

        const atrCandles =
            closedCandles.length >= 15
                ? closedCandles
                : candlesRaw.slice(-16, -1);

        if (atrCandles.length < 15) {
            return null;
        }

        let trSum = 0;

        for (
            let i = 1;
            i < atrCandles.length;
            i++
        ) {

            const h =
                atrCandles[i].high;

            const l =
                atrCandles[i].low;

            const pc =
                atrCandles[i - 1].close;

            trSum += Math.max(
                h - l,
                Math.abs(h - pc),
                Math.abs(l - pc)
            );
        }

        const atr =
            trSum /
            (atrCandles.length - 1);

        const volAvg1h =
            atrCandles
                .slice(-10)
                .reduce(
                    (sum, c) =>
                        sum + c.vol,
                    0
                ) / 10;

        const data = {
            instId: strInst,
            atr,
            price1h,
            volAvg1h
        };

        atrCache[strInst] = {
            ts: now,
            data
        };

        return data;

    } catch (err) {

        return (
            atrCache[
                String(instId)
            ]?.data || null
        );
    }
}

function calcTP_SL(
    last,
    atr,
    isLong
) {

    const tp =
        isLong
            ? last + (atr * 2.5)
            : last - (atr * 2.5);

    const sl =
        isLong
            ? last - (atr * 1.5)
            : last + (atr * 1.5);

    return {
        tp,
        sl
    };
}

/* ================== ORDER EXECUTION ================== */
async function placeOrder(
    instId,
    side,
    price,
    slPrice,
    tpPrice
) {

    try {

        const strInst =
            String(instId);

        const res = await fetch(
            `https://www.okx.com/api/v5/public/instruments` +
            `?instType=SWAP` +
            `&instId=${strInst}`
        );

        const instRes =
            await res.json();

        const info =
            instRes?.data?.[0];

        if (!info) {

            log(
                `❌ Không lấy được thông tin instrument cho ${strInst}`
            );

            return null;
        }

        const ctVal =
            parseFloat(info.ctVal);

        const lotSz =
            parseFloat(info.lotSz);

        const minSz =
            parseFloat(
                info.minSz || lotSz
            );

        const tickSz =
            parseFloat(
                info.tickSz || "0.0001"
            );

        if (
            !ctVal ||
            !lotSz ||
            !minSz ||
            !tickSz ||
            info.state !== 'live'
        ) {

            log(
                `❌ ${strInst}: Instrument không sẵn sàng để trade.`
            );

            return null;
        }

        const pPrec =
            String(tickSz).includes('.')
                ? String(tickSz)
                    .split('.')[1].length
                : 0;

        const qPrec =
            String(info.lotSz).includes('.')
                ? String(info.lotSz)
                    .split('.')[1].length
                : 0;

        let currentLeverage =
            defaultLeverage;

        let qtyStr = "";

        let leverageFixed = false;

        while (
            currentLeverage >= 10
        ) {

            const levRes =
                await okxApiRequest(
                    '/account/set-leverage',
                    'POST',
                    {
                        instId: strInst,
                        lever:
                            currentLeverage.toString(),
                        mgnMode: 'cross'
                    }
                );

            if (
                levRes?.code === '0' ||
                levRes?.code === '32115'
            ) {

                let rawQty =
                    (
                        capitalPerTrade *
                        currentLeverage
                    ) /
                    (
                        price *
                        ctVal
                    );

                let qty =
                    Math.floor(
                        rawQty / lotSz
                    ) * lotSz;

                if (qty < minSz) {

                    log(
                        `⚠️ ${strInst}: Vốn không đủ mở vị thế tối thiểu.`
                    );

                    return null;
                }

                qtyStr =
                    qPrec > 0
                        ? qty.toFixed(qPrec)
                        : String(
                            Math.round(qty)
                        );

                leverageFixed = true;

                break;

            } else if (
                levRes?.code === '59102'
            ) {

                if (
                    currentLeverage > 50
                ) {
                    currentLeverage = 50;

                } else if (
                    currentLeverage > 30
                ) {
                    currentLeverage = 30;

                } else if (
                    currentLeverage > 20
                ) {
                    currentLeverage = 20;

                } else if (
                    currentLeverage > 10
                ) {
                    currentLeverage = 10;

                } else {
                    break;
                }

            } else {

                log(
                    `❌ ${strInst}: Không set được leverage ${currentLeverage}x.`
                );

                return null;
            }
        }

        if (!leverageFixed) {
            return null;
        }

        const tpStr =
            tpPrice.toFixed(pPrec);

        const slStr =
            slPrice.toFixed(pPrec);

        const attachAlgoOrds = [

            {
                "algoOrdType":
                    "take_profit",

                "sz":
                    qtyStr,

                "tpTriggerPx":
                    tpStr,

                "tpOrdPx":
                    "-1",

                "tpTriggerPxType":
                    "last"
            },

            {
                "algoOrdType":
                    "stop_loss",

                "sz":
                    qtyStr,

                "slTriggerPx":
                    slStr,

                "slOrdPx":
                    "-1",

                "slTriggerPxType":
                    "last"
            }

        ];

        const sideLower =
            String(side).toLowerCase();

        /* ===== NET MODE ===== */

        const orderResult =
            await okxApiRequest(
                '/trade/order',
                'POST',
                {
                    instId:
                        strInst,

                    tdMode:
                        'cross',

                    side:
                        sideLower,

                    ordType:
                        'market',

                    sz:
                        qtyStr,

                    posSide:
                        'net',

                    attachAlgoOrds:
                        attachAlgoOrds
                }
            );

        if (
            orderResult &&
            orderResult.code === '0' &&
            orderResult.data?.[0]?.ordId
        ) {

            const orderId =
                orderResult
                    .data[0]
                    .ordId;

            const quantity =
                parseFloat(qtyStr);

            const notional =
                quantity *
                price *
                ctVal;

            const margin =
                notional /
                currentLeverage;

            log(
                `✅ MỞ LỆNH THÀNH CÔNG ` +
                `${strInst} ` +
                `${sideLower.toUpperCase()} ` +
                `${currentLeverage}x ` +
                `| TP ${tpStr} ` +
                `| SL ${slStr}`
            );

            const orderInfo = {

                id:
                    orderId,

                instId:
                    strInst,

                side:
                    sideLower,

                price,

                quantity,

                slPrice,

                tpPrice,

                margin,

                status:
                    'open',

                ts:
                    Date.now()
            };

            activeOrders[strInst] =
                orderInfo;

            return orderInfo;
        }

        if (orderResult) {

            const detail =
                orderResult.data?.[0] || {};

            log(
                `❌ ${strInst}: ORDER FAILED ` +
                `code=${orderResult.code} ` +
                `sCode=${detail.sCode || ''} ` +
                `msg=${detail.sMsg || orderResult.msg || ''}`
            );

        } else {

            log(
                `❌ ${strInst}: Không nhận được response từ OKX khi đặt lệnh.`
            );
        }

        return null;

    } catch (err) {

        log(
            `❌ ${instId}: Exception placeOrder - ${err.message}`
        );

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

            const {
                availBal
            } = await getAccountBalance();

            if (
                availBal <
                capitalPerTrade
            ) {

                log(
                    `⚠️ Số dư khả dụng ` +
                    `${availBal.toFixed(4)} USDT ` +
                    `< ${capitalPerTrade} USDT.`
                );

                return;
            }
        }

        const res =
            await axios.get(
                OKX_TICKERS
            );

        const j =
            res.data;

        if (!j?.data) return;

        /*
         * Lấy 120 SWAP USDT có thanh khoản cao nhất
         * làm universe quét.
         * Sau đó xếp hạng hoàn toàn theo Delta 1H.
         */

        const rawTickers =
            j.data

                .filter(t =>
                    t.instId &&
                    String(t.instId)
                        .endsWith('-SWAP') &&
                    String(t.instId)
                        .includes('-USDT-')
                )

                .filter(t =>
                    !EXCLUDE_PREFIXES.some(
                        ex =>
                            String(t.instId)
                                .startsWith(ex)
                    )
                )

                .map(t => {

                    const last =
                        parseFloat(
                            t.last || 0
                        );

                    return {

                        ...t,

                        instId:
                            String(t.instId),

                        last,

                        vol24h:
                            Number(
                                t.volCcy24h ||
                                t.vol24h ||
                                0
                            )
                    };
                })

                .filter(t =>
                    t.last > 0
                )

                .sort(
                    (a, b) =>
                        b.vol24h -
                        a.vol24h
                )

                .slice(0, 120);

        const top120Tickers =
            rawTickers;

        if (
            !top120Tickers.length
        ) {
            return;
        }

        /* ===== QUÉT 120 CẶP ===== */

        const atrPriceMap = {};

        for (
            let i = 0;
            i < top120Tickers.length;
            i += CONCURRENCY_LIMIT
        ) {

            const chunk =
                top120Tickers.slice(
                    i,
                    i + CONCURRENCY_LIMIT
                );

            const rs =
                await Promise.all(
                    chunk.map(
                        t =>
                            fetchATR_Price1h(
                                t.instId
                            )
                    )
                );

            rs.forEach(r => {

                if (r?.instId) {

                    atrPriceMap[
                        r.instId
                    ] = r;
                }
            });

            log(
                `🔎 Quét ` +
                `${Math.min(
                    i + chunk.length,
                    top120Tickers.length
                )}` +
                `/${top120Tickers.length}`
            );

            /*
             * 5 request / 250ms
             * ≈ 20 request/giây,
             * phù hợp tốc độ REST candles.
             */

            if (
                i + CONCURRENCY_LIMIT <
                top120Tickers.length
            ) {
                await sleep(250);
            }
        }

        /* ===== BUILD CANDIDATES THEO DELTA 1H ===== */

        const candidates = [];

        for (
            const t of top120Tickers
        ) {

            const inst =
                String(t.instId);

            const last =
                Number(t.last);

            const dataInfo =
                atrPriceMap[inst] || {};

            const {
                atr,
                price1h
            } = dataInfo;

            if (
                !last ||
                !price1h ||
                !atr
            ) {
                continue;
            }

            const delta =
                (
                    (last - price1h) /
                    price1h
                ) * 100;

            const side =
                delta > 0
                    ? 'buy'
                    : 'sell';

            const {
                tp,
                sl
            } =
                calcTP_SL(
                    last,
                    atr,
                    side === 'buy'
                );

            candidates.push({

                instId:
                    inst,

                last,

                price1h,

                delta,

                delta24h:
                    t.open24h > 0
                        ? (
                            (last -
                                Number(t.open24h)) /
                            Number(t.open24h)
                        ) * 100
                        : 0,

                atr,

                tp,

                sl,

                side,

                vol24h:
                    Number(
                        t.vol24h || 0
                    )
            });
        }

        const pumpCandidates =
            candidates

                .filter(
                    x => x.delta > 0
                )

                .sort(
                    (a, b) =>
                        b.delta -
                        a.delta
                );

        const dumpCandidates =
            candidates

                .filter(
                    x => x.delta < 0
                )

                .sort(
                    (a, b) =>
                        a.delta -
                        b.delta
                );

        /*
         * =====================================================
         * GIỮ CỐ ĐỊNH TOP 5 PUMP
         * CHỈ THAY SLOT ĐÃ ĐÓNG
         * =====================================================
         */

        if (
            topPump.length === 0
        ) {

            topPump =
                pumpCandidates
                    .slice(0, TOP_N)
                    .map(x => ({

                        ...x,

                        locked:
                            true,

                        closed:
                            false,

                        status:
                            'open'
                    }));

        } else {

            for (
                let i = 0;
                i < topPump.length;
                i++
            ) {

                const current =
                    topPump[i];

                const live =
                    candidates.find(
                        x =>
                            x.instId ===
                            current.instId
                    );

                /*
                 * Coin còn vị thế:
                 * chỉ cập nhật giá / delta,
                 * KHÔNG thay coin.
                 */

                if (
                    live &&
                    !current.closed
                ) {

                    current.last =
                        live.last;

                    current.delta =
                        live.delta;

                    current.delta24h =
                        live.delta24h;

                    current.vol24h =
                        live.vol24h;
                }

                /*
                 * Coin đã đóng:
                 * chỉ lúc này mới tìm coin mới.
                 */

                if (
                    current.closed
                ) {

                    const replacement =
                        pumpCandidates.find(
                            x =>

                                !topPump.some(
                                    p =>
                                        p.instId ===
                                        x.instId
                                ) &&

                                !topDump.some(
                                    p =>
                                        p.instId ===
                                        x.instId
                                ) &&

                                !activeOrders[
                                    x.instId
                                ]
                        );

                    if (
                        replacement
                    ) {

                        topPump[i] = {

                            ...replacement,

                            locked:
                                true,

                            closed:
                                false,

                            status:
                                'open'
                        };

                        log(
                            `🔄 PUMP thay ` +
                            `${current.instId} → ` +
                            `${replacement.instId}`
                        );
                    }
                }
            }
        }

        /*
         * =====================================================
         * GIỮ CỐ ĐỊNH TOP 5 DUMP
         * CHỈ THAY SLOT ĐÃ ĐÓNG
         * =====================================================
         */

        if (
            topDump.length === 0
        ) {

            topDump =
                dumpCandidates
                    .slice(0, TOP_N)
                    .map(x => ({

                        ...x,

                        locked:
                            true,

                        closed:
                            false,

                        status:
                            'open'
                    }));

        } else {

            for (
                let i = 0;
                i < topDump.length;
                i++
            ) {

                const current =
                    topDump[i];

                const live =
                    candidates.find(
                        x =>
                            x.instId ===
                            current.instId
                    );

                /*
                 * Coin còn vị thế:
                 * chỉ cập nhật giá / delta,
                 * KHÔNG thay coin.
                 */

                if (
                    live &&
                    !current.closed
                ) {

                    current.last =
                        live.last;

                    current.delta =
                        live.delta;

                    current.delta24h =
                        live.delta24h;

                    current.vol24h =
                        live.vol24h;
                }

                /*
                 * Coin đã đóng:
                 * chỉ lúc này mới tìm coin mới.
                 */

                if (
                    current.closed
                ) {

                    const replacement =
                        dumpCandidates.find(
                            x =>

                                !topPump.some(
                                    p =>
                                        p.instId ===
                                        x.instId
                                ) &&

                                !topDump.some(
                                    p =>
                                        p.instId ===
                                        x.instId
                                ) &&

                                !activeOrders[
                                    x.instId
                                ]
                        );

                    if (
                        replacement
                    ) {

                        topDump[i] = {

                            ...replacement,

                            locked:
                                true,

                            closed:
                                false,

                            status:
                                'open'
                        };

                        log(
                            `🔄 DUMP thay ` +
                            `${current.instId} → ` +
                            `${replacement.instId}`
                        );
                    }
                }
            }
        }

        /*
         * =====================================================
         * WS CHỈ THEO DÕI 10 COIN ĐANG HIỂN THỊ
         * =====================================================
         */

        subscribeWS([

            ...topPump.map(
                x => x.instId
            ),

            ...topDump.map(
                x => x.instId
            )

        ]);

        /*
         * =====================================================
         * VÀO LỆNH TRỰC TIẾP
         * KHÔNG masterFlow
         * KHÔNG trailing
         * TỐI ĐA 10 VỊ THẾ
         * =====================================================
         */

        if (isTrading) {

            for (
                const p of [
                    ...topPump,
                    ...topDump
                ]
            ) {

                if (
                    Object.keys(
                        activeOrders
                    ).length >= 10
                ) {
                    break;
                }

                if (
                    !p ||
                    p.closed ||
                    activeOrders[
                        p.instId
                    ]
                ) {
                    continue;
                }

                /*
                 * TP/SL được giữ nguyên theo
                 * thời điểm coin được chọn.
                 * Giá hiện tại chỉ dùng cho realtime display.
                 */

                await placeOrder(
                    p.instId,
                    p.side,
                    p.last,
                    p.sl,
                    p.tp
                );
            }
        }

    } catch (err) {

        log(
            'Lỗi scan: ' +
            err.message
        );

    } finally {

        isScanning = false;
    }
}

async function monitorOrders() {

    try {

        const posRes =
            await okxApiRequest(
                '/account/positions?instType=SWAP'
            );

        if (
            !posRes ||
            posRes.code !== '0'
        ) {
            return;
        }

        const positions =
            (posRes.data || [])
                .filter(
                    p =>
                        Math.abs(+p.pos) > 0
                );

        const runningInstIds =
            positions.map(
                p =>
                    String(p.instId)
            );

        /*
         * Đồng bộ các vị thế thật trên OKX
         * vào activeOrders.
         */

        for (
            const p of positions
        ) {

            const id =
                String(p.instId);

            if (
                !activeOrders[id]
            ) {

                activeOrders[id] = {

                    id:
                        p.posId || '',

                    instId:
                        id,

                    side:
                        Number(p.pos) >= 0
                            ? 'buy'
                            : 'sell',

                    price:
                        Number(
                            p.avgPx || 0
                        ),

                    quantity:
                        Math.abs(
                            Number(
                                p.pos || 0
                            )
                        ),

                    status:
                        'open',

                    ts:
                        Date.now()
                };
            }
        }

        /*
         * Nếu vị thế biến mất khỏi OKX
         * → TP / SL / đóng thủ công
         * → mở khóa slot Top 5.
         */

        for (
            const id in activeOrders
        ) {

            if (
                !runningInstIds.includes(id)
            ) {

                /*
                 * Không xóa quá sớm ngay sau khi
                 * vừa đặt lệnh và OKX chưa kịp
                 * phản hồi position.
                 */

                if (
                    activeOrders[id]?.ts &&
                    Date.now() -
                        activeOrders[id].ts <
                        15000
                ) {
                    continue;
                }

                delete activeOrders[id];

                const pump =
                    topPump.find(
                        x =>
                            x.instId === id
                    );

                if (pump) {

                    pump.closed =
                        true;

                    pump.status =
                        'closed';
                }

                const dump =
                    topDump.find(
                        x =>
                            x.instId === id
                    );

                if (dump) {

                    dump.closed =
                        true;

                    dump.status =
                        'closed';
                }

                log(
                    `♻️ Vị thế ${id} đã đóng → ` +
                    `slot Top 5 được phép thay coin ` +
                    `ở vòng scan kế tiếp.`
                );
            }
        }

    } catch (e) {

        log(
            `❌ Lỗi đồng bộ positions: ` +
            `${e.message}`
        );
    }
}

function setTradingState(state) {

    isTrading =
        Boolean(state);

    log(
        `Trạng thái Auto Trade: ` +
        `${
            isTrading
                ? 'BẬT 🟢'
                : 'TẮT 🔴'
        }`
    );

    return isTrading;
}

function getTradingState() {
    return isTrading;
}

async function runBotCycle() {

    await monitorOrders();

    await scanOnce();
}

/* ================== EXPORTS ================== */
module.exports = {

    runBotCycle,

    setTradingState,

    getTradingState,

    get activeOrders() {
        return activeOrders;
    },

    get tradeHistory() {
        return tradeHistory;
    },

    get topPump() {
        return topPump;
    },

    get topDump() {
        return topDump;
    }
};
