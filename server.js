/* ========================================================
   server.js - BACKEND SERVER (EXPRESS.JS)
   ======================================================== */
const express = require('express');
const axios = require('axios');
const crypto = require('crypto');
const path = require('path');
const cors = require('cors');

// 1. Import botEngine chuẩn đường dẫn Linux (.js)
const botEngine = require('./botEngine.js');

const app = express();
const PORT = process.env.PORT || 3000;

app.use(cors());
app.use(express.json());

// Phục vụ tệp tĩnh và định tuyến trang chủ index.html
app.use(express.static(__dirname));

app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'index.html'));
});

/* ========================================================
   1. UTILS & SIGNATURE HELPERS
   ======================================================== */

// Hàm tạo chữ ký HMAC-SHA256 cho OKX Private API
function generateOkxSignature(timestamp, method, requestPath, body = '') {
  const secretKey = process.env.OKX_SECRET_KEY || '';
  const message = timestamp + method.toUpperCase() + requestPath + body;
  return crypto.createHmac('sha256', secretKey).update(message).digest('base64');
}

// Endpoint Health Check cho UptimeRobot giữ Render luôn chạy 24/7
app.get('/health', (req, res) => {
  res.status(200).send('OK - Bot is running');
});

/* ========================================================
   2. OKX DIRECT & PROXY API ENDPOINTS
   ======================================================== */

// PUBLIC API: Lấy giá thị trường (Không cần Secret/Passphrase)
app.get('/api/okx/ticker', async (req, res) => {
  try {
    const instId = req.query.instId || 'BTC-USDT';
    const response = await axios.get(`https://www.okx.com/api/v5/market/ticker?instId=${instId}`);
    res.json(response.data);
  } catch (error) {
    res.status(500).json({ error: error.response ? error.response.data : error.message });
  }
});

// PRIVATE API: Lấy số dư tài khoản
app.get('/api/okx/balance', async (req, res) => {
  try {
    const timestamp = new Date().toISOString();
    const method = 'GET';
    const requestPath = '/api/v5/account/balance';

    const signature = generateOkxSignature(timestamp, method, requestPath);

    const response = await axios.get(`https://www.okx.com${requestPath}`, {
      headers: {
        'OK-ACCESS-KEY': process.env.OKX_API_KEY || '',
        'OK-ACCESS-SIGN': signature,
        'OK-ACCESS-TIMESTAMP': timestamp,
        'OK-ACCESS-PASSPHRASE': process.env.OKX_PASSPHRASE || '',
        'Content-Type': 'application/json'
      }
    });

    res.json(response.data);
  } catch (error) {
    res.status(500).json({ error: error.response ? error.response.data : error.message });
  }
});

// PRIVATE API: Đặt lệnh giao dịch (POST Request)
app.post('/api/okx/order', async (req, res) => {
  try {
    const timestamp = new Date().toISOString();
    const method = 'POST';
    const requestPath = '/api/v5/trade/order';
    const bodyString = JSON.stringify(req.body);

    const signature = generateOkxSignature(timestamp, method, requestPath, bodyString);

    const response = await axios.post(`https://www.okx.com${requestPath}`, req.body, {
      headers: {
        'OK-ACCESS-KEY': process.env.OKX_API_KEY || '',
        'OK-ACCESS-SIGN': signature,
        'OK-ACCESS-TIMESTAMP': timestamp,
        'OK-ACCESS-PASSPHRASE': process.env.OKX_PASSPHRASE || '',
        'Content-Type': 'application/json'
      }
    });

    res.json(response.data);
  } catch (error) {
    res.status(500).json({ error: error.response ? error.response.data : error.message });
  }
});

// PROXY CHUNG DÀNH CHO FRONTEND GỌI MỌI API OKX KHÔNG BỊ LỖI CORS
app.use('/api/okx-proxy/*', async (req, res) => {
  try {
    const targetPath = req.originalUrl.replace('/api/okx-proxy', '/api/v5');
    const method = req.method;
    const timestamp = new Date().toISOString();
    let bodyString = '';

    if (method !== 'GET' && method !== 'HEAD' && req.body && Object.keys(req.body).length > 0) {
      bodyString = JSON.stringify(req.body);
    }

    const signature = generateOkxSignature(timestamp, method, targetPath, bodyString);

    const headers = {
      'OK-ACCESS-KEY': process.env.OKX_API_KEY || '',
      'OK-ACCESS-SIGN': signature,
      'OK-ACCESS-TIMESTAMP': timestamp,
      'OK-ACCESS-PASSPHRASE': process.env.OKX_PASSPHRASE || '',
      'Content-Type': 'application/json'
    };

    const axiosConfig = {
      method: method,
      url: `https://www.okx.com${targetPath}`,
      headers: headers
    };

    if (bodyString) {
      axiosConfig.data = req.body;
    }

    const response = await axios(axiosConfig);
    res.json(response.data);
  } catch (error) {
    res.status(error.response?.status || 500).json({ error: error.response ? error.response.data : error.message });
  }
});

/* ========================================================
   3. AUTO TRADE ENGINE API & CONTROLLER
   ======================================================== */

// 1. Kích hoạt Auto Trade
app.post('/api/autotrade/start', (req, res) => {
  const currentState = botEngine.getTradingState();
  if (currentState) {
    return res.json({ ok: true, running: true, message: 'Bot đang chạy ngầm rồi' });
  }

  botEngine.setTradingState(true);
  console.log('🚀 AUTO TRADE: KÍCH HOẠT CHẠY NGẦM TRÊN RENDER');

  res.json({ ok: true, running: true });
});

// 2. Dừng Auto Trade
app.post('/api/autotrade/stop', (req, res) => {
  botEngine.setTradingState(false);
  console.log('🛑 AUTO TRADE: ĐÃ NGẮT TOÀN BỘ LUỒNG CHẠY NGẦM');

  res.json({ ok: true, running: false });
});

// 3. Toggle trạng thái Bật/Tắt Auto Trade
app.post('/api/bot/toggle', (req, res) => {
  const { enable } = req.body;
  const currentState = botEngine.getTradingState();
  const newState = (typeof enable === 'boolean') ? enable : !currentState;

  botEngine.setTradingState(newState);

  res.json({
    ok: true,
    success: true,
    running: newState,
    isTrading: newState,
    message: `Đã ${newState ? 'BẬT 🟢' : 'TẮT 🔴'} Auto Trade thành công.`
  });
});

// 4. Lấy trạng thái BOT và đồng bộ với Frontend
app.get(['/api/autotrade/status', '/api/bot/status'], (req, res) => {
  const isRunning = botEngine.getTradingState();
  res.json({
    ok: true,
    success: true,
    running: isRunning,
    isTrading: isRunning,
    activeOrders: botEngine.activeOrders || {},
    tradeHistory: botEngine.tradeHistory || []
  });
});

/* ========================================================
   4. VÒNG LẶP AUTO TRADE CHẠY NGẦM (BOT LOOP 24/7)
   ======================================================== */

const SCAN_INTERVAL = 15000; // Quét tín hiệu và monitor mỗi 15 giây

setInterval(async () => {
  try {
    await botEngine.runBotCycle();
  } catch (err) {
    console.error('❌ Lỗi Bot ngầm Render:', err.message);
  }
}, SCAN_INTERVAL);

/* ========================================================
   5. START SERVER
   ======================================================== */

app.listen(PORT, () => {
  console.log(`=================================`);
  console.log(`🚀 Server Node.js đang chạy tại port: ${PORT}`);
  console.log(`🤖 BOT Engine đã tích hợp và sẵn sàng!`);
  console.log(`=================================`);
});
