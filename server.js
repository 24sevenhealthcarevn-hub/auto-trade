const express = require('express');
const axios = require('axios');
const crypto = require('crypto');
const path = require('path');

const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.json());
// Phục vụ tệp giao diện index.html tĩnh
app.use(express.static(path.join(__dirname)));

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

// 1. PUBLIC API: Lấy giá thị trường (Không cần Secret/Passphrase)
app.get('/api/okx/ticker', async (req, res) => {
  try {
    const instId = req.query.instId || 'BTC-USDT';
    const response = await axios.get(`https://www.okx.com/api/v5/market/ticker?instId=${instId}`);
    res.json(response.data);
  } catch (error) {
    res.status(500).json({ error: error.response ? error.response.data : error.message });
  }
});

// 2. PRIVATE API: Lấy số dư tài khoản (Yêu cầu đầy đủ 3 khóa + Signature)
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

// 3. PRIVATE API: Đặt lệnh giao dịch (POST Request)
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

// 4. PROXY CHUNG DÀNH CHO FRONTEND GỌI MỌI API OKX KHÔNG BỊ LỖI CORS
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
   VÒNG LẶP NGHẦM CHẠY TRÊN RENDER (KHÔNG CẦN BẬT TRÌNH DUYỆT)
   ======================================================== */
async function startServerAutoTradeLoop() {
  console.log("🚀 Server Bot đang chạy ngầm 24/7 trên Render...");
  while (true) {
    try {
      // Thực hiện giữ nhịp quét ngầm 2 phút/lần
      await new Promise(resolve => setTimeout(resolve, 120000));
    } catch (err) {
      console.error("Lỗi vòng lặp Server Bot:", err.message);
      await new Promise(resolve => setTimeout(resolve, 10000));
    }
  }
}

app.listen(PORT, () => {
  console.log(`Server OKX Proxy đang chạy tại port ${PORT}`);
  startServerAutoTradeLoop();
});
