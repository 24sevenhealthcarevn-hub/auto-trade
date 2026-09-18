const express = require('express');
const path = require('path');
const app = express();
const PORT = process.env.PORT || 10000;

// Phục vụ các file tĩnh (HTML, JS, CSS)
app.use(express.static(__dirname));

app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, 'index.html'));
});

app.listen(PORT, () => {
  console.log(`Server đang chạy tại port ${PORT}`);
});
