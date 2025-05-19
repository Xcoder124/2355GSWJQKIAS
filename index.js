const express = require('express');
const axios = require('axios');
const cors = require('cors');
const app = express();

app.use(cors()); // Allow frontend requests
app.use(express.json());

app.post('/get-mlbb-username', async (req, res) => {
  const { userId, zoneId } = req.body;
  
  if (!userId || !zoneId) {
    return res.status(400).json({ error: "UserID and ZoneID are required" });
  }

  try {
    const response = await axios.post('https://order-sg.codashop.com/validate', {
      userId: userId.toString(),
      zoneId: zoneId.toString(),
      voucherTypeName: "MOBILE_LEGENDS",
      deviceId: require('crypto').randomUUID(),
      country: "sg"
    }, {
      headers: {
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0",
        "Origin": "https://www.codashop.com",
        "Referer": "https://www.codashop.com/sg/mobile-legends"
      }
    });

    const username = decodeURIComponent(response.data.result?.username || "")
      .replace(/\+/g, ' ')
      .trim();
    
    res.json({ username });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Proxy running on port ${PORT}`));
