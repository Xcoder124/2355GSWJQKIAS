const express = require('express');
const axios = require('axios');
const cors = require('cors');
const fs = require('fs');
const crypto = require('crypto');
const app = express();

// Cache configuration
const CACHE_FILE = 'cache.json';
let usernameCache = new Map();

// Load existing cache on server start
if (fs.existsSync(CACHE_FILE)) {
  try {
    const data = fs.readFileSync(CACHE_FILE, 'utf8');
    usernameCache = new Map(JSON.parse(data));
    console.log(`Loaded ${usernameCache.size} cached entries from file`);
  } catch (err) {
    console.error('Error loading cache file:', err);
  }
}

// Save cache to file every 60 seconds
setInterval(() => {
  try {
    fs.writeFileSync(CACHE_FILE, JSON.stringify(Array.from(usernameCache.entries())));
    console.log(`Cache saved (${usernameCache.size} entries)`);
  } catch (err) {
    console.error('Error saving cache:', err);
  }
}, 60000);

app.use(cors());
app.use(express.json());

app.post('/get-mlbb-username', async (req, res) => {
  const { userId, zoneId } = req.body;
  
  if (!userId || !zoneId) {
    return res.status(400).json({ error: "UserID and ZoneID are required" });
  }

  const cacheKey = `${userId}|${zoneId}`;
  if (usernameCache.has(cacheKey)) {
    return res.json({ 
      username: usernameCache.get(cacheKey),
      cached: true
    });
  }

  try {
    const response = await axios.post('https://order-sg.codashop.com/validate', {
      userId: userId.toString(),
      zoneId: zoneId.toString(),
      voucherTypeName: "MOBILE_LEGENDS",
      deviceId: crypto.randomUUID(),
      country: "sg"
    }, {
      headers: {
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0",
        "Origin": "https://www.codashop.com",
        "Referer": "https://www.codashop.com/"
      }
    });

    const username = decodeURIComponent(response.data.result?.username || "")
      .replace(/\+/g, ' ')
      .trim();

    usernameCache.set(cacheKey, username);
    res.json({ username });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.get('/cache', (req, res) => {
  const cacheEntries = Array.from(usernameCache.entries()).map(([key, value]) => ({
    userId_zoneId: key,
    username: value
  }));
  res.json(cacheEntries);
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Server running on port ${PORT}`));
