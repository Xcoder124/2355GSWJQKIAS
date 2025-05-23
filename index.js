const express = require('express');
const axios = require('axios');
const cors = require('cors');
const fs = require('fs');
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
  } catch (err) {
    console.error('Error saving cache:', err);
  }
}, 60000);

app.use(cors());
app.use(express.json());

// Existing endpoint
app.post('/get-mlbb-username', async (req, res) => {
  /* ... keep existing implementation the same ... */
});

// New endpoint to view cache
app.get('/cache', (req, res) => {
  const cacheEntries = Array.from(usernameCache.entries()).map(([key, value]) => ({
    userId_zoneId: key,
    username: value
  }));
  res.json(cacheEntries);
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Proxy running on port ${PORT}`));
