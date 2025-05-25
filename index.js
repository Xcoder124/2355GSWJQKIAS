const express = require('express');
const axios = require('axios');
const cors = require('cors');
const fs = require('fs');
const crypto = require('crypto');
const app = express();

// Import Firebase Admin SDK
const admin = require('firebase-admin');

try {
    const serviceAccount = require('/etc/secrets/serviceAccountKey.json'); 
    admin.initializeApp({
        credential: admin.credential.cert(serviceAccount)
    });
    console.log("Firebase Admin SDK initialized successfully.");
} catch (error) {
    console.error("Error initializing Firebase Admin SDK:", error);
    process.exit(1);
}

const dbAdmin = admin.firestore(); // Firestore instance from Admin SDK
const TimestampAdmin = admin.firestore.Timestamp;
const FieldValueAdmin = admin.firestore.FieldValue;
// ----- END FIREBASE ADMIN INITIALIZATION -----


// Cache configuration (your existing code)
const CACHE_FILE = 'cache.json';
let usernameCache = new Map();

if (fs.existsSync(CACHE_FILE)) {
    try {
        const data = fs.readFileSync(CACHE_FILE, 'utf8');
        usernameCache = new Map(JSON.parse(data));
        console.log(`Loaded ${usernameCache.size} cached entries from file`);
    } catch (err) {
        console.error('Error loading cache file:', err);
    }
}

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
        if (username) { // Only cache if username is found
            usernameCache.set(cacheKey, username);
        }
        res.json({ username });
    } catch (error) {
        console.error("Error fetching MLBB username:", error.response ? error.response.data : error.message);
        res.status(500).json({ error: "Failed to fetch username from Codashop API." });
    }
});

app.get('/cache', (req, res) => {
    const cacheEntries = Array.from(usernameCache.entries()).map(([key, value]) => ({
        userId_zoneId: key,
        username: value
    }));
    res.json(cacheEntries);
});


// ----- NEW ENDPOINTS FOR GIFTING FLOW -----

// Middleware for Firebase ID Token Authentication (RECOMMENDED FOR PRODUCTION)
async function authenticateToken(req, res, next) {
    const authHeader = req.headers.authorization;
    if (!authHeader || !authHeader.startsWith('Bearer ')) {
        return res.status(403).send('Unauthorized: No token provided.');
    }
    const idToken = authHeader.split('Bearer ')[1];
    try {
        const decodedToken = await admin.auth().verifyIdToken(idToken);
        req.user = decodedToken; // Add user info to request object
        next();
    } catch (error) {
        console.error('Error verifying Firebase ID token:', error);
        res.status(403).send('Unauthorized: Invalid token.');
    }
}


const TransactionTypeServer = { // Define on server to avoid client-side mismatch issues
    Order: 'Order',
    Receive: 'Receive',
    Sent: 'Sent',
    Redeemed: 'Redeemed',
    Gifted: 'Gifted',
    Received_Gift: 'Received Gift',
    Refund: 'Refund'
};


// Endpoint 1: After a gift order is successfully created by the sender on the client
app.post('/api/gift-order-processed', authenticateToken, async (req, res) => {
    const {
        orderId,
        recipientUid,
        senderUid, // userId from client
        senderDisplayName,
        senderPhotoURL,
        productName,
        quantity,
        category, // productGroupName from client
        orderReference,
        giftExpirationISO // giftExpiration.toDate().toISOString() from client
    } = req.body;

    if (!orderId || !recipientUid || !senderUid || !productName || !quantity || !category || !orderReference || !giftExpirationISO) {
        return res.status(400).json({ error: "Missing required fields for processing gift order." });
    }

    try {
        const batch = dbAdmin.batch();
        const recipientTransactionRef = dbAdmin.collection("users").doc(recipientUid).collection("transactions").doc();
        const recipientUserRef = dbAdmin.collection("users").doc(recipientUid);

        const recipientTransactionRecData = {
            timestamp: FieldValueAdmin.serverTimestamp(),
            type: TransactionTypeServer.Received_Gift,
            amount: 0,
            reference: recipientTransactionRef.id,
            description: `Received a gift: ${productName} from ${senderDisplayName || 'a friend'}`,
            externalReference: orderReference,
            relatedDocId: orderId,
            metadata: {
                orderId: orderId,
                orderReference: orderReference,
                productName: productName,
                quantity: quantity,
                category: category,
                isGift: true,
                status: "pending",
                orderDate: new Date().toISOString(), // Or use orderTimestamp from client if passed
                giftSenderId: senderUid,
                giftSenderDisplayName: senderDisplayName,
                giftSenderPhotoURL: senderPhotoURL || `https://api.dicebear.com/7.x/identicon/svg?seed=${senderUid}`,
                giftExpiration: giftExpirationISO,
            }
        };
        batch.set(recipientTransactionRef, recipientTransactionRecData);

        const recipientUserUpdates = {
            giftReceivedCount: FieldValueAdmin.increment(1),
            lastGiftReceivedAt: FieldValueAdmin.serverTimestamp(),
            [`transactionStats.${TransactionTypeServer.Received_Gift}`]: FieldValueAdmin.increment(1),
        };
        // Using update with { merge: true } on set if the user doc might not exist and you want to create it.
        // Or ensure user doc exists. For now, update assumes it exists based on prior validation.
        batch.update(recipientUserRef, recipientUserUpdates); // This will fail if recipientUserRef does not exist.
        // Consider: batch.set(recipientUserRef, recipientUserUpdates, { merge: true }); if it might not exist.

        await batch.commit();
        console.log(`Successfully processed recipient side for gift order: ${orderId}`);
        res.status(200).json({ success: true, message: "Recipient gift data processed." });
    } catch (error) {
        console.error("Error in /api/gift-order-processed:", error);
        res.status(500).json({ error: "Failed to process recipient gift data.", details: error.message });
    }
});


// Endpoint 2: After a gift is successfully claimed by the recipient on the client
app.post('/api/gift-claimed-processed', authenticateToken, async (req, res) => {
    const { orderId, senderUid } = req.body;

    if (!orderId || !senderUid) {
        return res.status(400).json({ error: "Missing orderId or senderUid." });
    }

    try {
        const senderTransactionsQuery = dbAdmin.collection("users").doc(senderUid).collection("transactions")
            .where("relatedDocId", "==", orderId)
            .where("type", "==", TransactionTypeServer.Gifted); // Ensure this matches the type used by client

        const senderTransactionsSnap = await senderTransactionsQuery.get();

        if (senderTransactionsSnap.empty) {
            console.warn(`Sender's 'Gifted' transaction not found for order ${orderId}, sender ${senderUid} during claim processing.`);
            // Not necessarily an error for this endpoint, but good to log.
            return res.status(404).json({ success: false, message: "Sender's gifted transaction not found." });
        }

        const batch = dbAdmin.batch();
        senderTransactionsSnap.docs.forEach(doc => {
            batch.update(doc.ref, {
                'metadata.status': "claimed",
                lastUpdatedAt: FieldValueAdmin.serverTimestamp()
            });
        });

        await batch.commit();
        console.log(`Successfully updated sender's transaction for claimed gift order: ${orderId}`);
        res.status(200).json({ success: true, message: "Sender's transaction updated for claimed gift." });
    } catch (error) {
        console.error("Error in /api/gift-claimed-processed:", error);
        res.status(500).json({ error: "Failed to update sender's transaction for claimed gift.", details: error.message });
    }
});

// Endpoint 3: After a gift is successfully refunded by the sender on the client
app.post('/api/gift-refund-processed', authenticateToken, async (req, res) => {
    const { orderId, recipientUid } = req.body;

    if (!orderId || !recipientUid) {
        return res.status(400).json({ error: "Missing orderId or recipientUid." });
    }

    try {
        const receiverTransactionsQuery = dbAdmin.collection("users").doc(recipientUid).collection("transactions")
            .where("relatedDocId", "==", orderId)
            .where("type", "==", TransactionTypeServer.Received_Gift);

        const receiverTransactionsSnap = await receiverTransactionsQuery.get();

        if (receiverTransactionsSnap.empty) {
            console.warn(`Recipient's 'Received_Gift' transaction not found for order ${orderId}, recipient ${recipientUid} during refund processing.`);
            return res.status(404).json({ success: false, message: "Recipient's received gift transaction not found." });
        }

        const batch = dbAdmin.batch();
        receiverTransactionsSnap.docs.forEach(doc => {
            batch.update(doc.ref, {
                'metadata.status': "expired", // Or 'refunded_by_sender'
                status: "expired",
                lastUpdatedAt: FieldValueAdmin.serverTimestamp()
            });
        });

        await batch.commit();
        console.log(`Successfully updated recipient's transaction for refunded gift order: ${orderId}`);
        res.status(200).json({ success: true, message: "Recipient's transaction updated for refunded gift." });
    } catch (error) {
        console.error("Error in /api/gift-refund-processed:", error);
        res.status(500).json({ error: "Failed to update recipient's transaction for refunded gift.", details: error.message });
    }
});


// (Optional but Recommended) Endpoint 4: For voucher redemption count update
app.post('/api/voucher-redeemed', authenticateToken, async (req, res) => {
    const { voucherId, userId } = req.body; // userId is the user who redeemed it

    if (!voucherId || !userId) {
        return res.status(400).json({ error: "Missing voucherId or userId." });
    }

    try {
        const voucherRef = dbAdmin.collection("vouchers").doc(voucherId);
        await voucherRef.update({
            redemptionCount: FieldValueAdmin.increment(1),
            lastRedeemedBy: userId,
            lastRedeemedAt: FieldValueAdmin.serverTimestamp()
        });
        console.log(`Voucher ${voucherId} redeemed by ${userId}`);
        res.status(200).json({ success: true, message: "Voucher redeemed successfully." });
    } catch (error) {
        console.error("Error in /api/voucher-redeemed:", error);
        if (error.code === 5) { // Firestore 'NOT_FOUND' error
            return res.status(404).json({ error: "Voucher not found." });
        }
        res.status(500).json({ error: "Failed to update voucher.", details: error.message });
    }
});


const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Server running on port ${PORT}`));
