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

app.post('/api/claim-gift', authenticateToken, async (req, res) => {
    const { orderId } = req.body;
    const claimingUserId = req.user.uid;

    if (!orderId) {
        return res.status(400).json({ success: false, error: "Missing orderId." });
    }

    const orderRef = dbAdmin.collection("orders").doc(orderId);

    try {
        const claimResult = await dbAdmin.runTransaction(async (transaction) => {
            // --- ALL READS MUST COME FIRST ---
            const orderDoc = await transaction.get(orderRef); // READ 1

            if (!orderDoc.exists) {
                throw new Error("Gift order not found.");
            }
            const orderData = orderDoc.data(); // Data from READ 1

            // Query for recipient's "Received_Gift" transaction
            const receiverTransactionsQuery = dbAdmin.collection("users").doc(claimingUserId).collection("transactions")
                .where("relatedDocId", "==", orderId)
                .where("type", "==", TransactionTypeServer.Received_Gift);
            const receiverTransactionsSnap = await transaction.get(receiverTransactionsQuery); // READ 2

            // Query for sender's "Gifted" transaction (using orderData from READ 1)
            let senderGiftedTxSnap; // Declare here
            if (orderData.giftSenderId) { // Ensure giftSenderId exists before querying
                 const senderGiftedTxQuery = dbAdmin.collection("users").doc(orderData.giftSenderId).collection("transactions")
                    .where("relatedDocId", "==", orderId)
                    .where("type", "==", TransactionTypeServer.Gifted);
                 senderGiftedTxSnap = await transaction.get(senderGiftedTxQuery); // READ 3
            } else {
                console.error(`Critical: giftSenderId missing for orderId: ${orderId} (in transaction pre-fetch)`);
                // Handle this case - maybe throw an error or set senderGiftedTxSnap to indicate it's empty/not found
                // For now, if it's missing, the later check for senderGiftedTxSnap.empty will handle it.
            }

            // --- VALIDATIONS (using data from the reads above) ---
            if (!orderData.isGift) throw new Error("This is not a gift order.");
            if (orderData.status !== 'sent_gift') {
                if (orderData.status === 'claimed') throw new Error("This gift has already been claimed.");
                throw new Error("This gift is not in a claimable state.");
            }
            if (orderData.giftRecipientUid !== claimingUserId) throw new Error("This gift is not intended for you.");
            const giftExpirationDate = orderData.giftExpiration ? orderData.giftExpiration.toDate() : null;
            if (giftExpirationDate && giftExpirationDate.getTime() <= Date.now()) throw new Error("This gift has expired.");
            if (!orderData.giftSenderId) { // Redundant check but good for safety
                console.error(`Critical: giftSenderId missing for orderId: ${orderId}`);
                throw new Error("Gift sender information is missing.");
            }
            if (receiverTransactionsSnap.empty) {
                throw new Error("Your gift reception record is missing. Please contact support.");
            }
            const receiverGiftTransactionRef = receiverTransactionsSnap.docs[0].ref;
            // --- END OF VALIDATIONS ---


            // Product details
            const productNameForTx = orderData.productName || "Product";
            const productGroupNameForTx = orderData.productGroupName || "Category";
            const quantityForTx = orderData.quantity || 1;
            const originalGiftPriceForTx = (orderData.singleItemPrice || 0) * quantityForTx;

            // --- ALL WRITES COME AFTER ALL READS ---
            // 1. Update the original order document
            transaction.update(orderRef, {
                status: "claimed",
                claimedBy: claimingUserId,
                claimedAt: FieldValueAdmin.serverTimestamp(),
                lastUpdatedAt: FieldValueAdmin.serverTimestamp()
            });

            // 2. Create a new "Order" transaction for the recipient
            const recipientNewOrderTxRef = dbAdmin.collection("users").doc(claimingUserId).collection("transactions").doc();
            const recipientNewOrderTxData = { /* ... as before ... */
                timestamp: FieldValueAdmin.serverTimestamp(),
                type: TransactionTypeServer.Order,
                amount: 0,
                reference: recipientNewOrderTxRef.id,
                description: `Claimed gift: ${productNameForTx} from ${orderData.giftSenderDisplayName || 'a friend'}`,
                externalReference: orderData.referenceNumber,
                relatedDocId: orderId,
                metadata: {
                    orderId: orderId,
                    orderReference: orderData.referenceNumber,
                    productName: productNameForTx,
                    quantity: quantityForTx,
                    category: productGroupNameForTx,
                    isGift: true,
                    giftSenderId: orderData.giftSenderId,
                    giftSenderDisplayName: orderData.giftSenderDisplayName,
                    giftSenderPhotoURL: orderData.giftSenderPhotoURL || `https://api.dicebear.com/7.x/identicon/svg?seed=${orderData.giftSenderId}`,
                    status: "completed",
                    orderDate: new Date().toISOString(),
                    originalGiftPrice: originalGiftPriceForTx,
                    deliveryDetails: { userEmail: req.user.email, ...(orderData.deliveryDetails || {}) }
                }
            };
            transaction.set(recipientNewOrderTxRef, recipientNewOrderTxData);

            // 3. Update the receiver's original "Received_Gift" transaction
            transaction.update(receiverGiftTransactionRef, {
                'metadata.status': "claimed",
                status: "claimed",
                lastUpdatedAt: FieldValueAdmin.serverTimestamp()
            });

            // 4. Update recipient's user document
            const recipientUserRef = dbAdmin.collection("users").doc(claimingUserId);
            transaction.update(recipientUserRef, {
                orderCount: FieldValueAdmin.increment(1),
                lastOrderAt: FieldValueAdmin.serverTimestamp(),
                giftClaimedCount: FieldValueAdmin.increment(1),
                lastGiftClaimedAt: FieldValueAdmin.serverTimestamp(),
                [`transactionStats.${TransactionTypeServer.Order}`]: FieldValueAdmin.increment(1)
            });

            // 5. (Optional) Update sender's "Gifted" transaction status (using senderGiftedTxSnap from READ 3)
            if (senderGiftedTxSnap && !senderGiftedTxSnap.empty) {
                senderGiftedTxSnap.docs.forEach(doc => {
                    transaction.update(doc.ref, {
                        'metadata.status': "claimed",
                        lastUpdatedAt: FieldValueAdmin.serverTimestamp()
                    });
                });
            } else if (orderData.giftSenderId) { // Only warn if we expected to find it
                console.warn(`Sender's 'Gifted' transaction not found for order ${orderId} (sender: ${orderData.giftSenderId}) during server-side claim's write phase.`);
            }

            return { ...orderData, status: "claimed", claimedBy: claimingUserId, claimedAt: "PENDING_SERVER_TIME", lastUpdatedAt: "PENDING_SERVER_TIME", orderId: orderRef.id };
        });

        // Transaction successful
        // Manually construct the 'claimedAt' and 'lastUpdatedAt' for the response if needed,
        // or refetch the order document if the client absolutely needs the resolved server timestamps.
        // For simplicity, the client might just use the fact that the call was successful.
        const finalClaimResult = { ...claimResult };
        // If you returned "PENDING_SERVER_TIME", you might want to fetch the final doc
        // const updatedOrderDoc = await orderRef.get();
        // finalClaimResult.claimedAt = updatedOrderDoc.data().claimedAt;
        // finalClaimResult.lastUpdatedAt = updatedOrderDoc.data().lastUpdatedAt;


        res.status(200).json({
            success: true,
            message: "Gift claimed successfully.",
            claimedOrderData: finalClaimResult
        });

    } catch (error) {
        console.error(`SERVER ERROR in /api/claim-gift for order ${orderId}, user ${claimingUserId}:`, error);
        res.status(500).json({ success: false, error: error.message || "Failed to claim gift due to a server error." });
    }
});

app.post('/api/update-claimed-gift-delivery-details', authenticateToken, async (req, res) => {
    const { orderId, deliveryDetailsFromRecipient } = req.body;
    const claimingUserId = req.user.uid;

    if (!orderId || !deliveryDetailsFromRecipient) {
        return res.status(400).json({ success: false, error: "Missing orderId or delivery details." });
    }
    if (typeof deliveryDetailsFromRecipient !== 'object' || deliveryDetailsFromRecipient === null) {
        return res.status(400).json({ success: false, error: "Invalid delivery details format." });
    }

    try {
        const claimUpdateResult = await dbAdmin.runTransaction(async (transaction) => {
            const orderRef = dbAdmin.collection("orders").doc(orderId);
            const orderDoc = await transaction.get(orderRef);

            if (!orderDoc.exists) {
                throw new Error("Order not found. Cannot update delivery details.");
            }
            const orderData = orderDoc.data();

            if (orderData.status !== "claimed" || orderData.claimedBy !== claimingUserId) {
                throw new Error("This gift cannot be updated at this time or does not belong to you.");
            }

            const recipientOrderTxQuery = dbAdmin.collection("users").doc(claimingUserId)
                .collection("transactions")
                .where("relatedDocId", "==", orderId)
                .where("type", "==", TransactionTypeServer.Order)
                .limit(1);
            const recipientOrderTxSnap = await transaction.get(recipientOrderTxQuery);

            if (recipientOrderTxSnap.empty) {
                console.error(`Recipient's claimed order transaction (type: Order, relatedDocId: ${orderId}) not found for user ${claimingUserId}.`);
                throw new Error("Recipient's claimed order transaction record not found.");
            }
            const recipientOrderTxRef = recipientOrderTxSnap.docs[0].ref;

            transaction.update(recipientOrderTxRef, {
                'metadata.deliveryDetails': deliveryDetailsFromRecipient,
                lastUpdatedAt: FieldValueAdmin.serverTimestamp()
            });
            transaction.update(orderRef, { // Also update main order for admin/record keeping
                deliveryDetails: deliveryDetailsFromRecipient,
                lastUpdatedAt: FieldValueAdmin.serverTimestamp()
            });
            return { updatedOrderId: orderId, newDeliveryDetails: deliveryDetailsFromRecipient };
        });
        res.status(200).json({
            success: true,
            message: "Delivery details updated successfully for your claimed gift.",
            data: claimUpdateResult
        });
    } catch (error) {
        console.error(`SERVER ERROR in /api/update-claimed-gift-delivery-details for order ${orderId}:`, error);
        res.status(500).json({ success: false, error: error.message || "Failed to update delivery details." });
    }
});

app.post('/api/check-redemption-code', authenticateToken, async (req, res) => {
    const { code } = req.body;
    const userId = req.user.uid;

    if (!code) {
        return res.status(400).json({ success: false, error: "Redemption code is required." });
    }

    try {
        const rewardsQuery = dbAdmin.collection("rewards").where("code", "==", code.toUpperCase());
        const rewardsSnapshot = await rewardsQuery.get();

        if (rewardsSnapshot.empty) {
            return res.status(404).json({ success: false, error: "Invalid or expired redemption code." });
        }

        const rewardDoc = rewardsSnapshot.docs[0];
        const rewardData = rewardDoc.data();
        const rewardId = rewardDoc.id;

        // Check expiration
        if (rewardData.expirationDate) {
            const expirationDate = (rewardData.expirationDate.toDate) ? rewardData.expirationDate.toDate() : new Date(rewardData.expirationDate);
            if (new Date() > expirationDate) {
                return res.status(400).json({ success: false, error: "This code has expired." });
            }
        }

        // Check global redemption limit
        const redemptionCount = Number(rewardData.redemptionCount) || 0;
        const maxRedemptions = Number(rewardData.maxRedemptions);
        if (maxRedemptions > 0 && redemptionCount >= maxRedemptions) {
            return res.status(400).json({ success: false, error: "This code has reached its global redemption limit." });
        }

        // Check if user has already redeemed this specific reward (if applicable based on type)
        // For 'choices' and 'airdrop' type, a user typically redeems it once.
        // For 'form' or 'redemptionKey', it might depend on your specific logic.
        if (rewardData.type === 'choices' || rewardData.type === 'airdrop' || rewardData.type === 'form') {
            const userTransactionsRef = dbAdmin.collection("users").doc(userId).collection("transactions");
            const priorRedemptionQuery = userTransactionsRef
                .where("relatedDocId", "==", rewardId)
                .where("type", "in", [TransactionTypeServer.Receive, TransactionTypeServer.Redeemed]); // "Receive" for choices, "Redeemed" for airdrop/form
            const priorRedemptionSnapshot = await priorRedemptionQuery.get();

            if (!priorRedemptionSnapshot.empty) {
                return res.status(400).json({ success: false, error: "You have already redeemed/claimed this code." });
            }
        }

        // Return necessary reward data (excluding sensitive info if any)
        res.status(200).json({
            success: true,
            message: "Code is valid.",
            reward: {
                id: rewardId,
                title: rewardData.title,
                type: rewardData.type,
                value: rewardData.value, // Be cautious about exposing this if it's not meant for client display before claim
                imageUrl: rewardData.imageUrl,
                instructions: rewardData.instructions,
                formFields: rewardData.formFields, // For 'form' type
                RedemptionKeyHint: rewardData.RedemptionKeyHint, // For 'redemptionKey' type
                // Do NOT return the actual RedemptionKey here
                maxRedemptions: rewardData.maxRedemptions,
                redemptionCount: rewardData.redemptionCount
            }
        });

    } catch (error) {
        console.error("Error in /api/check-redemption-code:", error);
        res.status(500).json({ success: false, error: "Server error checking code. Please try again." });
    }
});


// Endpoint 2: Process Redemption (handles 'choices' type directly, initiates others)
app.post('/api/redeem-code', authenticateToken, async (req, res) => {
    const { code } = req.body; // Client sends the code they want to redeem
    const userId = req.user.uid;

    if (!code) {
        return res.status(400).json({ success: false, error: "Redemption code is required." });
    }

    try {
        const result = await dbAdmin.runTransaction(async (transaction) => {
            const rewardsQuery = dbAdmin.collection("rewards").where("code", "==", code.toUpperCase());
            const rewardsSnapshot = await transaction.get(rewardsQuery);

            if (rewardsSnapshot.empty) {
                throw new Error("Invalid or expired redemption code.");
            }

            const rewardDocRef = rewardsSnapshot.docs[0].ref;
            const rewardData = rewardsSnapshot.docs[0].data();
            const rewardId = rewardDocRef.id;

            // --- Re-validate reward (expiration, limits) within transaction ---
            if (rewardData.expirationDate) {
                const expirationDate = (rewardData.expirationDate.toDate) ? rewardData.expirationDate.toDate() : new Date(rewardData.expirationDate);
                if (new Date() > expirationDate) throw new Error("This code has expired.");
            }
            const redemptionCount = Number(rewardData.redemptionCount) || 0;
            const maxRedemptions = Number(rewardData.maxRedemptions);
            if (maxRedemptions > 0 && redemptionCount >= maxRedemptions) {
                throw new Error("This code has reached its global redemption limit.");
            }

            // --- Check user's prior redemption within transaction ---
            const userTransactionsRef = dbAdmin.collection("users").doc(userId).collection("transactions");
            // Adjust types based on what you check for "already redeemed" for each reward type
            let relevantTxTypes = [TransactionTypeServer.Receive, TransactionTypeServer.Redeemed];
            if (rewardData.type === 'redemptionKey') { // Example: Maybe redemptionKey can be used multiple times?
                // relevantTxTypes = []; // Or a specific type if one is logged
            }

            if (relevantTxTypes.length > 0) {
                const priorRedemptionQuery = userTransactionsRef
                    .where("relatedDocId", "==", rewardId)
                    .where("type", "in", relevantTxTypes);
                const priorRedemptionSnapshot = await transaction.get(priorRedemptionQuery);
                if (!priorRedemptionSnapshot.empty) {
                    throw new Error("You have already redeemed/claimed this code.");
                }
            }

            const userRef = dbAdmin.collection("users").doc(userId);
            const userDoc = await transaction.get(userRef);
            if (!userDoc.exists) {
                throw new Error("User data not found. Cannot process redemption.");
            }
            const userData = userDoc.data();

            // --- Perform actions based on reward type ---
            let transactionType, transactionAmount, transactionDescription, responseMessage;
            let userBalanceUpdate = {};
            let rewardUpdate = {
                redemptionCount: FieldValueAdmin.increment(1),
                lastRedeemedAt: FieldValueAdmin.serverTimestamp()
            };
            let additionalResponseData = {};

            if (rewardData.type === 'choices') {
                transactionType = TransactionTypeServer.Receive; // Using 'Receive' for balance addition
                transactionAmount = Number(rewardData.value) || 0;
                if (transactionAmount <= 0) throw new Error("Invalid reward value for 'choices' type.");

                transactionDescription = `Redeemed: ${rewardData.title || rewardData.code}`;
                responseMessage = `🎉 Code redeemed! +${transactionAmount.toLocaleString()} added to your balance.`;
                userBalanceUpdate = {
                    balance: FieldValueAdmin.increment(transactionAmount)
                };
                // For 'choices' type, the 'value' of the reward itself might be a pool that decreases.
                // If `rewardData.value` is the total available points in the reward itself that deplete:
                // rewardUpdate.value = FieldValueAdmin.increment(-transactionAmount); // If reward.value is a pool
                // Ensure rewardUpdate.value doesn't go below 0 if it's a pool.
                // For simplicity here, assuming rewardData.value is the fixed amount given per redemption.
            } else if (rewardData.type === 'airdrop') {
                transactionType = TransactionTypeServer.Redeemed; // A claim record, not direct value
                transactionAmount = 0; // Airdrop itself doesn't add balance directly
                transactionDescription = `Airdrop claimed: ${rewardData.title || rewardData.code}`;
                responseMessage = `Airdrop "${rewardData.title || rewardData.code}" claimed! Proceed to select your reward(s).`;
                additionalResponseData = { proceedTo: 'choicesModal', rewardValueForChoices: rewardData.value };
            } else if (rewardData.type === 'form') {
                transactionType = TransactionTypeServer.Redeemed; // Record of claim
                transactionAmount = 0; // Form itself doesn't add balance directly
                transactionDescription = `Form access code redeemed: ${rewardData.title || rewardData.code}`;
                responseMessage = `Code "${rewardData.title || rewardData.code}" accepted. Please fill the form.`;
                additionalResponseData = { proceedTo: 'formModal' };
            } else if (rewardData.type === 'redemptionKey') {
                transactionType = TransactionTypeServer.Redeemed; // Record of claim
                transactionAmount = 0;
                transactionDescription = `Key code input: ${rewardData.title || rewardData.code}`;
                responseMessage = `Code "${rewardData.title || rewardData.code}" is a special key. Enter it in the next step.`;
                additionalResponseData = { proceedTo: 'redemptionKeyModal' };
            } else {
                throw new Error(`Unsupported reward type: ${rewardData.type}`);
            }

            // Update reward document
            transaction.update(rewardDocRef, rewardUpdate);

            // Update user document
            const finalUserUpdates = {
                ...userBalanceUpdate,
                transactionCount: FieldValueAdmin.increment(1),
                lastTransaction: FieldValueAdmin.serverTimestamp(),
                [`transactionStats.${transactionType}`]: FieldValueAdmin.increment(1)
            };
            if (userData.firstTransaction === null && (userData.transactionCount === 0 || typeof userData.transactionCount === 'undefined')) {
                finalUserUpdates.firstTransaction = FieldValueAdmin.serverTimestamp();
            }
            transaction.update(userRef, finalUserUpdates);

            // Create transaction record for the user
            const newTransactionRef = userTransactionsRef.doc();
            transaction.set(newTransactionRef, {
                timestamp: FieldValueAdmin.serverTimestamp(),
                type: transactionType,
                amount: transactionAmount,
                reference: newTransactionRef.id,
                description: transactionDescription,
                externalReference: rewardData.code,
                relatedDocId: rewardId,
                metadata: {
                    rewardTitle: rewardData.title || "N/A",
                    rewardType: rewardData.type,
                    code: rewardData.code,
                    ...(rewardData.type === 'airdrop' && { airdropNominalValue: rewardData.value })
                }
            });

            const newBalance = (userData.balance || 0) + (userBalanceUpdate.balance ? transactionAmount : 0);

            return {
                message: responseMessage,
                newBalance: newBalance,
                transactionId: newTransactionRef.id,
                rewardType: rewardData.type,
                rewardDetails: { // Send back some reward details for client UI
                    id: rewardId,
                    title: rewardData.title,
                    value: rewardData.value, // Original value for airdrop choices, etc.
                    imageUrl: rewardData.imageUrl,
                    instructions: rewardData.instructions,
                    formFields: rewardData.formFields,
                    RedemptionKeyHint: rewardData.RedemptionKeyHint
                },
                ...additionalResponseData
            };
        }); // End of Firestore Transaction

        res.status(200).json({ success: true, ...result });

    } catch (error) {
        console.error("Error in /api/redeem-code:", error);
        res.status(500).json({ success: false, error: error.message || "Server error redeeming code." });
    }
});


const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Server running on port ${PORT}`));
