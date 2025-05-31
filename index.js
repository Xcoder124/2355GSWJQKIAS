const express = require('express');
const axios = require('axios');
const cors =require('cors');
const fs = require('fs');
const crypto = require('crypto');
const app = express();

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

const dbAdmin = admin.firestore();
const TimestampAdmin = admin.firestore.Timestamp;
const FieldValueAdmin = admin.firestore.FieldValue;

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
    } catch (err) {
        console.error('Error saving cache:', err);
    }
}, 60000);

app.use(cors());
app.use(express.json());

async function authenticateToken(req, res, next) {
    const authHeader = req.headers.authorization;
    if (!authHeader || !authHeader.startsWith('Bearer ')) {
        return res.status(403).send('Unauthorized: No token provided.');
    }
    const idToken = authHeader.split('Bearer ')[1];
    try {
        const decodedToken = await admin.auth().verifyIdToken(idToken);
        req.user = decodedToken;
        next();
    } catch (error) {
        console.error('Error verifying Firebase ID token:', error);
        res.status(403).send('Unauthorized: Invalid token.');
    }
}

const TransactionTypeServer = {
    Order: 'Order',
    Receive: 'Receive', // Matches 'Receive' in app.redemption.js
    Sent: 'Sent',
    Redeemed: 'Redeemed', // Matches 'redemption' (mapped to 'Redeemed') in app.redemption.js
    Gifted: 'Gifted',
    Received_Gift: 'Received Gift',
    Refund: 'Refund',
    BirthdayBonus: 'Birthday Bonus
};

function calculateServerFees(price) {
    if (price < 0) return 0;
    if (price >= 100 && price <= 10000) return 500;
    if (price >= 10100 && price <= 99900) return 1000;
    if (price >= 100000 && price <= 500000) return 3000;
    return 0;
}

async function fetchProductFromDB(productId) {
    const categoriesSnapshot = await dbAdmin.collection('Choices').get();
    for (const categoryDoc of categoriesSnapshot.docs) {
        const categoryData = categoryDoc.data();
        for (const groupName in categoryData) {
            if (categoryData[groupName] && typeof categoryData[groupName] === 'object' && categoryData[groupName].products) {
                if (categoryData[groupName].products[productId]) {
                    const product = categoryData[groupName].products[productId];
                    if (product.price === undefined || isNaN(parseFloat(product.price))) {
                        console.error(`Product ${productId} in ${categoryDoc.id}/${groupName} has invalid price.`);
                        return null;
                    }
                    return {
                        id: productId,
                        name: product.name,
                        price: parseFloat(product.price),
                        description: product.description,
                        image: product.image,
                        productGroupName: groupName,
                        mainCategoryName: categoryDoc.id,
                        isProductGroupUnavailable: (typeof categoryData[groupName].availability === 'string' && categoryData[groupName].availability.toLowerCase() === 'unavailable')
                    };
                }
            }
        }
    }
    console.warn(`Product with ID ${productId} not found across all categories.`);
    return null;
}

async function validateVoucherOnServer(voucherCode, productPrice, quantity, userId) {
    const result = { isValid: false, deduction: 0, message: '', appliedVoucherData: null };
    if (!voucherCode) {
        result.message = "No voucher code provided.";
        return result;
    }

    const vouchersRef = dbAdmin.collection("vouchers");
    const q = vouchersRef.where("code", "==", voucherCode.toUpperCase());
    const querySnapshot = await q.get();

    if (querySnapshot.empty) {
        result.message = 'Invalid voucher code.';
        return result;
    }

    const voucherDoc = querySnapshot.docs[0];
    const voucherData = { id: voucherDoc.id, ...voucherDoc.data() };

    const userSnap = await dbAdmin.collection("users").doc(userId).get();
    if (!userSnap.exists) {
        result.message = 'User not found for voucher validation.';
        return result;
    }
    const userData = userSnap.data();

    if (userData.redeemedVoucherIds && userData.redeemedVoucherIds.includes(voucherDoc.id)) {
        result.message = 'You have already redeemed this voucher.';
        return result;
    }

    const redemptionCount = Number(voucherData.redemptionCount) || 0;
    const maxRedemptions = Number(voucherData.maxRedemptions);
    if (typeof maxRedemptions === 'number' && !isNaN(maxRedemptions) && redemptionCount >= maxRedemptions) {
        result.message = 'This voucher has reached its maximum redemption limit.';
        return result;
    }

    if (voucherData.expirationDate && voucherData.expirationDate.toDate() < new Date()) {
        result.message = 'This voucher has expired.';
        return result;
    }

    if (voucherData.privacy && voucherData.privacy !== "global") {
        if (voucherData.privacy !== userData.email && voucherData.privacy !== userData.displayName) {
            result.message = 'This voucher is not applicable to your account.';
            return result;
        }
    }

    const orderSubtotal = productPrice * quantity;
    const currentOrderFees = calculateServerFees(productPrice);

    if (voucherData.type === "Discount Voucher") {
        if (voucherData.ordersAmount && quantity < voucherData.ordersAmount) {
            result.message = `You need at least ${voucherData.ordersAmount} of this item to use this voucher.`;
            return result;
        }
        if (voucherData.validPrice && orderSubtotal < voucherData.validPrice) {
            result.message = `Order subtotal must be at least ${voucherData.validPrice.toLocaleString()} for this voucher.`;
            return result;
        }
        result.deduction = parseFloat(voucherData.amount) || 0;
    } else if (voucherData.type === "Fee Voucher") {
        if (voucherData.validFee && currentOrderFees < voucherData.validFee) {
            result.message = `Order fees must be at least ${voucherData.validFee.toLocaleString()} for this voucher.`;
            return result;
        }
        result.deduction = Math.min((parseFloat(voucherData.amount) / 100) * currentOrderFees, currentOrderFees);
    } else {
        result.message = 'Unknown voucher type.';
        return result;
    }

    result.isValid = true;
    result.message = `${voucherData.type} applied successfully!`;
    result.appliedVoucherData = { ...voucherData };
    return result;
}

function generateOrderReference() {
    const ts = Date.now();
    const randomSuffix = Array(5).fill(null).map(() => Math.random().toString(36).charAt(2)).join('').toUpperCase();
    return `ORD-${ts}-${randomSuffix}`;
}

app.post('/api/create-order', authenticateToken, async (req, res) => {
    const senderUid = req.user.uid;
    const senderEmail = req.user.email;

    const {
        relatedProductDetails,
        orderSpecificDetails,
        voucherInfo,
        giftDetails,
        giftExpirationISO
    } = req.body;

    if (!relatedProductDetails || !relatedProductDetails.productId || typeof relatedProductDetails.quantity !== 'number' || relatedProductDetails.quantity < 1 || relatedProductDetails.quantity > 5) {
        return res.status(400).json({ success: false, error: "Invalid product details or quantity." });
    }
    if (!orderSpecificDetails) {
        return res.status(400).json({ success: false, error: "Missing order specific details." });
    }
    if (giftDetails && giftDetails.isGift) {
        if (!giftDetails.giftRecipientUid || !giftExpirationISO) {
            return res.status(400).json({ success: false, error: "Missing recipient UID or expiration for gift." });
        }
        if (giftDetails.giftRecipientUid === senderUid) {
            return res.status(400).json({ success: false, error: "You cannot gift an item to yourself." });
        }
    } else if (!orderSpecificDetails.userEmail) {
         return res.status(400).json({ success: false, error: "Delivery email is required for non-gift orders." });
    }


    try {
        const senderUserRef = dbAdmin.collection("users").doc(senderUid);
        const product = await fetchProductFromDB(relatedProductDetails.productId);

        if (!product) {
            return res.status(404).json({ success: false, error: "Product not found or is invalid." });
        }
        if (product.isProductGroupUnavailable) {
            return res.status(400).json({ success: false, error: `Product group '${product.productGroupName}' is currently unavailable.` });
        }
        if (product.price < 0 || product.price > 500000) {
            return res.status(400).json({ success: false, error: "Product price is invalid or exceeds limit." });
        }

        const serverCalculatedSingleItemPrice = product.price;
        const serverCalculatedFees = calculateServerFees(serverCalculatedSingleItemPrice);
        const quantity = (giftDetails && giftDetails.isGift) ? 1 : relatedProductDetails.quantity;

        let serverVoucherDeduction = 0;
        let validatedVoucherData = null;
        if (voucherInfo && voucherInfo.code && !(giftDetails && giftDetails.isGift)) {
            const voucherValidationResult = await validateVoucherOnServer(voucherInfo.code, serverCalculatedSingleItemPrice, quantity, senderUid);
            if (voucherValidationResult.isValid) {
                serverVoucherDeduction = voucherValidationResult.deduction;
                validatedVoucherData = voucherValidationResult.appliedVoucherData;
            } else {
                return res.status(400).json({ success: false, error: `Voucher Error: ${voucherValidationResult.message}` });
            }
        }

        const finalAmountPaid = (serverCalculatedSingleItemPrice * quantity) + serverCalculatedFees - serverVoucherDeduction;
        if (finalAmountPaid < 0) {
            return res.status(400).json({ success: false, error: "Total amount is invalid after deductions." });
        }

        let recipientDataSnapshot = null;
        let recipientPhotoURL = null; // Variable to store recipient's photoURL
        if (giftDetails && giftDetails.isGift) {
            recipientDataSnapshot = await dbAdmin.collection("users").doc(giftDetails.giftRecipientUid).get();
            if (!recipientDataSnapshot.exists) {
                return res.status(404).json({ success: false, error: "Gift recipient user not found." });
            }
            // Get the recipient's photoURL
            recipientPhotoURL = recipientDataSnapshot.data().photoURL || null;
        }

        const orderReference = generateOrderReference();
        const newOrderRef = dbAdmin.collection("orders").doc();

        const transactionResult = await dbAdmin.runTransaction(async (transaction) => {
            const senderDocSnap = await transaction.get(senderUserRef);
            if (!senderDocSnap.exists) throw new Error("Sender user data not found during transaction.");
            const senderData = senderDocSnap.data();
            const senderCurrentBalance = Number(senderData.balance) || 0;

            if (senderCurrentBalance < finalAmountPaid) {
                throw new Error("Insufficient balance (checked during transaction).");
            }

            const orderTimestamp = FieldValueAdmin.serverTimestamp();
            let giftExpirationTimestamp = null;
            if (giftDetails && giftDetails.isGift && giftExpirationISO) {
                try {
                    giftExpirationTimestamp = TimestampAdmin.fromDate(new Date(giftExpirationISO));
                } catch (e) { throw new Error("Invalid gift expiration date format."); }
            }

            const orderData = {
                orderId: newOrderRef.id, userId: senderUid, userEmail: senderEmail,
                referenceNumber: orderReference, productName: product.name, productId: product.id,
                productGroupName: product.productGroupName, mainCategoryName: product.mainCategoryName,
                quantity: quantity, singleItemPrice: serverCalculatedSingleItemPrice, totalFees: serverCalculatedFees,
                voucherCode: validatedVoucherData?.code || null,
                voucherDeduction: serverVoucherDeduction,
                voucherId: validatedVoucherData?.id || null,
                finalAmountPaid: finalAmountPaid, orderTimestamp: orderTimestamp,
                status: (giftDetails && giftDetails.isGift) ? "sent_gift" : "pending",
                deliveryDetails: orderSpecificDetails,
                isGift: !!(giftDetails && giftDetails.isGift),
                giftTo: giftDetails?.giftRecipientAccountID || null,
                giftToFullName: giftDetails && giftDetails.isGift && recipientDataSnapshot ? (recipientDataSnapshot.data().displayName || giftDetails.giftRecipientAccountID) : null,
                giftToEmail: giftDetails && giftDetails.isGift && recipientDataSnapshot ? recipientDataSnapshot.data().email : null,
                giftActualRecipientPhotoURL: recipientPhotoURL, // MODIFICATION: Added recipient's photo URL
                giftRecipientUid: giftDetails?.giftRecipientUid || null,
                giftSenderId: (giftDetails && giftDetails.isGift) ? senderUid : null,
                giftSenderDisplayName: (giftDetails && giftDetails.isGift) ? (senderData.displayName || senderEmail.split('@')[0]) : null,
                giftSenderPhotoURL: (giftDetails && giftDetails.isGift) ? (senderData.photoURL || `https://api.dicebear.com/7.x/identicon/svg?seed=${senderUid}`) : null,
                giftExpiration: giftExpirationTimestamp,
                lastUpdatedAt: FieldValueAdmin.serverTimestamp()
            };
            transaction.set(newOrderRef, orderData);

            const senderTransactionRef = senderUserRef.collection("transactions").doc();
            const senderTransactionType = (giftDetails && giftDetails.isGift) ? TransactionTypeServer.Gifted : TransactionTypeServer.Order;
            const senderTransactionDesc = (giftDetails && giftDetails.isGift)
                ? `Gifted: ${product.name} to ${orderData.giftToFullName || orderData.giftTo}`
                : `Order: ${product.name}`;
            const senderTransactionData = {
                timestamp: FieldValueAdmin.serverTimestamp(), type: senderTransactionType, amount: -Math.abs(finalAmountPaid),
                reference: senderTransactionRef.id, description: senderTransactionDesc, externalReference: orderReference,
                relatedDocId: newOrderRef.id,
                metadata: {
                    orderId: newOrderRef.id, orderReference: orderReference, productName: product.name, quantity: quantity,
                    category: product.productGroupName, isGift: orderData.isGift, status: (giftDetails && giftDetails.isGift) ? "completed" : "pending",
                    orderDate: new Date(Date.now() - new Date().getTimezoneOffset() * 60000).toISOString(),
                    ...(orderData.isGift && { giftTo: orderData.giftTo, giftToFullName: orderData.giftToFullName, giftToEmail: orderData.giftToEmail, giftExpiration: giftExpirationISO }),
                    ...(validatedVoucherData && !orderData.isGift && { voucherCode: validatedVoucherData.code, voucherDeduction: serverVoucherDeduction, voucherId: validatedVoucherData.id })
                }
            };
            transaction.set(senderTransactionRef, senderTransactionData);

            const senderUserUpdates = {
                balance: FieldValueAdmin.increment(-Math.abs(finalAmountPaid)),
                orderCount: FieldValueAdmin.increment(1), lastTransaction: FieldValueAdmin.serverTimestamp(),
                [`transactionStats.${senderTransactionType}`]: FieldValueAdmin.increment(1)
            };
            if (!senderData.firstTransaction) senderUserUpdates.firstTransaction = FieldValueAdmin.serverTimestamp();
            if (validatedVoucherData && !orderData.isGift) senderUserUpdates.redeemedVoucherIds = FieldValueAdmin.arrayUnion(validatedVoucherData.id);
            transaction.update(senderUserRef, senderUserUpdates);

            if (orderData.isGift && recipientDataSnapshot) {
                const recipientUserRefInternal = dbAdmin.collection("users").doc(orderData.giftRecipientUid);
                const recipientTransactionRef = recipientUserRefInternal.collection("transactions").doc();
                const recipientTransactionData = {
                    timestamp: FieldValueAdmin.serverTimestamp(), type: TransactionTypeServer.Received_Gift, amount: 0,
                    reference: recipientTransactionRef.id, description: `Received a gift: ${product.name} from ${orderData.giftSenderDisplayName}`,
                    externalReference: orderReference, relatedDocId: newOrderRef.id,
                    metadata: {
                        orderId: newOrderRef.id, orderReference: orderReference, productName: product.name, quantity: quantity,
                        category: product.productGroupName, isGift: true, status: "pending",
                        orderDate: new Date(Date.now() - new Date().getTimezoneOffset() * 60000).toISOString(),
                        giftSenderId: senderUid, giftSenderDisplayName: orderData.giftSenderDisplayName, giftSenderPhotoURL: orderData.giftSenderPhotoURL,
                        giftExpiration: giftExpirationISO
                    }
                };
                transaction.set(recipientTransactionRef, recipientTransactionData);
                transaction.update(recipientUserRefInternal, {
                    giftReceivedCount: FieldValueAdmin.increment(1),
                    lastGiftReceivedAt: FieldValueAdmin.serverTimestamp(),
                    [`transactionStats.${TransactionTypeServer.Received_Gift}`]: FieldValueAdmin.increment(1)
                });
            }

            if (validatedVoucherData && !orderData.isGift) {
                transaction.update(dbAdmin.collection("vouchers").doc(validatedVoucherData.id), {
                    redemptionCount: FieldValueAdmin.increment(1),
                    lastRedeemedBy: senderUid,
                    lastRedeemedAt: FieldValueAdmin.serverTimestamp()
                });
            }
            return { orderId: newOrderRef.id, orderReference: orderReference, finalAmountPaid: finalAmountPaid };
        });

        res.status(201).json({
            success: true, message: "Order processed successfully!",
            orderId: transactionResult.orderId, orderReference: transactionResult.orderReference,
            finalAmountPaid: transactionResult.finalAmountPaid
        });

    } catch (error) {
        console.error(`SERVER ERROR /api/create-order for user ${senderUid}:`, error);
        res.status(500).json({ success: false, error: error.message || "Failed to create order." });
    }
});

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
        if (username) {
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

app.post('/api/gift-order-processed', authenticateToken, async (req, res) => {
    const {
        orderId, recipientUid, senderUid, senderDisplayName, senderPhotoURL,
        productName, quantity, category, orderReference, giftExpirationISO
    } = req.body;

    if (!orderId || !recipientUid || !senderUid || !productName || !quantity || !category || !orderReference || !giftExpirationISO) {
        return res.status(400).json({ error: "Missing required fields for processing gift order." });
    }
    try {
        const batch = dbAdmin.batch();
        const recipientTransactionRef = dbAdmin.collection("users").doc(recipientUid).collection("transactions").doc();
        const recipientUserRef = dbAdmin.collection("users").doc(recipientUid);

        const recipientTransactionRecData = {
            timestamp: FieldValueAdmin.serverTimestamp(), type: TransactionTypeServer.Received_Gift, amount: 0,
            reference: recipientTransactionRef.id, description: `Received a gift: ${productName} from ${senderDisplayName || 'a friend'}`,
            externalReference: orderReference, relatedDocId: orderId,
            metadata: {
                orderId: orderId, orderReference: orderReference, productName: productName, quantity: quantity, category: category,
                isGift: true, status: "pending", orderDate: new Date(Date.now() - new Date().getTimezoneOffset() * 60000).toISOString(),
                giftSenderId: senderUid, giftSenderDisplayName: senderDisplayName,
                giftSenderPhotoURL: senderPhotoURL || `https://api.dicebear.com/7.x/identicon/svg?seed=${senderUid}`,
                giftExpiration: giftExpirationISO,
            }
        };
        batch.set(recipientTransactionRef, recipientTransactionRecData);

        const recipientUserUpdates = {
            giftReceivedCount: FieldValueAdmin.increment(1), lastGiftReceivedAt: FieldValueAdmin.serverTimestamp(),
            [`transactionStats.${TransactionTypeServer.Received_Gift}`]: FieldValueAdmin.increment(1),
        };
        batch.update(recipientUserRef, recipientUserUpdates);
        await batch.commit();
        res.status(200).json({ success: true, message: "Recipient gift data processed." });
    } catch (error) {
        console.error("Error in /api/gift-order-processed:", error);
        res.status(500).json({ error: "Failed to process recipient gift data.", details: error.message });
    }
});

app.post('/api/voucher-redeemed', authenticateToken, async (req, res) => {
    const { voucherId, userId } = req.body;
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
        res.status(200).json({ success: true, message: "Voucher redeemed successfully." });
    } catch (error) {
        console.error("Error in /api/voucher-redeemed:", error);
        if (error.code === 5) {
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
        const orderDoc = await orderRef.get();

        if (!orderDoc.exists) {
            return res.status(404).json({ success: false, error: "Gift order not found." });
        }

        const orderData = orderDoc.data();
        const giftOrderDataForClient = { ...orderData, orderId: orderDoc.id }; 

        if (!giftOrderDataForClient.isGift) {
            return res.status(400).json({ success: false, error: "This is not a gift order." });
        }
        if (giftOrderDataForClient.status !== 'sent_gift') {
            if (giftOrderDataForClient.status === 'claimed') {
                return res.status(400).json({ success: false, error: "This gift has already been claimed." });
            }
            return res.status(400).json({ success: false, error: `This gift is not in a claimable state (current status: ${giftOrderDataForClient.status}).` });
        }
        if (giftOrderDataForClient.giftRecipientUid !== claimingUserId) {
            return res.status(403).json({ success: false, error: "This gift is not intended for you." });
        }
        const giftExpirationDate = giftOrderDataForClient.giftExpiration ? giftOrderDataForClient.giftExpiration.toDate() : null;
        if (giftExpirationDate && giftExpirationDate.getTime() <= Date.now()) {
            return res.status(400).json({ success: false, error: "This gift has expired." });
        }
        if (!giftOrderDataForClient.giftSenderId) {
            return res.status(500).json({ success: false, error: "Gift sender information is missing from the order." });
        }
        
        if (!giftOrderDataForClient.productId || typeof giftOrderDataForClient.singleItemPrice === 'undefined' || typeof giftOrderDataForClient.quantity === 'undefined') {
            console.error("Critical data (productId, singleItemPrice, or quantity) missing from orderData for orderId:", orderId);
            return res.status(500).json({ success: false, error: "Internal server error: Essential product information missing from gift order data." });
        }


        res.status(200).json({
            success: true,
            message: "Gift is valid, proceed on providing your in-game details...",
            claimedOrderData: giftOrderDataForClient
        });

    } catch (error) {
        console.error(`SERVER ERROR /api/claim-gift (validation phase) for order ${orderId}, user ${claimingUserId}:`, error);
        res.status(500).json({ success: false, error: error.message || "Failed to validate gift for claiming." });
    }
});

app.post('/api/update-claimed-gift-delivery-details', authenticateToken, async (req, res) => {
    const { orderId, deliveryDetailsFromRecipient } = req.body;
    const claimingUserId = req.user.uid;

    if (!orderId || !deliveryDetailsFromRecipient || typeof deliveryDetailsFromRecipient !== 'object') {
        return res.status(400).json({ success: false, error: "Missing or invalid orderId or delivery details." });
    }
    try {
        const claimUpdateResult = await dbAdmin.runTransaction(async (transaction) => {
            const orderRef = dbAdmin.collection("orders").doc(orderId);
            const orderDoc = await transaction.get(orderRef);
            if (!orderDoc.exists) throw new Error("Order not found.");
            const orderData = orderDoc.data();

            if (orderData.status !== "claimed" || orderData.claimedBy !== claimingUserId) {
                throw new Error("Gift cannot be updated or does not belong to you.");
            }

            const recipientOrderTxQuery = dbAdmin.collection("users").doc(claimingUserId).collection("transactions")
                .where("relatedDocId", "==", orderId).where("type", "==", TransactionTypeServer.Order).limit(1);
            const recipientOrderTxSnap = await transaction.get(recipientOrderTxQuery);
            if (recipientOrderTxSnap.empty) throw new Error("Recipient's claimed order transaction not found.");

            const recipientOrderTxRef = recipientOrderTxSnap.docs[0].ref;
            transaction.update(recipientOrderTxRef, {
                'metadata.deliveryDetails': deliveryDetailsFromRecipient, lastUpdatedAt: FieldValueAdmin.serverTimestamp()
            });
            transaction.update(orderRef, {
                deliveryDetails: deliveryDetailsFromRecipient, lastUpdatedAt: FieldValueAdmin.serverTimestamp()
            });
            return { updatedOrderId: orderId, newDeliveryDetails: deliveryDetailsFromRecipient };
        });
        res.status(200).json({ success: true, message: "Delivery details updated.", data: claimUpdateResult });
    } catch (error) {
        console.error(`SERVER ERROR /api/update-claimed-gift-delivery-details for order ${orderId}:`, error);
        res.status(500).json({ success: false, error: error.message || "Failed to update delivery details." });
    }
});

// START: New Code Redemption Endpoints
app.post('/api/check-redemption-code', authenticateToken, async (req, res) => {
    const { code } = req.body;
    const userId = req.user.uid;

    if (!code) return res.status(400).json({ success: false, error: "Redemption code is required." });

    try {
        const rewardsQuery = dbAdmin.collection("rewards").where("code", "==", code.toUpperCase());
        const rewardsSnapshot = await rewardsQuery.get();

        if (rewardsSnapshot.empty) return res.status(404).json({ success: false, error: "Invalid or expired redemption code." });

        const rewardDoc = rewardsSnapshot.docs[0];
        const rewardData = rewardDoc.data();
        const rewardId = rewardDoc.id;

        if (rewardData.expirationDate) {
            const expirationDate = (rewardData.expirationDate.toDate) ? rewardData.expirationDate.toDate() : new Date(rewardData.expirationDate);
            if (new Date() > expirationDate) return res.status(400).json({ success: false, error: "This code has expired." });
        }

        const redemptionCount = Number(rewardData.redemptionCount) || 0;
        const maxRedemptions = Number(rewardData.maxRedemptions);
        if (maxRedemptions > 0 && redemptionCount >= maxRedemptions) {
            return res.status(400).json({ success: false, error: "This code has reached its global redemption limit." });
        }

        // Check if user has already redeemed/claimed this specific reward
        // This logic needs to be consistent with how app.redemption.js determines "already redeemed"
        // For 'choices', it checks for 'Receive' type. For 'airdrop', it checks for 'Redeemed' type.
        // 'form' and 'redemptionKey' might also need similar checks if they are one-time claims per user.
        let relevantTxTypes = [];
        if (rewardData.type === 'choices') relevantTxTypes.push(TransactionTypeServer.Receive);
        if (rewardData.type === 'airdrop' || rewardData.type === 'form' || rewardData.type === 'redemptionKey') {
            // Assuming 'redemptionKey' also logs a 'Redeemed' transaction upon successful key entry
            relevantTxTypes.push(TransactionTypeServer.Redeemed);
        }

        if (relevantTxTypes.length > 0) {
            const userTransactionsRef = dbAdmin.collection("users").doc(userId).collection("transactions");
            const priorRedemptionQuery = userTransactionsRef
                .where("relatedDocId", "==", rewardId)
                .where("type", "in", relevantTxTypes);
            const priorRedemptionSnapshot = await priorRedemptionQuery.get();
            if (!priorRedemptionSnapshot.empty) return res.status(400).json({ success: false, error: "You have already redeemed/claimed this code." });
        }
        
        // Exclude sensitive data like RedemptionKey from this initial check response
        const { RedemptionKey, ...safeRewardData } = rewardData;

        res.status(200).json({
            success: true, message: "Code is valid.",
            reward: {
                id: rewardId,
                ...safeRewardData
                // Ensure all necessary fields for the client UI are here
                // title, type, value, imageUrl, instructions, formFields, RedemptionKeyHint, maxRedemptions, redemptionCount
            }
        });
    } catch (error) {
        console.error("Error in /api/check-redemption-code:", error);
        res.status(500).json({ success: false, error: "Server error checking code." });
    }
});

app.post('/api/redeem-code', authenticateToken, async (req, res) => {
    const { code, formData, enteredRedemptionKey } = req.body; // formData for 'form', enteredRedemptionKey for 'redemptionKey'
    const userId = req.user.uid;

    if (!code) return res.status(400).json({ success: false, error: "Redemption code is required." });

    try {
        const result = await dbAdmin.runTransaction(async (transaction) => {
            const rewardsQuery = dbAdmin.collection("rewards").where("code", "==", code.toUpperCase());
            const rewardsSnapshot = await transaction.get(rewardsQuery);
            if (rewardsSnapshot.empty) throw new Error("Invalid or expired redemption code.");

            const rewardDocRef = rewardsSnapshot.docs[0].ref;
            const rewardData = rewardsSnapshot.docs[0].data();
            const rewardId = rewardDocRef.id;

            // === Start: Validations within transaction ===
            if (rewardData.expirationDate) {
                const expirationDate = (rewardData.expirationDate.toDate) ? rewardData.expirationDate.toDate() : new Date(rewardData.expirationDate);
                if (new Date() > expirationDate) throw new Error("This code has expired.");
            }
            const redemptionCount = Number(rewardData.redemptionCount) || 0;
            const maxRedemptions = Number(rewardData.maxRedemptions);
            if (maxRedemptions > 0 && redemptionCount >= maxRedemptions) {
                throw new Error("This code has reached its global redemption limit.");
            }

            const userTransactionsRef = dbAdmin.collection("users").doc(userId).collection("transactions");
            let relevantTxTypes = [];
            if (rewardData.type === 'choices') relevantTxTypes.push(TransactionTypeServer.Receive);
            if (rewardData.type === 'airdrop' || rewardData.type === 'form' || rewardData.type === 'redemptionKey') {
                relevantTxTypes.push(TransactionTypeServer.Redeemed);
            }

            if (relevantTxTypes.length > 0) {
                const priorRedemptionQuery = userTransactionsRef
                    .where("relatedDocId", "==", rewardId)
                    .where("type", "in", relevantTxTypes);
                const priorRedemptionSnapshot = await transaction.get(priorRedemptionQuery);
                if (!priorRedemptionSnapshot.empty) throw new Error("You have already redeemed/claimed this code.");
            }
            // === End: Validations within transaction ===

            const userRef = dbAdmin.collection("users").doc(userId);
            const userDoc = await transaction.get(userRef);
            if (!userDoc.exists) throw new Error("User data not found.");
            const userData = userDoc.data();

            let transactionType, transactionAmount, transactionDescription, responseMessage;
            let userBalanceUpdate = {}; // Stores balance increment/decrement
            let rewardUpdate = {
                redemptionCount: FieldValueAdmin.increment(1),
                lastRedeemedAt: FieldValueAdmin.serverTimestamp(),
                lastRedeemedBy: userId // Good to track who redeemed it last
            };
            let additionalResponseData = {}; // For client-side conditional logic (e.g., show modal)

            // Handle specific reward types
            if (rewardData.type === 'choices') {
                transactionType = TransactionTypeServer.Redeemed;
                transactionAmount = Number(rewardData.value) || 0;
                if (transactionAmount <= 0) throw new Error("Invalid reward value for 'choices' type.");
                transactionDescription = `Redeemed: ${rewardData.title || rewardData.code}`;
                responseMessage = `Code redeemed! +${transactionAmount.toLocaleString()} added to your balance.`;
                userBalanceUpdate = { balance: FieldValueAdmin.increment(transactionAmount) };
            } else if (rewardData.type === 'airdrop') {
                transactionType = TransactionTypeServer.Redeemed;
                transactionAmount = 0; // Airdrop claim itself doesn't add value here
                transactionDescription = `Airdrop claimed: ${rewardData.title || rewardData.code}`;
                responseMessage = `Airdrop "${rewardData.title || rewardData.code}" claimed! Proceed to select your reward(s).`;
                // Client will likely need to know the airdrop's value to manage product selection logic
                additionalResponseData = { proceedTo: 'choicesModal', rewardValueForChoices: rewardData.value };
            } else if (rewardData.type === 'form') {
                if (!formData) throw new Error("Form data is required for this redemption type.");
                // Save formData to a "submissions" collection
                const submissionRef = dbAdmin.collection("submissions").doc(); // Auto-generate ID
                transaction.set(submissionRef, {
                    ...formData,
                    rewardId: rewardId,
                    rewardCode: rewardData.code,
                    userId: userId,
                    userEmail: userData.email, // Good to have for context
                    submittedAt: FieldValueAdmin.serverTimestamp()
                });
                transactionType = TransactionTypeServer.Redeemed;
                transactionAmount = 0; // Or a nominal value if applicable
                transactionDescription = `Form submitted for: ${rewardData.title || rewardData.code}`;
                responseMessage = `Form for "${rewardData.title || rewardData.code}" submitted successfully.`;
                additionalResponseData = { submissionId: submissionRef.id };
            } else if (rewardData.type === 'redemptionKey') {
                if (!rewardData.RedemptionKey) throw new Error("Reward is misconfigured: Missing RedemptionKey.");
                if (enteredRedemptionKey !== rewardData.RedemptionKey) throw new Error("Incorrect redemption key entered.");

                transactionType = TransactionTypeServer.Redeemed;
                transactionAmount = 0; // Or nominal if key itself has value
                transactionDescription = `Key code redeemed: ${rewardData.title || rewardData.code}`;
                responseMessage = `Code "${rewardData.title || rewardData.code}" and key accepted!`;
                // Include secret message if any, client will handle display
                if (rewardData.secretMessage) additionalResponseData.secretMessage = rewardData.secretMessage;
            } else {
                throw new Error(`Unsupported reward type: ${rewardData.type}`);
            }

            // Update reward document
            transaction.update(rewardDocRef, rewardUpdate);

            // Prepare user document updates
            const finalUserUpdates = {
                ...userBalanceUpdate, // This applies FieldValueAdmin.increment correctly
                transactionCount: FieldValueAdmin.increment(1),
                lastTransaction: FieldValueAdmin.serverTimestamp(),
                [`transactionStats.${transactionType}`]: FieldValueAdmin.increment(1)
            };
            if (userData.firstTransaction === null || typeof userData.transactionCount === 'undefined' || userData.transactionCount === 0 ) {
                 finalUserUpdates.firstTransaction = FieldValueAdmin.serverTimestamp();
            }
            transaction.update(userRef, finalUserUpdates);

            // Create new transaction document for the user
            const newTransactionRef = userTransactionsRef.doc(); // Define ref
            transaction.set(newTransactionRef, {
                timestamp: FieldValueAdmin.serverTimestamp(),
                type: transactionType,
                amount: transactionAmount,
                reference: newTransactionRef.id,
                description: transactionDescription,
                externalReference: rewardData.code, // The code user entered
                relatedDocId: rewardId, // The ID of the reward document
                metadata: { // Add any useful metadata
                    rewardTitle: rewardData.title || "N/A",
                    rewardType: rewardData.type,
                    code: rewardData.code,
                    ...(rewardData.type === 'airdrop' && { airdropNominalValue: rewardData.value })
                    // For 'form', could add submissionId here if needed.
                }
            });
            
            const newBalance = (userData.balance || 0) + (userBalanceUpdate.balance ? transactionAmount : 0);

            return {
                message: responseMessage,
                newBalance: newBalance, // Calculate new balance for response
                transactionId: newTransactionRef.id,
                rewardType: rewardData.type, // Useful for client to decide next steps
                rewardDetails: { // Send back some reward details for UI update
                    id: rewardId,
                    title: rewardData.title,
                    value: rewardData.value, // Current value after potential deductions if any (though usually not for redemption)
                    imageUrl: rewardData.imageUrl,
                    instructions: rewardData.instructions,
                    // Omit sensitive fields like RedemptionKey
                },
                ...additionalResponseData // e.g., proceedTo, secretMessage
            };
        });
        res.status(200).json({ success: true, ...result });
    } catch (error) {
        console.error("Error in /api/redeem-code:", error);
        res.status(500).json({ success: false, error: error.message || "Server error redeeming code." });
    }
});
// END: New Code Redemption Endpoints


app.post('/api/process-gift-refund', authenticateToken, async (req, res) => {
    const { orderId, senderTxId } = req.body;
    const senderUid = req.user.uid;

    if (!orderId || !senderTxId) {
        return res.status(400).json({ success: false, error: "Missing orderId or senderTxId." });
    }

    const orderRef = dbAdmin.collection("orders").doc(orderId);
    const senderUserRef = dbAdmin.collection("users").doc(senderUid);
    const senderGiftedTxRef = senderUserRef.collection("transactions").doc(senderTxId);

    try {
        const refundResult = await dbAdmin.runTransaction(async (transaction) => {
            // --- PHASE 1: ALL READS ---
            const orderDoc = await transaction.get(orderRef);
            if (!orderDoc.exists) throw new Error("Gift order not found.");
            const orderData = orderDoc.data();

            const senderDoc = await transaction.get(senderUserRef);
            if (!senderDoc.exists) throw new Error("Sender user data not found.");
            // const senderData = senderDoc.data(); // Data can be extracted if needed for validations

            const senderGiftedTxDoc = await transaction.get(senderGiftedTxRef);
            if (!senderGiftedTxDoc.exists) throw new Error("Sender's original 'Gifted' transaction not found.");

            // Perform validations using data from reads before any other reads that depend on this data
            if (!orderData.isGift) throw new Error("This order is not a gift and cannot be refunded through this process.");
            if (orderData.userId !== senderUid) throw new Error("You are not authorized to refund this gift.");
            if (orderData.status !== 'sent_gift' && orderData.status !== 'pending') {
                if (orderData.status === 'refunded') throw new Error("This gift has already been refunded.");
                if (orderData.status === 'claimed') throw new Error("This gift has been claimed and cannot be refunded.");
                throw new Error(`This gift cannot be refunded due to its current status: ${orderData.status}.`);
            }
            const giftExpirationDate = orderData.giftExpiration ? orderData.giftExpiration.toDate() : null;
            if (!giftExpirationDate || giftExpirationDate.getTime() > Date.now()) {
                throw new Error("This gift has not yet expired and cannot be refunded.");
            }
            if (orderData.claimedBy) throw new Error("This gift has already been claimed.");
            if (!orderData.giftRecipientUid) throw new Error("Recipient UID is missing in order data, cannot process refund fully.");

            // Last set of reads: Recipient's "Received_Gift" transaction
            // This read depends on orderData.giftRecipientUid, which was read above.
            let recipientGiftTxSnapshot = null; // Initialize
            const recipientUserRef = dbAdmin.collection("users").doc(orderData.giftRecipientUid);
            const recipientGiftTxQuery = recipientUserRef.collection("transactions")
                .where("relatedDocId", "==", orderId)
                .where("type", "==", TransactionTypeServer.Received_Gift)
                .limit(1);
            recipientGiftTxSnapshot = await transaction.get(recipientGiftTxQuery); // This must be the last read operation

            // --- PHASE 2: ALL WRITES ---
            const refundAmount = (orderData.singleItemPrice || 0) * (orderData.quantity || 1);
            if (refundAmount <= 0) { 
                console.error("Calculated refundAmount is zero or negative for order:", orderId, "Price:", orderData.singleItemPrice, "Qty:", orderData.quantity);
                throw new Error("Invalid refund amount calculated (must be greater than 0 for a gift refund).");
            }

            // 1. Update the main order document
            transaction.update(orderRef, {
                status: "refunded",
                refundedBy: senderUid,
                refundedAt: FieldValueAdmin.serverTimestamp(),
                lastUpdatedAt: FieldValueAdmin.serverTimestamp()
            });

            // 2. Update sender's original "Gifted" transaction metadata
            transaction.update(senderGiftedTxRef, {
                'metadata.status': "refunded",
                lastUpdatedAt: FieldValueAdmin.serverTimestamp()
            });

            // 3. Update sender's user document (balance, stats)
            const senderUserUpdates = {
                balance: FieldValueAdmin.increment(refundAmount),
                lastTransaction: FieldValueAdmin.serverTimestamp(),
                [`transactionStats.${TransactionTypeServer.Refund}`]: FieldValueAdmin.increment(1)
            };
            transaction.update(senderUserRef, senderUserUpdates);

            // 4. Create a new "Refund" transaction for the sender
            const newRefundTxRef = senderUserRef.collection("transactions").doc(); 
            const refundTxData = {
                timestamp: FieldValueAdmin.serverTimestamp(),
                type: TransactionTypeServer.Refund,
                amount: refundAmount,
                reference: newRefundTxRef.id,
                description: `Refund for expired gift: ${orderData.productName || 'Unknown Product'} (Order Ref: ${orderData.referenceNumber || orderId})`,
                externalReference: orderData.referenceNumber || orderId,
                relatedDocId: orderId,
                metadata: {
                    refundType: 'Gift Expiration',
                    originalOrderId: orderId,
                    originalOrderRef: orderData.referenceNumber,
                    productName: orderData.productName,
                    refundedTo: req.user.email, 
                    status: 'completed' 
                }
            };
            transaction.set(newRefundTxRef, refundTxData); 

            // 5. Update recipient's "Received_Gift" transaction metadata (if it exists)
            if (recipientGiftTxSnapshot && !recipientGiftTxSnapshot.empty) {
                const recipientGiftTxDocRef = recipientGiftTxSnapshot.docs[0].ref;
                transaction.update(recipientGiftTxDocRef, {
                    'metadata.status': "expired", 
                    lastUpdatedAt: FieldValueAdmin.serverTimestamp()
                });
            } else {
                console.warn(`Recipient's 'Received_Gift' transaction for order ${orderId} (recipient ${orderData.giftRecipientUid}) not found. Skipping its update.`);
            }

            return {
                message: "Gift refunded successfully.",
                refundedAmount: refundAmount,
                orderId: orderId
            };
        });

        res.status(200).json({ success: true, ...refundResult });

    } catch (error) {
        console.error(`SERVER ERROR /api/process-gift-refund for order ${orderId}, user ${senderUid}:`, error);
        res.status(500).json({ success: false, error: error.message || "Failed to process gift refund." });
    }
});

app.post('/api/finalize-gift-claim', authenticateToken, async (req, res) => {
    const { originalOrderId, deliveryDetailsFromRecipient } = req.body;
    const claimingUserId = req.user.uid;

    if (!originalOrderId || !deliveryDetailsFromRecipient || typeof deliveryDetailsFromRecipient !== 'object') {
        return res.status(400).json({ success: false, error: "Missing or invalid orderId or delivery details." });
    }
    if (!deliveryDetailsFromRecipient.userEmail) {
        return res.status(400).json({ success: false, error: "Recipient's delivery email is missing in details." });
    }

    const orderRef = dbAdmin.collection("orders").doc(originalOrderId);

    try {
        const finalizedClaimResult = await dbAdmin.runTransaction(async (transaction) => {
            const orderDoc = await transaction.get(orderRef); 
            if (!orderDoc.exists) {
                throw new Error("Gift order not found.");
            }
            const orderData = orderDoc.data();

            const receiverTransactionsQuery = dbAdmin.collection("users").doc(claimingUserId).collection("transactions")
                .where("relatedDocId", "==", originalOrderId)
                .where("type", "==", TransactionTypeServer.Received_Gift)
                .limit(1);
            const receiverTransactionsSnap = await transaction.get(receiverTransactionsQuery); 

            let senderGiftedTxSnap = null; 
            if (orderData.giftSenderId) { 
                const senderGiftedTxQuery = dbAdmin.collection("users").doc(orderData.giftSenderId).collection("transactions")
                    .where("relatedDocId", "==", originalOrderId)
                    .where("type", "==", TransactionTypeServer.Gifted)
                    .limit(1);
                senderGiftedTxSnap = await transaction.get(senderGiftedTxQuery); 
            }


            if (!orderData.isGift) throw new Error("This is not a gift order.");
            if (orderData.status !== 'sent_gift') {
                if (orderData.status === 'claimed') throw new Error("This gift has already been claimed.");
                throw new Error(`This gift cannot be claimed due to its current status: ${orderData.status}.`);
            }
            if (orderData.giftRecipientUid !== claimingUserId) throw new Error("This gift is not intended for you.");
            const giftExpirationDate = orderData.giftExpiration ? orderData.giftExpiration.toDate() : null;
            if (giftExpirationDate && giftExpirationDate.getTime() <= Date.now()) throw new Error("This gift has expired and can no longer be claimed.");
            if (!orderData.giftSenderId) throw new Error("Critical: Gift sender information is missing from the order data.");
            if (receiverTransactionsSnap.empty) throw new Error("Recipient's gift reception record is missing. Cannot finalize claim.");



            const serverTimestamp = FieldValueAdmin.serverTimestamp(); 

            const updatedOrderDataForReturn = {
                ...orderData, 
                orderId: orderDoc.id, 
                status: "claimed",
                claimedBy: claimingUserId,
                claimedAt: serverTimestamp, 
                deliveryDetails: deliveryDetailsFromRecipient,
                lastUpdatedAt: serverTimestamp 
            };

            transaction.update(orderRef, {
                status: "claimed",
                claimedBy: claimingUserId,
                claimedAt: serverTimestamp,
                deliveryDetails: deliveryDetailsFromRecipient,
                lastUpdatedAt: serverTimestamp
            });

            const productNameForTx = orderData.productName || "Product";
            const productGroupNameForTx = orderData.productGroupName || "Category";
            const quantityForTx = orderData.quantity || 1;
            const originalGiftPriceForTx = (orderData.singleItemPrice || 0) * quantityForTx;

            const recipientNewOrderTxRef = dbAdmin.collection("users").doc(claimingUserId).collection("transactions").doc();
            transaction.set(recipientNewOrderTxRef, {
                timestamp: serverTimestamp,
                type: TransactionTypeServer.Order,
                amount: 0,
                reference: recipientNewOrderTxRef.id,
                description: `Claimed gift: ${productNameForTx} from ${orderData.giftSenderDisplayName || 'a friend'}`,
                externalReference: orderData.referenceNumber,
                relatedDocId: originalOrderId,
                metadata: {
                    orderId: originalOrderId,
                    orderReference: orderData.referenceNumber,
                    productName: productNameForTx,
                    quantity: quantityForTx,
                    category: productGroupNameForTx,
                    isGift: true,
                    giftSenderId: orderData.giftSenderId,
                    giftSenderDisplayName: orderData.giftSenderDisplayName,
                    giftSenderPhotoURL: orderData.giftSenderPhotoURL || `https://api.dicebear.com/7.x/identicon/svg?seed=${orderData.giftSenderId}`,
                    status: "completed",
                    orderDate: new Date(Date.now() - new Date().getTimezoneOffset() * 60000).toISOString(),
                    originalGiftPrice: originalGiftPriceForTx,
                    deliveryDetails: deliveryDetailsFromRecipient
                }
            });

            const receiverGiftTransactionRef = receiverTransactionsSnap.docs[0].ref;
            transaction.update(receiverGiftTransactionRef, {
                'metadata.status': "claimed",
                lastUpdatedAt: serverTimestamp
            });

            transaction.update(dbAdmin.collection("users").doc(claimingUserId), {
                orderCount: FieldValueAdmin.increment(1),
                lastOrderAt: serverTimestamp,
                giftClaimedCount: FieldValueAdmin.increment(1),
                lastGiftClaimedAt: serverTimestamp,
                [`transactionStats.${TransactionTypeServer.Order}`]: FieldValueAdmin.increment(1)
            });

            if (senderGiftedTxSnap && !senderGiftedTxSnap.empty) {
                const senderGiftedTxRef = senderGiftedTxSnap.docs[0].ref;
                transaction.update(senderGiftedTxRef, {
                    'metadata.status': "claimed",
                    lastUpdatedAt: serverTimestamp
                });
            }
            
            return updatedOrderDataForReturn;
        });

        res.status(200).json({
            success: true,
            message: "Gift claimed successfully and delivery details saved!",
            claimedOrderData: finalizedClaimResult 
        });

    } catch (error) {
        console.error(`SERVER ERROR /api/finalize-gift-claim for order ${originalOrderId}, user ${claimingUserId}:`, error);
        res.status(500).json({ success: false, error: error.message || "Failed to finalize gift claim." });
    }
});

// Add these helper functions (if they don't exist or to ensure they align)
function determineCollectionFromType(typeString) {
    if (typeString === 'Discount Voucher' || typeString === 'Fee Voucher' || typeString === 'Voucher Type') { // 'Voucher Type' seems to be a possible stored type for Fee Voucher
        return 'vouchers';
    }
    return 'rewards';
}

// --- ADMIN PANEL API ENDPOINTS ---

// Create Entry (Reward/Voucher)
app.post('/api/admin/entries', authenticateToken, async (req, res) => {
    try {
        let data = req.body;
        const targetCollectionName = determineCollectionFromType(data.type);

        if (data.expirationDate) {
            const dateObj = new Date(data.expirationDate);
            if (!isNaN(dateObj.getTime())) {
                data.expirationDate = TimestampAdmin.fromDate(dateObj);
            } else {
                // Keep as string or handle as error if date must be valid
                console.warn("Invalid expirationDate string during creation:", data.expirationDate);
                // Or delete data.expirationDate; or return res.status(400).json({ error: "Invalid expiration date format." });
            }
        }
        
        data.createdAt = FieldValueAdmin.serverTimestamp(); // Backend timestamp

        const collRef = dbAdmin.collection(targetCollectionName);
        // Let Firestore generate the ID
        const newDocRef = collRef.doc();
        await newDocRef.set(data);
        const newDocId = newDocRef.id;

        // Log to rewardHistory
        await dbAdmin.collection('rewardHistory').add({
            action: `Created ${targetCollectionName === 'vouchers' ? 'voucher' : 'reward'} in document ${newDocId}`,
            documentId: newDocId,
            targetCollection: targetCollectionName,
            timestamp: FieldValueAdmin.serverTimestamp(),
            user: "AdminPanel" // Or req.user.uid if you want to log the specific admin
        });

        res.status(201).json({ success: true, id: newDocId, message: `${targetCollectionName === 'vouchers' ? 'Voucher' : 'Reward'} created successfully` });
    } catch (error) {
        console.error("Error in POST /api/admin/entries:", error);
        res.status(500).json({ success: false, error: error.message || "Failed to create entry." });
    }
});

// Get Entry (for editing)
app.get('/api/admin/entries/:collectionName/:id', authenticateToken, async (req, res) => {
    try {
        const { collectionName, id } = req.params;
        if (collectionName !== 'rewards' && collectionName !== 'vouchers') {
            return res.status(400).json({ success: false, error: "Invalid collection name." });
        }
        const docRef = dbAdmin.collection(collectionName).doc(id);
        const docSnap = await docRef.get();

        if (!docSnap.exists) {
            return res.status(404).json({ success: false, error: "Entry not found." });
        }
        let data = docSnap.data();
        // Convert Firestore Timestamp to ISO string for datetime-local input
        if (data.expirationDate && data.expirationDate instanceof TimestampAdmin) {
            data.expirationDate = data.expirationDate.toDate().toISOString().slice(0, 16);
        }
        if (data.createdAt && data.createdAt instanceof TimestampAdmin) {
            data.createdAt = data.createdAt.toDate().toISOString();
        }
         if (data.lastRedeemedAt && data.lastRedeemedAt instanceof TimestampAdmin) {
            data.lastRedeemedAt = data.lastRedeemedAt.toDate().toISOString();
        }


        res.status(200).json({ success: true, id: docSnap.id, data: data });
    } catch (error) {
        console.error(`Error in GET /api/admin/entries/${req.params.collectionName}/${req.params.id}:`, error);
        res.status(500).json({ success: false, error: error.message || "Failed to fetch entry." });
    }
});

// Update Entry
app.put('/api/admin/entries/:collectionName/:id', authenticateToken, async (req, res) => {
    try {
        const { collectionName, id } = req.params;
        let data = req.body;

        if (collectionName !== 'rewards' && collectionName !== 'vouchers') {
            return res.status(400).json({ success: false, error: "Invalid collection name." });
        }

        // Prevent changing ID or original createdAt
        delete data.id;
        delete data.createdAt; 

        if (data.expirationDate) {
            const dateObj = new Date(data.expirationDate);
            if (!isNaN(dateObj.getTime())) {
                data.expirationDate = TimestampAdmin.fromDate(dateObj);
            } else if (data.expirationDate === null || data.expirationDate === '') {
                 data.expirationDate = null; // Allow clearing the date
            } else {
                console.warn("Invalid expirationDate string during update:", data.expirationDate);
                // return res.status(400).json({ error: "Invalid expiration date format." });
            }
        } else {
             // If expirationDate is not provided in payload, ensure it's not accidentally set to something wrong
             // or explicitly set to null if intended to be cleared and not present
            if (data.hasOwnProperty('expirationDate') && data.expirationDate === undefined) {
                 data.expirationDate = null;
            }
        }
        
        data.lastUpdatedAt = FieldValueAdmin.serverTimestamp(); // Add/update lastUpdatedAt timestamp

        const docRef = dbAdmin.collection(collectionName).doc(id);
        await docRef.update(data);

        // Log to rewardHistory
        await dbAdmin.collection('rewardHistory').add({
            action: `Updated ${collectionName === 'vouchers' ? 'voucher' : 'reward'} in document ${id}`,
            documentId: id,
            targetCollection: collectionName,
            timestamp: FieldValueAdmin.serverTimestamp(),
            user: "AdminPanel"
        });

        res.status(200).json({ success: true, message: "Entry updated successfully." });
    } catch (error) {
        console.error(`Error in PUT /api/admin/entries/${req.params.collectionName}/${req.params.id}:`, error);
        res.status(500).json({ success: false, error: error.message || "Failed to update entry." });
    }
});

// Delete Entry
app.delete('/api/admin/entries/:collectionName/:id', authenticateToken, async (req, res) => {
    try {
        const { collectionName, id } = req.params;
        if (collectionName !== 'rewards' && collectionName !== 'vouchers') {
            return res.status(400).json({ success: false, error: "Invalid collection name." });
        }

        const docRef = dbAdmin.collection(collectionName).doc(id);
        await docRef.delete();

        // Log to rewardHistory
        await dbAdmin.collection('rewardHistory').add({
            action: `Deleted ${collectionName === 'vouchers' ? 'voucher' : 'reward'} from document ${id}`,
            documentId: id,
            targetCollection: collectionName,
            timestamp: FieldValueAdmin.serverTimestamp(),
            user: "AdminPanel"
        });

        res.status(200).json({ success: true, message: "Entry deleted successfully." });
    } catch (error) {
        console.error(`Error in DELETE /api/admin/entries/${req.params.collectionName}/${req.params.id}:`, error);
        res.status(500).json({ success: false, error: error.message || "Failed to delete entry." });
    }
});

// List History
app.get('/api/admin/history', authenticateToken, async (req, res) => {
    try {
        const historySnapshot = await dbAdmin.collection('rewardHistory')
                                          .orderBy('timestamp', 'desc') // Already sorted in client, but good practice
                                          .get();
        const entries = historySnapshot.docs.map(docSnap => {
            const data = docSnap.data();
            // Convert Firestore Timestamps to ISO strings
            const ts = data.timestamp instanceof TimestampAdmin ? data.timestamp.toDate() : null;
            return {
                id: docSnap.id,
                ...data,
                timestamp: ts ? ts.toISOString() : null, // Send as ISO string
                // Client expects 'date' and 'time' strings, it can derive them or backend can provide
                date: ts ? ts.toLocaleDateString() : 'Unknown date',
                time: ts ? ts.toLocaleTimeString() : 'Unknown time'
            };
        });
        res.status(200).json({ success: true, entries: entries });
    } catch (error) {
        console.error("Error in GET /api/admin/history:", error);
        res.status(500).json({ success: false, error: error.message || "Failed to load history." });
    }
});

// Search Entry by Code
app.post('/api/admin/search-entry', authenticateToken, async (req, res) => {
    const { code } = req.body;
    if (!code) {
        return res.status(400).json({ success: false, error: "Code is required for search." });
    }
    try {
        let foundDoc = null;
        let foundInCollection = '';

        const rewardsQuery = dbAdmin.collection('rewards').where('code', '==', code.toUpperCase());
        let snap = await rewardsQuery.get();
        if (!snap.empty) {
            foundDoc = snap.docs[0];
            foundInCollection = 'rewards';
        } else {
            const vouchersQuery = dbAdmin.collection('vouchers').where('code', '==', code.toUpperCase());
            snap = await vouchersQuery.get();
            if (!snap.empty) {
                foundDoc = snap.docs[0];
                foundInCollection = 'vouchers';
            }
        }

        if (foundDoc) {
            res.status(200).json({ success: true, found: true, id: foundDoc.id, collectionName: foundInCollection, data: foundDoc.data() });
        } else {
            res.status(404).json({ success: false, found: false, message: `Code "${code}" not found.` });
        }
    } catch (error) {
        console.error("Error in POST /api/admin/search-entry:", error);
        res.status(500).json({ success: false, error: error.message || "Search failed." });
    }
});

// Endpoint to fetch users for privacy modal (if not already available/secured)
// This assumes your 'users' collection is readable by the admin service account.
app.get('/api/admin/users', authenticateToken, async (req, res) => {
    try {
        const usersSnapshot = await dbAdmin.collection('users').get();
        const users = usersSnapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
        res.status(200).json({ success: true, users });
    } catch (error) {
        console.error("Error fetching users for admin panel:", error);
        res.status(500).json({ success: false, error: "Failed to fetch users." });
    }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`API Server running on port ${PORT}`));
