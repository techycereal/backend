const cors = require('cors')
console.time("express");
const http = require('http');
const express = require('express');
console.timeEnd("express");
const multer = require('multer')
console.time("azure-blob");
const { BlobServiceClient } = require('@azure/storage-blob')
console.timeEnd("azure-blob");
console.time("database");
const { getCustomizationsLibrary, saveCustomizationsLibrary, sendEmailVerification, sendSecretAuth, getTempAuth, saveTempAuth, addDrinks, getReports, getOffers, addItem, natsPush, getData, updateItem, deleteItem, natsGet, pushOffer, natsPurchases, getPurchases, getName, getDrinks, getUser, finishTutorial, saveCode, createBusinessName } = require('./lib/database')
const { sendSimpleMessage } = require('./lib/send_email')
console.timeEnd("database");
const axios = require('axios')
const app = express()
const session = require('express-session');
const { SquareClient, SquareEnvironment } = require("square")
const { z } = require('zod')
const rateLimit = require('express-rate-limit')
console.time("firebase");
const { verifyToken } = require('./lib/firebase');
console.timeEnd("firebase");
const server = http.createServer(app);
const WebSocket = require("ws");
const wss = new WebSocket.Server({ server });
const clientMap = new Map();
const crypto = require('crypto');
const jwt = require('jsonwebtoken');

wss.on("connection", (ws) => {
  console.log("WebSocket client connected");
  let assignedId = null;

  ws.on("message", (msg) => {
    try {
      const data = JSON.parse(msg);

      // --- 1. RESUME / IDENTIFY LOGIC ---
      if (data.type === "connect") {
        // Use provided ID (reconnect) or generate a new one
        assignedId = data.clientId || crypto.randomUUID();
        console.log(assignedId)

        // Update the map with the current active socket
        clientMap.set(assignedId, ws); 
        
        console.log(`Client ${assignedId} synced.`);

        ws.send(JSON.stringify({ 
          type: "connect", 
          clientId: assignedId, 
          message: 'Session Synced' 
        }));
        return;
      }
    } catch (err) {
      console.error("⚠️ WS Message Parse Error:", err);
    }
  });

  ws.on("close", () => {
    // We remove the socket reference so we don't try to send data 
    // to a closed pipe, but we keep the "session" logic if needed.
    if (assignedId) {
        console.log(`🔌 Connection paused for: ${assignedId}`);
        // Optional: Set a timeout to delete from clientMap after 5 mins of inactivity
    }
  });

  ws.on("error", (err) => {
    console.error("❌ WebSocket error:", err);
  });
});

// Request sanitization middleware - runs on all requests
const sanitizeRequest = (req, res, next) => {
  // Sanitize body, query, and params
  if (req.body && typeof req.body === 'object') {
    Object.keys(req.body).forEach(key => {
      if (typeof req.body[key] === 'string') {
        req.body[key] = req.body[key].trim()
      }
    })
  }

  if (req.query && typeof req.query === 'object') {
    Object.keys(req.query).forEach(key => {
      if (typeof req.query[key] === 'string') {
        req.query[key] = req.query[key].trim()
      }
    })
  }

  if (req.params && typeof req.params === 'object') {
    Object.keys(req.params).forEach(key => {
      if (typeof req.params[key] === 'string') {
        req.params[key] = req.params[key].trim()
      }
    })
  }

  next()
}

// Rate limiting configurations
const generalLimiter = rateLimit({
  windowMs: 15 * 60 * 1000, // 15 minutes
  max: 100, // Limit each IP to 100 requests per windowMs
  message: {
    error: 'Too many requests from this IP, please try again later.'
  },
  standardHeaders: true,
  legacyHeaders: false,
})

const authLimiter = rateLimit({
  windowMs: 60 * 60 * 1000, // 1 hour
  max: 5, // Limit auth attempts to 5 per hour
  message: {
    error: 'Too many authentication attempts, please try again later.'
  },
  standardHeaders: true,
  legacyHeaders: false,
})

// Centralized Zod validation error handler
const handleValidationError = (error, req, res, next) => {
  if (error.name === 'ZodError') {
    return res.status(400).json({
      error: 'Validation failed',
      details: error.errors.map(err => ({
        field: err.path.join('.'),
        message: err.message
      }))
    })
  }
  next(error)
}
 
app.use(cors({
  origin: '*', // your frontend URL
  credentials: true // allow sending cookies
}));
app.use(sanitizeRequest) // Add sanitization middleware
app.use(generalLimiter) // Add general rate limiting
app.use(express.json())

// Request-scoped user caching middleware
const cacheUser = async (req, res, next) => {
  if (req.user && !req.userData) {
    try {
      req.userData = await getUser(req.user);
    } catch (err) {
      console.error('Failed to cache user data:', err);
      return res.status(500).json({ error: 'Authentication error' });
    }
  }
  next();
};

app.use(session({
  secret: process.env.SESSION_SECRET || 'fallback-secret-change-in-production',
  resave: false,
  saveUninitialized: true,
  cookie: { secure: false } // set true in production with HTTPS
}));
// Multer in-memory storage (file will go straight to Blob Storage)
const storage = multer.memoryStorage()
const upload = multer({
  storage,
  limits: {
    fileSize: 5 * 1024 * 1024, // 5MB limit
  },
  fileFilter: (req, file, cb) => {
    // Allow only image files
    if (file.mimetype.startsWith('image/')) {
      cb(null, true);
    } else {
      cb(new Error('Only image files are alloweds'), false);
    }
  }
})
// Azure Blob Storage setup
const AZURE_STORAGE_CONNECTION_STRING = process.env.AZURE_STORAGE_CONNECTION_STRING
const containerName = 'product-images'
const blobServiceClient = BlobServiceClient.fromConnectionString(AZURE_STORAGE_CONNECTION_STRING)
const containerClient = blobServiceClient.getContainerClient(containerName)

const SQUARE_REDIRECT_URL = 'http://localhost:3001/square/callback';

// API config
const SQUARE_API_URL = 'https://connect.squareupsandbox.com/v2';


// Step 1: Login redirect (with stricter rate limiting for auth)
app.get('/square/login', authLimiter, (req, res) => {
  const url = `https://connect.squareup.com/oauth2/authorize?client_id=${process.env.SQUARE_APP_ID}&scope=CUSTOMERS_READ&session=false&redirect_uri=${encodeURIComponent(SQUARE_REDIRECT_URL)}`;
  console.log("Redirecting to:", url); // 👈 log the final URL
  res.redirect(`https://connect.squareup.com/oauth2/authorize?client_id=${process.env.SQUARE_APP_ID}&scope=CUSTOMERS_READ&session=false&redirect_uri=${encodeURIComponent(SQUARE_REDIRECT_URL)}`);
});
function requireSquareLogin(req, res, next) {
  const token = req.session.squareAccessToken;
  if (!token) {
    return res.status(401).json({ error: 'You must connect your Square account first.' });
  }
  // attach token to req for convenience
  console.log(token)
  req.squareAccessToken = token;
  next();
}

async function deleteBlob(fileUrl) {
  if (!fileUrl) return;
  try {
    const url = new URL(fileUrl);
    // Assuming the URL format: https://<account>.blob.core.windows.net/<container>/<blob>
    const [containerName, ...blobParts] = url.pathname.slice(1).split("/");
    const blobName = blobParts.join("/");
    const containerClient = blobServiceClient.getContainerClient(containerName);
    const blockBlobClient = containerClient.getBlockBlobClient(blobName);
    await blockBlobClient.deleteIfExists();
    console.log(`✅ Blob deleted: ${blobName}`);
  } catch (err) {
    console.error("❌ Failed to delete blob:", err);
  }
}

// Ensure container exists (non-blocking with error handling)
// ;(async () => {
//   try {
//     const exists = await containerClient.exists()
//     if (!exists) {
//       await containerClient.create()
//       console.log(`✅ Created container: ${containerName}`)
//     }
//   } catch (err) {
//     console.error('⚠️ Failed to initialize Azure container (server will continue):', err.message)
//   }
// })()



// Logout route
app.post('/logout', verifyToken, (req, res) => {
  req.session.destroy((err) => {
    if (err) {
      console.error("❌ Error destroying session:", err);
      return res.status(500).json({ error: "Logout failed" });
    }
    res.clearCookie("connect.sid", { path: "/" }); // clear session cookie
    res.status(200).json({ message: "Logged out successfully" });
  });
});

app.post('/add_business_name', verifyToken, cacheUser, async (req, res) => {
  try {
    const { name } = req.body
    const token = req.user
    console.log(req.body)
    await createBusinessName(name, token)
    res.status(201).json({message: name})
  } catch(err) {
    console.log(err)
    res.status(500).json({error: err})
  }
});

app.get('/square/customers', requireSquareLogin, async (req, res) => {
  try {
    const response = await axios.get(`${SQUARE_API_URL}/customers`, {
      headers: {
        'Authorization': `Bearer ${req.squareAccessToken}`,
        'Content-Type': 'application/json',
        'Square-Version': '2025-08-20'
      }
    });
    console.log(response.data)
    res.json(response.data);
  } catch (err) {
    console.error(err.response?.data || err.message);
    res.status(500).json({ error: 'Failed to fetch customers' });
  }
});

// OAuth callback
// app.get('/square/callback', async (req, res) => {
//   const client = new SquareClient({});
//     const { code, state } = req.query;
//     try {
//         const result = await client.oAuth.obtainToken({
//             clientId: process.env.SQUARE_APP_ID,
//             clientSecret: process.env.SQUARE_APP_SECRET,
//             code,
//             grantType: 'authorization_code'
//         });
        
//         // Save to Cosmos DB instead of putting it in the URL
//         await saveTempAuth(state, result.accessToken, "L7SDWNY6TWWVB");

//         // Redirect back to app with status ONLY
//         res.redirect(`myapp://auth-callback?status=success&state=${state}`);
//     } catch (err) {
//         console.log(err)
//         res.redirect(`myapp://auth-callback?status=error`);
//     }
// });



app.get('/square/callback', async (req, res) => {
    const { code, state } = req.query;

    try {
        // 1. Exchange the code for the token (keeping the SDK for this part is fine)
        const client = new SquareClient({});
        const response = await client.oAuth.obtainToken({
            clientId: process.env.SQUARE_APP_ID,
            clientSecret: process.env.SQUARE_APP_SECRET,
            code,
            grantType: 'authorization_code'
        });

        const accessToken = response.accessToken;

        // 2. Manual Fetch Request
        const locationsResponse = await fetch('https://connect.squareup.com/v2/locations', {
            method: 'GET',
            headers: {
                'Authorization': `Bearer ${accessToken}`,
                'Square-Version': '2026-01-22', // Best practice: include the version
                'Content-Type': 'application/json'
            }
        });

        const data = await locationsResponse.json();

        if (!locationsResponse.ok) {
            // This will tell you EXACTLY why Square is unhappy
            console.error("Square API Error Body:", JSON.stringify(data, null, 2));
            throw new Error(`Square API responded with ${locationsResponse.status}`);
        }

        const locations = data.locations;
        console.log("Locations found:", locations.length);

        // Continue with your logic...
        const firstLocation = locations.find(l => l.status === 'ACTIVE');
        await saveTempAuth(state, accessToken, firstLocation.id);

        res.redirect(`myapp://auth-callback?status=success&state=${state}`);

    } catch (err) {
        console.error("Auth Error:", err);
        res.redirect(`myapp://auth-callback?status=error`);
    }
});

// OAuth callback
app.get('/square/callback_sandbox', async (req, res) => {
  const client = new SquareClient({
        environment: SquareEnvironment.Sandbox, 
        accessToken: process.env.SQUARE_SANDBOX_ACCESS // Use your Sandbox Personal Access Token here
    });
    const { code, state } = req.query;
    try {

      const result = await client.oAuth.obtainToken({
            clientId: process.env.SQUARE_APP_SANDBOX_ID,
            clientSecret: process.env.SQUARE_APP_SANDBOX_SECRET,
            code,
            grantType: 'authorization_code'
        });
        console.log(result)
        console.log(result.accessToken)
        // Save to Cosmos DB instead of putting it in the URL
        await saveTempAuth(state, result.accessToken, "L3WPD4BR5FK8A");

        // Redirect back to app with status ONLY
        res.redirect(`myapp://auth-callback?status=success&state=${state}`);
    } catch (err) {
        console.log(err)
        res.redirect(`myapp://auth-callback?status=error`);
    }
});

app.post('/api/bootstrap', async (req, res) => {
    const { state } = req.body;

    if (!state) {
        return res.status(400).json({ error: "Missing state parameter" });
    }

    try {
        // 1. Look up the token in Cosmos DB using the state ID
        const authData = await getTempAuth(state)

        if (!authData) {
            // If it's not there, it either expired (TTL) or never existed
            return res.status(404).json({ error: "Session not found or expired" });
        }

        // 2. Send the token back in the body (Encrypted by HTTPS)
        res.json({
            accessToken: authData.accessToken,
            locationId: authData.locationId
        });

        // 3. Security: Delete it immediately after it's been "claimed"
        //await container.item(state, state).delete();

    } catch (err) {
        console.error("Bootstrap error:", err);
        res.status(500).json({ error: "Internal server error" });
    }
});

app.get('/get_customizations', verifyToken, cacheUser, async (req, res) => {
  try {
    // Call the decoupled data accessor function
    const modifiers = await getCustomizationsLibrary(req.userData);
    res.status(200).json(modifiers);
  } catch (err) {
    console.error(`❌ [Server Error] Failed on /get_customizations: ${err.message || err}`);
    res.status(500).json({ error: "Could not fetch customizations library" });
  }
});

// ==========================================
// POST Route: Save/Update Global Customization Library
// ==========================================
// Example update inside your Express router file:
app.post("/save_customizations", verifyToken, cacheUser, async (req, res) => {
  try {
    const { modifiers, customizationRules } = req.body;
    const token = req.user

    const updatedDoc = await saveCustomizationsLibrary(modifiers, customizationRules, token);
    
    res.status(200).json({ 
      size: updatedDoc.modifiers.size,
      toppings: updatedDoc.modifiers.toppings,
      extras: updatedDoc.modifiers.extras,
      free_sides: updatedDoc.modifiers.free_sides,
      paid_sides: updatedDoc.modifiers.paid_sides
    });
  } catch (err) {
    console.log(err)
    res.status(500).json({ error: err.message });
  }
});



app.post('/delete_data', verifyToken, cacheUser, async (req, res) => {
  try {
    const DeleteItemSchema = z.object({
      id: z.string().min(1, "Item ID is required"),
      // Fallback to a placeholder string if fileUrl is empty/missing, so it passes Zod
      fileUrl: z.string().optional().default("")
    });

    // 1. Safely parse incoming arguments through the schema matrix
    const item = DeleteItemSchema.parse(req.body);
    console.log("🗑️ Validated request received for item deletion:", item.id);

    // 2. Only attempt to erase the cloud blob if a real URL was assigned to the item doc
    if (item.fileUrl && item.fileUrl.startsWith("http")) {
      try {
        await deleteBlob(item.fileUrl);
      } catch (blobErr) {
        console.warn(`⚠️ Non-fatal storage alert: Blob erase skipped or not found: ${blobErr.message}`);
      }
    }

    // 3. Execute the database layer removal hook (Passing req.token or matching wrapper context)
    // Note: Make sure 'token' is available here (likely extracted from verifyToken onto req.token or req.headers)
    const activeToken = req.user;
    await deleteItem(item.id, activeToken);

    res.status(200).json({ message: 'Successful deletion' }); // Changed from 201 (Created) to 200 (OK)

  } catch (err) {
    // =========================================================================
    // CRITICAL FIX: INTERCEPT ZOD ERRORS TO PREVENT NODE.JS INSPECT CRASH
    // =========================================================================
    if (err instanceof z.ZodError) {
      console.warn("⚠️ Validation constraints failed on /delete_data path:", err.errors);
      return res.status(400).json({ 
        error: "Validation constraints failed", 
        details: err.errors 
      });
    }

    // Log generic system operational crashes safely
    console.error("❌ Exception caught inside /delete_data route layer:", err.message || err);
    res.status(500).json({ error: err.message || "An unexpected server error occurred." });
  }
});


app.put('/edit_data', verifyToken, cacheUser, upload.single('file'), async (req, res) => {
  // 1. Updated validation parameters extending your original Zod framework
  const ItemSchema = z.object({
    id: z.string().min(1, "ID is Required"),
    item: z.string().min(1, "Item name is required"),
    price: z
      .string()
      .refine(val => !isNaN(Number(val)) && Number(val) >= 0, {
        message: "Price must be a non-negative number",
      }),
    quantity: z
      .string()
      .refine(val => !isNaN(Number(val)) && Number.isInteger(Number(val)) && Number(val) >= 0, {
        message: "Quantity must be a non-negative integer",
      }),
    category: z.string().min(1, "Category is required"),
    description: z.string().max(500, "Description too long").optional().default(""),
    type: z.string().min(1, "Type Required"),
    
    linkedModifierIds: z.string().transform((val) => {
      try { return JSON.parse(val); } catch { return []; }
    }),
    allCurrentModifiers: z.string().transform((val) => {
      try { return JSON.parse(val); } catch { return {}; }
    }),
    customizationRules: z.string().transform((val) => {
      try { return JSON.parse(val); } catch { return null; }
    }).optional()
  });

  try {
    const rawBody = { ...req.body };
    const activeToken = req.user;

    // 2. Manage the incoming file asset streaming to Azure Blob storage
    if (req.file) {
      const blobName = Date.now() + '-' + req.file.originalname;
      const blockBlobClient = containerClient.getBlockBlobClient(blobName);
      await blockBlobClient.upload(req.file.buffer, req.file.size);
      rawBody.fileUrl = blockBlobClient.url;
      console.log(`📤 Fresh image file uploaded to Blob Storage: ${rawBody.fileUrl}`);
    }

    // 3. Parse incoming arguments through the transformed Zod blueprint schema
    const parsedData = ItemSchema.parse({ ...rawBody, type: 'product' });

    const defaultRules = parsedData.customizationRules || {
      size: { min_selectable: 1, max_selectable: 1 },
      toppings: { min_selectable: 0, max_selectable: 3 },
      extras: { min_selectable: 0, max_selectable: 1 },
      free_sides: { min_selectable: 0, max_selectable: 1 },
      paid_sides: { min_selectable: 0, max_selectable: 1 },
    };

    // Reassemble payload object structure seamlessly preserving media attachment pointers
    const productPayload = {
      item: parsedData.item,
      price: Number(parsedData.price),
      quantity: Number(parsedData.quantity),
      category: parsedData.category,
      description: parsedData.description,
      linkedModifierIds: parsedData.linkedModifierIds, 
      customizationRules: defaultRules,
      fileUrl: parsedData.fileUrl,
      type: parsedData.type
    };

    // =========================================================================
    // 4. PERSIST UNIQUE SNAPSHOT SNAPS FOR INLINE MODIFICATIONS
    // =========================================================================
    if (parsedData.allCurrentModifiers && Object.keys(parsedData.allCurrentModifiers).length > 0) {
      try {
        const cosmosContainer = await initializeCosmosContainer();
        const businessId = await getUser(req.user)
        
        const timestamp = Date.now();
        const libraryId = `modifiers_lib_${businessId}_${timestamp}`;

        // Deduplicate incoming options to ensure absolute name string uniqueness
        const cleanModifiers = {};
        Object.keys(parsedData.allCurrentModifiers).forEach((categoryKey) => {
          const initialArray = parsedData.allCurrentModifiers[categoryKey] || [];
          const uniqueMap = new Map();
          
          initialArray.forEach((mod) => {
            if (mod && mod.name) {
              const uniqueTokenKey = mod.name.trim().toLowerCase();
              if (!uniqueMap.has(uniqueTokenKey)) {
                uniqueMap.set(uniqueTokenKey, {
                  id: mod.id,
                  name: mod.name.trim(),
                  price: Number(mod.price) || 0
                });
              }
            }
          });
          cleanModifiers[categoryKey] = Array.from(uniqueMap.values());
        });

        const newLibDoc = {
          id: libraryId, 
          type: "modifier_library",
          business: businessId, 
          created_at: new Date(timestamp).toISOString(),
          modifiers: cleanModifiers,
          customizationRules: defaultRules
        };

        await cosmosContainer.items.create(newLibDoc, { partitionKey: businessId });
        console.log(`🎯 Unique historic modifier snapshot saved on edit session. ID: ${libraryId}`);
      } catch (modSaveErr) {
        console.error("Non-fatal alert: Modifier database index stream failure: ", modSaveErr);
      }
    }

    // 5. Fire your updated database item updater block
    const resource = await updateItem(parsedData.id, productPayload, activeToken);
    
    res.status(200).json({ 
      message: 'Successful item update adjustment', 
      item: resource 
    });

  } catch (err) {
    console.error(`❌ [Server Error] Route failed on /edit_data: ${err.message || err}`);
    
    if (err instanceof z.ZodError) {
      return res.status(400).json({ 
        error: "Validation constraints failed", 
        details: err.errors 
      });
    }
    
    res.status(500).json({ error: err.message || "An unexpected server error occurred" });
  }
});

app.post('/add_data', verifyToken, cacheUser, upload.single('file'), async (req, res) => {
  // 1. Updated validation parameters extending your original Zod framework
  const ItemSchema = z.object({
    item: z.string().min(1, "Item name is required"),
    price: z
      .string()
      .refine(val => !isNaN(Number(val)) && Number(val) >= 0, {
        message: "Price must be a non-negative number",
      }),
    quantity: z
      .string()
      .optional()
      .default("")
      .refine(val => val === "" || (!isNaN(Number(val)) && Number.isInteger(Number(val)) && Number(val) >= 0), {
        message: "Quantity must be a non-negative integer",
      }),
    category: z.string().min(1, "Category is required"),
    description: z.string().max(500, "Description too long").optional().default(""),
    
    linkedModifierIds: z.string().transform((val) => {
      try { return JSON.parse(val); } catch { return []; }
    }),
    allCurrentModifiers: z.string().transform((val) => {
      try { return JSON.parse(val); } catch { return {}; }
    }),
    // NEW ZOD PARAMETER: INTERCEPT AND TRANSFORM STRUNG BOUNDARY CONFIGS
    customizationRules: z.string().transform((val) => {
      try { return JSON.parse(val); } catch { return null; }
    }).optional()
  });

  try {
    const rawBody = { ...req.body };

    // 2. Manage the incoming file asset streaming to Azure Blob storage
    if (req.file) {
      const blobName = Date.now() + '-' + req.file.originalname;
      const blockBlobClient = containerClient.getBlockBlobClient(blobName);
      await blockBlobClient.upload(req.file.buffer, req.file.size);
      rawBody.fileUrl = blockBlobClient.url;
      console.log(`📤 File uploaded to Blob Storage: ${rawBody.fileUrl}`);
    }

    // 3. Parse incoming arguments through the transformed Zod blueprint schema
    const parsedData = ItemSchema.parse(rawBody);

    const defaultRules = parsedData.customizationRules || {
      size: { min_selectable: 1, max_selectable: 1 },
      toppings: { min_selectable: 0, max_selectable: 3 },
      extras: { min_selectable: 0, max_selectable: 1 },
      free_sides: { min_selectable: 0, max_selectable: 1 },
      paid_sides: { min_selectable: 0, max_selectable: 1 },
    };

    // Reassemble payload object structure seamlessly preserving media attachment pointers
    const productPayload = {
      item: parsedData.item,
      price: Number(parsedData.price),
      quantity: Number(parsedData.quantity),
      category: parsedData.category,
      description: parsedData.description,
      linkedModifierIds: parsedData.linkedModifierIds, 
      customizationRules: defaultRules,
      fileUrl: rawBody.fileUrl || req.body.fileUrl || ""
    };

    // =========================================================================
    // 4. FIXED: PERSIST UNIQUE INDEPENDENT MODIFIERS & CUSTOMIZATION RULES 
    // =========================================================================
    if (parsedData.allCurrentModifiers && Object.keys(parsedData.allCurrentModifiers).length > 0) {
      try {
        const cosmosContainer = await initializeCosmosContainer();
        const businessId = req.userData.business;
        
        // Match the timestamp strategy from your save helper function
        const timestamp = Date.now();
        const libraryId = `modifiers_lib_${businessId}_${timestamp}`;

        // Deduplicate incoming options to ensure absolute name string uniqueness
        const cleanModifiers = {};
        Object.keys(parsedData.allCurrentModifiers).forEach((categoryKey) => {
          const initialArray = parsedData.allCurrentModifiers[categoryKey] || [];
          const uniqueMap = new Map();
          
          initialArray.forEach((mod) => {
            if (mod && mod.name) {
              const uniqueTokenKey = mod.name.trim().toLowerCase();
              if (!uniqueMap.has(uniqueTokenKey)) {
                uniqueMap.set(uniqueTokenKey, {
                  id: mod.id,
                  name: mod.name.trim(),
                  price: Number(mod.price) || 0
                });
              }
            }
          });
          cleanModifiers[categoryKey] = Array.from(uniqueMap.values());
        });

        const newLibDoc = {
          id: libraryId, 
          type: "modifier_library",
          business: businessId, 
          created_at: new Date(timestamp).toISOString(),
          modifiers: cleanModifiers,
          customizationRules: defaultRules
        };

        // Create the unique fresh history snapshot directly inside your container partition
        await cosmosContainer.items.create(newLibDoc, { partitionKey: businessId });
        console.log(`🎯 Unique historic modifier snapshot saved. ID: ${libraryId}`);
      } catch (modSaveErr) {
        console.error("Non-fatal alert: Modifier database index stream failure: ", modSaveErr);
      }
    }

    // 5. Fire your standard baseline creation wrapper to drop product item details inside Cosmos
    const resource = await addItem(productPayload, req.user);
    
    res.status(201).json({ 
      message: 'Successful post', 
      item: resource 
    });

  } catch (err) {
    console.error(`❌ [Server Error] Route failed on /add_data: ${err.message || err}`);
    
    if (err instanceof z.ZodError) {
      return res.status(400).json({ 
        error: "Validation constraints failed", 
        details: err.errors 
      });
    }
    
    res.status(500).json({ error: err.message || "An unexpected server crash occurred" });
  }
});

app.get('/get_data', verifyToken, async (req, res) => {
    try{
        const token = req.user
        const resources = await getData(token)
        res.status(200).json({data: resources})
    } catch(err) {
        console.error(err)
        res.status(500).json({ error: err.message })
    }    
})

app.get('/get_drinks', verifyToken, async (req, res) => {
    try{
        const token = req.user
        const resources = await getDrinks(token)
        res.status(200).json({data: resources})
    } catch(err) {
        console.error(err)
        res.status(500).json({ error: err.message })
    }    
})

app.get('/get_offers', verifyToken, async (req, res) => {
    try{
        const token = req.user
        const resources = await getOffers(token)
        res.status(200).json({data: resources})
    } catch(err) {
        console.error(err)
        res.status(500).json({ error: err.message })
    }    
})


app.get('/get_purchases', verifyToken, async (req, res) => {
    try{
        const token = req.user
        const resources = await natsPurchases(token)
        res.status(200).json({data: resources})
    } catch(err) {
        console.error(err)
        res.status(500).json({ error: err.message })
    }    
})

app.get('/get_tutorial', verifyToken, async (req, res) => {
    try{
        const token = req.user
        const resources = await getUser(token)
        res.status(200).json({data: resources})
    } catch(err) {
        console.error(err)
        res.status(500).json({ error: err.message })
    }    
})

app.put('/finish_tutorial', verifyToken, async (req, res) => {
    try{
        const token = req.user
        const { section } = req.body
        console.log(section)
        console.log('TUTORIALLLLLLL')
        const resources = await finishTutorial(token, section)
        res.status(200).json({data: resources})
    } catch(err) {
        console.error(err)
        res.status(500).json({ error: err.message })
    }    
})

// PUT /natspush
app.put('/natspush', verifyToken, async (req, res) => {
  try {
    const token = req.user
    await natsPush(token)
    res.status(200).json({ message: 'Successful update' })
  } catch (err) {
    console.error(err)
    res.status(500).json({ error: err.message })
  }
})

app.get('/get_emails', verifyToken, async (req, res) => {
  try {
    const token = req.user
    const result = await natsGet(token)
    console.log(result)
    res.status(200).json({ message: 'Successful update', emails: result.emailData })
  } catch (err) {
    console.error(err)
    res.status(500).json({ error: err.message })
  }
})

app.post('/add_offer', verifyToken, async (req, res) => {
  try {
    const DealSchema = z.object({
  name: z.string(),
  future: z.nullable(z.any()),
  show: z.number(),
});

const DealsSchema = z.record(z.string(), DealSchema);

const RootSchema = z.object({
  deals: DealsSchema,
  id: z.string(),
});

    const token = req.user;
    const selectedDeals = req.body;
    console.log('REQ BODY:', selectedDeals); // should now show an object
    const validatedDeals = RootSchema.parse(selectedDeals);

    const response = await pushOffer(validatedDeals, token);
    res.status(201).json({ message: 'Successful insertion', data: response });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});


app.get('/get_report', verifyToken, async (req, res) => {
  try {
    const token = req.user
    const data = await getPurchases(token)
    console.log(data)
    res.status(201).json({ message: data })
  } catch (err) {
    console.error(err)
    res.status(500).json({ error: err.message })
  }
})

app.get('/get_time_report', verifyToken, async (req, res) => {
    try {
        const token = req.user
        const data = await getReports(token)
        res.status(200).json({message: data})
    } catch(err) {
        console.error(err)
        res.status(500).json({error: err.message})
    }
})

app.get('/get_name', verifyToken, async (req, res) => {
    try {
        const token = req.user
        const data = await getName(token)
        res.status(200).json({message: data})
    } catch(err) {
        console.error(err)
        res.status(500).json({error: err.message})
    }
})

app.post('/add_drinks', verifyToken, async (req, res) => {
  console.log(req.body)
  const DrinkSchema = z.object({
  drinks: z
    .array(z.string().min(1, 'Drink name cannot be empty'))
    .min(1, 'Must Have At Least One Drink'),
});
  try {
    const data = req.body;
    const drinks = DrinkSchema.parse(data)
    const token = req.user
    const response = await addDrinks(drinks.drinks, token)
    console.log(response)
    res.status(201);
  } catch(err) {
    console.log(err)
  }
})

app.post('/saveCode', verifyToken, async (req, res) => {
  console.log(req.body)
  try {
    const data = req.body;
    console.log(data)
    const token = req.user
    const response = await saveCode(data.inventoryCode, token)
    console.log(response)
    res.status(200).json({"message": "SUCCESS"});
  } catch(err) {
    console.log(err)
  }
})

app.post('/send_email', async (req, res) => {
  try {
    const data = req.body;
    const response = await sendSimpleMessage(data.subject, data.body, data.email, data.context)
    console.log(response)
    res.status(200).json({"message": "SUCCESS"});
  } catch(err) {
    console.log(err)
  }
})

app.post('/sandbox_login', verifyToken, async (req, res) => {
  try {
    const token = req.user
    console.log(token)
    console.log(token.user_id)
    if (token.user_id === "5B9bq9r8UXQUynfVH87Qeh8WKrX2") {
      res.status(200).json({"accessToken": process.env.SANDBOX_TEST_TOKEN, "locationId": process.env.SANDBOX_TEST_LOCATION});
    } else {
      res.status(200).json({"accessToken": process.env.SANDBOX_TEST_TOKEN, "locationId": process.env.SANDBOX_TEST_LOCATION});
    }
  } catch(err) {
    console.log(err)
  }
})


app.post('/api/secret', verifyToken, async (req, res) => {
  try {
    const data = req.body;
    const token = req.user
    console.log(data)
    await sendSecretAuth(data.secret, token, data.clientId)
    res.status(200).json({"message": "success"});
  } catch(err) {
    console.log(err)
  }
})

app.post('/secret_success', async (req, res) => {
  const { clientId, message } = req.body;
  if(message === "failure") {
    ws.send(JSON.stringify({
        type: "secret_failure",
        message: "success"
      }));
  }
  
  console.log(`Initial success received for ${clientId}. Waiting 5s for reconnect...`);

  // 1. Tell the sender (the QR Scanner/Backend) that we've started the wait
  res.status(202).json({ status: "waiting_for_client_reconnect" });

  // 2. Wait for the Android app to potentially finish its Square Intent
  setTimeout(() => {
    const ws = clientMap.get(clientId);

    if (ws && ws.readyState === 1) {
      ws.send(JSON.stringify({
        type: "secret",
        message: "success"
      }));
      console.log(`✅ Delayed delivery successful for ${clientId}`);
    } else {
      console.log(`❌ Client ${clientId} failed to reconnect in time. Queueing instead.`);
      // Fallback: Put it in the pending queue we discussed earlier
      pendingMessages.set(clientId, { type: "secret", message: "success" });
    }
  }, 15000); // 5 second grace period
});



// Use the SAME secret you used to sign the token in the sendEmail function
const JWT_SECRET = "your_super_hidden_server_secret_key"; 

app.post('/accepted_emails', async (req, res) => {
  try {
    const payload = req.body;
    console.log(payload)
    // 1. Mailgun sends a lot of data, we want 'event-data'
    const eventData = payload['event-data'];
    console.log("THIS IS AN EVENT DATA")
    console.log(eventData)
    
    if (!eventData) {
      console.log("Empty or malformed webhook received");
      return res.sendStatus(400);
    }

    // 2. Extract our "hidden" token from user-variables
    const encryptedToken = eventData['user-variables']?.ref;
    console.log("THIS IS AN ENCRYPTED TOKEN")
    console.log(encryptedToken)
    

    if (encryptedToken) {
      try {
        // 3. UNLOCK the token to get the Pi ID
        const decoded = jwt.verify(encryptedToken, JWT_SECRET);
        console.log("THIS IS AN decoded TOKEN")
        console.log(decoded)
        const piId = decoded.piId;
        await sendEmailVerification(eventData.recipient, piId)
        console.log(`🎯 Webhook for Pi: ${piId}`);
        console.log(`📧 Event: ${eventData.event} | Recipient: ${eventData.recipient}`);

      } catch (jwtErr) {
        console.log(jwtErr);
      }
    } else {
      console.log("No custom Pi ID found in this webhook.");
    }

    // 5. Always tell Mailgun you received the data (200 OK)
    // If you don't, Mailgun will keep retrying and spamming your server.
    res.sendStatus(200);

  } catch (err) {
    console.error("Webhook Route Error:", err);
    res.status(500).send("Internal Server Error");
  }
});

// Add centralized error handling middleware (must be last)
app.use(handleValidationError)

const port = process.env.PORT || 3001

// Wrap server startup in error handling
process.on('uncaughtException', (err) => {
  console.error('❌ Uncaught Exception:', err);
  process.exit(1);
});

process.on('unhandledRejection', (reason, promise) => {
  console.error('❌ Unhandled Rejection at:', promise, 'reason:', reason);
  process.exit(1);
});

server.listen(port, () => {
  console.log(`✅ Server listening on port ${port}`);
});

server.on('error', (err) => {
  console.error('❌ Server error:', err);
  process.exit(1);
});

setImmediate(async () => {
  try {
    const exists = await containerClient.exists();
    if (!exists) await containerClient.create();
    console.log("Azure container ready");
  } catch (err) {
    console.error("Azure init failed:", err.message);
  }
});
