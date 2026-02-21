const cors = require('cors')
console.time("express");
const express = require('express');
console.timeEnd("express");
const multer = require('multer')
console.time("azure-blob");
const { BlobServiceClient } = require('@azure/storage-blob')
console.timeEnd("azure-blob");
console.time("database");
const { addDrinks, getReports, getOffers, addItem, natsPush, getData, updateItem, deleteItem, natsGet, pushOffer, natsPurchases, getPurchases, getName, getDrinks, getUser, finishTutorial, saveCode } = require('./lib/database')
console.timeEnd("database");
const axios = require('axios')
const app = express()
const session = require('express-session');
const { SquareClient } = require("square")
const { z } = require('zod')
const rateLimit = require('express-rate-limit')

console.time("firebase");
const { verifyToken } = require('./lib/firebase');
console.timeEnd("firebase");


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
app.post('/logout', (req, res) => {
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
app.get('/square/callback', async (req, res) => {
    const { code, state } = req.query;
    const client = new SquareClient({});

// Now, call the token exchange endpoint:

    try {
        const response = await client.oAuth.obtainToken({
    clientId: process.env.SQUARE_APP_ID,
    clientSecret: process.env.SQUARE_APP_SECRET,
    code: code,
    grantType: "authorization_code",
});

        const access_token = response.accessToken
        console.log('Access token received:', access_token);

        const locationId = "L7SDWNY6TWWVB"
        if (!locationId) throw new Error('No locations found');

        // Redirect back to app with token + location
        res.setHeader('Content-Type', 'text/html');
        res.redirect(`myapp://auth-callback?token=${access_token}&locationId=${locationId}`);
    } catch (err) {
        console.error('OAuth exchange failed:', err.response?.data || err.message);
        res.status(500).send('OAuth exchange failed');
    }
});



// POST /add_data with file upload
app.post('/add_data', verifyToken, cacheUser, upload.single('file'), async (req, res) => {
    const ItemSchema = z.object({
  item: z.string().min(1, "Item name is required"),
  price: z
    .string()
    .refine(val => !isNaN(Number(val)) && Number(val) >= 0, {
      message: "Price must be a non-negative number",
    }),
  quantity: z
    .string()
    .refine(val => Number.isInteger(Number(val)) && Number(val) >= 0, {
      message: "Quantity must be a non-negative integer",
    }),
  category: z.string().min(1, "Category is required"),
  description: z.string().max(500, "Description too long"),
});
  try {
    // Validate the data
    const item = req.body
    // Upload file to Azure Blob Storage
    if (req.file) {
      const blobName = Date.now() + '-' + req.file.originalname
      const blockBlobClient = containerClient.getBlockBlobClient(blobName)
      await blockBlobClient.upload(req.file.buffer, req.file.size)
      item.fileUrl = blockBlobClient.url // store Blob URL in Cosmos DB
      console.log(`📤 File uploaded to Blob Storage: ${item.fileUrl}`)
    }
    const validatedItem = ItemSchema.parse(item)
    const resource = await addItem(validatedItem, req.userData)
    res.status(201).json({ message: 'Successful post', item: resource })
  } catch (err) {
    console.error(err)
    res.status(500).json({ error: err.message })
  }
})

app.post('/delete_data', verifyToken, cacheUser, async (req, res) => {
  try {
    const DeleteItemSchema = z.object({
  id: z.string().min(1, "Item ID is required"),
  fileUrl: z.string().url("Invalid file URL"),
});
    const item = DeleteItemSchema.parse(req.body)
    const token = req.user
    console.log(item)
    await deleteBlob(item.fileUrl)
    await deleteItem(item.id, req.userData)
    res.status(201).json({ message: 'Successful deletion' })
  } catch (err) {
    console.error(err)
    res.status(500).json({ error: err.message })
  }
})


app.put('/edit_data', verifyToken, upload.single('file'), async (req, res) => {
  const ItemSchema = z.object({
  item: z.string().min(1, "Item name is required"),
  price: z
    .string()
    .refine(val => !isNaN(Number(val)) && Number(val) >= 0, {
      message: "Price must be a non-negative number",
    }),
  quantity: z
    .string()
    .refine(val => Number.isInteger(Number(val)) && Number(val) >= 0, {
      message: "Quantity must be a non-negative integer",
    }),
  category: z.string().min(1, "Category is required"),
  description: z.string().max(500, "Description too long"),
  fileUrl: z.string().min(1, "File is Required"),
  id: z.string().min(1, "ID is Required"),
  type: z.string().min(1, "Type Required"),
});
  try {
    const item = req.body
  const token = req.user
    // Upload file to Azure Blob Storage
    if (req.file) {
      const blobName = Date.now() + '-' + req.file.originalname
      const blockBlobClient = containerClient.getBlockBlobClient(blobName)
      await blockBlobClient.upload(req.file.buffer, req.file.size)
      item.fileUrl = blockBlobClient.url // store Blob URL in Cosmos DB
      console.log(`📤 File uploaded to Blob Storage: ${item.fileUrl}`)
    }

    console.log(item)
    const parsedItem = ItemSchema.parse({...item, type: 'product'})
    await updateItem(parsedItem.id, parsedItem, token)
    res.status(201).json({ message: 'Successful post', item })
  } catch (err) {
    console.error(err)
    res.status(500).json({ error: err.message })
  }
})


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
        console.log(token)
        console.log('TUTORIALLLLLLL')
        const resources = await finishTutorial(token)
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
    res.status(201);
  } catch(err) {
    console.log(err)
  }
})


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

const server = app.listen(port, () => {
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
