const express = require('express')
const cors = require('cors')
const multer = require('multer')
const { BlobServiceClient } = require('@azure/storage-blob')
const path = require('path')
const { createBusinessName, getReports, getOffers, addItem, natsPush, getData, updateItem, deleteItem, natsGet, pushOffer, natsPurchases, getPurchases } = require('./lib/database')
const axios = require('axios')
const app = express()
const session = require('express-session');
const { SquareClient } = require("square")
app.use(cors({
  origin: 'http://localhost:5173', // your frontend URL
  credentials: true // allow sending cookies
}));
app.use(express.json())
const { verifyToken } = require('./lib/firebase');

app.use(session({
  secret: 'your-strong-secret', // change to a strong random string
  resave: false,
  saveUninitialized: true,
  cookie: { secure: false } // set true in production with HTTPS
}));
// Multer in-memory storage (file will go straight to Blob Storage)
const storage = multer.memoryStorage()
const upload = multer({ storage })
let i = 20;
// Azure Blob Storage setup
const AZURE_STORAGE_CONNECTION_STRING = process.env.AZURE_STORAGE_CONNECTION_STRING
const containerName = 'product-images'
const blobServiceClient = BlobServiceClient.fromConnectionString(AZURE_STORAGE_CONNECTION_STRING)
const containerClient = blobServiceClient.getContainerClient(containerName)

const SQUARE_REDIRECT_URL = 'http://localhost:3001/square/callback';

// API config
const SQUARE_API_URL = 'https://connect.squareupsandbox.com/v2';

// Step 1: Login redirect
app.get('/square/login', (req, res) => {
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

app.post('/add_business_name', verifyToken, async (req, res) => {
  try {
    const { name } = req.body
    console.log(req.body)
    await createBusinessName(name)
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
        res.redirect(`myapp://auth-callback?token=${access_token}&locationId=${locationId}`);
    } catch (err) {
        console.error('OAuth exchange failed:', err.response?.data || err.message);
        res.status(500).send('OAuth exchange failed');
    }
});


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


// Ensure container exists
;(async () => {
  const exists = await containerClient.exists()
  if (!exists) {
    await containerClient.create()
    console.log(`✅ Created container: ${containerName}`)
  }
})()


// POST /add_data with file upload
app.post('/add_data', verifyToken, upload.single('file'), async (req, res) => {
  try {
    const item = req.body

    // Upload file to Azure Blob Storage
    if (req.file) {
      const blobName = Date.now() + '-' + req.file.originalname
      const blockBlobClient = containerClient.getBlockBlobClient(blobName)
      await blockBlobClient.upload(req.file.buffer, req.file.size)
      item.fileUrl = blockBlobClient.url // store Blob URL in Cosmos DB
      console.log(`📤 File uploaded to Blob Storage: ${item.fileUrl}`)
    }
    item.business = 'Alex'

    const resource = await addItem(item)
    res.status(201).json({ message: 'Successful post', item: resource })
  } catch (err) {
    console.error(err)
    res.status(500).json({ error: err.message })
  }
})

app.post('/delete_data', verifyToken, async (req, res) => {
  try {
    const item = req.body

    await deleteBlob(item.fileUrl)
    await deleteItem(item.id, 'Alex', item)
    res.status(201).json({ message: 'Successful deletion' })
  } catch (err) {
    console.error(err)
    res.status(500).json({ error: err.message })
  }
})


app.put('/edit_data', verifyToken, upload.single('file'), async (req, res) => {
  try {
    const item = req.body

    // Upload file to Azure Blob Storage
    if (req.file) {
      const blobName = Date.now() + '-' + req.file.originalname
      const blockBlobClient = containerClient.getBlockBlobClient(blobName)
      await blockBlobClient.upload(req.file.buffer, req.file.size)
      item.fileUrl = blockBlobClient.url // store Blob URL in Cosmos DB
      console.log(`📤 File uploaded to Blob Storage: ${item.fileUrl}`)
    }

    console.log(item)

    await updateItem(item.id, 'Alex', item)
    res.status(201).json({ message: 'Successful post', item })
  } catch (err) {
    console.error(err)
    res.status(500).json({ error: err.message })
  }
})


app.get('/get_data', verifyToken, async (req, res) => {
    try{
        const resources = await getData()
        res.status(200).json({data: resources})
    } catch(err) {
        console.error(err)
        res.status(500).json({ error: err.message })
    }    
})

app.get('/get_offers', verifyToken, async (req, res) => {
    try{
        const resources = await getOffers()
        res.status(200).json({data: resources})
    } catch(err) {
        console.error(err)
        res.status(500).json({ error: err.message })
    }    
})


app.get('/get_purchases', verifyToken, async (req, res) => {
    try{
        const resources = await natsPurchases()
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
    const result = await natsGet()
    console.log(result)
    console.log('test')
    res.status(200).json({ message: 'Successful update', emails: result.emailData })
  } catch (err) {
    console.error(err)
    res.status(500).json({ error: err.message })
  }
})

app.post('/add_offer', verifyToken, async (req, res) => {
  try {
    const selectedDeals = req.body
    console.log(selectedDeals)
    const response = await pushOffer(selectedDeals)
    res.status(201).json({ message: 'Successful deletion', data: response })
  } catch (err) {
    console.error(err)
    res.status(500).json({ error: err.message })
  }
})

app.get('/get_report', verifyToken, async (req, res) => {
  try {
    const data = await getPurchases()
    console.log(data)
    res.status(201).json({ message: data })
  } catch (err) {
    console.error(err)
    res.status(500).json({ error: err.message })
  }
})

app.get('/get_time_report', verifyToken, async (req, res) => {
    try {
        const data = await getReports()
        res.status(200).json({message: data})
    } catch(err) {
        console.error(err)
        res.status(500).json({error: err.message})
    }
})


const port = process.env.PORT || 3001
app.listen(port, () => {
  console.log(`Listening on port ${port}`)
})
