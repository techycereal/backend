const { CosmosClient,  } = require("@azure/cosmos");
const { connect, StringCodec } = require("nats");
const crypto = require("crypto");
const dotenv = require('dotenv')
dotenv.config()


// Cosmos DB credentials
const endpoint = process.env.COSMOS_URI;
const key = process.env.COSMOS_KEY;
const client = new CosmosClient({ endpoint, key });

// NATS server (no authentication)
const natsServer = process.env.FQDN;

// Singleton instances for connection pooling
let cosmosContainer = null;
let natsConnection = null;
let natsCodec = null;

const databaseId = "cosmicworks";
const containerId = "businesses";

// Initialize Cosmos DB connection once
async function initializeCosmosContainer() {
  if (!cosmosContainer) {
    const { database } = await client.databases.createIfNotExists({ id: databaseId });
    console.log(`✅ Database ready: ${database.id}`);

    const { container } = await database.containers.createIfNotExists({
      id: containerId,
      partitionKey: { paths: ["/business"] },
    });
    console.log(`✅ Container ready: ${container.id}`);
    cosmosContainer = container;
  }
  return cosmosContainer;
}

async function initializeNATS() {
  // 1. If we have a connection and it's NOT closed, reuse it.
  // This includes the "reconnecting" state!
  if (natsConnection && !natsConnection.isClosed()) {
    return { nc: natsConnection, sc: natsCodec };
  }

  console.log("🔄 Initializing/Reconnecting to NATS...");

  try {
    natsConnection = await connect({
      servers: natsServer,
      reconnect: true,
      maxReconnectAttempts: -1, // Keep trying forever
      waitOnFirstConnect: true, // Crucial: prevents crash if NATS is down at boot
    });

    natsCodec = StringCodec();

    // Monitor the terminal close state
    natsConnection.closed().then((err) => {
      console.log("⚠️ NATS connection reached terminal state. Clearing instance.");
      if (err) console.error(`Reason: ${err.message}`);
      natsConnection = null;
    });

    // Optional: Log status changes to see it working in real-time
    (async () => {
      for await (const s of natsConnection.status()) {
        console.log(`📡 NATS Status Update: ${s.type}`);
      }
    })().catch(() => {});

    console.log("✅ Connected to NATS");
    return { nc: natsConnection, sc: natsCodec };

  } catch (err) {
    console.error("❌ NATS Connection Failed:", err.message);
    // Return null or throw so the calling function (like natsPush) knows it failed
    return { nc: null, sc: null };
  }
}




async function deleteItem(id, token) {
  const container = await initializeCosmosContainer();
  const { business } = await getUser(token)
  try {
    // Delete Cosmos DB item
    await container.item(id, business).delete();
    console.log(`✅ Item deleted: ${id}`);
  } catch (err) {
    console.error("❌ Failed to delete item:", err);
    throw err;
  }
}

async function getUser(token) {
  const container = await initializeCosmosContainer();
  console.log(token.uid)
  const querySpec = {
    query: "SELECT * FROM c WHERE c.id = @id",
    parameters: [{ name: "@id", value: token.uid }],
  };

  const { resources } = await container.items.query(querySpec).fetchAll();
  console.log("📌 Query results:", resources);
  const { device_id, business, name, tutorial, id } = resources[0]
  console.log(device_id)
  return { device_id, business, name, tutorial, id };
}

async function updateItem(id, updatedItem, token) {
  const container = await initializeCosmosContainer();
  const { business } = await getUser(token);
  
  try {
    // Replace the existing item item definition matching its multi-tenant profile partition
    const { resource } = await container.item(id, business).replace({
      ...updatedItem, // Spreads out validated, parsed parameters cleanly
      id,             // Assures unique key parameter cannot be modified
      business        // Keeps isolation matching partition parameters
    });
    
    console.log(`✅ Item document updated successfully inside Cosmos: ${resource.id}`);
    return resource;
  } catch (err) {
    console.error("❌ Failed to update product item document within database:", err);
    throw err;
  }
}

async function addItem(item, token) {
  const container = await initializeCosmosContainer()
  const { business } = await getUser(token)
  item.type = 'product'
  console.log(item)
  item.business = business
  const result = await container.items.create(item);
  console.log(result.resource)
  return result.resource
}


async function getData(token) {
    const container = await initializeCosmosContainer();
    const { business } = await getUser(token);
    console.log(`Getting data for business partition: ${business}`);

    // 1. Define distinct SQL queries
    const productQuery = {
        query: "SELECT * FROM c WHERE c.business = @business AND c.type = 'product'",
        parameters: [{ name: "@business", value: business }],
    };

    // Pull the latest modified configuration record based on the custom timestamp parameter
    const libraryQuery = {
        query: "SELECT TOP 1 * FROM c WHERE c.business = @business AND c.type = 'modifier_library' ORDER BY c.id DESC",
        parameters: [{ name: "@business", value: business }]
    };

    // 2. Execute queries in parallel configuration options matrix
    const [productRes, libraryRes] = await Promise.all([
        container.items.query(productQuery, { partitionKey: business }).fetchAll(),
        container.items.query(libraryQuery, { partitionKey: business }).fetchAll()
    ]);

    const products = productRes.resources || [];
    const libraryDoc = libraryRes.resources ? libraryRes.resources[0] : null;
    
    console.log(`📦 Cloud matched ${products.length} products.`);

    // 3. Build a fast lookup map for all available choices
    const modifierMap = {};
    if (libraryDoc && libraryDoc.modifiers) {
        Object.keys(libraryDoc.modifiers).forEach(category => {
            const currentCategoryItems = libraryDoc.modifiers[category] || [];
            currentCategoryItems.forEach(mod => {
                modifierMap[mod.id] = {
                    name: mod.name,
                    price: mod.price,
                    category: category 
                };
            });
        });
    }

    // 4. Resolve the global selection constraints rules layer
    const globalRules = libraryDoc?.customizationRules || {};

    const fallbackRules = {
        size: { min_selectable: 1, max_selectable: 1 },
        toppings: { min_selectable: 0, max_selectable: 99 }, // Expanded fallback rules
        extras: { min_selectable: 0, max_selectable: 99 },   // Expanded fallback rules
        free_sides: { min_selectable: 0, max_selectable: 99 },
        paid_sides: { min_selectable: 0, max_selectable: 99 }
    };

    // 5. Hydrate each product safely with choices and constraints
    const hydratedProducts = products.map(product => {
        const linkedIds = product.linkedModifierIds || [];
        
        // Map dynamic string ids against our compiled modifiers map definition entries
        const customizations = linkedIds
            .map(id => {
                const foundModifier = modifierMap[id];
                if (foundModifier) {
                    return {
                        id: id,
                        name: foundModifier.name, 
                        price: Number(foundModifier.price) || 0,
                        category: foundModifier.category
                    };
                }
                return null;
            })
            .filter(item => item !== null); 

        // Generate product specific constraint mapping
        const productSpecificRules = {};
        
        // Iterate through categories this item actually uses
        customizations.forEach(mod => {
            if (!productSpecificRules[mod.category]) {
                
                // =========================================================================
                // 🌟 THE HIERARCHY FIX: Check item-level rules FIRST, then global, then system defaults
                // =========================================================================
                const itemLevelRule = product.customizationRules?.[mod.category];
                const categoryRule = itemLevelRule || globalRules[mod.category] || fallbackRules[mod.category] || { min_selectable: 0, max_selectable: 99 };
                
                // Safely extract from nested arrays if Cosmos wrapped them unexpectedly
                const targetRule = Array.isArray(categoryRule) ? categoryRule[0] : categoryRule;

                const minVal = targetRule?.min_selectable !== undefined ? targetRule.min_selectable : targetRule?.min;
                const maxVal = targetRule?.max_selectable !== undefined ? targetRule.max_selectable : targetRule?.max;

                productSpecificRules[mod.category] = {
                    min_selectable: minVal !== undefined && minVal !== null ? Number(minVal) : 0,
                    max_selectable: maxVal !== undefined && maxVal !== null ? Number(maxVal) : 99
                };
            }
        });

        return {
          id: product.id,
          item: product.item,
          price: Number(product.price) || 0,
          category: product.category,
          quantity: Number(product.quantity) || 0,
          description: product.description || "",
          fileUrl: product.fileUrl,
          type: product.type || "product",
          business: product.business,
          customizations,
          customizationRules: productSpecificRules 
        };
    });

    return hydratedProducts;
}
async function natsPush(token) {
    const { nc, sc } = await initializeNATS();
    
    // 'resources' now implicitly carries the parsed name, prices, and categories inside it!
    const resources = await getData(token);
    const { device_id, name } = await getUser(token);
    
    // Publish the robust JSON packet to the Raspberry Pi subscription subject listener
    const msg = await nc.request(device_id, sc.encode(JSON.stringify({ resources, name })), { timeout: 5000 });
    console.log("📥 Received acknowledgement response back from Raspberry Pi device client:", msg);
    console.log(`📤 Published completed menu data matrix packets to NATS subject: ${device_id}`);
}

async function pushOffer(offer, token) {
  const { nc, sc } = await initializeNATS();
  const container = await initializeCosmosContainer();
  const { device_id, business } = await getUser(token)

  // Add business and type to the offer
  const offerWithBusiness = {
    ...offer,
    business: business,
    type: "offer",
  };

  console.log(offerWithBusiness);

  let offerId = offerWithBusiness.id;
  let finalResource;

  try {
    if (offerId) {
      // Try updating existing offer
      const { resource: updated } = await container
        .item(offerId, business)
        .replace(offerWithBusiness);

      finalResource = updated;
      console.log(`🔄 Updated offer with id ${offerId}`);
    } else {
      // No id provided → create new
      const { resource: created } = await container.items.create(offerWithBusiness);
      offerId = created.id;
      finalResource = created;
      console.log(`➕ Created new offer with id ${offerId}`);
    }
  } catch (err) {
    // If update fails, create new
    const { resource: created } = await container.items.create(offerWithBusiness);
    offerId = created.id;
    finalResource = created;
    console.log(`➕ Created new offer with id ${offerId} (fallback after error)`);
  }

  // ✅ Only send pure JSON (no circular refs)
  nc.publish(
    `${device_id}.offer`,
    sc.encode(JSON.stringify({ deal: finalResource, id: offerId }))
  );

  console.log(`📤 Published offer with id ${offerId} to NATS`);

  return finalResource;
}
async function getOffers(token) {
    const container = await initializeCosmosContainer()
    const { business } = await getUser(token)
    const querySpec = {
        query: "SELECT * FROM c WHERE c.business = @business AND c.type = 'offer'",
        parameters: [{ name: "@business", value: business }],
    };
    console.log('here')
    const { resources } = await container.items.query(querySpec).fetchAll();
    console.log("📌 Query results:", resources);
    return resources
}

async function natsGet(token) {
  const { nc, sc } = await initializeNATS();
  const { device_id } = await getUser(token)
  try {
    // Ask Pi for emails
    const subject = `${device_id}.request`;

    console.log(`📤 Sending request to ${subject}`);

    // send a request and wait for reply
    const msg = await nc.request(subject, sc.encode("send_emails"), { timeout: 3000 });

    // decode reply
    const data = JSON.parse(sc.decode(msg.data));
    console.log("📥 Received reply from Pi:", data);
    return data;
  } catch (err) {
    console.error("❌ NATS request failed:", err.message);
    return null;
  }
}

async function natsPurchases(token) {
  const { nc, sc } = await initializeNATS();
  console.log(token)
  const { device_id } = await getUser(token)
  try {
    // Ask Pi for emails
    const subject = `${device_id}.purchase`;

    console.log(`📤 Sending request to ${subject}`);

    // send a request and reply
    const msg = await nc.request(subject, sc.encode("get_purchases"), { timeout: 3000 });

    // decode reply
    const data = JSON.parse(sc.decode(msg.data));
    console.log(data)
    await inputPurchases(data.data, token)
    return data;
  } catch (err) {
    console.error("❌ NATS request failed:", err.message);
    return null;
  }
}


function getDayKey(ts) {
  return new Date(ts).toISOString().split("T")[0]; // YYYY-MM-DD
}

function getWeekKey(ts) {
  const date = new Date(ts);
  const oneJan = new Date(date.getFullYear(), 0, 1);
  const days = Math.floor((+date - +oneJan) / 86400000);
  const week = Math.ceil((days + oneJan.getDay() + 1) / 7);
  return `${date.getFullYear()}-W${week}`;
}

function getMonthKey(ts) {
  const date = new Date(ts);
  return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, "0")}`;
}

function getYearKey(ts) {
  const date = new Date(ts);
  return `${date.getFullYear()}`;
}

async function inputPurchases(rows, token) {
  const container = await initializeCosmosContainer();
  const { business } = await getUser(token);

  // --- 1. GROUP RAW ROWS BY STABLE ORDER ID ---
  const grouped = {};
for (const row of rows) {
  let tsValue = typeof row.timestamp === 'string' 
    ? Number(row.timestamp.split('.')[0]) 
    : Number(row.timestamp);

  const dateObj = new Date(tsValue);
  if (isNaN(dateObj.getTime())) {
    console.warn(`Skipping row with invalid timestamp: ${row.timestamp}`);
    continue;
  }

  const baseItemPrice = parseFloat(row.price || 0);
  const itemQuantity = Number(row.quantity || 1);
  const orderId = String(row.id);
  const modifications = row.customizations || [];

  if (!grouped[orderId]) {
    grouped[orderId] = {
      id: crypto.randomUUID(),
      orderId,
      email: row.email,
      timestamp: dateObj.toISOString(), 
      items: [],
      totalPrice: 0,
      business: business,
      type: "report",
    };
  }

  // 🌟 1. Tally up all customization upcharges belonging to this line item
  const modsSubtotalCost = modifications.reduce(
    (sum, mod) => sum + (parseFloat(mod.price) || 0), 
    0
  );

  // 🌟 2. Combined item package cost = Main Item + Customizations
  const fullSingleUnitCost = baseItemPrice + modsSubtotalCost;

  grouped[orderId].items.push({
    item: row.item,
    quantity: itemQuantity,
    price: fullSingleUnitCost, // Saved with mods included for item-level cost tracking
    basePrice: baseItemPrice,
    customizations: modifications 
  });

  // 🌟 3. FIXED: Accumulate total using (Main Item + Customizations) × Quantity
  grouped[orderId].totalPrice += (fullSingleUnitCost * itemQuantity);
}

  const orders = Object.values(grouped);

  // --- 2. CALCULATE REQUIRED PERIOD KEYS ---
  const requiredAggKeys = new Set();
  const orderPeriods = orders.map(order => {
    const ts = new Date(order.timestamp);
    const periods = {
      day: getDayKey(ts),
      week: getWeekKey(ts),
      month: getMonthKey(ts),
      year: getYearKey(ts),
    };
    Object.entries(periods).forEach(([type, key]) => requiredAggKeys.add(`${type}-${key}`));
    return { order, periods };
  });

  // --- 3. PRELOAD ONLY RELEVANT AGGREGATES ---
  const existingAggs = {};
  if (requiredAggKeys.size > 0) {
    const keysArray = Array.from(requiredAggKeys);
    const existingAggsQuery = await container.items
      .query({
        query: `SELECT * FROM c WHERE c.type = "timeReport" AND c.business = @business AND ARRAY_CONTAINS(@keys, c.id)`,
        parameters: [
          { name: "@business", value: business },
          { name: "@keys", value: keysArray }
        ],
      })
      .fetchAll();

    for (const agg of existingAggsQuery.resources) {
      existingAggs[agg.id] = agg;
    }
  }

  // --- 4. BUILD AGGREGATES IN MEMORY ---
  const aggregates = {};
  for (const { order, periods } of orderPeriods) {
    await container.items.create(order);

    for (const [type, periodKey] of Object.entries(periods)) {
      const aggId = `${type}-${periodKey}`;

      if (!aggregates[aggId]) {
        const existingAgg = existingAggs[aggId];
        aggregates[aggId] = existingAgg
          ? { ...existingAgg }
          : {
              id: aggId,
              periodType: type,
              period: periodKey,
              business: business,
              totalRevenue: 0,
              orderCount: 0,
              itemsSold: {},
              uniqueCustomers: [],
              processedTransactions: [],
              type: "timeReport",
            };
      }

      const agg = aggregates[aggId];
      agg.totalRevenue += order.totalPrice;
      agg.orderCount += 1;

      for (const item of order.items) {
        agg.itemsSold[item.item] = (agg.itemsSold[item.item] || 0) + item.quantity;
        
        // 🌟 OPTIONAL ANALYTICS EXTENSION: If you want to track popular mod counts inside your aggregates
        if (item.customizations && item.customizations.length > 0) {
          if (!agg.customizationsSold) agg.customizationsSold = {};
          item.customizations.forEach(mod => {
            agg.customizationsSold[mod.name] = (agg.customizationsSold[mod.name] || 0) + item.quantity;
          });
        }
      }

      if (order.email && !agg.uniqueCustomers.includes(order.email)) {
        agg.uniqueCustomers.push(order.email);
      }
      agg.processedTransactions.push(order.orderId);
    }
  }

  // --- 5. UPSERT AGGREGATES ---
  for (const aggId in aggregates) {
    try {
      await container.items.upsert(aggregates[aggId]);
    } catch (err) {
      console.error(`Failed to upsert aggregate ${aggId}`, err);
    }
  }

  await removePurchases(token);
  return "success";
}


async function removePurchases(token) {
    const { nc, sc } = await initializeNATS();
    const { device_id } = await getUser(token)
  try {
    // Ask Pi for emails
    const subject = `${device_id}.remove`;

    console.log(`📤 Sending request to ${subject}`);

    // send a request and reply
    const msg = await nc.request(subject, sc.encode("send_emails"), { timeout: 3000 });

    // decode reply
    const data = JSON.parse(sc.decode(msg.data));
    console.log("📥 Received reply from Pi:", data);
    console.log('test')
    return data;
  } catch (err) {
    console.error("❌ NATS request failed:", err.message);
    return null;
  }
}


async function getPurchases(token){
    const container = await initializeCosmosContainer()
    const { business } = await getUser(token)
    const querySpec = {
        query: "SELECT * FROM c WHERE c.business = @business AND c.type = 'report'",
        parameters: [{ name: "@business", value: business }],
    };
    const { resources } = await container.items.query(querySpec).fetchAll();
    console.log("📌 Query results:", resources);
    return resources
}

async function getReports(token){
    const container = await initializeCosmosContainer()
    const { business } = await getUser(token)
    const querySpec = {
        query: "SELECT * FROM c WHERE c.business = @business AND c.type = 'timeReport'",
        parameters: [{ name: "@business", value: business }],
    };
    const { resources } = await container.items.query(querySpec).fetchAll();
    return resources
}

async function getDrinks(token){
    const container = await initializeCosmosContainer()
    const { business } = await getUser(token)
    const querySpec = {
        query: "SELECT * FROM c WHERE c.business = @business AND c.id = 'drinks'",
        parameters: [{ name: "@business", value: business }],
    };
    const { resources } = await container.items.query(querySpec).fetchAll();
    return resources
}

async function getName(token){
    const { name } = await getUser(token)
    return name
}


async function createBusinessName(name, token) {
  const { nc, sc } = await initializeNATS();
  const { device_id, business } = await getUser(token);

  // Publish to NATS
  const subject = `${device_id}.name`;
  nc.publish(subject, sc.encode(JSON.stringify({ name })));

  const container = await initializeCosmosContainer();
  const uid = token.uid; // UID as document ID and partitionKey

  try {
    // Patch only the 'name' property
    const { resource: updatedDoc } = await container.item(uid, business).patch([
      { op: "replace", path: "/name", value: name }
    ]);

    console.log("Updated document:", updatedDoc);
    return updatedDoc;
  } catch (err) {
    if (err.code === 404) {
      // Document does not exist → create with initial structure
      const { resource: createdDoc } = await container.items.create({
        id: uid,
        business: "",      // optional default or from getUser
        device_id,
        name
      });

      console.log("Created new document:", createdDoc);
      return createdDoc;
    } else {
      throw err;
    }
  }
}
async function sendTokens(merchant_id, refresh_token, access_token, location_id, token) {
    const { nc, sc } = await initializeNATS();
    const { device_id } = await getUser(token)
    const subject = `${device_id}.token`;
    console.log(merchant_id)
    console.log(refresh_token)
    console.log(access_token)
    nc.publish(subject, sc.encode(JSON.stringify({ merchant_id, refresh_token, access_token, location_id })));
    return merchant_id
}

async function addDrinks(drinks, token) {
  const { nc, sc } = await initializeNATS();
  console.log('TOKEN')
  console.log(token)
  const { device_id, business } = await getUser(token)
  const subject = `${device_id}.drinks`;
  const container = await initializeCosmosContainer()
  const result = await container.items.upsert({id: 'drinks', drinks: drinks, business: business});
  nc.publish(subject, sc.encode(JSON.stringify({drinks: drinks})));
  console.log(result)
  return result
}



async function finishTutorial(token, section) {
  const container = await initializeCosmosContainer();
  const { business } = await getUser(token);

  // Define the staircase order
  const steps = ["window", "reports", "offers", "settings"];
  
  // Find the index of the current section being finished
  const currentIndex = steps.indexOf(section);

  try {
    const patches = [
      // 1. Set the current section to false (finished)
      {
        op: "replace",
        path: `/tutorial/${section}`,
        value: false,
      }
    ];

    // 2. OPTIONAL: If there is a NEXT step in the staircase, 
    // we ensure it is explicitly set to true (active)
    if (currentIndex !== -1 && currentIndex < steps.length - 1) {
      const nextSection = steps[currentIndex + 1];
      patches.push({
        op: "replace",
        path: `/tutorial/${nextSection}`,
        value: true,
      });
    }

    // Apply the patch to the specific document
    const { resource } = await container
      .item(token.uid, business)
      .patch(patches);

    console.log(`✅ Staircase updated: Finished ${section}.`);
    return resource;
  } catch (err) {
    console.error(`❌ Failed to patch tutorial staircase for ${section}:`, err);
    throw err;
  }
}

async function saveCode(inventoryCode, token) {
  if (!inventoryCode || inventoryCode.length !== 6) {
    throw new Error("Inventory code must be exactly 6 digits.");
  }
  const { nc, sc } = await initializeNATS();
  const { device_id, business } = await getUser(token);

  // 1️⃣ Publish update to device via NATS
  const subject = `${device_id}.inventoryCode`;
  console.log(subject)
  nc.publish(
    subject,
    sc.encode(JSON.stringify({ inventoryCode }))
  );

  // 2️⃣ Save in Cosmos
  const container = await initializeCosmosContainer();

  try {
    const { resource: updatedDoc } = await container
      .item('offlineCode', business)
      .patch([
        {
          op: "replace",
          path: "/offlineInventoryCode",
          value: inventoryCode,
        },
      ]);

    console.log("Inventory code updated:", updatedDoc);
    return updatedDoc;

  } catch (err) {

    if (err.code === 404) {
      // Create new document if missing
      const { resource: createdDoc } = await container.items.create({
        id: 'offlineCode',
        business,
        offlineInventoryCode: inventoryCode,
        createdAt: new Date().toISOString(),
      });

      console.log("Inventory code document created:", createdDoc);
      return createdDoc;
    }

    throw err;
  }
}

async function saveTempAuth(state, accessToken, locationId) {
  try {
    const container = await initializeCosmosContainer();
    await container.items.upsert({
        id: state,           // We use the 'state' as the lookup ID
        accessToken,
        locationId,
        ttl: 300,
        business: "SQUARE_AUTH"            // Automatically deletes after 5 minutes (security!)
    });
  } catch(err) {
    console.log(err)
  }
}

async function getTempAuth(state) {
  const container = await initializeCosmosContainer();
  const { resource: authData } = await container.item(state, "SQUARE_AUTH").read();
  console.log(authData)
  return authData
}

async function sendSecretAuth(secret, token, clientId) {
  try {
    const { nc, sc } = await initializeNATS();
    const { device_id } = await getUser(token);

  // 1️⃣ Publish update to device via NATS
  const subject = `${device_id}.secretAuth`;
  nc.publish(
    subject,
    sc.encode(JSON.stringify({ secret, clientId }))
  );
  return 'success'
  } catch(err) {
    console.log(err)
  }
}

async function sendEmailVerification(email, device_id) {
  try {
    const { nc, sc } = await initializeNATS();

  // 1️⃣ Publish update to device via NATS
  const subject = `${device_id}.emailVerification`;
  nc.publish(
    subject,
    sc.encode(JSON.stringify({ email }))
  );
  return 'success'
  } catch(err) {
    console.log(err)
  }
}

// ==========================================
// DB Helper: Get Customizations Library (With Rules Support)
// ==========================================
async function getCustomizationsLibrary(userData) {
  const cosmosContainer = await initializeCosmosContainer();
  const businessId = userData.business;
  const libraryId = `modifiers_lib_${businessId}`;

  try {
    const { resource } = await cosmosContainer.item(libraryId, businessId).read();
    
    if (resource) {
      // Return both the modifiers collection AND the dynamic selection constraints
      return {
        modifiers: resource.modifiers || { size: [], toppings: [], extras: [], free_sides: [], paid_sides: [] },
        customizationRules: resource.customizationRules || {
          size: { min_selectable: 1, max_selectable: 1 },
          toppings: { min_selectable: 0, max_selectable: 3 },
          extras: { min_selectable: 0, max_selectable: 1 },
          free_sides: { min_selectable: 0, max_selectable: 1 },
          paid_sides: { min_selectable: 0, max_selectable: 1 }
        }
      };
    }
  } catch (readErr) {
    console.log(`ℹ️ Initializing fresh customizations library document context for truck: ${businessId}`);
  }

  // Fallback defaults if no document exists yet inside your Cosmos Container
  return { 
    modifiers: { size: [], toppings: [], extras: [], free_sides: [], paid_sides: [] },
    customizationRules: {
      size: { min_selectable: 1, max_selectable: 1 },
      toppings: { min_selectable: 0, max_selectable: 3 },
      extras: { min_selectable: 0, max_selectable: 1 },
      free_sides: { min_selectable: 0, max_selectable: 1 },
      paid_sides: { min_selectable: 0, max_selectable: 1 }
    }
  };
}

// ==========================================
// DB Helper: Save Customizations Library (With Rules Support)
// ==========================================
async function saveCustomizationsLibrary(incomingModifiers, incomingRules, userData) {
  const cosmosContainer = await initializeCosmosContainer();
  
  // 1. Force strict string normalization on the business identifier
  const { business } = await getUser(userData);
  const businessId = String(business).trim();
  if (!businessId || businessId.toLowerCase() === "undefined") {
    throw new Error("❌ Security Exception: Cannot save customization library without a valid business partition context.");
  }

  // =========================================================================
  // 🌟 DESIGN PIVOT: ONE STATIC MASTER DOCUMENT PER BUSINESS PARTITION
  // No timestamps appended to the ID means no historical duplicate clutter!
  // =========================================================================
  const libraryId = `modifiers_lib_${businessId}`;
  console.log(`🔒 Synchronizing Master Multi-Tenant Document. ID: ${libraryId} | Partition: ${businessId}`);

  // 2. Deduplication Engine: Keep modifier items inside arrays strictly unique by name
  const cleanModifiers = {};
  if (incomingModifiers && typeof incomingModifiers === 'object') {
    Object.keys(incomingModifiers).forEach((categoryKey) => {
      const initialArray = incomingModifiers[categoryKey] || [];
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
  } else {
    cleanModifiers = incomingModifiers;
  }

  const defaultRules = incomingRules || {
    size: { min_selectable: 1, max_selectable: 1 },
    toppings: { min_selectable: 0, max_selectable: 99 },
    extras: { min_selectable: 0, max_selectable: 99 },
    free_sides: { min_selectable: 0, max_selectable: 99 },
    paid_sides: { min_selectable: 0, max_selectable: 99 }
  };

  // 3. Assemble our single master document payload
  const masterLibDoc = {
    id: libraryId,                         // Lock static target pointer key row
    type: "modifier_library",
    business: businessId,                  // Partition Key property matching your container structure
    last_updated: new Date().toISOString(),
    modifiers: cleanModifiers,
    customizationRules: incomingRules || defaultRules 
  };
  
  // =========================================================================
  // 🌟 CRITICAL ATOMIC SWITCH: USE UPSERT INSTEAD OF CREATE
  // This automatically inserts if missing, or overwrites seamlessly if present!
  // =========================================================================
  const { resource } = await cosmosContainer.items.upsert(masterLibDoc, { partitionKey: businessId });
  return resource;
}

module.exports = { getCustomizationsLibrary, saveCustomizationsLibrary, sendEmailVerification, sendSecretAuth, getTempAuth, saveTempAuth, addDrinks, sendTokens, createBusinessName, getReports, getPurchases, natsPurchases, pushOffer, addItem, natsPush, getData, updateItem, deleteItem, natsGet, getOffers, getName, getDrinks, getUser, finishTutorial, saveCode }
