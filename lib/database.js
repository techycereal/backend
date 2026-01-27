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

// Initialize NATS connection once
async function initializeNATS() {
  if (!natsConnection) {
    natsConnection = await connect({ servers: natsServer });
    natsCodec = StringCodec();
    console.log("✅ Connected to NATS");
  }
  return { nc: natsConnection, sc: natsCodec };
}

async function addItem(item, userData) {
  const container = await initializeCosmosContainer()
  item.type = 'product'
  console.log(userData)
  item.business = userData.business
  const result = await container.items.create(item);
  console.log(result.resource)
  return result.resource
}


async function deleteItem(id, userData) {
  const container = await initializeCosmosContainer();
  try {
    // Delete Cosmos DB item
    await container.item(id, userData.business).delete();
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

async function getData(token) {
    const container = await initializeCosmosContainer()
    const { business } = await getUser(token)
    console.log(business)
    const querySpec = {
        query: "SELECT * FROM c WHERE c.business = @business AND c.type = 'product'",
        parameters: [{ name: "@business", value: business }],
    };
    const { resources } = await container.items.query(querySpec).fetchAll();
    console.log("📌 Query results:", resources);
    return resources
}

async function updateItem(id, updatedItem, token) {
  const container = await initializeCosmosContainer();
  const { business } = await getUser(token)
  try {
    // Replace the existing item
    const { resource } = await container.item(id, business).replace({
      ...updatedItem, // updated fields
      id,             // keep the same id
      business        // keep the same partition key
    });
    console.log(`✅ Item updated: ${resource.id}`);
    return resource;
  } catch (err) {
    console.error("❌ Failed to update item:", err);
    throw err;
  }
}



async function natsPush(token) {
    const {nc, sc} = await initializeNATS()
    const resources = await getData(token)
    const { device_id } = await getUser(token)
    nc.publish(device_id, sc.encode(JSON.stringify(resources)));
    console.log("📤 Published query results to NATS on subject device.123");

    // Close NATS connection gracefully
    await nc.drain();
    console.log("✅ NATS connection closed");
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

  await nc.drain();
  console.log("✅ NATS connection closed");

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
  } finally {
    // Close NATS connection gracefully
    await nc.drain();
    console.log("✅ NATS connection closed");
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
  } finally {
    // Close NATS connection gracefully
    await nc.drain();
    console.log("✅ NATS connection closed");
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
  console.log("Rows received:", rows);

  const container = await initializeCosmosContainer();
  const { business } = await getUser(token)
  // --- 1. GROUP RAW ROWS BY STABLE ORDER ID ---
  const grouped = {};
  for (const row of rows) {
    row.timestamp = new Date(Number(row.timestamp.split('.')[0]));

    console.log(row.timestamp)
    const orderId = String(row.id); // stable business ID
    if (!grouped[orderId]) {
      grouped[orderId] = {
        id: crypto.randomUUID(), // random Cosmos PK
        orderId,
        email: row.email,
        timestamp: row.timestamp,
        items: [],
        totalPrice: 0,
        business: business,
        type: "report",
      };
    }

    grouped[orderId].items.push({
      item: row.item,
      quantity: row.quantity,
      price: row.price,
    });

    grouped[orderId].totalPrice += row.price; // still cents
  }

  const orders = Object.values(grouped);

  // --- 2. PRELOAD EXISTING AGGREGATES ---
  const existingAggsQuery = await container.items
  .query({
    query: `SELECT * FROM c WHERE c.type = "timeReport" AND c.business = @business`,
    parameters: [{ name: "@business", value: business }],
  })
  .fetchAll();
  const existingAggs = {};
  for (const agg of existingAggsQuery.resources) {
    existingAggs[agg.id] = agg;
  }

  // --- 3. BUILD AGGREGATES IN MEMORY ---
  const aggregates = {};

  for (const order of orders) {

    // Save raw order
    await container.items.create(order);

    const ts = order.timestamp;
    const periods = {
      day: getDayKey(ts),
      week: getWeekKey(ts),
      month: getMonthKey(ts),
      year: getYearKey(ts),
    };

    for (const [type, periodKey] of Object.entries(periods)) {
      const aggId = `${type}-${periodKey}`;

      // Start with existing aggregate if it exists
      const existingAgg = existingAggs[aggId];
      if (!aggregates[aggId]) {
        aggregates[aggId] = existingAgg
          ? { ...existingAgg }
          : {
              id: aggId,
              periodType: type,
              period: periodKey,
              business: order.business,
              totalRevenue: 0,
              orderCount: 0,
              itemsSold: {},
              uniqueCustomers: [],
              processedTransactions: [],
              type: "timeReport",
            };
      }

      const agg = aggregates[aggId];

      // Update totals
      agg.totalRevenue += order.totalPrice; // still cents
      agg.orderCount += 1;

      for (const item of order.items) {
        agg.itemsSold[item.item] =
          (agg.itemsSold[item.item] || 0) + item.quantity;
      }

      if (order.email && !agg.uniqueCustomers.includes(order.email)) {
        agg.uniqueCustomers.push(order.email);
      }

      agg.processedTransactions.push(order.orderId);
    }
  }

  // --- 4. UPSERT AGGREGATES ---
  for (const aggId in aggregates) {
    const agg = aggregates[aggId];

    try {
      await container.items.upsert(agg);
      console.log(`✅ Updated aggregate ${agg.id}`);
    } catch (err) {
      console.error(`❌ Failed to upsert aggregate ${agg.id}`, err);
    }
  }

  // --- 5. REMOVE RAW PURCHASES ---
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
  } finally {
    // Close NATS connection gracefully
    await nc.drain();
    console.log("✅ NATS connection closed");
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



async function finishTutorial(token) {
  const container = await initializeCosmosContainer();
  const { business } = await getUser(token);
  // tutorialItemId = the actual id of the tutorial document

  try {
    // Update only the tutorial field
    const { resource } = await container
      .item(token.uid, business)
      .patch([
        {
          op: "replace",       // operation type
          path: "/tutorial",   // only update the 'tutorial' field
          value: {
            window: false,
            offers: false,
            reports: false,
            settings: false,
          },
        },
      ]);

    console.log(`✅ Tutorial field updated: ${resource.id}`);
    return resource;
  } catch (err) {
    console.error("❌ Failed to update tutorial field:", err);
    throw err;
  }
}



module.exports = { addDrinks, sendTokens, createBusinessName, getReports, getPurchases, natsPurchases, pushOffer, addItem, natsPush, getData, updateItem, deleteItem, natsGet, getOffers, getName, getDrinks, getUser, finishTutorial }
