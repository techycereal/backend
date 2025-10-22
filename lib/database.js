const { CosmosClient,  } = require("@azure/cosmos");
const { connect, StringCodec } = require("nats");
const dotenv = require('dotenv')
dotenv.config()

// Cosmos DB credentials
const endpoint = process.env.COSMOS_URI;
const key = process.env.COSMOS_KEY;
const client = new CosmosClient({ endpoint, key });

// NATS server (no authentication)
const natsServer = process.env.FQDN;

const databaseId = "cosmicworks";
const containerId = "businesses";


async function nats() {
    // Connect to NATS
    const nc = await connect({ servers: natsServer });
    const sc = StringCodec();
    console.log("✅ Connected to NATS");
    return {nc, sc}
}


async function connectDatabase() {
    const { database } = await client.databases.createIfNotExists({ id: databaseId });
    console.log(`✅ Database ready: ${database.id}`);

    // Ensure container exists
    const { container } = await database.containers.createIfNotExists({
    id: containerId,
    partitionKey: { paths: ["/business"] },
    });
    console.log(`✅ Container ready: ${container.id}`);
    return container
}

async function addItem(item) {
  const container = await connectDatabase()
  item.type = 'product'
  const result = await container.items.create(item);
  console.log(result.resource)
  return result.resource
}

async function deleteItem(id, business, fileUrl) {
  const container = await connectDatabase();
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
  const container = await connectDatabase();
  console.log(token.uid)
  const querySpec = {
    query: "SELECT c.device_id FROM c WHERE c.id = @id",
    parameters: [{ name: "@id", value: token.uid }],
  };

  const { resources } = await container.items.query(querySpec).fetchAll();
  console.log("📌 Query results:", resources);
  const { device_id } = resources[0]
  console.log(device_id)
  return device_id;
}

async function getData() {
    const container = await connectDatabase()
    const querySpec = {
        query: "SELECT * FROM c WHERE c.business = @business AND c.type = 'product'",
        parameters: [{ name: "@business", value: "Alex" }],
    };
    const { resources } = await container.items.query(querySpec).fetchAll();
    console.log("📌 Query results:", resources);
    return resources
}

async function updateItem(id, business, updatedItem) {
  const container = await connectDatabase();
  
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
    const {nc, sc} = await nats()
    const resources = await getData()
    const id = await getUser(token)
    nc.publish(id, sc.encode(JSON.stringify(resources)));
    console.log("📤 Published query results to NATS on subject device.123");

    // Close NATS connection gracefully
    await nc.drain();
    console.log("✅ NATS connection closed");
}

async function pushOffer(offer) {
  const { nc, sc } = await nats();
  const container = await connectDatabase();

  // Add business and type to the offer
  const offerWithBusiness = {
    ...offer,
    business: "Alex",
    type: "offer"
  };
  console.log(offerWithBusiness)
  let offerId = offerWithBusiness.id;
  let finalResource
  if (offerId) {
    try {
      if (offerId) {
        // Update existing offer
        const offer = await container.item(offerId, 'Alex').replace(offerWithBusiness);
        finalResource = offer
        console.log(`🔄 Updated offer with id ${offerId}`);
      } else {
        // ID provided but not found → create new
        const { resource: created } = await container.items.create(offerWithBusiness);
        offerId = created.id;
        finalResource = created
        console.log(`➕ Created new offer with id ${offerId}`);
      }
    } catch (err) {
      // Read failed → create new
      const { resource: created } = await container.items.create(offerWithBusiness);
      offerId = created.id;
      finalResource = created
      console.log(`➕ Created new offer with id ${offerId} (read failed)`);
    }
  } else {
    // No id → create new
    const { resource: created } = await container.items.create(offerWithBusiness);
    finalResource = created
    offerId = created.id;
    console.log(`➕ Created new offer with id ${offerId}`);
  }

  // Publish to NATS
  nc.publish(
    "device.123.offer",
    sc.encode(JSON.stringify({ deal: offerWithBusiness, id: offerId }))
  );
  console.log(`📤 Published offer with id ${offerId} to NATS`);

  await nc.drain();
  console.log("✅ NATS connection closed");
  return finalResource
}

async function getOffers() {
    const container = await connectDatabase()
    const querySpec = {
        query: "SELECT * FROM c WHERE c.business = @business AND c.type = 'offer'",
        parameters: [{ name: "@business", value: "Alex" }],
    };
    console.log('here')
    const { resources } = await container.items.query(querySpec).fetchAll();
    console.log("📌 Query results:", resources);
    return resources
}


async function natsGet() {
  const { nc, sc } = await nats();

  try {
    // Ask Pi for emails
    const subject = "device.123.request";

    console.log(`📤 Sending request to ${subject}`);

    // send a request and wait for reply
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

async function natsPurchases() {
  const { nc, sc } = await nats();

  try {
    // Ask Pi for emails
    const subject = "device.123.purchase";

    console.log(`📤 Sending request to ${subject}`);

    // send a request and wait for reply
    const msg = await nc.request(subject, sc.encode("get_purchases"), { timeout: 3000 });

    // decode reply
    const data = JSON.parse(sc.decode(msg.data));
    await inputPurchases(data)
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

// --- Main Aggregation ---
async function inputPurchases(items) {
  const container = await connectDatabase();

  // all aggregates batched here
  const aggregates = {};

  for (let i in items.data) {
    const purchase = items.data[i];
    purchase.type = "report";
    purchase.business = "Alex";

    // ✅ Skip if this transaction already exists
    const { resource: existing } = await container
      .item(purchase.id, purchase.id)
      .read()
      .catch(() => ({ resource: null }));

    if (existing) {
      console.log(`⚠️ Skipping duplicate purchase ${purchase.id}`);
      continue;
    }

    // Save raw transaction
    console.log(purchase)
    await container.items.create(purchase);

    // Use timestamp from first item
    const firstItem = purchase.items[0];
    const key = Object.keys(firstItem)[0];
    const ts = firstItem[key].timestamp;

    // Build period keys
    const periods = {
      day: getDayKey(ts),
      week: getWeekKey(ts),
      month: getMonthKey(ts),
      year: getYearKey(ts),
    };

    // Apply purchase to all period aggregates
    for (const [type, keyVal] of Object.entries(periods)) {
      const aggId = `${type}-${keyVal}`;

      if (!aggregates[aggId]) {
        aggregates[aggId] = {
          id: aggId,
          periodType: type,     // day | week | month | year
          period: keyVal,       // actual string like 2025-09-08, 2025-W36
          business: purchase.business,
          totalRevenue: 0,
          orderCount: 0,
          itemsSold: {},
          uniqueCustomers: [],
          processedTransactions: []
        };
      }

      const aggDoc = aggregates[aggId];

      // Revenue + orders
      aggDoc.totalRevenue += purchase.price / 100;
      aggDoc.orderCount += 1;

      // Items sold
      purchase.items.forEach((obj) => {
        const k = Object.keys(obj)[0];
        const entry = obj[k];
        aggDoc.itemsSold[entry.item] =
          (aggDoc.itemsSold[entry.item] || 0) + entry.quantity;
      });

      // Unique customers
      if (!aggDoc.uniqueCustomers.includes(purchase.email)) {
        aggDoc.uniqueCustomers.push(purchase.email);
      }

      // Track transaction
      aggDoc.processedTransactions.push(purchase.id);
    }
  }

  // --- Save all aggregates ---
  for (const aggId in aggregates) {
    const aggDoc = aggregates[aggId];

    // Check existing doc
    const { resource: existingAgg } = await container
      .item(aggDoc.id, aggDoc.id)
      .read()
      .catch(() => ({ resource: null }));

    if (existingAgg) {
      // Merge old + new
      aggDoc.totalRevenue += existingAgg.totalRevenue;
      aggDoc.orderCount += existingAgg.orderCount;

      for (const item in existingAgg.itemsSold) {
        aggDoc.itemsSold[item] =
          (aggDoc.itemsSold[item] || 0) + existingAgg.itemsSold[item];
      }

      aggDoc.uniqueCustomers = Array.from(
        new Set([...existingAgg.uniqueCustomers, ...aggDoc.uniqueCustomers])
      );

      aggDoc.processedTransactions = Array.from(
        new Set([
          ...existingAgg.processedTransactions,
          ...aggDoc.processedTransactions
        ])
      );
    }

    await container.items.upsert(aggDoc);
    console.log(`✅ Updated aggregate ${aggDoc.id}`);
  }

  await removePurchases()

  return "success";
}

async function removePurchases() {
    const { nc, sc } = await nats();

  try {
    // Ask Pi for emails
    const subject = "device.123.remove";

    console.log(`📤 Sending request to ${subject}`);

    // send a request and wait for reply
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


async function getPurchases(){
    const container = await connectDatabase()
    const querySpec = {
        query: "SELECT * FROM c WHERE c.business = @business AND c.type = 'report'",
        parameters: [{ name: "@business", value: "Alex" }],
    };
    const { resources } = await container.items.query(querySpec).fetchAll();
    console.log("📌 Query results:", resources);
    return resources
}

async function getReports(){
    const container = await connectDatabase()
    const querySpec = {
        query: "SELECT * FROM c WHERE c.business = @business AND c.type = 'timeReport'",
        parameters: [{ name: "@business", value: "Alex" }],
    };
    const { resources } = await container.items.query(querySpec).fetchAll();
    return resources
}

async function createBusinessName(name) {
    const { nc, sc } = await nats();
    const subject = "device.123.name";
    nc.publish(subject, sc.encode(JSON.stringify({ name })));
    const container = await connectDatabase()
    const result = await container.items.upsert({id: 'Alex', name: name});
    console.log(result.resource)
    return result.resource
}

async function sendTokens(merchant_id, refresh_token, access_token, location_id) {
    const { nc, sc } = await nats();
    const subject = "device.123.token";
    console.log(merchant_id)
    console.log(refresh_token)
    console.log(access_token)
    nc.publish(subject, sc.encode(JSON.stringify({ merchant_id, refresh_token, access_token, location_id })));
    return merchant_id
}



module.exports = { sendTokens, createBusinessName, getReports, getPurchases, natsPurchases, pushOffer, addItem, natsPush, getData, updateItem, deleteItem, natsGet, getOffers }