"use strict";
const mongoose = require("mongoose");
const pLimit = require("p-limit");

// In-memory cache for geocoding results
const geocodeCache = new Map();

// Define the TestRecord model for the testrecords collection
const TestRecord = mongoose.model(
  "LicenseRecord",
  new mongoose.Schema(
    {
      business_address: String,
      location: {
        type: {
          type: String,
          enum: ["Point"],
        },
        coordinates: {
          type: [Number], // [longitude, latitude]
        },
      },
    },
    {
      collection: "testrecords", // Explicitly specify collection name
    },
  ),
);

// Helper to geocode an address using OpenStreetMap Nominatim
async function geocodeAddress(address) {
  // Check cache first
  if (geocodeCache.has(address)) {
    return geocodeCache.get(address);
  }

  const url = `https://nominatim.openstreetmap.org/search?format=json&q=${encodeURIComponent(
    address,
  )}&limit=1`;
  try {
    const response = await fetch(url, {
      headers: {
        "User-Agent": "CanojaScrapper/1.0 alimuradbukhari12345@gmail.com",
        Accept: "application/json",
      },
    });

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}: ${response.statusText}`);
    }

    const data = await response.json();
    let result = null;

    if (data && data.length > 0) {
      result = {
        lat: parseFloat(data[0].lat),
        lng: parseFloat(data[0].lon),
      };
    }

    // Cache the result (including null results to avoid retrying)
    geocodeCache.set(address, result);
    return result;
  } catch (err) {
    console.warn(
      `Geocoding failed for address: ${address}. Error: ${err.message}`,
    );
    // Cache null result to avoid retrying failed addresses
    geocodeCache.set(address, null);
    return null;
  }
}

// Process records in batches with bulk operations
async function processBatch(records) {
  const bulkOps = [];

  for (const record of records) {
    try {
      const coords = await geocodeAddress(record.business_address);
      if (coords) {
        bulkOps.push({
          updateOne: {
            filter: { _id: record._id },
            update: {
              $set: {
                location: {
                  type: "Point",
                  coordinates: [coords.lng, coords.lat],
                },
              },
            },
          },
        });
        console.log(`Geocoded ${record._id}: ${coords.lat}, ${coords.lng}`);
      } else {
        console.warn(
          `Could not geocode address for record ${record._id}: ${record.business_address}`,
        );
      }
    } catch (err) {
      console.error(`Error processing record ${record._id}:`, err);
    }
  }

  // Bulk update all successful geocodes
  if (bulkOps.length > 0) {
    await TestRecord.bulkWrite(bulkOps, { ordered: false });
    console.log(`Bulk updated ${bulkOps.length} records`);
  }
}

// Add delay between batches to respect rate limits
function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function main() {
  // Connect to the cannabis_licenses database
  await mongoose.connect(
    process.env.MONGODB_URI || "mongodb://localhost:27017/cannabis_licenses",
    {
      useNewUrlParser: true,
      useUnifiedTopology: true,
    },
  );

  console.log("Connected to cannabis_licenses database");

  // Query for ALL records that have a business_address and don't have location data
  const query = {
    business_address: { $exists: true, $ne: "" },
    $or: [
      { location: { $exists: false } },
      { location: null },
      { "location.coordinates": { $exists: false } },
      { "location.coordinates": null },
      { "location.coordinates": [] },
    ],
  };

  // Get total count first
  const totalCount = await TestRecord.countDocuments(query);
  console.log(
    `Found ${totalCount} records to geocode in testrecords collection.`,
  );

  if (totalCount === 0) {
    console.log("No records found to geocode. Exiting.");
    await mongoose.disconnect();
    return;
  }

  const BATCH_SIZE = 100;
  const CONCURRENCY = 10;
  const DELAY_BETWEEN_BATCHES = 1000; // 1 second delay between batches

  let processed = 0;
  const limit = pLimit(CONCURRENCY);

  // Process in batches using cursor for memory efficiency
  const cursor = TestRecord.find(query)
    .select("_id business_address location")
    .lean() // Use lean() for better performance
    .cursor();

  let batch = [];
  const batchPromises = [];

  for (
    let record = await cursor.next();
    record != null;
    record = await cursor.next()
  ) {
    batch.push(record);

    if (batch.length >= BATCH_SIZE) {
      // Process current batch
      const currentBatch = [...batch];
      batchPromises.push(
        limit(async () => {
          await processBatch(currentBatch);
          processed += currentBatch.length;
          console.log(
            `Progress: ${processed}/${totalCount} (${(
              (processed / totalCount) *
              100
            ).toFixed(1)}%)`,
          );
        }),
      );

      batch = [];

      // Add delay every few batches to respect rate limits
      if (batchPromises.length % 5 === 0) {
        await delay(DELAY_BETWEEN_BATCHES);
      }
    }
  }

  // Process remaining records
  if (batch.length > 0) {
    batchPromises.push(
      limit(async () => {
        await processBatch(batch);
        processed += batch.length;
        console.log(
          `Progress: ${processed}/${totalCount} (${(
            (processed / totalCount) *
            100
          ).toFixed(1)}%)`,
        );
      }),
    );
  }

  // Wait for all batches to complete
  await Promise.allSettled(batchPromises);

  await mongoose.disconnect();
  console.log(
    `Done. Processed ${processed} records. Cache size: ${geocodeCache.size}`,
  );
}

if (require.main === module) {
  main().catch(console.error);
}
