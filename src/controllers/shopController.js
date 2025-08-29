const axios = require("axios");
const LicenseRecord = require("../models/LicenseRecord");

// Configuration
const GOOGLE_MAPS_API_KEY = process.env.GOOGLE_API_KEY;
const searchQueries = [
  "Smoke Shop",
  "Cannabis retailer",
  "Cannabis distributor",
  "Cannabis manufacturer",
  "Cannabis cultivator",
];

// --- Get Place Details for additional info ---
async function getPlaceDetails(placeId) {
  try {
    const fields =
      "place_id,name,formatted_address,geometry,rating,user_ratings_total,price_level,types,opening_hours,photos,business_status";
    const url = `https://maps.googleapis.com/maps/api/place/details/json?place_id=${placeId}&fields=${fields}&key=${GOOGLE_MAPS_API_KEY}`;

    const response = await axios.get(url);
    if (response.data.result) {
      return response.data.result;
    }
    return null;
  } catch (error) {
    console.error(
      `Error fetching place details for ${placeId}:`,
      error.message,
    );
    return null;
  }
}

// --- Get photo URL from photo reference ---
function getPhotoUrl(photoReference, maxWidth = 400) {
  if (!photoReference) return null;
  return `https://maps.googleapis.com/maps/api/place/photo?maxwidth=${maxWidth}&photo_reference=${photoReference}&key=${GOOGLE_MAPS_API_KEY}`;
}

// --- Fetch Nearby Shops from Google with enhanced data ---
async function getNearbyShops(lat, lng, radius) {
  let results = [];

  for (const query of searchQueries) {
    const url = `https://maps.googleapis.com/maps/api/place/nearbysearch/json?location=${lat},${lng}&radius=${radius}&keyword=${encodeURIComponent(
      query,
    )}&key=${GOOGLE_MAPS_API_KEY}`;
    const response = await axios.get(url);

    if (response.data.results) {
      // Add smoke_shop flag based on the search query
      const queryResults = response.data.results.map((place) => ({
        ...place,
        smoke_shop: query === "Smoke Shop",
      }));
      results = results.concat(queryResults);
    }
  }

  // Remove duplicates by place_id, keeping smoke_shop as true if any instance had it
  const uniqueResults = Object.values(
    results.reduce((acc, place) => {
      if (acc[place.place_id]) {
        // If this place already exists, keep smoke_shop as true if either instance has it
        acc[place.place_id].smoke_shop =
          acc[place.place_id].smoke_shop || place.smoke_shop;
      } else {
        acc[place.place_id] = place;
      }
      return acc;
    }, {}),
  );

  // Enhance each result with detailed information
  const enhancedResults = [];
  console.log(
    `Fetching detailed info for ${uniqueResults.length} unique places...`,
  );

  for (let i = 0; i < uniqueResults.length; i++) {
    const place = uniqueResults[i];
    console.log(
      `Fetching details for place ${i + 1}/${uniqueResults.length}: ${place.name}`,
    );

    const placeDetails = await getPlaceDetails(place.place_id);

    if (placeDetails) {
      // Combine basic place data with detailed data
      const enhancedPlace = {
        ...place,
        // Override with more detailed info from place details
        name: placeDetails.name || place.name,
        formatted_address: placeDetails.formatted_address || place.vicinity,
        geometry: placeDetails.geometry || place.geometry,
        rating: placeDetails.rating || place.rating,
        user_ratings_total:
          placeDetails.user_ratings_total || place.user_ratings_total,
        price_level: placeDetails.price_level || place.price_level,
        types: placeDetails.types || place.types,
        business_status: placeDetails.business_status,

        // New enhanced data
        opening_hours: placeDetails.opening_hours
          ? {
              open_now: placeDetails.opening_hours.open_now || false,
              periods: placeDetails.opening_hours.periods || [],
              weekday_text: placeDetails.opening_hours.weekday_text || [],
            }
          : {
              open_now: null,
              periods: [],
              weekday_text: [],
            },

        // Photos - get the first photo if available
        photo_url:
          placeDetails.photos && placeDetails.photos.length > 0
            ? getPhotoUrl(placeDetails.photos[0].photo_reference, 400)
            : null,

        // Additional photo references for multiple images if needed
        photos: placeDetails.photos
          ? placeDetails.photos.map((photo) => ({
              photo_reference: photo.photo_reference,
              height: photo.height,
              width: photo.width,
              photo_url: getPhotoUrl(photo.photo_reference, 400),
            }))
          : [],
      };

      enhancedResults.push(enhancedPlace);
    } else {
      // If place details fetch failed, use original data with null values for new fields
      enhancedResults.push({
        ...place,
        business_status: null,
        opening_hours: {
          open_now: null,
          periods: [],
          weekday_text: [],
        },
        photo_url: null,
        photos: [],
      });
    }

    // Add a small delay to avoid hitting rate limits
    await new Promise((resolve) => setTimeout(resolve, 100));
  }

  console.log(
    `Enhanced ${enhancedResults.length} places with detailed information`,
  );
  return enhancedResults;
}

// --- Haversine Distance (meters) ---
function haversineDistance(lat1, lon1, lat2, lon2) {
  const R = 6371e3;
  const toRad = (x) => (x * Math.PI) / 180;
  const phi1 = toRad(lat1);
  const phi2 = toRad(lat2);
  const deltaPhi = toRad(lat2 - lat1);
  const deltaLambda = toRad(lon2 - lon1);

  const a =
    Math.sin(deltaPhi / 2) ** 2 +
    Math.cos(phi1) * Math.cos(phi2) * Math.sin(deltaLambda / 2) ** 2;
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));

  return R * c;
}

// --- Normalize names for fuzzy compare ---
function normalizeName(name) {
  if (!name) return "";
  return name.toLowerCase().replace(/[^a-z0-9]/g, "");
}

// Compare shops endpoint
async function compareShops(req, res) {
  try {
    const { lat, lng, radius = 5000 } = req.body;

    console.log(`\n=== DEBUG: Starting comparison ===`);
    console.log(
      `Request coordinates: lat=${lat}, lng=${lng}, radius=${radius}`,
    );

    // Fetch from Google with detailed logging
    console.log("\n--- Fetching from Google Maps ---");
    const googleShops = await getNearbyShops(lat, lng, radius);
    console.log(`Found ${googleShops.length} Google shops with enhanced data`);

    // Log first few Google shops for debugging
    googleShops.slice(0, 3).forEach((shop, i) => {
      console.log(
        `Google Shop ${i + 1}: "${shop.name}" at (${
          shop.geometry.location.lat
        }, ${shop.geometry.location.lng}) - Smoke Shop: ${
          shop.smoke_shop
        }, Open Now: ${shop.opening_hours.open_now}, Has Photo: ${
          shop.photo_url ? "Yes" : "No"
        }`,
      );
    });

    // Fetch from your DB with detailed logging
    console.log("\n--- Fetching from Database ---");
    const govRecords = await LicenseRecord.find({
      "location.coordinates": { $exists: true, $ne: [] },
      "location.coordinates.0": { $exists: true },
      "location.coordinates.1": { $exists: true },
    });

    console.log(
      `Found ${govRecords.length} government records with coordinates`,
    );

    // Log first few DB records for debugging
    govRecords.slice(0, 3).forEach((record, i) => {
      const [dbLng, dbLat] = record.location.coordinates;
      console.log(
        `DB Record ${i + 1}: "${
          record.business_name
        }" at (${dbLat}, ${dbLng}) - License Status: ${record.license_status}`,
      );
    });

    const matches = [];
    let totalComparisons = 0;
    let validComparisons = 0;

    console.log("\n--- Starting Matching Process ---");

    // Create a map to store license status for each Google shop
    const googleShopStatusMap = new Map();

    for (const shop of googleShops) {
      const gLat = shop.geometry.location.lat;
      const gLng = shop.geometry.location.lng;

      // Initialize all shops with no license status (not found in DB)
      googleShopStatusMap.set(shop.place_id, {
        license_status: null,
        matchedRecord: null,
      });

      for (const record of govRecords) {
        totalComparisons++;

        // Check if coordinates exist and are valid
        if (
          !record.location ||
          !record.location.coordinates ||
          record.location.coordinates.length < 2
        ) {
          console.log(
            `Skipping record ${record.business_name}: No valid coordinates`,
          );
          continue;
        }

        const [dbLng, dbLat] = record.location.coordinates;

        // Skip if coordinates are invalid
        if (!dbLat || !dbLng || isNaN(dbLat) || isNaN(dbLng)) {
          console.log(
            `Skipping record ${record.business_name}: Invalid coordinates (${dbLat}, ${dbLng})`,
          );
          continue;
        }

        validComparisons++;
        const distance = haversineDistance(gLat, gLng, dbLat, dbLng);

        // Log every comparison for debugging (limit to first 10 to avoid spam)
        if (validComparisons <= 10) {
          console.log(
            `Comparing "${shop.name}" (${gLat}, ${gLng}) with "${
              record.business_name
            }" (${dbLat}, ${dbLng}) - Distance: ${Math.round(distance)}m`,
          );
        }

        // First check distance (within 100m)
        if (distance <= 100) {
          console.log(`Distance match found: ${Math.round(distance)}m`);
          console.log(`   Google: "${shop.name}" at (${gLat}, ${gLng})`);
          console.log(
            `   DB: "${record.business_name}" at (${dbLat}, ${dbLng})`,
          );

          // Now check name similarity - normalize names first
          const googleName = normalizeName(shop.name);
          const dbBusinessName = normalizeName(record.business_name);
          const dbDbaName = normalizeName(record.dba);

          console.log(`   Normalized Google name: "${googleName}"`);
          console.log(`   Normalized DB business name: "${dbBusinessName}"`);
          console.log(`   Normalized DB DBA name: "${dbDbaName}"`);

          // Check if Google name matches either business_name or dba
          const businessNameMatch =
            googleName.includes(dbBusinessName) ||
            dbBusinessName.includes(googleName);
          const dbaNameMatch =
            dbDbaName &&
            (googleName.includes(dbDbaName) || dbDbaName.includes(googleName));

          // ONLY for matched shops, get license_status from DB
          if (businessNameMatch || dbaNameMatch) {
            console.log(
              `FULL MATCH FOUND! Distance: ${Math.round(
                distance,
              )}m, Name match: ${businessNameMatch ? "business_name" : "dba"}`,
            );
            console.log(`   License status from DB: ${record.license_status}`);

            // Store license status from DB for this matched shop
            googleShopStatusMap.set(shop.place_id, {
              license_status: record.license_status, // actual license status from DB
              matchedRecord: record,
            });

            matches.push({
              google: {
                name: shop.name,
                address: shop.formatted_address || shop.vicinity,
                lat: gLat,
                lng: gLng,
                place_id: shop.place_id,
                smoke_shop: shop.smoke_shop,
                open_now: shop.opening_hours.open_now,
                business_status: shop.business_status,
                photo_url: shop.photo_url,
                opening_hours: shop.opening_hours,
              },
              gov: {
                business_name: record.business_name,
                license_number: record.license_number,
                license_status: record.license_status,
                license_type: record.license_type,
                address: record.business_address,
                city: record.city,
                stateName: record.stateName,
                dba: record.dba,
                smoke_shop: record.smoke_shop,
              },
              distance: `${Math.round(distance)}m`,
              matchType: businessNameMatch ? "business_name" : "dba",
            });
          } else {
            console.log(`Distance match but no name similarity`);
          }
        }
      }
    }

    // Prepare ALL Google shops with their license status and enhanced data
    const allGoogleShops = googleShops.map((shop) => {
      const statusInfo = googleShopStatusMap.get(shop.place_id);
      return {
        name: shop.name,
        address: shop.formatted_address || shop.vicinity,
        lat: shop.geometry.location.lat,
        lng: shop.geometry.location.lng,
        place_id: shop.place_id,
        rating: shop.rating,
        user_ratings_total: shop.user_ratings_total,
        price_level: shop.price_level,
        types: shop.types,
        smoke_shop: shop.smoke_shop,
        business_status: shop.business_status,

        // Enhanced data
        open_now: shop.opening_hours.open_now,
        opening_hours: shop.opening_hours,
        photo_url: shop.photo_url,
        photos: shop.photos,

        // License data
        license_status: statusInfo.license_status, // null for non-matched, actual status for matched
        isMatched: statusInfo.matchedRecord !== null,
      };
    });

    console.log(`\n=== SUMMARY ===`);
    console.log(`Total Google shops: ${googleShops.length}`);
    console.log(`Total DB records: ${govRecords.length}`);
    console.log(`Total comparisons attempted: ${totalComparisons}`);
    console.log(`Valid comparisons: ${validComparisons}`);
    console.log(`Matches found: ${matches.length}`);
    console.log(
      `Shops with license status: ${
        allGoogleShops.filter((shop) => shop.license_status).length
      }`,
    );
    console.log(
      `Smoke shops: ${allGoogleShops.filter((shop) => shop.smoke_shop).length}`,
    );
    console.log(
      `Non-matched shops (license_status: null): ${
        allGoogleShops.filter((shop) => !shop.isMatched).length
      }`,
    );
    console.log(
      `Shops currently open: ${
        allGoogleShops.filter((shop) => shop.open_now === true).length
      }`,
    );
    console.log(
      `Shops with photos: ${
        allGoogleShops.filter((shop) => shop.photo_url).length
      }`,
    );

    res.json({
      success: true,
      data: {
        allShops: allGoogleShops,
        matches,
        debug: {
          googleShopsCount: googleShops.length,
          govRecordsCount: govRecords.length,
          totalComparisons,
          validComparisons,
          matchesFound: matches.length,
          shopsWithLicenseStatus: allGoogleShops.filter(
            (shop) => shop.license_status,
          ).length,
          smokeShops: allGoogleShops.filter((shop) => shop.smoke_shop).length,
          nonMatchedShops: allGoogleShops.filter((shop) => !shop.isMatched)
            .length,
          shopsCurrentlyOpen: allGoogleShops.filter(
            (shop) => shop.open_now === true,
          ).length,
          shopsWithPhotos: allGoogleShops.filter((shop) => shop.photo_url)
            .length,
        },
      },
    });
  } catch (error) {
    console.error("Error in compare-shops:", error);
    res.status(500).json({
      success: false,
      error: "Comparison failed",
      details: error.message,
    });
  }
}

// Test database endpoint
async function testDatabase(req, res) {
  try {
    const totalCount = await LicenseRecord.countDocuments();
    const withCoordinates = await LicenseRecord.countDocuments({
      "location.coordinates": { $exists: true, $ne: [] },
      "location.coordinates.0": { $exists: true },
      "location.coordinates.1": { $exists: true },
    });

    const sample = await LicenseRecord.findOne({
      "location.coordinates": { $exists: true, $ne: [] },
    });

    res.json({
      success: true,
      data: {
        totalRecords: totalCount,
        recordsWithCoordinates: withCoordinates,
        recordsWithoutCoordinates: totalCount - withCoordinates,
        sampleRecord: sample
          ? {
              business_name: sample.business_name,
              coordinates: sample.location?.coordinates,
              business_address: sample.business_address,
              city: sample.city,
              stateName: sample.stateName,
              license_status: sample.license_status,
              license_type: sample.license_type,
              smoke_shop: sample.smoke_shop,
            }
          : null,
      },
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      error: error.message,
    });
  }
}

module.exports = {
  getPlaceDetails,
  getPhotoUrl,
  getNearbyShops,
  haversineDistance,
  normalizeName,
  compareShops,
  testDatabase,
};
