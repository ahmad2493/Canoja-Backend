const axios = require("axios");
const LicenseRecord = require("../models/LicenseRecord");

// Configuration
const GOOGLE_MAPS_API_KEY = process.env.GOOGLE_API_KEY;

// Search queries to use
const SEARCH_QUERIES = [
  "Cannabis shop",
  "Smoke Shop",
  "Cannabis retailer",
  "Cannabis distributor",
  "Cannabis manufacturer",
  "Cannabis cultivator",
];

// In-memory storage for pagination tokens and query state
const paginationTokens = new Map();
const queryStates = new Map(); // Track which query we're currently on for each search

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

// --- Generate unique key for pagination ---
function generatePaginationKey(lat, lng, radius) {
  return `${lat}_${lng}_${radius}`;
}

// --- Generate unique key for specific query pagination ---
function generateQueryPaginationKey(lat, lng, radius, query) {
  return `${lat}_${lng}_${radius}_${query}`;
}

// --- Initialize query state for a search session ---
function initializeQueryState(lat, lng, radius) {
  const sessionKey = generatePaginationKey(lat, lng, radius);
  queryStates.set(sessionKey, {
    currentQueryIndex: 0,
    usedPlaceIds: new Set(), // Track already seen places to avoid duplicates
    queriesExhausted: new Set(), // Track which queries have no more results
    totalFetched: 0,
  });
  return sessionKey;
}

// --- Get current query for session ---
function getCurrentQuery(sessionKey) {
  const state = queryStates.get(sessionKey);
  if (!state || state.currentQueryIndex >= SEARCH_QUERIES.length) {
    return null;
  }
  return SEARCH_QUERIES[state.currentQueryIndex];
}

// --- Move to next query ---
function moveToNextQuery(sessionKey) {
  const state = queryStates.get(sessionKey);
  if (state) {
    state.currentQueryIndex++;
    console.log(
      `Moving to next query: ${getCurrentQuery(sessionKey) || "No more queries"}`,
    );
  }
}

// --- Fetch shops with current query ---
async function fetchShopsWithCurrentQuery(
  lat,
  lng,
  radius,
  sessionKey,
  isInitial = false,
) {
  const currentQuery = getCurrentQuery(sessionKey);
  if (!currentQuery) {
    console.log("No more queries available");
    return {
      results: [],
      next_page_token: null,
      has_more: false,
      query_used: null,
    };
  }

  const state = queryStates.get(sessionKey);
  const queryPaginationKey = generateQueryPaginationKey(
    lat,
    lng,
    radius,
    currentQuery,
  );

  try {
    let url, response;

    if (isInitial || !paginationTokens.has(queryPaginationKey)) {
      // Initial search for this query
      url = `https://maps.googleapis.com/maps/api/place/nearbysearch/json?location=${lat},${lng}&radius=${radius}&keyword=${encodeURIComponent(currentQuery)}&key=${GOOGLE_MAPS_API_KEY}`;
      console.log(`Initial search for query: "${currentQuery}"`);
    } else {
      // Use pagination token for this query
      const tokenData = paginationTokens.get(queryPaginationKey);

      // Check if token is still valid (tokens expire after a short time)
      const tokenAge = Date.now() - tokenData.timestamp;
      if (tokenAge > 300000) {
        // 5 minutes
        console.log(
          `Token expired for query "${currentQuery}", marking as exhausted`,
        );
        state.queriesExhausted.add(currentQuery);
        paginationTokens.delete(queryPaginationKey);
        moveToNextQuery(sessionKey);
        return await fetchShopsWithCurrentQuery(
          lat,
          lng,
          radius,
          sessionKey,
          true,
        );
      }

      url = `https://maps.googleapis.com/maps/api/place/nearbysearch/json?pagetoken=${tokenData.token}&key=${GOOGLE_MAPS_API_KEY}`;
      console.log(`Using pagination token for query: "${currentQuery}"`);

      // Google requires a delay before using pagetoken
      await new Promise((resolve) => setTimeout(resolve, 2000));
    }

    response = await axios.get(url);

    if (response.data.results) {
      // Filter out duplicates based on place_id
      const uniqueResults = response.data.results.filter((place) => {
        if (state.usedPlaceIds.has(place.place_id)) {
          return false;
        }
        state.usedPlaceIds.add(place.place_id);
        return true;
      });

      // Store/update pagination token for this query
      if (response.data.next_page_token) {
        paginationTokens.set(queryPaginationKey, {
          token: response.data.next_page_token,
          timestamp: Date.now(),
          lat,
          lng,
          radius,
          query: currentQuery,
        });
      } else {
        // No more pages for this query, mark as exhausted
        console.log(`Query "${currentQuery}" exhausted, moving to next`);
        state.queriesExhausted.add(currentQuery);
        paginationTokens.delete(queryPaginationKey);
      }

      // Take only what we need to reach 10 results
      const needed = 10;
      const results = uniqueResults.slice(0, needed);
      state.totalFetched += results.length;

      console.log(
        `Fetched ${results.length} unique results for query: "${currentQuery}"`,
      );

      // Check if we have more results available (either more pages for current query or more queries)
      const hasMoreInCurrentQuery = !!response.data.next_page_token;
      const hasMoreQueries =
        state.currentQueryIndex < SEARCH_QUERIES.length - 1;
      const hasMore = hasMoreInCurrentQuery || hasMoreQueries;

      return {
        results,
        next_page_token: response.data.next_page_token,
        has_more: hasMore,
        query_used: currentQuery,
        session_info: {
          current_query: currentQuery,
          current_query_index: state.currentQueryIndex,
          total_queries: SEARCH_QUERIES.length,
          total_fetched: state.totalFetched,
          unique_places_found: state.usedPlaceIds.size,
        },
      };
    }

    return {
      results: [],
      next_page_token: null,
      has_more: false,
      query_used: currentQuery,
      session_info: {
        current_query: currentQuery,
        current_query_index: state.currentQueryIndex,
        total_queries: SEARCH_QUERIES.length,
        total_fetched: state.totalFetched,
        unique_places_found: state.usedPlaceIds.size,
      },
    };
  } catch (error) {
    console.error(
      `Error fetching shops for query "${currentQuery}":`,
      error.message,
    );

    // Mark this query as problematic and move to next
    state.queriesExhausted.add(currentQuery);
    moveToNextQuery(sessionKey);

    // Try with next query if available
    if (getCurrentQuery(sessionKey)) {
      return await fetchShopsWithCurrentQuery(
        lat,
        lng,
        radius,
        sessionKey,
        true,
      );
    }

    return {
      results: [],
      next_page_token: null,
      has_more: false,
      query_used: currentQuery,
      error: error.message,
    };
  }
}

// --- Get next batch of shops (handles query progression and pagination) ---
async function getNextShopsBatch(lat, lng, radius, sessionKey) {
  const state = queryStates.get(sessionKey);
  if (!state) {
    throw new Error("No active session found. Please start a new search.");
  }

  let allResults = [];
  const targetCount = 10;

  while (allResults.length < targetCount && getCurrentQuery(sessionKey)) {
    const batchResponse = await fetchShopsWithCurrentQuery(
      lat,
      lng,
      radius,
      sessionKey,
    );

    if (batchResponse.results.length === 0) {
      // No results from current query, move to next
      moveToNextQuery(sessionKey);
      continue;
    }

    allResults.push(...batchResponse.results);

    // If we have enough results, return them
    if (allResults.length >= targetCount) {
      return {
        results: allResults.slice(0, targetCount),
        has_more: true,
        session_info: batchResponse.session_info,
      };
    }

    // If current query has no more pages, move to next query
    if (!batchResponse.next_page_token) {
      moveToNextQuery(sessionKey);
    }
  }

  // Return whatever we found
  const hasMoreQueries = getCurrentQuery(sessionKey) !== null;
  return {
    results: allResults,
    has_more: hasMoreQueries,
    session_info: state
      ? {
          current_query: getCurrentQuery(sessionKey),
          current_query_index: state.currentQueryIndex,
          total_queries: SEARCH_QUERIES.length,
          total_fetched: state.totalFetched,
          unique_places_found: state.usedPlaceIds.size,
        }
      : null,
  };
}

// --- Enhanced shop data processing ---
async function enhanceShopData(shops) {
  const enhancedResults = [];
  console.log(`Enhancing ${shops.length} shops with detailed information...`);

  for (let i = 0; i < shops.length; i++) {
    const place = shops[i];
    console.log(
      `Fetching details for place ${i + 1}/${shops.length}: ${place.name}`,
    );

    const placeDetails = await getPlaceDetails(place.place_id);

    if (placeDetails) {
      const enhancedPlace = {
        ...place,
        name: placeDetails.name || place.name,
        formatted_address: placeDetails.formatted_address || place.vicinity,
        geometry: placeDetails.geometry || place.geometry,
        rating: placeDetails.rating || place.rating,
        user_ratings_total:
          placeDetails.user_ratings_total || place.user_ratings_total,
        price_level: placeDetails.price_level || place.price_level,
        types: placeDetails.types || place.types,
        business_status: placeDetails.business_status,

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

        photo_url:
          placeDetails.photos && placeDetails.photos.length > 0
            ? getPhotoUrl(placeDetails.photos[0].photo_reference, 400)
            : null,

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
      enhancedResults.push({
        ...place,
        business_status: null,
        opening_hours: { open_now: null, periods: [], weekday_text: [] },
        photo_url: null,
        photos: [],
      });
    }

    // Rate limiting delay
    await new Promise((resolve) => setTimeout(resolve, 100));
  }

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

// --- Match shops with license records ---
function matchShopsWithLicenses(googleShops, govRecords) {
  const matches = [];
  const googleShopStatusMap = new Map();

  console.log("\n--- Starting Matching Process ---");

  // Initialize all shops with no license status
  googleShops.forEach((shop) => {
    googleShopStatusMap.set(shop.place_id, {
      license_status: null,
      matchedRecord: null,
    });
  });

  let totalComparisons = 0;
  let validComparisons = 0;

  for (const shop of googleShops) {
    const gLat = shop.geometry.location.lat;
    const gLng = shop.geometry.location.lng;

    for (const record of govRecords) {
      totalComparisons++;

      if (
        !record.location ||
        !record.location.coordinates ||
        record.location.coordinates.length < 2
      ) {
        continue;
      }

      const [dbLng, dbLat] = record.location.coordinates;

      if (!dbLat || !dbLng || isNaN(dbLat) || isNaN(dbLng)) {
        continue;
      }

      validComparisons++;
      const distance = haversineDistance(gLat, gLng, dbLat, dbLng);

      if (distance <= 100) {
        const googleName = normalizeName(shop.name);
        const dbBusinessName = normalizeName(record.business_name);
        const dbDbaName = normalizeName(record.dba);

        const businessNameMatch =
          googleName.includes(dbBusinessName) ||
          dbBusinessName.includes(googleName);
        const dbaNameMatch =
          dbDbaName &&
          (googleName.includes(dbDbaName) || dbDbaName.includes(googleName));

        if (businessNameMatch || dbaNameMatch) {
          console.log(
            `MATCH FOUND: ${shop.name} - License: ${record.license_status}`,
          );

          googleShopStatusMap.set(shop.place_id, {
            license_status: record.license_status,
            matchedRecord: record,
          });

          matches.push({
            google: {
              name: shop.name,
              address: shop.formatted_address || shop.vicinity,
              lat: gLat,
              lng: gLng,
              place_id: shop.place_id,
              open_now: shop.opening_hours?.open_now,
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
            },
            distance: `${Math.round(distance)}m`,
            matchType: businessNameMatch ? "business_name" : "dba",
          });
          break; // Found a match, no need to check other records
        }
      }
    }
  }

  return { matches, googleShopStatusMap, totalComparisons, validComparisons };
}

// --- Initial Compare Shops (starts multi-query search) ---
async function compareShops(req, res) {
  try {
    const { lat, lng, radius = 5000 } = req.body;

    console.log(`\n=== Starting multi-query comparison ===`);
    console.log(`Coordinates: lat=${lat}, lng=${lng}, radius=${radius}`);
    console.log(`Available queries: ${SEARCH_QUERIES.join(", ")}`);

    // Initialize query state for this search session
    const sessionKey = initializeQueryState(lat, lng, radius);

    // Get first batch of shops using multiple queries
    const googleResponse = await getNextShopsBatch(
      lat,
      lng,
      radius,
      sessionKey,
    );
    const enhancedShops = await enhanceShopData(googleResponse.results);

    // Get government records
    console.log("\n--- Fetching from Database ---");
    const govRecords = await LicenseRecord.find({
      "location.coordinates": { $exists: true, $ne: [] },
      "location.coordinates.0": { $exists: true },
      "location.coordinates.1": { $exists: true },
    });

    // Match shops with licenses
    const { matches, googleShopStatusMap, totalComparisons, validComparisons } =
      matchShopsWithLicenses(enhancedShops, govRecords);

    // Prepare response data
    const allGoogleShops = enhancedShops.map((shop) => {
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
        business_status: shop.business_status,
        open_now: shop.opening_hours?.open_now,
        opening_hours: shop.opening_hours,
        photo_url: shop.photo_url,
        photos: shop.photos,
        license_status: statusInfo?.license_status,
        isMatched: statusInfo?.matchedRecord !== null,
      };
    });

    console.log(`\n=== INITIAL SUMMARY ===`);
    console.log(`Shops returned: ${allGoogleShops.length}`);
    console.log(`Matches found: ${matches.length}`);
    console.log(`Has more results: ${googleResponse.has_more}`);
    console.log(`Session key: ${sessionKey}`);

    res.json({
      success: true,
      data: {
        shops: allGoogleShops,
        matches,
        pagination: {
          current_page: 1,
          has_more: googleResponse.has_more,
          total_shown: allGoogleShops.length,
          session_key: sessionKey, // Include session key for subsequent requests
        },
        search_info: {
          ...googleResponse.session_info,
          available_queries: SEARCH_QUERIES,
        },
        debug: {
          googleShopsCount: allGoogleShops.length,
          govRecordsCount: govRecords.length,
          totalComparisons,
          validComparisons,
          matchesFound: matches.length,
          sessionKey: sessionKey,
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

// --- Get More Shops (continues multi-query search) ---
async function getMoreShops(req, res) {
  try {
    const { lat, lng, radius = 5000, session_key } = req.body;

    if (!session_key) {
      return res.status(400).json({
        success: false,
        error: "session_key is required for pagination",
      });
    }

    console.log(`\n=== Fetching more shops ===`);
    console.log(`Coordinates: lat=${lat}, lng=${lng}, radius=${radius}`);
    console.log(`Session key: ${session_key}`);

    // Check if session exists
    if (!queryStates.has(session_key)) {
      return res.status(400).json({
        success: false,
        error: "Invalid or expired session_key. Please start a new search.",
      });
    }

    // Get next batch of shops
    const googleResponse = await getNextShopsBatch(
      lat,
      lng,
      radius,
      session_key,
    );

    if (googleResponse.results.length === 0) {
      return res.json({
        success: true,
        data: {
          shops: [],
          matches: [],
          pagination: {
            has_more: false,
            total_shown: 0,
            message: "No more results available from all queries",
            session_key: session_key,
          },
          search_info: googleResponse.session_info,
        },
      });
    }

    const enhancedShops = await enhanceShopData(googleResponse.results);

    // Get government records
    const govRecords = await LicenseRecord.find({
      "location.coordinates": { $exists: true, $ne: [] },
      "location.coordinates.0": { $exists: true },
      "location.coordinates.1": { $exists: true },
    });

    // Match new shops with licenses
    const { matches, googleShopStatusMap } = matchShopsWithLicenses(
      enhancedShops,
      govRecords,
    );

    // Prepare response data
    const allGoogleShops = enhancedShops.map((shop) => {
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
        business_status: shop.business_status,
        open_now: shop.opening_hours?.open_now,
        opening_hours: shop.opening_hours,
        photo_url: shop.photo_url,
        photos: shop.photos,
        license_status: statusInfo?.license_status,
        isMatched: statusInfo?.matchedRecord !== null,
      };
    });

    console.log(`\n=== MORE SHOPS SUMMARY ===`);
    console.log(`Additional shops returned: ${allGoogleShops.length}`);
    console.log(`New matches found: ${matches.length}`);
    console.log(`Has more results: ${googleResponse.has_more}`);

    res.json({
      success: true,
      data: {
        shops: allGoogleShops,
        matches,
        pagination: {
          has_more: googleResponse.has_more,
          total_shown: allGoogleShops.length,
          session_key: session_key,
        },
        search_info: {
          ...googleResponse.session_info,
          available_queries: SEARCH_QUERIES,
        },
        debug: {
          newShopsCount: allGoogleShops.length,
          newMatchesFound: matches.length,
          sessionKey: session_key,
        },
      },
    });
  } catch (error) {
    console.error("Error getting more shops:", error);
    res.status(500).json({
      success: false,
      error: "Failed to get more shops",
      details: error.message,
    });
  }
}

// --- Clear pagination tokens and query states ---
async function clearPaginationTokens(req, res) {
  try {
    const { lat, lng, radius, session_key } = req.body;

    if (session_key) {
      // Clear specific session
      queryStates.delete(session_key);

      // Clear related pagination tokens
      if (lat && lng && radius) {
        SEARCH_QUERIES.forEach((query) => {
          const queryKey = generateQueryPaginationKey(lat, lng, radius, query);
          paginationTokens.delete(queryKey);
        });
      }

      res.json({ success: true, message: "Specific session cleared" });
    } else if (lat && lng && radius) {
      // Clear all tokens for specific location
      const sessionKey = generatePaginationKey(lat, lng, radius);
      queryStates.delete(sessionKey);

      SEARCH_QUERIES.forEach((query) => {
        const queryKey = generateQueryPaginationKey(lat, lng, radius, query);
        paginationTokens.delete(queryKey);
      });

      res.json({ success: true, message: "Location-specific tokens cleared" });
    } else {
      // Clear all tokens and states
      paginationTokens.clear();
      queryStates.clear();
      res.json({ success: true, message: "All tokens and sessions cleared" });
    }
  } catch (error) {
    res.status(500).json({
      success: false,
      error: error.message,
    });
  }
}

// --- Get search session status ---
async function getSessionStatus(req, res) {
  try {
    const { session_key } = req.params;

    const state = queryStates.get(session_key);
    if (!state) {
      return res.status(404).json({
        success: false,
        error: "Session not found",
      });
    }

    res.json({
      success: true,
      data: {
        session_key: session_key,
        current_query: getCurrentQuery(session_key),
        current_query_index: state.currentQueryIndex,
        total_queries: SEARCH_QUERIES.length,
        available_queries: SEARCH_QUERIES,
        total_fetched: state.totalFetched,
        unique_places_found: state.usedPlaceIds.size,
        queries_exhausted: Array.from(state.queriesExhausted),
        has_more: getCurrentQuery(session_key) !== null,
      },
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      error: error.message,
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
  enhanceShopData,
  haversineDistance,
  normalizeName,
  compareShops, // Starts multi-query search
  getMoreShops, // Continues multi-query search
  clearPaginationTokens, // Clear tokens and sessions
  getSessionStatus, // Get session status
  testDatabase,
};
