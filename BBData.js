require("dotenv").config();
const axios = require("axios");
const fs = require("fs");
const path = require("path");

// Validate required environment variables
const requiredEnvVars = ["META_ACCESS_TOKEN", "META_ACCOUNT_ID"];
const missingVars = requiredEnvVars.filter((varName) => !process.env[varName]);

if (missingVars.length > 0) {
  console.error(
    `Error: Missing required environment variables: ${missingVars.join(", ")}`
  );
  console.error(
    "Please ensure META_ACCESS_TOKEN and META_ACCOUNT_ID are set in your .env file"
  );
  process.exit(1);
}

// Configuration
const config = {
  accessToken: process.env.META_ACCESS_TOKEN,
  accountId: process.env.META_ACCOUNT_ID,
  apiVersion: process.env.META_API_VERSION || "v22.0",
  baseUrl: "https://graph.facebook.com",
  // No filtering by name - grab all ads
  adNamePatterns: [""],
  accountPatterns: [""],
  dataDir: process.env.DATA_DIR || "./data",
  defaultLookbackDays: parseInt(process.env.LOOKBACK_DAYS, 10) || 150,
  retryAttempts: parseInt(process.env.RETRY_ATTEMPTS, 10) || 3,
  currency: process.env.CURRENCY || "USD",
  // Request throttling to avoid API rate limits
  requestDelay: parseInt(process.env.REQUEST_DELAY, 10) || 300, // ms
  // Options for filtering
  enableAdFiltering: false, // Disabled to get all ads
  onlyIncludeActiveAds: false, // Include all ads regardless of status
  skipFutureDateRanges: true,
  // Maximum ads to process (0 = no limit)
  maxAdsPerAccount: parseInt(process.env.MAX_ADS_PER_ACCOUNT, 10) || 0,
  // Google Sheets integration
  googleSheets: {
    enabled: process.env.GOOGLE_SHEETS_ENABLED === "true",
    keyFilePath:
      process.env.GOOGLE_SHEETS_KEY_FILE || "./google-service-account-key.json",
    spreadsheetId: "1tIbtZ6b9u3AKn9e8JXE8Qf3ZTB9rPVbSs6o3l_zkgUQ",
    allAdsSheet: process.env.GOOGLE_SHEETS_ALL_ADS_SHEET || "AllAds",
    adPerformanceSheet:
      process.env.GOOGLE_SHEETS_PERFORMANCE_SHEET || "AdPerformance",
    videoLinksSheet:
      process.env.GOOGLE_SHEETS_VIDEO_LINKS_SHEET || "VideoLinks",
  },
};

// Dependencies for Google Sheets API - only if enabled
let google, sheets;
try {
  if (config.googleSheets.enabled) {
    const { google: googleApi } = require("googleapis");
    google = googleApi;
    sheets = google.sheets("v4");
    console.log("Google Sheets API loaded successfully");
  }
} catch (error) {
  console.error("Error loading Google Sheets API dependencies:", error.message);
  console.error(
    "If you want to use Google Sheets integration, run: npm install googleapis"
  );
  config.googleSheets.enabled = false;
}

// Create output directory if it doesn't exist
if (!fs.existsSync(config.dataDir)) {
  fs.mkdirSync(config.dataDir, { recursive: true });
}

// Date utility functions
const formatDate = (date) => {
  return date.toISOString().split("T")[0];
};

const getDateRange = () => {
  const now = new Date();
  const startDate = new Date();
  startDate.setDate(now.getDate() - config.defaultLookbackDays);

  return {
    since: formatDate(startDate),
    until: formatDate(now),
  };
};

// Break date range into chunks for better API handling
function getDateChunks(startDate, endDate, chunkSizeDays = 30) {
  const chunks = [];
  const start = new Date(startDate);
  const end = new Date(endDate);

  console.log(
    `Creating date chunks from ${formatDate(start)} to ${formatDate(
      end
    )} with chunk size ${chunkSizeDays} days`
  );

  let chunkStart = new Date(start);

  while (chunkStart < end) {
    let chunkEnd = new Date(chunkStart);
    chunkEnd.setDate(chunkEnd.getDate() + chunkSizeDays - 1);

    // Ensure chunk end doesn't exceed the overall end date
    if (chunkEnd > end) {
      chunkEnd = new Date(end);
    }

    chunks.push({
      since: formatDate(chunkStart),
      until: formatDate(chunkEnd),
    });

    // Move to next chunk start
    chunkStart = new Date(chunkEnd);
    chunkStart.setDate(chunkStart.getDate() + 1);
  }

  return chunks;
}

// API request with retry logic
async function makeApiRequest(url, maxRetries = config.retryAttempts) {
  let lastError;

  for (let attempt = 0; attempt < maxRetries; attempt++) {
    try {
      // Add delay between retries with exponential backoff
      if (attempt > 0) {
        const delay = Math.pow(2, attempt) * 1000;
        console.log(
          `Retry attempt ${attempt + 1}/${maxRetries} after ${delay}ms`
        );
        await new Promise((resolve) => setTimeout(resolve, delay));
      }

      const response = await axios.get(url);
      return response.data;
    } catch (error) {
      lastError = error;

      const statusCode = error.response?.status;
      const errorMessage =
        error.response?.data?.error?.message || error.message;

      console.error(
        `API Error [Status: ${statusCode || "unknown"}]: ${errorMessage}`
      );

      // If this is a rate limit error (429) or server error (5xx), retry
      if (error.response && (statusCode === 429 || statusCode >= 500)) {
        // If we get a specific retry-after header, respect it
        if (error.response.headers["retry-after"]) {
          const retryAfter =
            parseInt(error.response.headers["retry-after"]) * 1000;
          await new Promise((resolve) => setTimeout(resolve, retryAfter));
        }
        continue;
      }

      // For other errors, only retry if we have attempts left
      if (attempt < maxRetries - 1) {
        continue;
      }

      // Otherwise don't retry
      throw error;
    }
  }

  // If we've exhausted retries, throw the last error
  throw lastError;
}

// Fetch paginated data with automatic handling of next pages
async function fetchPaginatedData(endpoint, params) {
  const url = buildApiUrl(endpoint, params);

  let allData = [];
  let nextUrl = url;

  while (nextUrl) {
    const response = await makeApiRequest(nextUrl);

    if (response.data && Array.isArray(response.data)) {
      allData = [...allData, ...response.data];
    }

    nextUrl = response.paging?.next || null;

    // Add a small delay between pagination requests
    if (nextUrl) {
      await new Promise((resolve) => setTimeout(resolve, config.requestDelay));
    }
  }

  return allData;
}

// Build API URL
const buildApiUrl = (endpoint, params = {}) => {
  const url = new URL(`${config.baseUrl}/${config.apiVersion}/${endpoint}`);

  // Add access token
  url.searchParams.append("access_token", config.accessToken);

  // Add other parameters
  Object.entries(params).forEach(([key, value]) => {
    if (typeof value === "object" && !Array.isArray(value)) {
      url.searchParams.append(key, JSON.stringify(value));
    } else if (Array.isArray(value)) {
      url.searchParams.append(key, value.join(","));
    } else {
      url.searchParams.append(key, value);
    }
  });

  return url.toString();
};

// Function to authenticate with Google Sheets using service account
async function getGoogleSheetsAuth() {
  try {
    if (!config.googleSheets.enabled) return null;

    const auth = new google.auth.GoogleAuth({
      keyFile: config.googleSheets.keyFilePath,
      scopes: ["https://www.googleapis.com/auth/spreadsheets"],
    });
    return await auth.getClient();
  } catch (error) {
    console.error("Error authenticating with Google Sheets:", error.message);
    console.error(
      "Check your service account credentials and make sure the file exists at:",
      config.googleSheets.keyFilePath
    );
    return null;
  }
}

// Function to save data to Google Sheets
async function saveToGoogleSheets(data, sheetName) {
  if (!config.googleSheets.enabled || !data || data.length === 0) {
    if (config.googleSheets.enabled && (!data || data.length === 0)) {
      console.log(`No data to save to Google Sheets for: ${sheetName}`);
    }
    return false;
  }

  try {
    // Check if spreadsheet ID is set
    if (!config.googleSheets.spreadsheetId) {
      console.error(
        "Google Sheets spreadsheet ID is not configured. Set GOOGLE_SHEETS_SPREADSHEET_ID in your .env file"
      );
      return false;
    }

    console.log(`Saving data to Google Sheets: ${sheetName}`);
    const authClient = await getGoogleSheetsAuth();

    if (!authClient) {
      console.error("Could not authenticate with Google Sheets");
      return false;
    }

    // Get all headers
    const headers = [...new Set(data.flatMap((obj) => Object.keys(obj)))];

    // Format the data for Sheets API
    const rows = [
      headers, // First row is headers
      ...data.map((row) =>
        headers.map((header) => {
          const value = row[header];

          // Format values for better display in sheets
          if (value === undefined || value === null) {
            return "";
          } else if (typeof value === "object") {
            return JSON.stringify(value);
          } else {
            return value;
          }
        })
      ),
    ];

    // Clear the existing data in the sheet
    await sheets.spreadsheets.values.clear({
      auth: authClient,
      spreadsheetId: config.googleSheets.spreadsheetId,
      range: `${sheetName}!A:ZZ`,
    });

    // Write the new data
    await sheets.spreadsheets.values.update({
      auth: authClient,
      spreadsheetId: config.googleSheets.spreadsheetId,
      range: `${sheetName}!A1`,
      valueInputOption: "RAW",
      resource: {
        values: rows,
      },
    });

    console.log(`Data successfully saved to Google Sheet: ${sheetName}`);
    return true;
  } catch (error) {
    console.error(
      `Error saving to Google Sheets (${sheetName}):`,
      error.message
    );
    if (error.message && error.message.includes("not found")) {
      console.error(
        `Sheet "${sheetName}" may not exist. Create it manually in your Google Spreadsheet.`
      );
    }
    return false;
  }
}

// Get account info
async function getAccountInfo(accountId) {
  try {
    const endpoint = `act_${accountId}`;
    const params = {
      fields: [
        "id",
        "name",
        "account_status",
        "currency",
        "business_name",
        "amount_spent",
      ],
    };

    const url = buildApiUrl(endpoint, params);
    return await makeApiRequest(url);
  } catch (error) {
    console.error(
      `Error fetching account info for ${accountId}:`,
      error.message
    );
    return {
      id: accountId,
      name: "Error fetching account",
      error: error.message,
    };
  }
}

// Get all ads from an account (no name pattern filtering)
async function fetchAllAds(accountId) {
  try {
    const endpoint = `act_${accountId}/ads`;
    const params = {
      fields: [
        "id",
        "name",
        "status",
        "adset_id",
        "adset_name",
        "campaign_id",
        "campaign_name",
        "created_time",
      ],
      limit: 500,
    };

    console.log(`Fetching all ads for account ${accountId}...`);
    const allAds = await fetchPaginatedData(endpoint, params);
    console.log(`Found ${allAds.length} total ads in account ${accountId}`);

    return allAds;
  } catch (error) {
    console.error(
      `Error fetching ads for account ${accountId}:`,
      error.message
    );
    return [];
  }
}

// Get video assets and their links for ads
async function getVideoLinks(adIds) {
  if (!adIds.length) return [];

  let videoLinks = [];
  const batchSize = 50;

  for (let i = 0; i < adIds.length; i += batchSize) {
    const batch = adIds.slice(i, i + batchSize);
    console.log(
      `Fetching video links for ads ${i + 1} to ${Math.min(
        i + batchSize,
        adIds.length
      )} of ${adIds.length}`
    );

    for (const adId of batch) {
      try {
        // First get the creative ID for this ad
        const adEndpoint = `${adId}`;
        const adParams = {
          fields: ["creative"],
        };
        const adUrl = buildApiUrl(adEndpoint, adParams);
        const adResponse = await makeApiRequest(adUrl);

        if (adResponse.creative && adResponse.creative.id) {
          const creativeId = adResponse.creative.id;

          // Now get the creative details including video ID
          const creativeEndpoint = `${creativeId}`;
          const creativeParams = {
            fields: [
              "id",
              "name",
              "title",
              "body",
              "object_type",
              "video_id",
              "video_permalink_url",
              "video_source_url",
              "asset_feed_spec",
            ],
          };
          const creativeUrl = buildApiUrl(creativeEndpoint, creativeParams);
          const creativeResponse = await makeApiRequest(creativeUrl);

          // If this creative has a video, get the video URL
          if (creativeResponse.video_id) {
            const videoId = creativeResponse.video_id;
            const videoEndpoint = `${videoId}`;
            const videoParams = {
              fields: [
                "id",
                "permalink_url",
                "title",
                "length",
                "video_insights",
              ],
            };
            const videoUrl = buildApiUrl(videoEndpoint, videoParams);
            const videoResponse = await makeApiRequest(videoUrl);

            videoLinks.push({
              ad_id: adId,
              creative_id: creativeId,
              video_id: videoId,
              ad_name: "", // Will be filled in later
              creative_name: creativeResponse.name || "",
              creative_title: creativeResponse.title || "",
              creative_body: creativeResponse.body || "",
              video_title: videoResponse.title || "",
              video_length: videoResponse.length || "",
              permalink_url: videoResponse.permalink_url || "",
              source_url: videoResponse.source || "",
              thumbnail_url: creativeResponse.thumbnail_url || "",
            });
          }
          // Handle image ads
          else if (creativeResponse.image_url) {
            videoLinks.push({
              ad_id: adId,
              creative_id: creativeId,
              video_id: "N/A",
              ad_name: "", // Will be filled in later
              creative_name: creativeResponse.name || "",
              creative_title: creativeResponse.title || "",
              creative_body: creativeResponse.body || "",
              video_title: "N/A",
              video_length: "N/A",
              permalink_url: "N/A",
              source_url: "N/A",
              image_url: creativeResponse.image_url || "",
              asset_type: "image",
            });
          }
        }

        // Add a small delay between API calls to respect rate limits
        await new Promise((resolve) =>
          setTimeout(resolve, config.requestDelay)
        );
      } catch (error) {
        console.error(
          `Error fetching video link for ad ID ${adId}: ${error.message}`
        );
      }
    }
  }

  return videoLinks;
}

// Get comprehensive performance data for ads
async function getAdPerformanceData(adIds, dateRange = getDateRange()) {
  if (!adIds.length) return [];

  try {
    const batchSize = 50;
    let allPerformanceData = [];

    // Use date chunking to avoid hitting API limits
    const now = new Date();
    const dateChunks = getDateChunks(
      dateRange.since,
      dateRange.until,
      30
    ).filter((chunk) => new Date(chunk.until) <= now);

    console.log(
      `Using ${dateChunks.length} date chunks from ${dateRange.since} to ${dateRange.until}`
    );

    // Process ads in batches
    for (let i = 0; i < adIds.length; i += batchSize) {
      const batch = adIds.slice(i, i + batchSize);
      console.log(
        `Processing ads ${i + 1} to ${Math.min(
          i + batchSize,
          adIds.length
        )} of ${adIds.length}`
      );

      // Process each date chunk for this batch
      for (const chunk of dateChunks) {
        console.log(
          `Fetching data for date range: ${chunk.since} to ${chunk.until}`
        );

        // Process each ad in the batch
        for (const adId of batch) {
          try {
            const endpoint = `${adId}/insights`;
            const params = {
              level: "ad",
              time_range: {
                since: chunk.since,
                until: chunk.until,
              },
              fields: [
                "ad_id",
                "ad_name",
                "adset_name",
                "campaign_name",
                "date_start",
                "impressions",
                "clicks",
                "spend",
                "reach",
                "frequency",
                "video_p25_watched_actions",
                "video_p50_watched_actions",
                "video_p100_watched_actions",
                "video_avg_time_watched_actions",
                "actions",
                "conversions",
                "conversion_values",
                "cost_per_action_type",
                "cost_per_conversion",
                "cost_per_inline_link_click",
                "website_ctr",
                "unique_clicks",
                "unique_link_clicks_ctr",
              ],
            };

            const url = buildApiUrl(endpoint, params);
            const response = await makeApiRequest(url);

            if (response?.data?.length) {
              console.log(
                `Received ${response.data.length} performance records for ad ${adId} in range ${chunk.since} to ${chunk.until}`
              );
              allPerformanceData.push(...response.data);
            } else {
              console.log(
                `No performance data for ad ${adId} in range ${chunk.since} to ${chunk.until}`
              );
            }

            // Small delay to respect rate limits
            await new Promise((resolve) =>
              setTimeout(resolve, config.requestDelay)
            );
          } catch (error) {
            console.error(
              `Error fetching insights for ad ID ${adId} in range ${chunk.since} to ${chunk.until}: ${error.message}`
            );
          }
        }
      }
    }

    console.log(
      `Total performance records collected: ${allPerformanceData.length}`
    );
    return allPerformanceData;
  } catch (error) {
    console.error(`Error fetching ad performance data:`, error.message);
    return [];
  }
}

// Process performance data to extract action values
function processPerformanceData(performanceData) {
  return performanceData.map((record) => {
    const processed = {
      ...record,
    };

    // Extract values from actions array if present
    if (record.actions && Array.isArray(record.actions)) {
      record.actions.forEach((action) => {
        if (action.action_type && action.value !== undefined) {
          processed[`action_${action.action_type}`] = action.value;
        }
      });
    }
    delete processed.actions;

    // Extract values from action_values array if present
    if (record.action_values && Array.isArray(record.action_values)) {
      record.action_values.forEach((action) => {
        if (action.action_type && action.value !== undefined) {
          processed[`action_value_${action.action_type}`] = action.value;
        }
      });
    }
    delete processed.action_values;

    // Extract video watch metrics
    [
      "video_p25_watched_actions",
      "video_p50_watched_actions",
      "video_p75_watched_actions",
      "video_p95_watched_actions",
      "video_p100_watched_actions",
      "video_avg_time_watched_actions",
    ].forEach((metric) => {
      if (
        record[metric] &&
        Array.isArray(record[metric]) &&
        record[metric][0]
      ) {
        processed[metric.replace("_actions", "")] = record[metric][0].value;
      }
      delete processed[metric];
    });

    // Extract CTR data
    if (
      record.website_ctr &&
      Array.isArray(record.website_ctr) &&
      record.website_ctr.length > 0
    ) {
      processed.website_ctr_value = record.website_ctr[0].value;
      delete processed.website_ctr;
    }

    // Extract cost per action type
    if (
      record.cost_per_action_type &&
      Array.isArray(record.cost_per_action_type)
    ) {
      record.cost_per_action_type.forEach((item) => {
        if (item.action_type && item.value !== undefined) {
          processed[`cost_per_${item.action_type}`] = item.value;
        }
      });
    }
    delete processed.cost_per_action_type;

    return processed;
  });
}

// Save results to CSV
function saveToCSV(data, filename) {
  if (!data || data.length === 0) {
    console.log(`No data to save for ${filename}`);
    return;
  }

  // Get all headers
  const headers = [...new Set(data.flatMap((obj) => Object.keys(obj)))];
  const csvHeader = headers.join(",") + "\n";

  // Convert rows with proper escaping
  const csvRows = data
    .map((row) =>
      headers
        .map((header) => {
          const value = row[header] === undefined ? "" : row[header];

          // Properly escape CSV values
          if (
            typeof value === "string" &&
            (value.includes(",") || value.includes('"') || value.includes("\n"))
          ) {
            return `"${value.replace(/"/g, '""')}"`;
          } else if (typeof value === "object" && value !== null) {
            return `"${JSON.stringify(value).replace(/"/g, '""')}"`;
          }

          return value;
        })
        .join(",")
    )
    .join("\n");

  // Write to file
  const filePath = path.join(
    config.dataDir,
    `${filename}-${formatDate(new Date())}.csv`
  );

  try {
    fs.writeFileSync(filePath, csvHeader + csvRows);
    console.log(`Data saved to ${filePath}`);
  } catch (error) {
    console.error(`Error saving file ${filePath}:`, error.message);
  }
}

// Convert status code to readable text
function getAccountStatusText(statusCode) {
  switch (statusCode) {
    case 1:
      return "Active";
    case 2:
      return "Disabled";
    case 3:
      return "Unsettled";
    case 7:
      return "Pending Review";
    case 8:
      return "Pending Closure";
    case 9:
      return "In Grace Period";
    case 100:
      return "Temporarily Unavailable";
    case 101:
      return "Closed";
    default:
      return `Unknown (${statusCode})`;
  }
}

// Main function
async function getMetaAccountData() {
  console.log(
    `Starting Meta data extraction for account ${
      config.accountId
    } at ${new Date().toLocaleString()}`
  );

  const dateRange = getDateRange();
  console.log(
    `Fetching data for the last ${config.defaultLookbackDays} days (${dateRange.since} to ${dateRange.until})`
  );

  // Start time tracking
  const startTime = Date.now();

  try {
    // Get account info
    console.log("Fetching account information...");
    const accountInfo = await getAccountInfo(config.accountId);
    console.log(
      `Account name: ${accountInfo.name}, Status: ${getAccountStatusText(
        accountInfo.account_status
      )}`
    );

    // Get all ads in the account
    console.log("Fetching all ads in the account...");
    const allAds = await fetchAllAds(config.accountId);

    if (allAds.length === 0) {
      console.log("No ads found in this account. Exiting.");
      process.exit(0);
    }

    console.log(`Found ${allAds.length} ads in account`);

    // Add account info to each ad
    allAds.forEach((ad) => {
      ad.account_id = config.accountId;
      ad.account_name = accountInfo.name || "Unknown";
    });

    // Limit ads if configured
    let adsToProcess = allAds;
    if (
      config.maxAdsPerAccount > 0 &&
      allAds.length > config.maxAdsPerAccount
    ) {
      console.log(
        `Limiting to ${config.maxAdsPerAccount} ads as specified in configuration`
      );
      adsToProcess = allAds.slice(0, config.maxAdsPerAccount);
    }

    const adIds = adsToProcess.map((ad) => ad.id);

    // Get performance data
    console.log("Fetching performance data for all ads...");
    const performanceData = await getAdPerformanceData(adIds, dateRange);
    const processedPerformanceData = processPerformanceData(performanceData);

    // Get video links
    console.log("Fetching video links for all ads...");
    let videoLinks = await getVideoLinks(adIds);

    // Add ad names to video links
    const adMap = new Map(allAds.map((ad) => [ad.id, ad]));
    videoLinks = videoLinks.map((link) => {
      const ad = adMap.get(link.ad_id);
      return {
        ...link,
        ad_name: ad ? ad.name : "Unknown",
        campaign_name: ad ? ad.campaign_name : "Unknown",
        adset_name: ad ? ad.adset_name : "Unknown",
      };
    });

    console.log(`Found ${videoLinks.length} video/image assets`);

    // Save results to CSV
    console.log("Saving results to CSV files...");
    saveToCSV(allAds, "meta-all-ads");
    saveToCSV(processedPerformanceData, "meta-ad-performance");
    saveToCSV(videoLinks, "meta-video-links");

    // Save to Google Sheets if enabled
    if (config.googleSheets.enabled) {
      console.log("\nSaving data to Google Sheets...");
      await saveToGoogleSheets(allAds, config.googleSheets.allAdsSheet);
      await saveToGoogleSheets(
        processedPerformanceData,
        config.googleSheets.adPerformanceSheet
      );
      await saveToGoogleSheets(videoLinks, config.googleSheets.videoLinksSheet);
    }

    // Calculate total elapsed time
    const totalElapsedMs = Date.now() - startTime;
    const totalElapsedMinutes = Math.floor(totalElapsedMs / 60000);
    const totalElapsedSeconds = ((totalElapsedMs % 60000) / 1000).toFixed(0);
    console.log(
      `\nData extraction completed at ${new Date().toLocaleString()}`
    );
    console.log(
      `Total execution time: ${totalElapsedMinutes}m ${totalElapsedSeconds}s`
    );
  } catch (error) {
    console.error("Error processing account data:", error.message);

    // Calculate elapsed time even on error
    const totalElapsedMs = Date.now() - startTime;
    const totalElapsedMinutes = Math.floor(totalElapsedMs / 60000);
    const totalElapsedSeconds = ((totalElapsedMs % 60000) / 1000).toFixed(0);
    console.log(
      `\nExecution failed after: ${totalElapsedMinutes}m ${totalElapsedSeconds}s`
    );
    process.exit(1);
  }
}

// Run the script
getMetaAccountData().catch((error) => {
  console.error("Fatal error:", error);
  process.exit(1);
});
