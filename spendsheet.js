require("dotenv").config();
const axios = require("axios");
const fs = require("fs");
const path = require("path");

// Validate required environment variables
const requiredEnvVars = ["META_ACCESS_TOKEN", "META_ACCOUNT_IDS"];
const missingVars = requiredEnvVars.filter((varName) => !process.env[varName]);

if (missingVars.length > 0) {
  console.error(
    `Error: Missing required environment variables: ${missingVars.join(", ")}`
  );
  console.error(
    "Please ensure META_ACCESS_TOKEN and META_ACCOUNT_IDS are set in your .env file"
  );
  console.error(
    "META_ACCOUNT_IDS should be a comma-separated list of account IDs"
  );
  process.exit(1);
}

// Configuration
const config = {
  accessToken: process.env.META_ACCESS_TOKEN,
  accountIds: process.env.META_ACCOUNT_IDS.split(",").map((id) => id.trim()),
  apiVersion: process.env.META_API_VERSION || "v18.0",
  baseUrl: "https://graph.facebook.com",
  dataDir: process.env.DATA_DIR || "./data",
  defaultLookbackDays: parseInt(process.env.LOOKBACK_DAYS, 10) || 30, // Reduced from 90 to 30 days
  retryAttempts: parseInt(process.env.RETRY_ATTEMPTS, 10) || 3,
  currency: process.env.CURRENCY || "USD",
  // Patterns to match in ad names
  adNamePatterns: ["HS", "HS_", "HS-", "CL"],
  // Request throttling to avoid API rate limits - reduced to speed up processing
  requestDelay: parseInt(process.env.REQUEST_DELAY, 10) || 200, // ms - optimized for ~55 min runtime
  // Options for more aggressive filtering
  enableAdFiltering: process.env.ENABLE_AD_FILTERING !== "false", // Enabled by default
  onlyIncludeActiveAds: process.env.ONLY_INCLUDE_ACTIVE_ADS !== "false", // Only include active/paused ads
  skipFutureDateRanges: process.env.SKIP_FUTURE_DATE_RANGES !== "false", // Skip future date ranges
  enableSampleTesting: process.env.ENABLE_SAMPLE_TESTING !== "false", // Test a sample ad before processing full batch
  // Maximum ads to process per account (0 = no limit)
  maxAdsPerAccount: parseInt(process.env.MAX_ADS_PER_ACCOUNT, 10) || 0,
  // Maximum accounts to process (0 = no limit)
  maxAccounts: parseInt(process.env.MAX_ACCOUNTS, 10) || 0,
  // Google Sheets integration
  googleSheets: {
    enabled: process.env.GOOGLE_SHEETS_ENABLED === "true",
    keyFilePath:
      process.env.GOOGLE_SHEETS_KEY_FILE || "./google-service-account-key.json",
    spreadsheetId: process.env.GOOGLE_SHEETS_SPREADSHEET_ID || "",
    matchingAdsSheet:
      process.env.GOOGLE_SHEETS_MATCHING_ADS_SHEET || "MatchingAds",
    adLevelSpendSheet:
      process.env.GOOGLE_SHEETS_AD_LEVEL_SPEND_SHEET || "AdLevelSpend",
    adSummarySheet: process.env.GOOGLE_SHEETS_AD_SUMMARY_SHEET || "AdSummary",
    campaignSummarySheet:
      process.env.GOOGLE_SHEETS_CAMPAIGN_SUMMARY_SHEET || "CampaignSummary",
    accountSummarySheet:
      process.env.GOOGLE_SHEETS_ACCOUNT_SUMMARY_SHEET || "AccountSummary",
  },
};

// Dependencies for Google Sheets API - MOVED HERE AFTER config is defined
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
  const startDate = new Date(now.getFullYear(), now.getMonth(), 1); // First day of current month
  const endDate = now; // Today

  return {
    since: formatDate(startDate),
    until: formatDate(endDate),
  };
};

// Break date range into chunks for better API handling
function getDateChunks(startDate, endDate, chunkSizeDays = 30) {
  const chunks = [];
  const startTime = new Date(startDate).getTime();
  const endTime = new Date(endDate).getTime();
  const dayInMs = 86400000; // 24 * 60 * 60 * 1000

  for (
    let chunkStart = startTime;
    chunkStart < endTime;
    chunkStart += chunkSizeDays * dayInMs
  ) {
    const chunkEnd = Math.min(
      chunkStart + (chunkSizeDays - 1) * dayInMs,
      endTime
    );

    chunks.push({
      since: formatDate(new Date(chunkStart)),
      until: formatDate(new Date(chunkEnd)),
    });
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
      range: `${sheetName}!A1:ZZ`,
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

// Get all ads from an account that match the name patterns
async function fetchAdsMatchingPattern(accountId) {
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
        "effective_status",
        "created_time",
        "updated_time",
      ],
      limit: 500,
    };

    console.log(`Fetching all ads for account ${accountId}...`);
    const allAds = await fetchPaginatedData(endpoint, params);
    console.log(`Found ${allAds.length} total ads in account ${accountId}`);

    // Filter for ads that match any of the patterns AND are active
    const matchingAds = allAds.filter((ad) => {
      if (!ad.name) return false;

      // Check if ad name contains any of our patterns
      const nameMatches = config.adNamePatterns.some((pattern) =>
        ad.name.includes(pattern)
      );

      // Only return ads that match the name pattern and are active
      return nameMatches;
    });

    console.log(
      `Found ${
        matchingAds.length
      } ads matching patterns ${config.adNamePatterns.join(
        ", "
      )} in account ${accountId}`
    );

    // Further filter to only include active ads
    const activeStatuses = ["ACTIVE", "PAUSED"];
    const activeAds = matchingAds.filter((ad) => {
      // Check effective_status for active ads
      return activeStatuses.includes(ad.effective_status);
    });

    console.log(
      `Filtered down to ${activeAds.length} active ads out of ${matchingAds.length} matching ads`
    );

    return activeAds;
  } catch (error) {
    console.error(
      `Error fetching ads for account ${accountId}:`,
      error.message
    );
    return [];
  }
}

// Get spend data for specific ads
async function getAdSpendData(adIds, dateRange = getDateRange()) {
  if (!adIds.length) return [];

  try {
    // Process ads in smaller batches to avoid URL length limits
    const batchSize = 50;
    let allSpendData = [];

    // Use date chunking to avoid hitting API limits
    // Filter out future date ranges - they won't have data
    const now = new Date();
    const dateChunks = getDateChunks(
      dateRange.since,
      dateRange.until,
      30
    ).filter((chunk) => new Date(chunk.until) <= now);

    console.log(
      `Using ${dateChunks.length} valid date chunks (excluding future dates)`
    );

    if (dateChunks.length === 0) {
      console.log(
        `No valid date chunks found - all date ranges appear to be in the future`
      );
      return [];
    }

    // Create a map to track ads with zero data to avoid rechecking them
    const adsWithNoData = new Set();

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

        // Test with multiple sample ads first to avoid unnecessary API calls
        if (batch.length > 5 && config.enableSampleTesting) {
          // Take up to 4 sample ads (or fewer if batch is smaller)
          const sampleSize = Math.min(4, batch.length);
          const sampleAdIds = batch.slice(0, sampleSize);
          let hasData = false;

          console.log(
            `Testing ${sampleSize} sample ads for data in this date range...`
          );

          // Test each sample ad
          for (const sampleAdId of sampleAdIds) {
            try {
              const sampleEndpoint = `${sampleAdId}/insights`;
              const sampleParams = {
                level: "ad",
                time_increment: 1,
                time_range: chunk,
                fields: ["spend"],
              };

              const sampleUrl = buildApiUrl(sampleEndpoint, sampleParams);
              const sampleResponse = await makeApiRequest(sampleUrl);

              // If any sample ad has data, we'll process the batch
              if (sampleResponse.data && sampleResponse.data.length > 0) {
                console.log(
                  `Sample ad ${sampleAdId} has data, will process full batch`
                );
                hasData = true;
                break; // One ad with data is enough to process the batch
              } else {
                console.log(
                  `Sample ad ${sampleAdId} has no data for this date range`
                );
              }

              // Add a small delay between sample tests
              await new Promise((resolve) =>
                setTimeout(resolve, config.requestDelay / 2)
              );
            } catch (error) {
              console.error(
                `Error testing sample ad ${sampleAdId}: ${error.message}`
              );
              // Continue with next sample ad
            }
          }

          // If none of the sample ads had data, skip this batch
          if (!hasData) {
            console.log(
              `None of the ${sampleSize} sample ads had data for this date range, skipping batch...`
            );
            continue; // Skip to next date chunk
          }
        }

        // Process each ad individually, skipping those we already know have no data
        for (const adId of batch) {
          // Skip ads we've already determined have no data
          if (adsWithNoData.has(adId)) {
            continue;
          }

          try {
            const endpoint = `${adId}/insights`;

            const params = {
              level: "ad",
              time_increment: 1, // Daily data
              time_range: chunk,
              fields: [
                "ad_id",
                "ad_name",
                "adset_id",
                "adset_name",
                "campaign_id",
                "campaign_name",
                "date_start",
                "date_stop",
                "impressions",
                "clicks",
                "cpc",
                "cpm",
                "ctr",
                "reach",
                "spend",
                "frequency",
                "conversions",
                "cost_per_conversion",
                "website_ctr",
                "outbound_clicks",
                "actions",
              ],
            };

            const url = buildApiUrl(endpoint, params);
            const response = await makeApiRequest(url);

            if (response && response.data && Array.isArray(response.data)) {
              // Filter out zero spend records immediately
              const nonZeroSpendData = response.data.filter(
                (record) => parseFloat(record.spend || 0) > 0
              );

              if (nonZeroSpendData.length > 0) {
                allSpendData = [...allSpendData, ...nonZeroSpendData];
                console.log(
                  `Got ${nonZeroSpendData.length} data points with spend for ad ${adId}`
                );
              } else {
                // If the ad has no non-zero spend for this chunk, mark it
                console.log(`No spend data for ad ${adId} in this date range`);
                // Add to our tracking set if all queries so far have returned zero spend
                if (!allSpendData.some((record) => record.ad_id === adId)) {
                  adsWithNoData.add(adId);
                }
              }
            } else {
              console.log(`No data returned for ad ${adId}`);
            }

            // Add a small delay between requests
            await new Promise((resolve) =>
              setTimeout(resolve, config.requestDelay)
            );
          } catch (error) {
            console.error(
              `Error fetching insights for ad ID ${adId}: ${error.message}`
            );
            // Continue with next ad rather than failing entire batch
          }
        }
      }
    }

    console.log(
      `Total number of ads with no spend data: ${adsWithNoData.size}`
    );
    console.log(`Total records with non-zero spend: ${allSpendData.length}`);

    return allSpendData;
  } catch (error) {
    console.error(`Error fetching ad spend data:`, error.message);
    return [];
  }
}

// Process spend data to extract action values (conversions, etc.)
function processSpendData(spendData) {
  return spendData.map((record) => {
    const processed = { ...record };

    // Extract values from actions array if present
    if (record.actions && Array.isArray(record.actions)) {
      record.actions.forEach((action) => {
        if (action.action_type && action.value !== undefined) {
          processed[`action_${action.action_type}`] = action.value;
        }
      });
    }

    // Delete the original actions array to avoid duplicating data
    delete processed.actions;

    // Extract website CTR data if present
    if (
      record.website_ctr &&
      Array.isArray(record.website_ctr) &&
      record.website_ctr.length > 0
    ) {
      processed.website_ctr_value = record.website_ctr[0].value;
      delete processed.website_ctr;
    }

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

// Helper function to determine which pattern the ad matched
function getPatternMatched(adName) {
  if (!adName) return "Unknown";

  for (const pattern of config.adNamePatterns) {
    if (adName.includes(pattern)) {
      return pattern;
    }
  }

  return "Unknown";
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

// Main function to get ad-level spend data
async function getAdLevelSpend() {
  console.log(
    `Starting Meta ad-level spend data extraction at ${new Date().toLocaleString()}`
  );
  console.log(
    `Found ${config.accountIds.length} accounts in environment variables`
  );

  if (config.maxAccounts > 0 && config.accountIds.length > config.maxAccounts) {
    console.log(
      `Limiting to ${config.maxAccounts} accounts as specified in configuration`
    );
    config.accountIds = config.accountIds.slice(0, config.maxAccounts);
  }

  console.log(
    `Looking for ads matching patterns: ${config.adNamePatterns.join(", ")}`
  );
  console.log(`Request delay between API calls: ${config.requestDelay}ms`);
  console.log(
    `Filtering options: ${config.enableAdFiltering ? "Enabled" : "Disabled"}`
  );
  if (config.enableAdFiltering) {
    console.log(
      `- Only include active ads: ${config.onlyIncludeActiveAds ? "Yes" : "No"}`
    );
    console.log(
      `- Skip future date ranges: ${config.skipFutureDateRanges ? "Yes" : "No"}`
    );
    console.log(
      `- Use sample testing: ${config.enableSampleTesting ? "Yes" : "No"}`
    );
  }

  console.log(
    `Google Sheets integration: ${
      config.googleSheets.enabled ? "Enabled" : "Disabled"
    }`
  );
  if (config.googleSheets.enabled) {
    console.log(`- Spreadsheet ID: ${config.googleSheets.spreadsheetId}`);
    console.log(`- Key file path: ${config.googleSheets.keyFilePath}`);
  }

  const dateRange = getDateRange();
  console.log(
    `Fetching data for the last ${config.defaultLookbackDays} days (${dateRange.since} to ${dateRange.until})`
  );

  const accountInfoList = [];
  let allAds = [];
  let allAdSpendData = [];

  // Start time tracking
  const startTime = Date.now();

  // Process each account
  for (let i = 0; i < config.accountIds.length; i++) {
    const accountId = config.accountIds[i];
    console.log(
      `\nProcessing account ${i + 1}/${config.accountIds.length}: ${accountId}`
    );

    try {
      // Get account info
      const accountInfo = await getAccountInfo(accountId);
      accountInfoList.push(accountInfo);

      // Get all ads matching our patterns
      let matchingAds = await fetchAdsMatchingPattern(accountId);

      // Limit number of ads per account if configured
      if (
        config.maxAdsPerAccount > 0 &&
        matchingAds.length > config.maxAdsPerAccount
      ) {
        console.log(
          `Limiting to ${config.maxAdsPerAccount} ads as specified in configuration`
        );
        matchingAds = matchingAds.slice(0, config.maxAdsPerAccount);
      }

      // Add account info to each ad
      matchingAds.forEach((ad) => {
        ad.account_id = accountId;
        ad.account_name = accountInfo.name || "Unknown";
      });

      allAds = [...allAds, ...matchingAds];

      // If we found matching ads, get their spend data
      if (matchingAds.length > 0) {
        const adIds = matchingAds.map((ad) => ad.id);
        console.log(`Fetching spend data for ${adIds.length} matching ads...`);

        const adSpendData = await getAdSpendData(adIds, dateRange);

        // Add account info to each spend record
        adSpendData.forEach((record) => {
          record.account_id = accountId;
          record.account_name = accountInfo.name || "Unknown";
        });

        allAdSpendData = [...allAdSpendData, ...adSpendData];
      }

      // Add a delay between processing accounts
      if (i < config.accountIds.length - 1) {
        console.log(`Pausing before next account...`);
        await new Promise((resolve) => setTimeout(resolve, 1000));
      }

      // Calculate and log elapsed time
      const elapsedMs = Date.now() - startTime;
      const elapsedMinutes = Math.floor(elapsedMs / 60000);
      const elapsedSeconds = ((elapsedMs % 60000) / 1000).toFixed(0);
      console.log(`Time elapsed: ${elapsedMinutes}m ${elapsedSeconds}s`);
    } catch (error) {
      console.error(`Error processing account ${accountId}:`, error.message);
    }
  }

  if (allAdSpendData.length === 0) {
    console.log(
      `\nNo ads with spend data found matching the criteria. Try adjusting the date range or check if the ads were active during this period.`
    );

    // Calculate total elapsed time
    const totalElapsedMs = Date.now() - startTime;
    const totalElapsedMinutes = Math.floor(totalElapsedMs / 60000);
    const totalElapsedSeconds = ((totalElapsedMs % 60000) / 1000).toFixed(0);
    console.log(
      `Total execution time: ${totalElapsedMinutes}m ${totalElapsedSeconds}s`
    );

    return;
  }

  console.log(
    `\nFound ${allAds.length} ads matching patterns across all accounts`
  );
  console.log(
    `Collected ${allAdSpendData.length} spend data records with non-zero spend`
  );

  // Process the spend data to extract action values
  const processedSpendData = processSpendData(allAdSpendData);

  // Filter ads list to only include those with non-zero spend
  // Enhance the filtering for ads with no spend
  const adsWithSpend = new Set();
  allAdSpendData.forEach((record) => {
    // Only count ads with actual non-zero spend
    if (parseFloat(record.spend || 0) > 0) {
      adsWithSpend.add(record.ad_id);
    }
  });

  // Filter out ads with zero spend
  const filteredAds = allAds.filter((ad) => adsWithSpend.has(ad.id));
  console.log(
    `Filtered down to ${filteredAds.length} ads with actual non-zero spend data`
  );

  console.log(
    `Filtered down to ${filteredAds.length} ads with actual spend data`
  );

  // Save results to CSV
  saveToCSV(filteredAds, "meta-matching-ads-list");
  saveToCSV(processedSpendData, "meta-ad-level-spend");

  // Generate summary data
  const { adSummary, campaignSummary, accountSummary } = generateSpendSummaries(
    processedSpendData,
    accountInfoList
  );

  // Save to Google Sheets if enabled
  if (config.googleSheets.enabled) {
    console.log("\nSaving data to Google Sheets...");
    await saveToGoogleSheets(filteredAds, config.googleSheets.matchingAdsSheet);
    await saveToGoogleSheets(
      processedSpendData,
      config.googleSheets.adLevelSpendSheet
    );
    await saveToGoogleSheets(adSummary, config.googleSheets.adSummarySheet);
    await saveToGoogleSheets(
      campaignSummary,
      config.googleSheets.campaignSummarySheet
    );
    await saveToGoogleSheets(
      accountSummary,
      config.googleSheets.accountSummarySheet
    );
  }

  // Calculate total elapsed time
  const totalElapsedMs = Date.now() - startTime;
  const totalElapsedMinutes = Math.floor(totalElapsedMs / 60000);
  const totalElapsedSeconds = ((totalElapsedMs % 60000) / 1000).toFixed(0);
  console.log(`\nData extraction completed at ${new Date().toLocaleString()}`);
  console.log(
    `Total execution time: ${totalElapsedMinutes}m ${totalElapsedSeconds}s`
  );
}

// Generate summary views of the spend data
function generateSpendSummaries(spendData, accountInfoList) {
  // Calculate date ranges for summary periods
  const now = new Date();
  const last30Days = new Date(now);
  last30Days.setDate(last30Days.getDate() - 30);
  const last30DaysStr = formatDate(last30Days);

  const last90Days = new Date(now);
  last90Days.setDate(last90Days.getDate() - 90);
  const last90DaysStr = formatDate(last90Days);

  // 1. Create ad-level summary
  const adSummary = [];
  const adSpendMap = {};

  // Group by ad_id and sum spend
  spendData.forEach((record) => {
    const adId = record.ad_id;
    const adName = record.ad_name;
    const date = record.date_start;
    const spend = parseFloat(record.spend || 0);
    const clicks = parseInt(record.clicks || 0);
    const impressions = parseInt(record.impressions || 0);

    if (!adSpendMap[adId]) {
      adSpendMap[adId] = {
        ad_id: adId,
        ad_name: adName,
        account_id: record.account_id,
        account_name: record.account_name,
        campaign_id: record.campaign_id,
        campaign_name: record.campaign_name,
        adset_id: record.adset_id,
        adset_name: record.adset_name,
        total_spend: 0,
        total_clicks: 0,
        total_impressions: 0,
        spend_last_30days: 0,
        clicks_last_30days: 0,
        impressions_last_30days: 0,
        pattern_matched: getPatternMatched(adName),
      };
    }

    // Add to totals
    adSpendMap[adId].total_spend += spend;
    adSpendMap[adId].total_clicks += clicks;
    adSpendMap[adId].total_impressions += impressions;

    // Add to 30 day totals if applicable
    if (date >= last30DaysStr) {
      adSpendMap[adId].spend_last_30days += spend;
      adSpendMap[adId].clicks_last_30days += clicks;
      adSpendMap[adId].impressions_last_30days += impressions;
    }
  });

  // Convert to array and calculate metrics
  Object.values(adSpendMap).forEach((ad) => {
    // Calculate CTR and CPC
    ad.total_ctr =
      ad.total_impressions > 0
        ? ((ad.total_clicks / ad.total_impressions) * 100).toFixed(2)
        : "0.00";

    ad.total_cpc =
      ad.total_clicks > 0
        ? (ad.total_spend / ad.total_clicks).toFixed(2)
        : "0.00";

    ad.ctr_last_30days =
      ad.impressions_last_30days > 0
        ? ((ad.clicks_last_30days / ad.impressions_last_30days) * 100).toFixed(
            2
          )
        : "0.00";

    ad.cpc_last_30days =
      ad.clicks_last_30days > 0
        ? (ad.spend_last_30days / ad.clicks_last_30days).toFixed(2)
        : "0.00";

    // Format spend values
    ad.total_spend = ad.total_spend.toFixed(2);
    ad.spend_last_30days = ad.spend_last_30days.toFixed(2);

    adSummary.push(ad);
  });

  // Sort by total spend (highest first)
  campaignSummary.sort(
    (a, b) => parseFloat(b.total_spend) - parseFloat(a.total_spend)
  );

  // 3. Create account-level summary
  const accountSummary = [];
  const accountSpendMap = {};

  // Use account info as a base and add spend data
  accountInfoList.forEach((account) => {
    accountSpendMap[account.id] = {
      account_id: account.id,
      account_name: account.name || "Unknown",
      business_name: account.business_name || "Unknown",
      currency: account.currency || config.currency,
      status: getAccountStatusText(account.account_status),
      matching_ads_spend: 0,
      matching_ads_spend_last_30days: 0,
      matching_ads_spend_last_90days: 0,
      matching_ads_count: 0,
      campaign_count: 0,
    };
  });

  // Add spend data from matching ads
  spendData.forEach((record) => {
    const accountId = record.account_id;
    const date = record.date_start;
    const spend = parseFloat(record.spend || 0);

    if (accountSpendMap[accountId]) {
      // Add to totals
      accountSpendMap[accountId].matching_ads_spend += spend;

      // Add to period totals
      if (date >= last30DaysStr) {
        accountSpendMap[accountId].matching_ads_spend_last_30days += spend;
      }
      if (date >= last90DaysStr) {
        accountSpendMap[accountId].matching_ads_spend_last_90days += spend;
      }
    }
  });

  // Count unique ads and campaigns per account
  const uniqueAdsPerAccount = {};
  const uniqueCampaignsPerAccount = {};

  adSummary.forEach((ad) => {
    const accountId = ad.account_id;

    if (!uniqueAdsPerAccount[accountId]) {
      uniqueAdsPerAccount[accountId] = new Set();
    }
    uniqueAdsPerAccount[accountId].add(ad.ad_id);

    if (!uniqueCampaignsPerAccount[accountId]) {
      uniqueCampaignsPerAccount[accountId] = new Set();
    }
    uniqueCampaignsPerAccount[accountId].add(ad.campaign_id);
  });

  // Update account summary with counts
  Object.keys(accountSpendMap).forEach((accountId) => {
    if (uniqueAdsPerAccount[accountId]) {
      accountSpendMap[accountId].matching_ads_count =
        uniqueAdsPerAccount[accountId].size;
    }

    if (uniqueCampaignsPerAccount[accountId]) {
      accountSpendMap[accountId].campaign_count =
        uniqueCampaignsPerAccount[accountId].size;
    }

    // Format spend values
    accountSpendMap[accountId].matching_ads_spend =
      accountSpendMap[accountId].matching_ads_spend.toFixed(2);
    accountSpendMap[accountId].matching_ads_spend_last_30days =
      accountSpendMap[accountId].matching_ads_spend_last_30days.toFixed(2);
    accountSpendMap[accountId].matching_ads_spend_last_90days =
      accountSpendMap[accountId].matching_ads_spend_last_90days.toFixed(2);

    accountSummary.push(accountSpendMap[accountId]);
  });

  // Calculate totals across all accounts
  const totalMatchingAdsSpend = accountSummary.reduce(
    (sum, account) => sum + parseFloat(account.matching_ads_spend),
    0
  );

  const totalLast30DaysSpend = accountSummary.reduce(
    (sum, account) => sum + parseFloat(account.matching_ads_spend_last_30days),
    0
  );

  const totalLast90DaysSpend = accountSummary.reduce(
    (sum, account) => sum + parseFloat(account.matching_ads_spend_last_90days),
    0
  );

  const totalMatchingAdsCount = accountSummary.reduce(
    (sum, account) => sum + account.matching_ads_count,
    0
  );

  const totalCampaignCount = accountSummary.reduce(
    (sum, account) => sum + account.campaign_count,
    0
  );

  // Add totals row
  accountSummary.push({
    account_id: "TOTAL",
    account_name: "All Accounts",
    business_name: "",
    currency: config.currency,
    status: "",
    matching_ads_spend: totalMatchingAdsSpend.toFixed(2),
    matching_ads_spend_last_30days: totalLast30DaysSpend.toFixed(2),
    matching_ads_spend_last_90days: totalLast90DaysSpend.toFixed(2),
    matching_ads_count: totalMatchingAdsCount,
    campaign_count: totalCampaignCount,
  });

  // Save summaries to CSV
  saveToCSV(adSummary, "meta-ad-level-summary");
  saveToCSV(campaignSummary, "meta-campaign-level-summary");
  saveToCSV(accountSummary, "meta-account-level-summary");

  // Log summary statistics
  console.log("\n===== Ad Spend Summary =====");
  console.log(`Total matching ads found: ${totalMatchingAdsCount}`);
  console.log(`Total campaigns with matching ads: ${totalCampaignCount}`);
  console.log(
    `Total spend for matching ads: ${totalMatchingAdsSpend.toFixed(2)} ${
      config.currency
    }`
  );
  console.log(
    `Total spend for matching ads (last 30 days): ${totalLast30DaysSpend.toFixed(
      2
    )} ${config.currency}`
  );
  console.log(
    `Total spend for matching ads (last 90 days): ${totalLast90DaysSpend.toFixed(
      2
    )} ${config.currency}`
  );

  // Return summary data for Google Sheets integration
  return {
    adSummary,
    campaignSummary,
    accountSummary,
  };
}

// Run the script
getAdLevelSpend().catch((error) => {
  console.error("Fatal error:", error);
  process.exit(1);
});
