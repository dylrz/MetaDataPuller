// Require necessary modules
require("dotenv").config();
console.log(
  "Loaded Token (first 5 chars):",
  process.env.META_ACCESS_TOKEN
    ? process.env.META_ACCESS_TOKEN.substring(0, 5)
    : "Not Loaded"
);
console.log("Loaded Account IDs:", process.env.META_ACCOUNT_IDS);
const axios = require("axios");
const fs = require("fs");
const path = require("path");

// --- Environment Variable Validation (Unchanged) ---
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

// --- Configuration ---
const config = {
  accessToken: process.env.META_ACCESS_TOKEN,
  accountIds: process.env.META_ACCOUNT_IDS.split(",").map((id) => id.trim()),
  apiVersion: process.env.META_API_VERSION || "v20.0", // Use a specific, stable version
  baseUrl: "https://graph.facebook.com",
  // Account-specific patterns override default
  accountPatterns: (() => {
    try {
      return process.env.ACCOUNT_PATTERNS
        ? JSON.parse(process.env.ACCOUNT_PATTERNS)
        : { default: ["HS", "HS_", "HS-", "CL", "DFAD"] }; // Default patterns if ACCOUNT_PATTERNS is not set
    } catch (e) {
      console.error("Error parsing ACCOUNT_PATTERNS env var:", e.message);
      return { default: ["HS", "HS_", "HS-", "CL", "DFAD"] }; // Fallback default
    }
  })(),
  // DEPRECATED: Use accountPatterns instead. Keeping for backwards compatibility check but ideally remove.
  // adNamePatterns: process.env.AD_NAME_PATTERNS?.split(",").map(p => p.trim()) || ["HS", "HS_", "HS-", "CL", "DFAD"],
  dataDir: process.env.DATA_DIR || "./data",
  defaultLookbackDays: parseInt(process.env.LOOKBACK_DAYS, 10) || 90, // Default lookback (can be overridden by specific range)
  retryAttempts: parseInt(process.env.RETRY_ATTEMPTS, 10) || 5, // Increased default retries
  currency: process.env.CURRENCY || "USD",
  // Request throttling: Delay between major API calls (e.g., fetching insights per chunk/account)
  requestDelay: parseInt(process.env.REQUEST_DELAY, 10) || 1000, // ms - Increased default delay (1 second)
  // Filtering options
  enableAdFiltering: process.env.ENABLE_AD_FILTERING !== "false", // Default: true
  onlyIncludeActiveAds: process.env.ONLY_INCLUDE_ACTIVE_ADS === "true", // Default: false (change if needed)
  skipFutureDateRanges: process.env.SKIP_FUTURE_DATE_RANGES !== "false", // Default: true
  enableSampleTesting: process.env.ENABLE_SAMPLE_TESTING === "true", // Default: false
  // Limits
  maxAdsPerAccount: parseInt(process.env.MAX_ADS_PER_ACCOUNT, 10) || 0, // 0 = no limit
  maxAccounts: parseInt(process.env.MAX_ACCOUNTS, 10) || 0, // 0 = no limit
  // Google Sheets Integration
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
  // --- NEW: Batch size for Ad IDs in insight filters ---
  insightAdIdBatchSize:
    parseInt(process.env.INSIGHT_AD_ID_BATCH_SIZE, 10) || 100, // Number of ad IDs per filter request
};

// --- Google Sheets Dependencies (Load conditionally AFTER config) ---
let google, sheets;
try {
  if (config.googleSheets.enabled) {
    const { google: googleApi } = require("googleapis");
    google = googleApi;
    sheets = google.sheets("v4");
    console.log("Google Sheets API loaded successfully.");
  }
} catch (error) {
  console.error("Error loading Google Sheets API dependencies:", error.message);
  console.error(
    "If you want to use Google Sheets integration, run: npm install googleapis"
  );
  config.googleSheets.enabled = false;
}

// --- Create output directory (Unchanged) ---
if (!fs.existsSync(config.dataDir)) {
  fs.mkdirSync(config.dataDir, { recursive: true });
}

// --- Utility Functions ---

const formatDate = (date) => {
  // Ensures the date is treated as UTC to avoid timezone shifts
  const d = new Date(date);
  const year = d.getUTCFullYear();
  const month = String(d.getUTCMonth() + 1).padStart(2, "0"); // getUTCMonth is 0-indexed
  const day = String(d.getUTCDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
};

// Calculate date range based on lookback days or defaults
const getDateRange = () => {
  const now = new Date();
  const endDate = new Date(now); // Today
  const startDate = new Date(now);
  startDate.setDate(now.getDate() - config.defaultLookbackDays);

  // Handle potential override from environment variables (optional)
  const sinceEnv = process.env.DATE_RANGE_SINCE;
  const untilEnv = process.env.DATE_RANGE_UNTIL;

  const finalSince = sinceEnv
    ? formatDate(new Date(sinceEnv))
    : formatDate(startDate);
  const finalUntil = untilEnv
    ? formatDate(new Date(untilEnv))
    : formatDate(endDate);

  // Ensure 'until' is not in the future if skipFutureDateRanges is true
  const todayStr = formatDate(new Date());
  const effectiveUntil =
    config.skipFutureDateRanges && finalUntil > todayStr
      ? todayStr
      : finalUntil;

  return {
    since: finalSince,
    until: effectiveUntil,
  };
};

// Break date range into chunks
function getDateChunks(startDateStr, endDateStr, chunkSizeDays = 30) {
  const chunks = [];
  const start = new Date(startDateStr + "T00:00:00Z"); // Use UTC for consistency
  const end = new Date(endDateStr + "T00:00:00Z"); // Use UTC for consistency
  const today = new Date();
  today.setUTCHours(0, 0, 0, 0); // Set today to UTC start of day

  console.log(
    `Creating date chunks from ${formatDate(start)} to ${formatDate(
      end
    )} with chunk size ${chunkSizeDays} days`
  );

  let currentChunkStart = new Date(start);

  while (currentChunkStart <= end) {
    // Skip future date ranges if configured
    if (config.skipFutureDateRanges && currentChunkStart > today) {
      console.log(
        `Skipping future date chunk starting ${formatDate(currentChunkStart)}`
      );
      break; // Stop creating chunks if we hit the future
    }

    let currentChunkEnd = new Date(currentChunkStart);
    currentChunkEnd.setUTCDate(
      currentChunkEnd.getUTCDate() + chunkSizeDays - 1
    );

    // Ensure chunk end doesn't exceed the overall end date
    if (currentChunkEnd > end) {
      currentChunkEnd = new Date(end);
    }

    // Ensure chunk end doesn't exceed today if skipping future dates
    if (config.skipFutureDateRanges && currentChunkEnd > today) {
      currentChunkEnd = new Date(today);
    }

    // Only add chunk if the start date is not after the end date
    if (currentChunkStart <= currentChunkEnd) {
      chunks.push({
        since: formatDate(currentChunkStart),
        until: formatDate(currentChunkEnd),
      });
    } else {
      console.log(
        `Skipping invalid chunk: start ${formatDate(
          currentChunkStart
        )} > end ${formatDate(currentChunkEnd)}`
      );
    }

    // Move to the next chunk's start date
    currentChunkStart = new Date(currentChunkEnd);
    currentChunkStart.setUTCDate(currentChunkStart.getUTCDate() + 1);
  }
  console.log(`Generated ${chunks.length} date chunks.`);
  // Log the date chunks to debug
  chunks.forEach((chunk, index) => {
    console.log(`  Chunk ${index + 1}: ${chunk.since} to ${chunk.until}`);
  });
  return chunks;
}

// --- API Request Functions ---

// Build API URL (Handles complex params better)
const buildApiUrl = (endpoint, params = {}) => {
  const url = new URL(`${config.baseUrl}/${config.apiVersion}/${endpoint}`);
  url.searchParams.append("access_token", config.accessToken);

  Object.entries(params).forEach(([key, value]) => {
    if (value === undefined || value === null) return; // Skip undefined/null params

    if (key === "filtering") {
      // Special handling for filtering param which expects JSON
      url.searchParams.append(key, JSON.stringify(value));
    } else if (key === "time_range") {
      // Special handling for time_range param which expects JSON
      url.searchParams.append(key, JSON.stringify(value));
    } else if (Array.isArray(value)) {
      url.searchParams.append(key, value.join(","));
    } else if (typeof value === "object") {
      // Generic object to JSON string (use cautiously)
      url.searchParams.append(key, JSON.stringify(value));
    } else {
      url.searchParams.append(key, value.toString());
    }
  });
  return url.toString();
};

// API request with enhanced retry logic
async function makeApiRequest(url, attempt = 1) {
  try {
    // console.log(`Making API Request (Attempt ${attempt}): ${url}`); // Optional: Verbose logging
    const response = await axios.get(url, { timeout: 60000 }); // Added timeout
    // console.log(`API Request Success: ${url}`); // Optional
    return response.data;
  } catch (error) {
    const statusCode = error.response?.status;
    const errorCode = error.response?.data?.error?.code;
    const errorSubcode = error.response?.data?.error?.error_subcode;
    const errorMessage = error.response?.data?.error?.message || error.message;

    console.error(
      `API Error [Status: ${statusCode || "N/A"}, Code: ${
        errorCode || "N/A"
      }, Subcode: ${
        errorSubcode || "N/A"
      }, Attempt: ${attempt}]: ${errorMessage}`
    );
    console.error(`Failed URL: ${url}`); // Log the failing URL

    if (attempt < config.retryAttempts) {
      let delay = Math.pow(2, attempt) * 1000; // Exponential backoff (1s, 2s, 4s, ...)

      // Check for specific rate limit errors (adjust codes as needed based on Meta docs)
      // 80004: Request limit reached
      // 4, 17, 32, 341: Temporary API issues
      // 613: Call takes too long, retry later
      const rateLimitOrServerErrors = [4, 17, 32, 341, 613, 80004];
      if (
        rateLimitOrServerErrors.includes(errorCode) ||
        statusCode === 429 ||
        (statusCode >= 500 && statusCode < 600)
      ) {
        // Respect 'Retry-After' header if present (value is in seconds)
        const retryAfterHeader = error.response?.headers?.["retry-after"];
        if (retryAfterHeader) {
          const retryAfterSeconds = parseInt(retryAfterHeader, 10);
          if (!isNaN(retryAfterSeconds) && retryAfterSeconds > 0) {
            delay = Math.max(delay, retryAfterSeconds * 1000); // Use whichever delay is longer
            console.log(
              `Respecting Retry-After header: waiting ${delay / 1000}s`
            );
          }
        }

        console.log(
          `Retrying attempt ${attempt + 1}/${config.retryAttempts} after ${
            delay / 1000
          }s...`
        );
        await new Promise((resolve) => setTimeout(resolve, delay));
        return makeApiRequest(url, attempt + 1); // Recursive call for retry
      }
    }
    // If exhausted retries or it's not a retryable error, throw
    console.error(
      `API request failed after ${attempt} attempts for URL: ${url}`
    );
    throw error; // Re-throw the error after final attempt or for non-retryable errors
  }
}

// Fetch paginated data (Removed internal delay, relies on makeApiRequest backoff)
async function fetchPaginatedData(endpoint, params) {
  let allData = [];
  let requestUrl = buildApiUrl(endpoint, params);

  while (requestUrl) {
    try {
      const response = await makeApiRequest(requestUrl); // Use retry logic here

      if (response?.data && Array.isArray(response.data)) {
        allData = allData.concat(response.data);
      } else if (response?.data) {
        console.warn("API response.data is not an array:", response.data);
        // Handle cases where a single object might be returned unexpectedly
        if (typeof response.data === "object") {
          allData.push(response.data);
        }
      }

      // Get next page URL from paging object
      requestUrl = response?.paging?.next || null;

      // Optional: Log pagination progress
      // if (requestUrl) console.log(`Workspaceing next page...`);
    } catch (error) {
      console.error(
        `Failed to fetch page for endpoint ${endpoint}. Stopping pagination for this request.`
      );
      // Decide if you want to return partial data or throw
      // For robustness, let's return what we have so far
      console.error(`Error details: ${error.message}`);
      requestUrl = null; // Stop pagination on error for this specific call
    }
  }
  return allData;
}

// --- Core Logic Functions ---

// Get account info (Unchanged, seems fine)
async function getAccountInfo(accountId) {
  try {
    const endpoint = `act_${accountId}`;
    const params = {
      fields: "id,name,account_status,currency,business_name,amount_spent",
    };
    const url = buildApiUrl(endpoint, params);
    return await makeApiRequest(url); // Use the updated request function
  } catch (error) {
    console.error(
      `Error fetching account info for ${accountId}:`,
      error.message
    );
    // Return a structured error object
    return {
      id: accountId,
      name: `Error fetching account ${accountId}`,
      error: true,
      message: error.message,
    };
  }
}

// Fetch ads matching pattern (Mostly unchanged, added logging)
async function fetchAdsMatchingPattern(accountId, accountInfo) {
  try {
    const endpoint = `act_${accountId}/ads`;
    const params = {
      fields:
        "id,name,status,effective_status,adset{id,name},campaign{id,name}", // Use nested syntax for related fields
      limit: 500, // Adjust limit based on typical response sizes / performance
    };

    console.log(
      `Workspaceing ads for account ${accountId} (${
        accountInfo?.name || "N/A"
      })...`
    );
    const allAdsRaw = await fetchPaginatedData(endpoint, params);

    // Flatten the structure and add campaign/adset names directly
    const allAds = allAdsRaw.map((ad) => ({
      id: ad.id,
      name: ad.name,
      status: ad.status,
      effective_status: ad.effective_status,
      adset_id: ad.adset?.id,
      adset_name: ad.adset?.name,
      campaign_id: ad.campaign?.id,
      campaign_name: ad.campaign?.name,
    }));

    console.log(`Found ${allAds.length} total ads in account ${accountId}`);

    // Determine patterns: Use account-specific if defined, else default
    const patternsToUse =
      config.accountPatterns[accountId] || config.accountPatterns.default;
    if (!patternsToUse || patternsToUse.length === 0) {
      console.warn(
        `No patterns defined for account ${accountId} or default. Skipping ad filtering.`
      );
      return allAds; // Or return [] if you ONLY want matching ads
    }
    console.log(
      `Using patterns for account ${accountId}: ${patternsToUse.join(", ")}`
    );

    // Filter ads by name pattern
    let matchingAds = allAds.filter((ad) => {
      if (!ad.name) return false;
      return patternsToUse.some((pattern) => ad.name.includes(pattern));
    });
    console.log(
      `Found ${matchingAds.length} ads matching patterns in account ${accountId}`
    );

    // Optional: Filter by status (active/paused)
    if (config.enableAdFiltering && config.onlyIncludeActiveAds) {
      const activeStatuses = ["ACTIVE", "PAUSED"]; // Define active statuses
      const originalCount = matchingAds.length;
      matchingAds = matchingAds.filter(
        (ad) =>
          activeStatuses.includes(ad.status?.toUpperCase()) ||
          activeStatuses.includes(ad.effective_status?.toUpperCase())
      );
      console.log(
        `Filtered down to ${matchingAds.length} active/paused ads (from ${originalCount})`
      );
    }

    return matchingAds;
  } catch (error) {
    console.error(
      `Error fetching ads for account ${accountId}:`,
      error.message
    );
    return []; // Return empty array on error
  }
}

// *** REVISED: Get spend data efficiently using account-level insights and filtering ***
async function getAdSpendData(
  accountId,
  accountName,
  matchingAdIds,
  dateRange
) {
  if (!matchingAdIds || matchingAdIds.length === 0) {
    console.log(
      `No matching ad IDs provided for account ${accountId}. Skipping spend fetch.`
    );
    return [];
  }

  console.log(
    `Workspaceing spend data for ${matchingAdIds.length} ads in account ${accountId} (${accountName}) from ${dateRange.since} to ${dateRange.until}`
  );

  let allSpendData = [];
  const dateChunks = getDateChunks(dateRange.since, dateRange.until, 30); // Use 30-day chunks or adjust

  if (dateChunks.length === 0) {
    console.log(
      `No valid date chunks generated for account ${accountId}. Skipping spend fetch.`
    );
    return [];
  }

  // Batch Ad IDs for the filtering parameter
  for (let i = 0; i < matchingAdIds.length; i += config.insightAdIdBatchSize) {
    const adIdBatch = matchingAdIds.slice(i, i + config.insightAdIdBatchSize);
    console.log(
      `Processing Ad ID Batch ${
        Math.floor(i / config.insightAdIdBatchSize) + 1
      } (Ads ${i + 1}-${Math.min(
        i + config.insightAdIdBatchSize,
        matchingAdIds.length
      )}) for account ${accountId}`
    );

    // Fetch data for each date chunk for the current batch of Ad IDs
    for (const chunk of dateChunks) {
      console.log(
        `  Fetching insights for date chunk: ${chunk.since} to ${chunk.until}`
      );

      // Add delay before each insight API call chunk
      await new Promise((resolve) => setTimeout(resolve, config.requestDelay));

      try {
        const endpoint = `act_${accountId}/insights`;
        const params = {
          level: "ad", // Get data at the ad level
          time_increment: 1, // Daily data
          time_range: chunk, // Use the date chunk {since: 'YYYY-MM-DD', until: 'YYYY-MM-DD'}
          fields:
            "ad_id,ad_name,adset_name,campaign_name,date_start,date_stop,spend", // Add other fields as needed (e.g., impressions, clicks, actions)
          filtering: [
            {
              // *** Filter by the batch of matching ad IDs ***
              field: "ad.id",
              operator: "IN",
              value: adIdBatch,
            },
          ],
          limit: 500, // Adjust limit as needed
        };

        // Use fetchPaginatedData to handle potential pagination within the insights response
        const insightsData = await fetchPaginatedData(endpoint, params);

        if (insightsData.length > 0) {
          // Filter out records with zero spend if desired (API might return them)
          const nonZeroSpend = insightsData.filter(
            (record) => parseFloat(record.spend || 0) > 0
          );
          if (nonZeroSpend.length > 0) {
            console.log(
              `    -> Received ${
                nonZeroSpend.length
              } non-zero spend records for chunk ${chunk.since}-${
                chunk.until
              } (Batch ${Math.floor(i / config.insightAdIdBatchSize) + 1})`
            );
            allSpendData.push(...nonZeroSpend);
          } else {
            console.log(
              `    -> Received ${
                insightsData.length
              } records, but all had zero spend for chunk ${chunk.since}-${
                chunk.until
              } (Batch ${Math.floor(i / config.insightAdIdBatchSize) + 1})`
            );
          }
        } else {
          console.log(
            `    -> No insights data returned for chunk ${chunk.since}-${
              chunk.until
            } (Batch ${Math.floor(i / config.insightAdIdBatchSize) + 1})`
          );
        }
      } catch (error) {
        console.error(
          `  Error fetching insights for account ${accountId}, chunk ${
            chunk.since
          }-${chunk.until}, Ad Batch ${
            Math.floor(i / config.insightAdIdBatchSize) + 1
          }: ${error.message}`
        );
        // Optional: Decide whether to continue to the next chunk/batch or stop
      }
    } // End date chunk loop
  } // End Ad ID batch loop

  console.log(
    `Collected ${allSpendData.length} total non-zero spend records for account ${accountId}`
  );
  return allSpendData;
}

// --- Data Processing and Saving ---

// Process spend data (Extract actions, etc.) - Simplified, adapt if needed
function processSpendData(spendData) {
  if (!Array.isArray(spendData)) return []; // Guard against non-array input
  return spendData.map((record) => ({
    // Keep original fields, maybe format spend
    ...record,
    spend: parseFloat(record.spend || 0).toFixed(2), // Ensure spend is a formatted number
    // Add extraction for 'actions', 'action_values', etc. if they are included in 'fields'
    // Example:
    // total_purchases: record.actions?.find(a => a.action_type === 'purchase')?.value || 0,
    // purchase_value: record.action_values?.find(a => a.action_type === 'purchase')?.value || 0,
  }));
}

// Save to CSV (Improved escaping and error handling)
function saveToCSV(data, filename) {
  if (!data || data.length === 0) {
    console.log(`No data to save for ${filename}.csv`);
    return;
  }

  try {
    // Dynamically get headers from all objects
    const headers = [...new Set(data.flatMap((obj) => Object.keys(obj)))];
    const csvHeader = headers.join(",") + "\n";

    // Convert rows to CSV strings with proper escaping
    const csvRows = data
      .map((row) => {
        return headers
          .map((header) => {
            const value = row[header];
            let escapedValue = "";

            if (value === undefined || value === null) {
              escapedValue = "";
            } else if (typeof value === "object") {
              // Stringify objects/arrays and escape quotes within JSON
              try {
                escapedValue = JSON.stringify(value).replace(/"/g, '""');
              } catch {
                escapedValue = "[Object]"; // Fallback for non-serializable objects
              }
            } else {
              escapedValue = String(value).replace(/"/g, '""'); // Escape double quotes
            }

            // Enclose in double quotes if it contains comma, newline, or double quote
            if (/[",\n]/.test(escapedValue)) {
              return `"${escapedValue}"`;
            }
            return escapedValue; // Return as is if no special characters
          })
          .join(",");
      })
      .join("\n");

    const filePath = path.join(
      config.dataDir,
      `${filename}-${formatDate(new Date())}.csv`
    );
    fs.writeFileSync(filePath, csvHeader + csvRows, "utf8"); // Specify encoding
    console.log(`Data successfully saved to ${filePath}`);
  } catch (error) {
    console.error(`Error saving data to CSV file ${filename}:`, error.message);
    console.error(error.stack); // Log stack trace for debugging
  }
}

// Get Account Status Text (Unchanged)
function getAccountStatusText(statusCode) {
  // Added check for undefined/null status
  if (statusCode === undefined || statusCode === null)
    return "Unknown (Status N/A)";
  const statusInt = parseInt(statusCode, 10);
  switch (statusInt) {
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

// Generate Spend Summaries (Minor improvements: robustness, clarity)
function generateSpendSummaries(spendData, accountInfoList) {
  console.log(`Generating spend summaries from ${spendData.length} records...`);

  if (!Array.isArray(spendData)) {
    console.error("Cannot generate summaries: spendData is not an array.");
    return { adSummary: [], campaignSummary: [], accountSummary: [] };
  }
  if (!Array.isArray(accountInfoList)) {
    console.error(
      "Cannot generate summaries: accountInfoList is not an array."
    );
    // Proceed without full account details if necessary, but log it
    accountInfoList = [];
  }

  const now = new Date();
  const currentYear = now.getFullYear();
  const startOfYear = new Date(Date.UTC(currentYear, 0, 1)); // Jan 1st UTC
  const startOfYearStr = formatDate(startOfYear);

  const last30Days = new Date(
    Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() - 30)
  );
  const last30DaysStr = formatDate(last30Days);

  console.log(`Year-to-date (YTD) calculation since: ${startOfYearStr}`);
  console.log(`Last 30 days calculation since: ${last30DaysStr}`);

  const adSpendMap = {};
  const campaignSpendMap = {};
  const accountSpendMap = {};

  // Initialize account map from accountInfoList
  accountInfoList.forEach((account) => {
    if (account?.id && !account.error) {
      // Only process valid accounts
      accountSpendMap[account.id] = {
        account_id: account.id,
        account_name: account.name || "Unknown",
        account_status: getAccountStatusText(account.account_status),
        matching_ads_spend_ytd: 0,
        matching_ads_spend_last_30days: 0,
        // Add total_amount_spent from account info if available
        account_total_lifetime_spend:
          parseFloat(account.amount_spent || 0) / 100, // amount_spent is often in cents
      };
    } else if (account?.id) {
      console.warn(
        `Skipping account ${account.id} in summary due to fetch error.`
      );
    }
  });

  // Aggregate spend data
  spendData.forEach((record) => {
    const adId = record.ad_id;
    const campaignId = record.campaign_id;
    const accountId = record.account_id; // Assuming account_id is added to records
    const date = record.date_start; // Ensure this is 'YYYY-MM-DD'
    const spend = parseFloat(record.spend || 0); // Already parsed in processSpendData

    if (!adId || !accountId || !date) {
      console.warn(
        "Skipping spend record due to missing ID, account ID, or date:",
        record
      );
      return;
    }

    // Initialize maps if entry doesn't exist
    if (!adSpendMap[adId]) {
      adSpendMap[adId] = {
        ad_id: adId,
        ad_name: record.ad_name || "Unknown",
        account_id: accountId,
        account_name:
          record.account_name ||
          accountSpendMap[accountId]?.account_name ||
          "Unknown",
        campaign_id: campaignId || "Unknown",
        campaign_name: record.campaign_name || "Unknown",
        total_spend_ytd: 0,
        spend_last_30days: 0,
      };
    }
    if (campaignId && !campaignSpendMap[campaignId]) {
      campaignSpendMap[campaignId] = {
        campaign_id: campaignId,
        campaign_name: record.campaign_name || "Unknown",
        account_id: accountId,
        account_name:
          record.account_name ||
          accountSpendMap[accountId]?.account_name ||
          "Unknown",
        total_spend_ytd: 0,
        spend_last_30days: 0,
      };
    }
    // Ensure account exists in accountSpendMap (might happen if account info failed but spend data exists)
    if (!accountSpendMap[accountId]) {
      accountSpendMap[accountId] = {
        account_id: accountId,
        account_name:
          record.account_name || "Unknown (Not in account info list)",
        account_status: "Unknown",
        matching_ads_spend_ytd: 0,
        matching_ads_spend_last_30days: 0,
        account_total_lifetime_spend: "N/A",
      };
    }

    // Accumulate spend based on date
    if (date >= startOfYearStr) {
      adSpendMap[adId].total_spend_ytd += spend;
      if (campaignId) campaignSpendMap[campaignId].total_spend_ytd += spend;
      accountSpendMap[accountId].matching_ads_spend_ytd += spend;
    }
    if (date >= last30DaysStr) {
      adSpendMap[adId].spend_last_30days += spend;
      if (campaignId) campaignSpendMap[campaignId].spend_last_30days += spend;
      accountSpendMap[accountId].matching_ads_spend_last_30days += spend;
    }
  });

  // Convert maps to arrays and format numbers
  const formatSummaryNumbers = (item) => {
    item.total_spend_ytd = (item.total_spend_ytd || 0).toFixed(2);
    item.spend_last_30days = (item.spend_last_30days || 0).toFixed(2);
    // For accounts, format matching spend
    if (item.matching_ads_spend_ytd !== undefined) {
      item.matching_ads_spend_ytd = (item.matching_ads_spend_ytd || 0).toFixed(
        2
      );
    }
    if (item.matching_ads_spend_last_30days !== undefined) {
      item.matching_ads_spend_last_30days = (
        item.matching_ads_spend_last_30days || 0
      ).toFixed(2);
    }
    // Format lifetime spend if present
    if (
      item.account_total_lifetime_spend !== undefined &&
      typeof item.account_total_lifetime_spend === "number"
    ) {
      item.account_total_lifetime_spend =
        item.account_total_lifetime_spend.toFixed(2);
    }
    return item;
  };

  const adSummary = Object.values(adSpendMap).map(formatSummaryNumbers);
  const campaignSummary =
    Object.values(campaignSpendMap).map(formatSummaryNumbers);
  const accountSummary =
    Object.values(accountSpendMap).map(formatSummaryNumbers);

  // Sort summaries (descending YTD spend)
  const sortByYtdSpendDesc = (a, b) =>
    parseFloat(b.total_spend_ytd || 0) - parseFloat(a.total_spend_ytd || 0);
  const sortAccountsByYtdSpendDesc = (a, b) =>
    parseFloat(b.matching_ads_spend_ytd || 0) -
    parseFloat(a.matching_ads_spend_ytd || 0);

  adSummary.sort(sortByYtdSpendDesc);
  campaignSummary.sort(sortByYtdSpendDesc);
  accountSummary.sort(sortAccountsByYtdSpendDesc);

  // Calculate and log overall totals
  const totalYTDSpend = accountSummary.reduce(
    (sum, acc) => sum + parseFloat(acc.matching_ads_spend_ytd || 0),
    0
  );
  const totalLast30DaysSpend = accountSummary.reduce(
    (sum, acc) => sum + parseFloat(acc.matching_ads_spend_last_30days || 0),
    0
  );

  console.log("\n===== Spend Summary Totals =====");
  console.log(
    `Total Matching Ads Spend YTD (${startOfYearStr} to ${formatDate(
      now
    )}): ${totalYTDSpend.toFixed(2)} ${config.currency}`
  );
  console.log(
    `Total Matching Ads Spend (Last 30 Days): ${totalLast30DaysSpend.toFixed(
      2
    )} ${config.currency}`
  );
  console.log("=============================\n");

  return { adSummary, campaignSummary, accountSummary };
}

// --- Google Sheets Functions (Mostly unchanged, added robustness) ---

async function getGoogleSheetsAuth() {
  if (!config.googleSheets.enabled) return null;
  if (!config.googleSheets.keyFilePath || !config.googleSheets.spreadsheetId) {
    console.error(
      "Google Sheets enabled, but keyFilePath or spreadsheetId is missing in config."
    );
    return null;
  }

  try {
    const auth = new google.auth.GoogleAuth({
      keyFile: config.googleSheets.keyFilePath,
      scopes: ["https://www.googleapis.com/auth/spreadsheets"],
    });
    const client = await auth.getClient();
    console.log("Google Sheets authenticated successfully.");
    return client;
  } catch (error) {
    console.error("Error authenticating with Google Sheets:", error.message);
    if (error.code === "ENOENT") {
      console.error(
        `Key file not found at: ${path.resolve(
          config.googleSheets.keyFilePath
        )}`
      );
    } else {
      console.error(
        "Check service account permissions and ensure the Sheets API is enabled in your Google Cloud project."
      );
    }
    return null;
  }
}

async function saveToGoogleSheets(data, sheetName) {
  if (!config.googleSheets.enabled) return false; // Not enabled
  if (!data || data.length === 0) {
    console.log(`No data to save to Google Sheet: ${sheetName}`);
    return false; // Nothing to save
  }
  if (!config.googleSheets.spreadsheetId) {
    console.error(
      "Cannot save to Google Sheets: GOOGLE_SHEETS_SPREADSHEET_ID is not set."
    );
    return false;
  }
  if (!sheets) {
    console.error(
      "Cannot save to Google Sheets: Sheets API client not initialized."
    );
    return false;
  }

  console.log(
    `Attempting to save ${data.length} rows to Google Sheet: ${sheetName}`
  );
  const authClient = await getGoogleSheetsAuth();
  if (!authClient) {
    console.error("Could not save to Google Sheets: Authentication failed.");
    return false;
  }

  try {
    // Dynamically get headers from all objects
    const headers = [...new Set(data.flatMap((obj) => Object.keys(obj)))];

    // Format data for Sheets API (ensure consistent order based on headers)
    const rows = [
      headers, // Header row
      ...data.map((row) =>
        headers.map((header) => {
          const value = row[header];
          if (value === undefined || value === null) return "";
          if (typeof value === "object") return JSON.stringify(value); // Stringify objects/arrays
          return value; // Keep numbers, strings, booleans as they are
        })
      ),
    ];

    // 1. Clear the sheet range first to prevent appending old data
    console.log(`Clearing existing data in sheet: ${sheetName}`);
    await sheets.spreadsheets.values.clear({
      auth: authClient,
      spreadsheetId: config.googleSheets.spreadsheetId,
      range: `${sheetName}!A1:ZZ`, // Clear a large range
    });
    console.log(`Sheet cleared. Writing new data...`);

    // 2. Write the new data
    const result = await sheets.spreadsheets.values.update({
      auth: authClient,
      spreadsheetId: config.googleSheets.spreadsheetId,
      range: `${sheetName}!A1`, // Start writing from A1
      valueInputOption: "USER_ENTERED", // Try USER_ENTERED to allow Sheets to interpret data types
      resource: { values: rows },
    });

    console.log(
      `Successfully saved ${
        result.data.updatedRows || rows.length
      } rows to Google Sheet: ${sheetName}`
    );
    return true;
  } catch (error) {
    console.error(
      `Error saving to Google Sheets (${sheetName}):`,
      error.code,
      error.message
    );
    if (error.response?.data?.error) {
      console.error(
        "Google API Error Details:",
        JSON.stringify(error.response.data.error, null, 2)
      );
      if (error.response.data.error.message.includes("Unable to parse range")) {
        console.error(
          `-> Check if the sheet name "${sheetName}" is correct and exists in the spreadsheet.`
        );
      }
      if (error.response.data.error.message.includes("permission")) {
        console.error(
          `-> Check if the Service Account has edit permissions for the Spreadsheet ID: ${config.googleSheets.spreadsheetId}`
        );
      }
    } else {
      console.error("Raw Error:", error);
    }
    return false;
  }
}

// --- Main Execution Function ---
async function getAdLevelSpend() {
  const overallStartTime = Date.now();
  console.log(
    `Starting Meta ad data extraction at ${new Date().toLocaleString()}`
  );
  console.log(`Using API Version: ${config.apiVersion}`);
  console.log(`Data directory: ${path.resolve(config.dataDir)}`);
  console.log(`Accounts provided: ${config.accountIds.length}`);

  // Apply maxAccounts limit
  let activeAccountIds = config.accountIds;
  if (config.maxAccounts > 0 && config.accountIds.length > config.maxAccounts) {
    console.log(
      `Limiting processing to the first ${config.maxAccounts} accounts.`
    );
    activeAccountIds = config.accountIds.slice(0, config.maxAccounts);
  }
  console.log(`Processing ${activeAccountIds.length} accounts.`);
  console.log(
    `Default Ad Name Patterns (used if no account-specific): ${
      config.accountPatterns.default?.join(", ") || "None"
    }`
  );
  console.log(
    `Account-specific patterns configured for: ${
      Object.keys(config.accountPatterns)
        .filter((k) => k !== "default")
        .join(", ") || "None"
    }`
  );
  console.log(`API Request Delay: ${config.requestDelay}ms`);
  console.log(`Retry Attempts: ${config.retryAttempts}`);
  console.log(`Ad Filtering Enabled: ${config.enableAdFiltering}`);
  if (config.enableAdFiltering) {
    console.log(`  - Only Active/Paused Ads: ${config.onlyIncludeActiveAds}`);
  }
  console.log(`Skip Future Date Ranges: ${config.skipFutureDateRanges}`);
  console.log(`Google Sheets Enabled: ${config.googleSheets.enabled}`);
  if (config.googleSheets.enabled) {
    console.log(
      `  - Spreadsheet ID: ${config.googleSheets.spreadsheetId || "NOT SET"}`
    );
    console.log(`  - Key File: ${config.googleSheets.keyFilePath}`);
  }

  const dateRange = getDateRange();
  console.log(
    `Workspaceing data for date range: ${dateRange.since} to ${dateRange.until}`
  );

  const allAccountInfo = [];
  let allMatchingAds = [];
  let allAdSpendData = [];

  // Process each account sequentially
  for (let i = 0; i < activeAccountIds.length; i++) {
    const accountId = activeAccountIds[i];
    const accountStartTime = Date.now();
    console.log(
      `\n--- Processing Account ${i + 1}/${
        activeAccountIds.length
      }: ${accountId} ---`
    );

    // Add delay before processing next account (except the first one)
    if (i > 0) {
      console.log(`Pausing ${config.requestDelay}ms before next account...`);
      await new Promise((resolve) => setTimeout(resolve, config.requestDelay));
    }

    try {
      // 1. Get Account Info
      const accountInfo = await getAccountInfo(accountId);
      if (accountInfo.error) {
        console.error(
          `Skipping account ${accountId} due to error fetching info: ${accountInfo.message}`
        );
        allAccountInfo.push(accountInfo); // Store error info
        continue; // Move to the next account
      }
      allAccountInfo.push(accountInfo);
      console.log(
        `Account Name: ${accountInfo.name}, Status: ${getAccountStatusText(
          accountInfo.account_status
        )}, Currency: ${accountInfo.currency}`
      );

      // 2. Fetch Ads Matching Pattern
      let matchingAds = await fetchAdsMatchingPattern(accountId, accountInfo);

      // Apply maxAdsPerAccount limit
      if (
        config.maxAdsPerAccount > 0 &&
        matchingAds.length > config.maxAdsPerAccount
      ) {
        console.log(
          `Limiting to ${config.maxAdsPerAccount} matching ads for this account (out of ${matchingAds.length}).`
        );
        matchingAds = matchingAds.slice(0, config.maxAdsPerAccount);
      }

      // Add account info to each ad object
      matchingAds.forEach((ad) => {
        ad.account_id = accountId;
        ad.account_name = accountInfo.name || "Unknown";
        // Add pattern matched if needed (requires adapting getPatternMatched or similar logic)
        // ad.pattern_matched = getPatternMatched(ad.name, config.accountPatterns[accountId] || config.accountPatterns.default);
      });
      allMatchingAds.push(...matchingAds);

      // 3. Get Spend Data for Matching Ads (if any)
      if (matchingAds.length > 0) {
        const adIds = matchingAds.map((ad) => ad.id);
        const adSpendData = await getAdSpendData(
          accountId,
          accountInfo.name,
          adIds,
          dateRange
        );

        // Add account info to each spend record
        adSpendData.forEach((record) => {
          record.account_id = accountId;
          record.account_name = accountInfo.name || "Unknown";
          // Link back to campaign/adset if needed using info from matchingAds
          const parentAd = matchingAds.find((ad) => ad.id === record.ad_id);
          if (parentAd) {
            record.campaign_id = parentAd.campaign_id; // Ensure campaign_id is present
            record.adset_id = parentAd.adset_id; // Ensure adset_id is present
          }
        });
        allAdSpendData.push(...adSpendData);
      } else {
        console.log(
          `No ads matched the specified patterns or filters for account ${accountId}.`
        );
      }

      const accountEndTime = Date.now();
      console.log(
        `--- Finished Account ${accountId} in ${
          (accountEndTime - accountStartTime) / 1000
        }s ---`
      );
    } catch (error) {
      // Catch errors specific to processing a single account
      console.error(`Error processing account ${accountId}: ${error.message}`);
      console.error(error.stack); // Log stack trace for detailed debugging
      // Optionally add placeholder error info to accountInfoList if account fetch failed earlier
      if (!allAccountInfo.find((info) => info.id === accountId)) {
        allAccountInfo.push({
          id: accountId,
          name: `Error processing account ${accountId}`,
          error: true,
          message: error.message,
        });
      }
    }
  } // End account loop

  console.log("\n--- Processing Complete ---");

  // Post-Processing and Saving
  if (allAdSpendData.length === 0) {
    console.log(
      "No spend data found for any matching ads across all processed accounts."
    );
    // Still might want to save the list of accounts processed
    saveToCSV(
      allAccountInfo.map((acc) => ({
        // Save basic account info even if no spend
        account_id: acc.id,
        account_name: acc.name,
        status: acc.error ? "Error" : getAccountStatusText(acc.account_status),
        error_message: acc.error ? acc.message : "",
      })),
      "meta-accounts-processed"
    );
  } else {
    console.log(
      `Total matching ads found across accounts: ${allMatchingAds.length}`
    );
    console.log(
      `Total non-zero spend records collected: ${allAdSpendData.length}`
    );

    // Process spend data (e.g., format numbers, extract actions if needed)
    const processedSpendData = processSpendData(allAdSpendData);

    // Filter the initial ads list to only include those that actually had spend data found
    const adsWithSpendIds = new Set(
      processedSpendData.map((record) => record.ad_id)
    );
    const finalMatchingAdsList = allMatchingAds.filter((ad) =>
      adsWithSpendIds.has(ad.id)
    );
    console.log(
      `Filtered matching ads list to ${finalMatchingAdsList.length} ads with reported spend.`
    );

    // Generate Summaries
    const { adSummary, campaignSummary, accountSummary } =
      generateSpendSummaries(processedSpendData, allAccountInfo);

    // Save to CSV
    console.log("\nSaving results to CSV...");
    saveToCSV(finalMatchingAdsList, "meta-matching-ads-list");
    saveToCSV(processedSpendData, "meta-ad-level-spend");
    saveToCSV(adSummary, "meta-ad-summary");
    saveToCSV(campaignSummary, "meta-campaign-summary");
    saveToCSV(accountSummary, "meta-account-summary");

    // Save to Google Sheets if enabled
    if (config.googleSheets.enabled) {
      console.log("\nSaving results to Google Sheets...");
      // Use await for each sheet to ensure sequential saving (optional, can run in parallel with Promise.all)
      await saveToGoogleSheets(
        finalMatchingAdsList,
        config.googleSheets.matchingAdsSheet
      );
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
  }

  // Log total execution time
  const overallEndTime = Date.now();
  const totalElapsedMs = overallEndTime - overallStartTime;
  const totalMinutes = Math.floor(totalElapsedMs / 60000);
  const totalSeconds = ((totalElapsedMs % 60000) / 1000).toFixed(1);
  console.log(`\nExtraction finished at ${new Date().toLocaleString()}`);
  console.log(`Total execution time: ${totalMinutes}m ${totalSeconds}s`);
}

// --- Run the Script ---
getAdLevelSpend().catch((error) => {
  console.error("\n--- !!! Unhandled Fatal Error !!! ---");
  console.error(error.message);
  console.error(error.stack);
  process.exit(1); // Exit with error code
});
