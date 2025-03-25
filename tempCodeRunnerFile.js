require("dotenv").config();
const axios = require("axios");
const fs = require("fs");
const path = require("path");

// Configuration (store these in .env file in production)
const config = {
  appId: process.env.META_APP_ID,
  accessToken: process.env.META_ACCESS_TOKEN,
  businessId: process.env.META_BUSINESS_ID, // Add your business/portfolio ID
  apiVersion: "v22.0",
  baseUrl: "https://graph.facebook.com",
  // Rate limiting config
  maxRequestsPerHour: 180, // Conservative limit
  retryAttempts: 5,
  // Ad naming restrictions
  adNameFilter: "HS_", // Only process ads with this prefix
};

// Create a request queue to manage API rate limits
class RequestQueue {
  constructor(requestsPerHour = 180) {
    this.queue = [];
    this.processing = false;
    this.interval = 3600000 / requestsPerHour; // Time between requests in ms
    this.lastRequestTime = 0;
  }

  add(requestFunction) {
    return new Promise((resolve, reject) => {
      this.queue.push({ requestFunction, resolve, reject });
      if (!this.processing) this.processQueue();
    });
  }

  async processQueue() {
    if (this.queue.length === 0) {
      this.processing = false;
      return;
    }

    this.processing = true;
    const { requestFunction, resolve, reject } = this.queue.shift();

    // Calculate time to wait before next request
    const now = Date.now();
    const timeToWait = Math.max(0, this.lastRequestTime + this.interval - now);

    if (timeToWait > 0) {
      await new Promise((r) => setTimeout(r, timeToWait));
    }

    try {
      const result = await requestFunction();
      this.lastRequestTime = Date.now();
      resolve(result);
    } catch (error) {
      reject(error);
    }

    // Process next item in queue
    this.processQueue();
  }
}

// Create a global request queue
const apiQueue = new RequestQueue(config.maxRequestsPerHour);

// Utility for date formatting
const formatDate = (date) => {
  return date.toISOString().split("T")[0];
};

// Get date range - improved with chunking capability
const getDateRange = (days = 30) => {
  const endDate = new Date();
  const startDate = new Date();
  startDate.setDate(startDate.getDate() - days);

  return {
    since: formatDate(startDate),
    until: formatDate(endDate),
  };
};

// Function to break date ranges into chunks for better API handling
function getDateChunks(startDate, endDate, chunkSizeDays = 14) {
  const chunks = [];
  let currentDate = new Date(startDate);
  const end = new Date(endDate);

  while (currentDate < end) {
    const chunkEnd = new Date(currentDate);
    chunkEnd.setDate(chunkEnd.getDate() + chunkSizeDays - 1);

    if (chunkEnd > end) {
      chunks.push({
        since: formatDate(currentDate),
        until: formatDate(end),
      });
    } else {
      chunks.push({
        since: formatDate(currentDate),
        until: formatDate(chunkEnd),
      });
    }

    currentDate.setDate(currentDate.getDate() + chunkSizeDays);
  }

  return chunks;
}

// Build API URL with parameters
const buildApiUrl = (endpoint, params = {}) => {
  const url = new URL(`${config.baseUrl}/${config.apiVersion}/${endpoint}`);

  // Add access token
  url.searchParams.append("access_token", config.accessToken);

  // Add other parameters
  Object.entries(params).forEach(([key, value]) => {
    // Handle special case for JSON objects that need to be stringified
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

// Enhanced API call function with retry logic
async function makeApiRequest(url, maxRetries = config.retryAttempts) {
  let lastError;

  for (let attempt = 0; attempt < maxRetries; attempt++) {
    try {
      // Add delay between retries, with exponential backoff
      if (attempt > 0) {
        const delay = Math.pow(2, attempt) * 1000; // Exponential backoff
        console.log(
          `Retry attempt ${attempt + 1}/${maxRetries} after ${delay}ms`
        );
        await new Promise((resolve) => setTimeout(resolve, delay));
      }

      const response = await axios.get(url);
      return response.data;
    } catch (error) {
      lastError = error;

      // If this is a rate limit error (429) or server error (5xx), retry
      if (
        error.response &&
        (error.response.status === 429 || error.response.status >= 500)
      ) {
        console.log(`Hit error ${error.response.status}, retrying...`);
        // If we get a specific retry-after header, respect it
        if (error.response.headers["retry-after"]) {
          const retryAfter =
            parseInt(error.response.headers["retry-after"]) * 1000;
          await new Promise((resolve) => setTimeout(resolve, retryAfter));
        }
        continue;
      }

      // For other errors, don't retry
      throw error;
    }
  }

  // If we've exhausted retries, throw the last error
  throw lastError;
}

// Fetch paginated data with automatic handling of next pages
async function fetchPaginatedData(
  endpoint,
  params,
  dataExtractor = (data) => data.data
) {
  const url = buildApiUrl(endpoint, params);

  return apiQueue.add(async () => {
    let allData = [];
    let nextUrl = url;

    while (nextUrl) {
      const response = await makeApiRequest(nextUrl);
      const data = dataExtractor(response);

      if (Array.isArray(data)) {
        allData = [...allData, ...data];
      }

      nextUrl = response.paging?.next || null;
    }

    return allData;
  });
}

// NEW: Function to fetch all accounts under a business (portfolio)
async function fetchBusinessAdAccounts() {
  try {
    const endpoint = `${config.businessId}/owned_ad_accounts`;
    const params = {
      fields: [
        "id",
        "name",
        "account_status",
        "currency",
        "business_name",
        "amount_spent",
      ],
      limit: 500,
    };

    return await fetchPaginatedData(endpoint, params);
  } catch (error) {
    console.error(
      "Error fetching business ad accounts:",
      error.response?.data || error.message
    );
    throw error;
  }
}

// Fetch account campaigns - optimized
async function fetchCampaigns(accountId) {
  try {
    const endpoint = `act_${accountId}/campaigns`;
    const params = {
      fields: ["id", "name", "status", "start_time", "daily_budget"],
      limit: 500, // Reduced for better API handling
    };

    const allCampaigns = await fetchPaginatedData(endpoint, params);

    // Filter for campaigns within the last 2 years
    const twoYearsAgo = new Date();
    twoYearsAgo.setFullYear(twoYearsAgo.getFullYear() - 2);

    return allCampaigns.filter((campaign) => {
      const createdTime = campaign.created_time
        ? new Date(campaign.created_time)
        : new Date();
      return createdTime >= twoYearsAgo;
    });
  } catch (error) {
    console.error(
      "Error fetching campaigns:",
      error.response?.data || error.message
    );
    throw error;
  }
}

// Fetch ad sets for a campaign - optimized
async function fetchAdSets(campaignId) {
  try {
    const endpoint = `${campaignId}/adsets`;
    const params = {
      fields: [
        "id",
        "name",
        "status",
        "daily_budget",
        "targeting",
        "optimization_goal",
        "bid_amount",
        "bid_strategy",
        "campaign_id",
        "start_time",
      ],
      limit: 500, // Adjusted limit
    };

    return await fetchPaginatedData(endpoint, params);
  } catch (error) {
    console.error(
      "Error fetching ad sets:",
      error.response?.data || error.message
    );
    throw error;
  }
}

// Fetch ads for an ad set - optimized AND filtered by naming convention
async function fetchAds(adSetId) {
  try {
    const endpoint = `${adSetId}/ads`;
    const params = {
      fields: [
        "id",
        "name",
        "status",
        "adset_id",
        "campaign_id",
        "start_time",
        "creative{id,name,image_url,link_url}", // Get creative info in same call
      ],
      limit: 500,
    };

    const ads = await fetchPaginatedData(endpoint, params);

    // Filter ads by naming convention
    const filteredAds = ads.filter((ad) => {
      return ad.name && ad.name.includes(config.adNameFilter);
    });

    // Process creatives
    return filteredAds.map((ad) => {
      // Extract creative info if available
      if (ad.creative && typeof ad.creative === "object") {
        ad.creative_details = ad.creative;
        delete ad.creative;
      }
      return ad;
    });
  } catch (error) {
    console.error("Error fetching ads:", error.response?.data || error.message);
    throw error;
  }
}

// Fetch insights with date chunking and improved fields
async function fetchInsights(
  objectId,
  level = "ad",
  dateRange = getDateRange()
) {
  try {
    // Use date chunking to avoid hitting API limits
    const dateChunks = getDateChunks(dateRange.since, dateRange.until, 7); // 7-day chunks
    let allInsights = [];

    for (const chunk of dateChunks) {
      const endpoint = `${objectId}/insights`;
      const params = {
        time_range: chunk,
        level,
        time_increment: 1, // Daily data
        fields: [
          "date_start",
          "date_stop",
          "campaign_name",
          "adset_name",
          "ad_name",
          "impressions",
          "clicks",
          "spend",
          "reach",
          "frequency",
          "cpc",
          "cpm",
          "cpp",
          "ctr",
          "conversions",
          "cost_per_conversion",
          "cost_per_unique_click",
          "outbound_clicks",
          "cost_per_outbound_click",
          "website_ctr",
          "website_purchase_roas",
          "actions",
          "action_values",
          "video_30_sec_watched_actions",
          "video_15_sec_watched_actions",
        ],
        limit: 500,
      };

      console.log(
        `Fetching ${level}-level insights for ${formatDate(
          new Date(chunk.since)
        )} to ${formatDate(new Date(chunk.until))}`
      );
      const chunkData = await fetchPaginatedData(endpoint, params);

      // Filter insights for ads matching naming convention if applicable
      let filteredData = chunkData;
      if (level === "ad") {
        filteredData = chunkData.filter(
          (insight) =>
            insight.ad_name && insight.ad_name.includes(config.adNameFilter)
        );
      }

      allInsights = [...allInsights, ...filteredData];
    }

    return allInsights;
  } catch (error) {
    console.error(
      "Error fetching insights:",
      error.response?.data || error.message
    );
    throw error;
  }
}

// Save data to CSV
function saveToCSV(data, filename) {
  if (!data || data.length === 0) {
    console.log(`No data to save for ${filename}`);
    return;
  }

  // Create directory if it doesn't exist
  const dir = "./data";
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir);
  }

  // For large datasets, we need to handle potential memory issues
  // Calculate a rough estimate of the size
  const sampleRowSize = JSON.stringify(data[0]).length;
  const estimatedSize = sampleRowSize * data.length;

  // For very large datasets, split into multiple files
  if (estimatedSize > 100000000) {
    // 100MB threshold
    const batchSize = Math.ceil(
      data.length / Math.ceil(estimatedSize / 100000000)
    );
    console.log(
      `Large dataset detected for ${filename}. Splitting into batches of ${batchSize} rows.`
    );

    for (let i = 0; i < data.length; i += batchSize) {
      const batch = data.slice(i, i + batchSize);
      const batchFilename = `${filename}-batch${Math.floor(i / batchSize) + 1}`;
      saveToCSVInternal(batch, batchFilename);
    }
  } else {
    saveToCSVInternal(data, filename);
  }
}

// Internal function to save data to CSV (same as your fixed version)
function saveToCSVInternal(data, filename) {
  if (!data.length) {
    console.log(`No data to save for ${filename}`);
    return;
  }

  // Process the data to extract useful information from nested objects
  const processedData = data.map((row) => {
    const processed = { ...row }; // Start with a copy of the original row

    // Generic handler for all array fields
    Object.keys(row).forEach((key) => {
      const value = row[key];

      // Check if the value is an array of objects
      if (
        Array.isArray(value) &&
        value.length > 0 &&
        typeof value[0] === "object"
      ) {
        // Delete the original array field
        delete processed[key];

        // Process each item in the array
        value.forEach((item) => {
          if (item.action_type && item.value !== undefined) {
            // For fields with action_type (most Meta API array fields)
            processed[`${key}_${item.action_type}`] = item.value;
          } else if (item.value !== undefined) {
            // For fields with just value but no action_type
            processed[`${key}_value`] = item.value;
          }
        });
      }
    });

    // Add special handling for specific fields (for backward compatibility)
    if (
      row.website_ctr &&
      Array.isArray(row.website_ctr) &&
      row.website_ctr.length > 0
    ) {
      processed.website_ctr_value = row.website_ctr[0].value || "";
    }

    if (
      row.website_purchase_roas &&
      Array.isArray(row.website_purchase_roas) &&
      row.website_purchase_roas.length > 0
    ) {
      processed.roas = row.website_purchase_roas[0].value || "";
    }

    return processed;
  });

  // Get all possible headers
  const headers = [
    ...new Set(processedData.flatMap((obj) => Object.keys(obj))),
  ];
  const csvHeader = headers.join(",") + "\n";

  // Convert rows
  const csvRows = processedData
    .map((row) =>
      headers
        .map((header) => {
          let value = row[header] || "";
          // Ensure objects don't end up in the CSV
          if (typeof value === "object" && value !== null) {
            if (Array.isArray(value)) {
              value = JSON.stringify(value);
            } else {
              value = "object";
            }
          }
          // Properly escape strings with commas
          if (typeof value === "string" && value.includes(",")) {
            return `"${value}"`;
          }
          return value;
        })
        .join(",")
    )
    .join("\n");

  // Write to file
  const filePath = path.join(
    "./data",
    `${filename}-${formatDate(new Date())}.csv`
  );
  fs.writeFileSync(filePath, csvHeader + csvRows);

  console.log(`Data saved to ${filePath}`);
}

// NEW: Function to pull data for all accounts in the portfolio
async function pullPortfolioData() {
  try {
    console.log("Fetching accounts under portfolio...");
    const accounts = await fetchBusinessAdAccounts();
    saveToCSV(accounts, "portfolio-accounts");

    console.log(`Found ${accounts.length} accounts in portfolio`);

    // Process each account
    for (let i = 0; i < accounts.length; i++) {
      const account = accounts[i];
      const accountId = account.id.replace("act_", ""); // Strip 'act_' prefix if present

      console.log(
        `\n===== Processing account ${i + 1}/${accounts.length}: ${
          account.name
        } (${account.id}) =====`
      );

      try {
        // Get campaigns for this account
        console.log("Fetching campaigns...");
        const campaigns = await fetchCampaigns(accountId);
        saveToCSV(campaigns, `account-${accountId}-campaigns`);

        if (campaigns.length === 0) {
          console.log(
            "No active campaigns found for this account, skipping..."
          );
          continue;
        }

        console.log(`Found ${campaigns.length} campaigns`);

        // Get account-level insights
        console.log("Fetching account-level insights...");
        const accountInsights = await fetchInsights(
          `act_${accountId}`,
          "account"
        );
        saveToCSV(accountInsights, `account-${accountId}-insights`);

        // Process campaigns in batches
        const campaignBatchSize = 5;
        for (let j = 0; j < campaigns.length; j += campaignBatchSize) {
          const campaignBatch = campaigns.slice(j, j + campaignBatchSize);
          console.log(
            `Processing campaigns ${j + 1} to ${Math.min(
              j + campaignBatchSize,
              campaigns.length
            )} of ${campaigns.length}`
          );

          // Process each campaign in the batch
          await Promise.all(
            campaignBatch.map(async (campaign) => {
              try {
                // Get campaign insights
                const campaignInsights = await fetchInsights(
                  campaign.id,
                  "campaign"
                );
                saveToCSV(campaignInsights, `campaign-${campaign.id}-insights`);

                // Get ad sets
                const adSets = await fetchAdSets(campaign.id);
                saveToCSV(adSets, `campaign-${campaign.id}-adsets`);

                // Process ad sets
                const adSetBatchSize = 5;
                for (let k = 0; k < adSets.length; k += adSetBatchSize) {
                  const adSetBatch = adSets.slice(k, k + adSetBatchSize);

                  await Promise.all(
                    adSetBatch.map(async (adSet) => {
                      try {
                        // Get ad set insights
                        const adSetInsights = await fetchInsights(
                          adSet.id,
                          "adset"
                        );
                        saveToCSV(adSetInsights, `adset-${adSet.id}-insights`);

                        // Get ads (filtered by naming convention)
                        const ads = await fetchAds(adSet.id);
                        if (ads.length > 0) {
                          saveToCSV(ads, `adset-${adSet.id}-ads`);

                          // Process ads
                          const adBatchSize = 10;
                          for (let l = 0; l < ads.length; l += adBatchSize) {
                            const adBatch = ads.slice(l, l + adBatchSize);

                            await Promise.all(
                              adBatch.map(async (ad) => {
                                try {
                                  const adInsights = await fetchInsights(
                                    ad.id,
                                    "ad"
                                  );
                                  saveToCSV(adInsights, `ad-${ad.id}-insights`);
                                } catch (error) {
                                  console.error(
                                    `Error processing ad ${ad.id}:`,
                                    error
                                  );
                                }
                              })
                            );
                          }
                        } else {
                          console.log(
                            `No ads matching naming filter "${config.adNameFilter}" found in ad set ${adSet.id}`
                          );
                        }
                      } catch (error) {
                        console.error(
                          `Error processing ad set ${adSet.id}:`,
                          error
                        );
                      }
                    })
                  );
                }
              } catch (error) {
                console.error(
                  `Error processing campaign ${campaign.id}:`,
                  error
                );
              }
            })
          );
        }
      } catch (error) {
        console.error(`Error processing account ${account.id}:`, error);
      }
    }

    console.log("\n===== Portfolio data pull complete! =====");
  } catch (error) {
    console.error("Error pulling portfolio data:", error);
  }
}

// Main execution function
async function main() {
  const args = process.argv.slice(2);
  const command = args[0];
  const param = args[1];

  try {
    // Display start time
    console.log(`Starting data pull at ${new Date().toLocaleString()}`);
    console.log(
      `Ad naming filter: Only processing ads containing "${config.adNameFilter}"`
    );

    switch (command) {
      case "portfolio":
        await pullPortfolioData();
        break;
      case "account":
        if (!param) {
          throw new Error("Account ID is required");
        }
        // Pull data for a single account
        await pullAccountData(param);
        break;
      case "campaign":
        if (!param) {
          throw new Error("Campaign ID is required");
        }
        await pullCampaignData(param);
        break;
      default:
        console.log(`
Usage:
  node index.js portfolio              - Pull data for all accounts in portfolio
  node index.js account [account_id]   - Pull data for a specific account
  node index.js campaign [campaign_id] - Pull data for a specific campaign
        `);
    }

    // Display end time
    console.log(`Data pull completed at ${new Date().toLocaleString()}`);
  } catch (error) {
    console.error("Error:", error.message);
    process.exit(1);
  }
}

// Function to pull all data for a single account
async function pullAccountData(accountId) {
  try {
    console.log(`Fetching data for account ${accountId}...`);

    // Get campaigns for this account
    const campaigns = await fetchCampaigns(accountId);
    saveToCSV(campaigns, `account-${accountId}-campaigns`);

    // Get account-level insights
    const accountInsights = await fetchInsights(`act_${accountId}`, "account");
    saveToCSV(accountInsights, `account-${accountId}-insights`);

    // Get campaign-level insights
    const campaignInsights = await fetchInsights(
      `act_${accountId}`,
      "campaign"
    );
    saveToCSV(campaignInsights, `account-${accountId}-campaign-insights`);

    // Get adset-level insights
    const adsetInsights = await fetchInsights(`act_${accountId}`, "adset");
    saveToCSV(adsetInsights, `account-${accountId}-adset-insights`);

    // Get ad-level insights (filtered by naming convention)
    const adInsights = await fetchInsights(`act_${accountId}`, "ad");
    const filteredAdInsights = adInsights.filter(
      (insight) =>
        insight.ad_name && insight.ad_name.includes(config.adNameFilter)
    );
    saveToCSV(filteredAdInsights, `account-${accountId}-ad-insights`);

    // Process campaigns to get structure
    console.log("Fetching detailed campaign, ad set, and ad data...");
    for (const campaign of campaigns) {
      await pullCampaignData(campaign.id);
    }

    console.log(`Data pull for account ${accountId} complete!`);
  } catch (error) {
    console.error(`Error pulling account data for ${accountId}:`, error);
    throw error;
  }
}

// Run the script
main();
