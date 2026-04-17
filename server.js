import express from "express";
import cors from "cors";
import dotenv from "dotenv";
import axios from "axios";
import { createClient } from "@supabase/supabase-js";
import path from "path";
import { fileURLToPath } from "url";

dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
app.use(cors());
app.use(express.json());

// Serve static assets (logo, etc.)
app.use("/assets", express.static(path.join(__dirname)));

// Serve the dashboard at root URL — also handles TikTok OAuth callback
app.get("/", async (req, res) => {
  const authCode = req.query.auth_code;
  if (authCode && TIKTOK_APP_ID && TIKTOK_APP_SECRET) {
    try {
      // Exchange auth_code for access token
      const tokenRes = await axios.post("https://business-api.tiktok.com/open_api/v1.3/oauth2/access_token/", {
        app_id: TIKTOK_APP_ID,
        secret: TIKTOK_APP_SECRET,
        auth_code: authCode,
        grant_type: "auth_code"
      });
      const tokenData = tokenRes.data?.data;
      if (!tokenData?.access_token) {
        console.error("TikTok token exchange failed:", tokenRes.data);
        return res.send(`<html><body style="background:#0D1117;color:#fff;font-family:sans-serif;display:flex;align-items:center;justify-content:center;height:100vh;"><div style="text-align:center;"><h2>TikTok Authorization Failed</h2><p>${JSON.stringify(tokenRes.data?.message || tokenRes.data)}</p><a href="/" style="color:#53B7E8;">Back to Dashboard</a></div></body></html>`);
      }

      // Get advertiser accounts
      const advRes = await axios.get("https://business-api.tiktok.com/open_api/v1.3/oauth2/advertiser/get/", {
        params: { app_id: TIKTOK_APP_ID, secret: TIKTOK_APP_SECRET },
        headers: { "Access-Token": tokenData.access_token }
      });
      const advertisers = advRes.data?.data?.list || [];

      // Store token for each advertiser
      const expiresAt = new Date(Date.now() + 24 * 60 * 60 * 1000).toISOString(); // 24h
      for (const adv of advertisers) {
        const { error } = await supabase.from("tiktok_tokens").upsert({
          advertiser_id: adv.advertiser_id,
          advertiser_name: adv.advertiser_name || null,
          access_token: tokenData.access_token,
          refresh_token: tokenData.refresh_token || null,
          token_expires_at: expiresAt,
          updated_at: new Date().toISOString()
        }, { onConflict: "advertiser_id" });
        if (error) console.error("Failed to store TikTok token:", error.message);
        else console.log(`TikTok authorized: ${adv.advertiser_name} (${adv.advertiser_id})`);
      }

      // Redirect to clean dashboard URL
      return res.send(`<html><body style="background:#0D1117;color:#fff;font-family:sans-serif;display:flex;align-items:center;justify-content:center;height:100vh;"><div style="text-align:center;"><h2 style="color:#34D399;">TikTok Connected!</h2><p>Authorized ${advertisers.length} advertiser account${advertisers.length !== 1 ? 's' : ''}.</p><p>TikTok ads will now appear on your dashboard.</p><a href="/" style="color:#53B7E8;font-size:18px;">Go to Dashboard &rarr;</a></div></body></html>`);
    } catch (err) {
      console.error("TikTok OAuth error:", err.response?.data || err.message);
      return res.send(`<html><body style="background:#0D1117;color:#fff;font-family:sans-serif;display:flex;align-items:center;justify-content:center;height:100vh;"><div style="text-align:center;"><h2>TikTok Authorization Error</h2><p>${err.message}</p><a href="/" style="color:#53B7E8;">Back to Dashboard</a></div></body></html>`);
    }
  }
  res.sendFile(path.join(__dirname, "dashboard-v3.html")); // Default: v3 with login + platform selector
});

// Legacy versions — kept for rollback
app.get("/v1", (req, res) => {
  res.sendFile(path.join(__dirname, "dashboard.html"));
});
app.get("/v2", (req, res) => {
  res.sendFile(path.join(__dirname, "dashboard-v2.html"));
});
app.get("/v3", (req, res) => {
  res.sendFile(path.join(__dirname, "dashboard-v3.html"));
});

const META_ACCESS_TOKEN = process.env.META_ACCESS_TOKEN;
const META_AD_ACCOUNT_ID = process.env.META_AD_ACCOUNT_ID;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY;
const TIKTOK_APP_ID = process.env.TIKTOK_APP_ID;
const TIKTOK_APP_SECRET = process.env.TIKTOK_APP_SECRET;
const PORT = process.env.PORT || 3000;

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

// Migration helper — adds hour_label column if missing
// Call GET /migrate once to add the column, then it's permanent


// ====== UTILITY FUNCTIONS ======

function safeNumber(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : 0;
}

function round(value, decimals = 2) {
  return Number(safeNumber(value).toFixed(decimals));
}

function hoursAgo(dateStr) {
  return (Date.now() - new Date(dateStr).getTime()) / (1000 * 60 * 60);
}

function computeHourLabel(hours) {
  const ordinals = ["First", "Second", "Third", "Fourth", "Fifth", "Sixth",
    "Seventh", "Eighth", "Ninth", "Tenth", "Eleventh", "Twelfth"];
  if (hours <= 0) return "Launch";
  if (hours <= 12) return ordinals[Math.max(0, Math.ceil(hours) - 1)] + " hour";
  const days = Math.ceil(hours / 24);
  return (days * 24) + " hours";
}

// Fallback keyword detection (used only when API objective is unavailable)
function detectCampaignTypeFallback(campaignName = "", adName = "") {
  const text = `${campaignName} ${adName}`.toLowerCase();
  if (text.includes("awareness") || text.includes("entertaining")) return "awareness";
  if (text.includes("engagement") || text.includes("education") || text.includes("educational")) return "engagement";
  if (text.includes("traffic")) return "traffic";
  return "awareness";
}

// Map Meta's campaign objective to our campaign type
function objectiveToCampaignType(objective = "") {
  const obj = objective.toUpperCase();
  // Meta's modern OUTCOME_ objectives
  if (obj.includes("AWARENESS") || obj.includes("REACH") || obj.includes("VIDEO_VIEWS") || obj.includes("BRAND_AWARENESS")) return "awareness";
  if (obj.includes("TRAFFIC") || obj.includes("LINK_CLICKS") || obj.includes("OUTCOME_TRAFFIC") || obj.includes("OUTCOME_LEADS")) return "traffic";
  if (obj.includes("ENGAGEMENT") || obj.includes("POST_ENGAGEMENT") || obj.includes("CONVERSIONS") || obj.includes("MESSAGES")) return "engagement";
  return null; // unknown — will fall back to keyword detection
}

function parseActions(actions = []) {
  let likes = 0, comments = 0, shares = 0, saves = 0, video3sViews = 0, linkClicks = 0, landingPageViews = 0;

  for (const action of actions) {
    const type = action.action_type;
    const value = safeNumber(action.value);
    if (type === "video_view") video3sViews += value;
    if (["like", "post_reaction", "onsite_post_reaction"].includes(type)) likes += value;
    if (["comment", "post_comment"].includes(type)) comments += value;
    if (["share", "post_share"].includes(type)) shares += value;
    if (["save", "post_save"].includes(type)) saves += value;
    if (["link_click", "outbound_click"].includes(type)) linkClicks += value;
    if (type === "landing_page_view") landingPageViews += value;
  }

  return { likes, comments, shares, saves, video3sViews, linkClicks, landingPageViews };
}

// ====== ABSOLUTE SCORING ======
// Each ad is scored independently using fixed formulas — no comparison to other ads.
// Scores are uncapped (no maximum). Higher = better.
//
// Traffic:   (CTR% × 25) + max(0, (20 − CPM) × 2) + (link_clicks × 0.1)
// Awareness: (reach / 1000 × 2) + max(0, (20 − CPM) × 2) + (view_rate × 2)
// Engagement: (shares × 5) + (saves × 3) + (comments × 2) + (likes × 0.5)

// ====== TARGET-BASED SCORING (Variant B) ======
// Score = 1.0 means on target, displayed as ×100 for points (100 pts = on target)
// Each component is capped at 2.0, so max score = 200 pts
const TARGETS = {
  awareness: {
    reach: 400000,    // Target reach
    cpm: 0.07,        // Target CPM (lower is better)
    viewRate: 15      // Target 3s view rate %
  },
  engagement: {
    shareRate: 2.0,   // Shares per 1K reach
    saveRate: 3.0,    // Saves per 1K reach
    commentRate: 1.5, // Comments per 1K reach
    likeRate: 20.0    // Likes per 1K reach
  },
  traffic: {
    ctr: 1.0,         // Target CTR %
    cpc: 0.007,       // Target CPC $ (lower is better)
    lpvr: 0.70,       // Target LPVR (landing page view rate)
    frequency: 2.0    // Target frequency (lower is better)
  }
};

function computeAbsoluteScores(rows) {
  for (const row of rows) {
    if (row.campaign_type === "awareness") {
      const reachRatio = Math.min(TARGETS.awareness.reach > 0 ? row.reach / TARGETS.awareness.reach : 0, 2.0);
      const cpmRatio = Math.min(row.cpm > 0 ? TARGETS.awareness.cpm / row.cpm : 0, 2.0);
      const viewRatio = Math.min(TARGETS.awareness.viewRate > 0 ? row.video_3s_view_rate / TARGETS.awareness.viewRate : 0, 2.0);
      const raw = 0.40 * reachRatio + 0.40 * cpmRatio + 0.20 * viewRatio;
      row.awareness_score = round(raw * 100);
      row.engagement_score = null;
      row.traffic_score = null;
    } else if (row.campaign_type === "engagement") {
      const reach1k = row.reach > 0 ? row.reach / 1000 : 0.001;
      const shareRate = row.shares / reach1k;
      const saveRate = row.saves / reach1k;
      const commentRate = row.comments / reach1k;
      const likeRate = row.likes / reach1k;
      const raw = 0.40 * Math.min(shareRate / TARGETS.engagement.shareRate, 2.0) +
                  0.30 * Math.min(saveRate / TARGETS.engagement.saveRate, 2.0) +
                  0.20 * Math.min(commentRate / TARGETS.engagement.commentRate, 2.0) +
                  0.10 * Math.min(likeRate / TARGETS.engagement.likeRate, 2.0);
      row.engagement_score = round(raw * 100);
      row.awareness_score = null;
      row.traffic_score = null;
    } else if (row.campaign_type === "traffic") {
      const ctrRatio = Math.min(TARGETS.traffic.ctr > 0 ? row.ctr / TARGETS.traffic.ctr : 0, 2.0);
      const cpcRatio = Math.min(row.cost_per_click > 0 ? TARGETS.traffic.cpc / row.cost_per_click : 0, 2.0);
      const lpvrRatio = Math.min(TARGETS.traffic.lpvr > 0 ? row.lpvr / TARGETS.traffic.lpvr : 0, 2.0);
      const freqRatio = Math.min(row.frequency > 0 ? TARGETS.traffic.frequency / row.frequency : 0, 2.0);
      const raw = 0.40 * ctrRatio + 0.30 * cpcRatio + 0.20 * lpvrRatio + 0.10 * freqRatio;
      row.traffic_score = round(raw * 100);
      row.awareness_score = null;
      row.engagement_score = null;
    }

    // Target-based boost thresholds: 100 = on target, max 200
    const score = row.awareness_score ?? row.engagement_score ?? row.traffic_score ?? 0;
    if (score >= 100) row.boost_recommendation = "boost";
    else if (score >= 70) row.boost_recommendation = "monitor";
    else row.boost_recommendation = "no boost";
  }
}

// ====== META API FUNCTIONS ======

// Fetch campaign objectives AND budgets from Meta
// Returns { objectives: { campaign_name -> objective }, objectivesById: { campaign_id -> objective }, budgets: { campaign_id -> { daily_budget, lifetime_budget } } }
async function fetchCampaignInfo() {
  const objectiveMap = {};  // campaign_name -> objective string
  const objectiveByIdMap = {};  // campaign_id -> objective string (reliable even after renames)
  const campaignBudgets = {}; // campaign_id -> { daily_budget, lifetime_budget }
  try {
    let url = `https://graph.facebook.com/v25.0/act_${META_AD_ACCOUNT_ID}/campaigns`;
    let hasMore = true;

    while (hasMore) {
      const { data } = await axios.get(url, {
        params: {
          access_token: META_ACCESS_TOKEN,
          fields: "id,name,objective,status,daily_budget,lifetime_budget",
          limit: 200
        }
      });

      for (const campaign of (data.data || [])) {
        objectiveMap[campaign.name] = campaign.objective || "";
        objectiveByIdMap[campaign.id] = campaign.objective || "";
        campaignBudgets[campaign.id] = {
          daily_budget: safeNumber(campaign.daily_budget) / 100, // Meta returns cents
          lifetime_budget: safeNumber(campaign.lifetime_budget) / 100
        };
        console.log(`Campaign: "${campaign.name}" (${campaign.id}) → objective: ${campaign.objective}, status: ${campaign.status}`);
      }

      if (data.paging?.next) {
        url = data.paging.next;
      } else {
        hasMore = false;
      }
    }

    console.log(`Loaded info for ${Object.keys(objectiveMap).length} campaigns`);
  } catch (err) {
    console.warn("Failed to fetch campaign info (non-blocking):", err.message);
  }
  return { objectives: objectiveMap, objectivesById: objectiveByIdMap, budgets: campaignBudgets };
}

// Fetch adset budgets (budgets are often set at adset level, not campaign level)
// Returns map: adset_name -> { daily_budget, lifetime_budget }
async function fetchAdsetBudgets() {
  const budgets = {}; // adset_name -> { daily_budget, lifetime_budget }
  try {
    const url = `https://graph.facebook.com/v25.0/act_${META_AD_ACCOUNT_ID}/adsets`;
    const { data } = await axios.get(url, {
      params: {
        access_token: META_ACCESS_TOKEN,
        fields: "id,name,campaign_id,daily_budget,lifetime_budget,status",
        limit: 200
      }
    });

    for (const adset of (data.data || [])) {
      budgets[adset.name] = {
        daily_budget: safeNumber(adset.daily_budget) / 100,
        lifetime_budget: safeNumber(adset.lifetime_budget) / 100,
        campaign_id: adset.campaign_id
      };
      console.log(`Adset: "${adset.name}" → lifetime_budget: ${adset.lifetime_budget}, daily_budget: ${adset.daily_budget}`);
    }

    console.log(`Loaded budgets for ${Object.keys(budgets).length} adsets`);
  } catch (err) {
    console.warn("Failed to fetch adset budgets (non-blocking):", err.message);
  }
  return budgets;
}

async function fetchMetaInsights(datePreset = "last_30d") {
  const url = `https://graph.facebook.com/v25.0/act_${META_AD_ACCOUNT_ID}/insights`;
  let allRows = [];
  let nextUrl = null;

  // First request
  const { data } = await axios.get(url, {
    params: {
      access_token: META_ACCESS_TOKEN,
      fields: "campaign_name,campaign_id,adset_name,ad_name,ad_id,impressions,reach,cpm,spend,frequency,actions,cost_per_action_type,date_start,date_stop",
      breakdowns: "publisher_platform",
      action_breakdowns: "action_type",
      level: "ad",
      date_preset: datePreset,
      limit: 200
    }
  });

  allRows = data.data || [];
  nextUrl = data.paging?.next || null;

  // Paginate through remaining results
  let pageNum = 1;
  while (nextUrl) {
    pageNum++;
    try {
      const { data: pageData } = await axios.get(nextUrl);
      const rows = pageData.data || [];
      if (rows.length === 0) break;
      allRows = allRows.concat(rows);
      nextUrl = pageData.paging?.next || null;
      console.log(`Meta insights page ${pageNum}: ${rows.length} rows (total so far: ${allRows.length})`);
    } catch (err) {
      console.warn(`Meta insights pagination failed on page ${pageNum}:`, err.message);
      break;
    }
  }

  console.log(`Meta insights: ${allRows.length} total rows across ${pageNum} page(s)`);
  return allRows;
}

// Fetch ad creative thumbnails, ad delivery status, and created_time from Meta — keyed by ad_id
// Returns { thumbnails: { ad_id: url }, statuses: { ad_id: "ACTIVE"|"COMPLETED"|... }, createdTimes: { ad_id: ISO string } }
async function fetchAdDetails() {
  const thumbnails = {};    // ad_id -> thumbnail_url
  const statuses = {};      // ad_id -> computed delivery status
  const createdTimes = {};  // ad_id -> created_time ISO string
  try {
    // Step 1: Get ads with campaign + adset details to determine true delivery status
    const adsUrl = `https://graph.facebook.com/v25.0/act_${META_AD_ACCOUNT_ID}/ads`;
    const { data: adsData } = await axios.get(adsUrl, {
      params: {
        access_token: META_ACCESS_TOKEN,
        fields: "id,name,effective_status,configured_status,created_time,creative{id},campaign{id,effective_status,budget_remaining,lifetime_budget,daily_budget,stop_time,start_time},adset{id,effective_status,budget_remaining,lifetime_budget,daily_budget,end_time,start_time}",
        limit: 200
      }
    });

    const ads = adsData.data || [];
    console.log(`Found ${ads.length} ads from Meta`);

    // Build creative_id -> [ad_ids] mapping and compute delivery status
    const creativeToAds = {};
    const now = new Date();

    for (const ad of ads) {
      createdTimes[ad.id] = ad.created_time || null;

      // Compute the real delivery status like Ads Manager does
      const adEffective = ad.effective_status || "UNKNOWN";
      const campaignEffective = ad.campaign?.effective_status || "UNKNOWN";
      const adsetEffective = ad.adset?.effective_status || "UNKNOWN";

      // Campaign/adset budget and schedule info
      const campaignBudgetRemaining = parseFloat(ad.campaign?.budget_remaining || "-1");
      const adsetBudgetRemaining = parseFloat(ad.adset?.budget_remaining || "-1");
      const campaignLifetime = parseFloat(ad.campaign?.lifetime_budget || "0");
      const adsetLifetime = parseFloat(ad.adset?.lifetime_budget || "0");
      const campaignStopTime = ad.campaign?.stop_time ? new Date(ad.campaign.stop_time) : null;
      const adsetEndTime = ad.adset?.end_time ? new Date(ad.adset.end_time) : null;

      let deliveryStatus;

      // If the ad itself is not active, use that status
      if (adEffective !== "ACTIVE") {
        deliveryStatus = adEffective; // PAUSED, DELETED, ARCHIVED, etc.
      }
      // Campaign is paused/off
      else if (campaignEffective !== "ACTIVE") {
        deliveryStatus = "CAMPAIGN_PAUSED";
      }
      // Adset is paused/off
      else if (adsetEffective !== "ACTIVE") {
        deliveryStatus = "ADSET_PAUSED";
      }
      // Campaign has a lifetime budget and it's been fully spent (remaining = 0)
      else if (campaignLifetime > 0 && campaignBudgetRemaining === 0) {
        deliveryStatus = "COMPLETED";
      }
      // Adset has a lifetime budget and it's been fully spent
      else if (adsetLifetime > 0 && adsetBudgetRemaining === 0) {
        deliveryStatus = "COMPLETED";
      }
      // Campaign has a stop time that has passed
      else if (campaignStopTime && campaignStopTime < now) {
        deliveryStatus = "COMPLETED";
      }
      // Adset has an end time that has passed
      else if (adsetEndTime && adsetEndTime < now) {
        deliveryStatus = "COMPLETED";
      }
      // Everything checks out — truly active
      else {
        deliveryStatus = "ACTIVE";
      }

      statuses[ad.id] = deliveryStatus;
      console.log(`Ad ${ad.id} (${ad.name?.substring(0,30)}): effective=${adEffective}, campaign=${campaignEffective}, adset=${adsetEffective}, budgetRemain=${campaignBudgetRemaining}/${adsetBudgetRemaining}, delivery=${deliveryStatus}`);

      const cid = ad.creative?.id;
      if (cid) {
        if (!creativeToAds[cid]) creativeToAds[cid] = [];
        creativeToAds[cid].push(ad.id);
      }
    }

    // Step 2: Fetch hi-res thumbnails from adcreatives endpoint (600px wide)
    const creativesUrl = `https://graph.facebook.com/v25.0/act_${META_AD_ACCOUNT_ID}/adcreatives`;
    const { data: creativesData } = await axios.get(creativesUrl, {
      params: {
        access_token: META_ACCESS_TOKEN,
        fields: "id,thumbnail_url,image_url",
        thumbnail_width: 600,
        thumbnail_height: 600,
        limit: 100
      }
    });

    for (const creative of (creativesData.data || [])) {
      const thumbUrl = creative.thumbnail_url || creative.image_url || null;
      const adIds = creativeToAds[creative.id] || [];
      for (const adId of adIds) {
        if (thumbUrl) {
          thumbnails[adId] = thumbUrl;
        }
      }
    }

    console.log(`Mapped ${Object.keys(thumbnails).length} thumbnails, ${Object.keys(statuses).length} statuses`);
    // Log delivery status summary
    const statusCounts = {};
    for (const s of Object.values(statuses)) {
      statusCounts[s] = (statusCounts[s] || 0) + 1;
    }
    console.log("Delivery status summary:", JSON.stringify(statusCounts));
  } catch (err) {
    console.warn("Could not fetch ad details:", err.message);
  }
  return { thumbnails, statuses, createdTimes };
}

// Debug endpoint to check what Meta returns for creatives
// Debug endpoint: see campaign objectives from Meta
app.get("/debug-campaigns", async (req, res) => {
  try {
    const campaignInfo = await fetchCampaignInfo();
    res.json(campaignInfo);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// One-time fix: update campaign_type for all existing Supabase snapshots
// Uses both campaign_id and campaign_name to look up objectives (handles renamed campaigns)
app.get("/fix-campaign-types", async (req, res) => {
  try {
    const { objectives, objectivesById } = await fetchCampaignInfo();

    // Get all snapshots with campaign_id for reliable matching
    const { data: snapshots, error: fetchErr } = await supabase
      .from("ad_snapshots")
      .select("id, campaign_name, campaign_id, campaign_type, publisher_platform")
      .order("id", { ascending: true });

    if (fetchErr) throw fetchErr;

    let updated = 0;
    let skipped = 0;
    const changes = [];

    for (const snap of snapshots) {
      const platform = (snap.publisher_platform || "").toLowerCase();

      if (platform === "tiktok") {
        skipped++;
        continue;
      }

      // Try campaign_id first (reliable even after renames), then fall back to name
      const apiObjective = (snap.campaign_id && objectivesById[snap.campaign_id])
        || objectives[snap.campaign_name]
        || "";
      const correctType = objectiveToCampaignType(apiObjective) || detectCampaignTypeFallback(snap.campaign_name, "");

      if (correctType && correctType !== snap.campaign_type) {
        const { error: updateErr } = await supabase
          .from("ad_snapshots")
          .update({ campaign_type: correctType })
          .eq("id", snap.id);

        if (!updateErr) {
          updated++;
          changes.push({
            id: snap.id,
            campaign_name: snap.campaign_name,
            campaign_id: snap.campaign_id,
            old_type: snap.campaign_type,
            new_type: correctType,
            objective: apiObjective
          });
        }
      } else {
        skipped++;
      }
    }

    res.json({
      total_snapshots: snapshots.length,
      updated,
      skipped,
      objectives,
      changes: changes.slice(0, 100)
    });
  } catch (err) {
    res.status(500).json({ error: err.message, stack: err.stack });
  }
});

app.get("/debug-creatives", async (req, res) => {
  try {
    const url = `https://graph.facebook.com/v25.0/act_${META_AD_ACCOUNT_ID}/ads`;
    const { data } = await axios.get(url, {
      params: {
        access_token: META_ACCESS_TOKEN,
        fields: "id,name,creative{id,thumbnail_url,image_url,object_story_spec}",
        limit: 100
      }
    });
    res.json(data.data || []);
  } catch (err) {
    res.status(500).json({ error: err.message, details: err.response?.data });
  }
});

// ====== AI INSIGHTS (Claude) ======

async function generateAdInsights(ads) {
  if (!ANTHROPIC_API_KEY || ads.length === 0) return;

  // Build a concise summary of all ads for Claude to analyze in one call
  const adSummaries = ads.map((ad, i) => {
    const score = ad.awareness_score ?? ad.engagement_score ?? 'N/A';
    const type = ad.campaign_type;
    if (type === 'awareness') {
      return `Ad ${i+1}: "${ad.ad_name}" (${ad.publisher_platform}) — Score: ${score}/100, Reach: ${ad.reach}, CPM: $${ad.cpm}, 3s View Rate: ${ad.video_3s_view_rate}%, Impressions: ${ad.impressions}`;
    } else if (type === 'traffic') {
      return `Ad ${i+1}: "${ad.ad_name}" (${ad.publisher_platform}) — Score: ${score}/100, Link Clicks: ${ad.link_clicks}, CTR: ${ad.ctr}%, CPM: $${ad.cpm}, Reach: ${ad.reach}, Impressions: ${ad.impressions}`;
    } else {
      return `Ad ${i+1}: "${ad.ad_name}" (${ad.publisher_platform}) — Score: ${score}/100, Shares: ${ad.shares}, Saves: ${ad.saves}, Comments: ${ad.comments}, Likes: ${ad.likes}`;
    }
  }).join('\n');

  const prompt = `You are an expert social media ad analyst for FibreGuard (stain-free textile technology). Analyze each ad and give a 1-2 sentence insight. Be specific about what's working or not, and give one actionable suggestion.

Format your response as JSON array with objects: [{"index": 0, "insight": "..."}, ...]

The ads:
${adSummaries}

Rules:
- Keep each insight under 120 characters
- Be direct and actionable (e.g. "Strong reach at low CPM — boost now" or "Low saves suggest weak CTA — try adding a hook")
- Reference specific metrics that stand out
- For awareness: focus on reach efficiency and view rates
- For engagement: focus on share/save ratio and audience resonance`;

  try {
    const response = await axios.post('https://api.anthropic.com/v1/messages', {
      model: 'claude-haiku-4-5-20251001',
      max_tokens: 1024,
      messages: [{ role: 'user', content: prompt }]
    }, {
      headers: {
        'x-api-key': ANTHROPIC_API_KEY,
        'anthropic-version': '2023-06-01',
        'content-type': 'application/json'
      }
    });

    const text = response.data.content[0].text;
    // Extract JSON from response (handle potential markdown wrapping)
    const jsonMatch = text.match(/\[[\s\S]*\]/);
    if (jsonMatch) {
      const insights = JSON.parse(jsonMatch[0]);
      for (const item of insights) {
        if (item.index >= 0 && item.index < ads.length && item.insight) {
          ads[item.index].ai_insight = item.insight.substring(0, 200);
        }
      }
      console.log(`Generated AI insights for ${insights.length} ads`);
    }
  } catch (err) {
    console.warn('AI insight generation failed (non-blocking):', err.message);
    // Non-blocking — capture continues without insights
  }
}

// ====== BUDGET STATUS HELPER ======
// Determines if an ad's budget is effectively spent.
//
// For LIFETIME budgets: simple — spend >= 95% of lifetime budget = done.
// For DAILY budgets (which FibreGuard uses): compare total campaign spend vs
// what the daily budget SHOULD have produced over the ad's lifetime.
// If the ad has been running 5+ days but only used a small fraction of its
// potential daily budget, it means Meta stopped delivering it = effectively done.
//
// totalCampaignSpend = sum of spend across ALL platforms for this campaign_id
// adCreatedTime = when the ad was created (ISO string)

// Map the computed delivery status to dashboard status
function mapMetaStatus(deliveryStatus) {
  switch (deliveryStatus) {
    case "ACTIVE":
      return "ACTIVE";
    case "PAUSED":
    case "CAMPAIGN_PAUSED":
    case "ADSET_PAUSED":
      return "PAUSED";
    case "COMPLETED":
      return "COMPLETED";
    case "PENDING_REVIEW":
    case "PREAPPROVED":
    case "PENDING_BILLING_INFO":
    case "IN_PROCESS":
      return "PENDING";
    default:
      return "COMPLETED"; // DELETED, ARCHIVED, DISAPPROVED, UNKNOWN → all completed/inactive
  }
}

// ====== TIKTOK API FUNCTIONS ======

async function getTikTokToken() {
  const { data, error } = await supabase
    .from("tiktok_tokens")
    .select("*")
    .order("updated_at", { ascending: false })
    .limit(1)
    .single();

  if (error || !data) return null;

  // Check if token needs refresh (expires within 1 hour)
  const expiresAt = new Date(data.token_expires_at);
  if (expiresAt < new Date(Date.now() + 60 * 60 * 1000) && data.refresh_token && TIKTOK_APP_ID && TIKTOK_APP_SECRET) {
    try {
      const res = await axios.post("https://business-api.tiktok.com/open_api/v1.3/oauth2/access_token/", {
        app_id: TIKTOK_APP_ID,
        secret: TIKTOK_APP_SECRET,
        grant_type: "refresh_token",
        refresh_token: data.refresh_token
      });
      const newToken = res.data?.data;
      if (newToken?.access_token) {
        const newExpires = new Date(Date.now() + 24 * 60 * 60 * 1000).toISOString();
        await supabase.from("tiktok_tokens").update({
          access_token: newToken.access_token,
          refresh_token: newToken.refresh_token || data.refresh_token,
          token_expires_at: newExpires,
          updated_at: new Date().toISOString()
        }).eq("advertiser_id", data.advertiser_id);
        console.log("TikTok token refreshed successfully");
        return { ...data, access_token: newToken.access_token };
      }
    } catch (err) {
      console.warn("TikTok token refresh failed:", err.message);
    }
  }

  return data;
}

async function fetchTikTokCampaigns(token) {
  if (!token) return {};
  try {
    // Fetch ALL campaigns (not just enabled) to get objectives for completed ads too
    const campaignObjectiveMap = {};
    const campaignStatusMap = {};
    for (const status of ["CAMPAIGN_STATUS_ENABLE", "CAMPAIGN_STATUS_DISABLE", "CAMPAIGN_STATUS_DELETE"]) {
      try {
        const campRes = await axios.get("https://business-api.tiktok.com/open_api/v1.3/campaign/get/", {
          params: {
            advertiser_id: token.advertiser_id,
            page_size: 200,
            filtering: JSON.stringify({ status })
          },
          headers: { "Access-Token": token.access_token }
        });
        const campaigns = campRes.data?.data?.list || [];
        for (const c of campaigns) {
          campaignObjectiveMap[c.campaign_id] = c.objective_type || c.objective || "";
          campaignStatusMap[c.campaign_id] = c.status || c.operation_status || status;
        }
      } catch (e) { /* skip this status */ }
    }
    console.log(`TikTok: loaded ${Object.keys(campaignObjectiveMap).length} campaigns with objectives`);
    return { campaignObjectiveMap, campaignStatusMap };
  } catch (err) {
    console.warn("TikTok campaign fetch failed:", err.response?.data || err.message);
    return { campaignObjectiveMap: {}, campaignStatusMap: {} };
  }
}

async function fetchTikTokAds(token) {
  if (!token) return [];
  try {
    // Use primary_status NOT_DELETE to get ALL ads (active + ended + disabled, just not deleted)
    const res = await axios.get("https://business-api.tiktok.com/open_api/v1.3/ad/get/", {
      params: {
        advertiser_id: token.advertiser_id,
        page_size: 200,
        filtering: JSON.stringify({ primary_status: "STATUS_NOT_DELETE" })
      },
      headers: { "Access-Token": token.access_token }
    });
    const ads = res.data?.data?.list || [];
    console.log(`TikTok: found ${ads.length} non-deleted ads`);
    console.log(`TikTok API response code: ${res.data?.code}, message: ${res.data?.message}`);

    // Log each ad's status for debugging
    for (const ad of ads) {
      ad._primary_status = ad.primary_status || ad.status || "";
    }
    return ads;
  } catch (err) {
    console.warn("TikTok ad fetch failed:", JSON.stringify(err.response?.data || err.message));
    // Fallback: try without any filtering
    try {
      console.log("TikTok: retrying ad fetch without status filter...");
      const res2 = await axios.get("https://business-api.tiktok.com/open_api/v1.3/ad/get/", {
        params: { advertiser_id: token.advertiser_id, page_size: 200 },
        headers: { "Access-Token": token.access_token }
      });
      const ads = res2.data?.data?.list || [];
      console.log(`TikTok fallback: found ${ads.length} ads, code: ${res2.data?.code}`);
      for (const ad of ads) {
        ad._primary_status = ad.primary_status || ad.status || "";
      }
      return ads;
    } catch (err2) {
      console.warn("TikTok ad fetch fallback also failed:", JSON.stringify(err2.response?.data || err2.message));
      return [];
    }
  }
}

// Fetch TikTok ad thumbnails from video/image creative info
// Returns map: ad_id -> thumbnail_url
async function fetchTikTokThumbnails(token, ads) {
  const thumbnails = {};
  if (!token || ads.length === 0) return thumbnails;

  try {
    // Collect video_ids, tiktok_item_ids, and image_ids from ads
    const videoIds = [];
    const tiktokItemIds = [];
    const imageAdMap = {}; // image_id -> [ad_ids]
    const videoAdMap = {}; // video_id -> [ad_ids]
    const itemAdMap = {}; // tiktok_item_id -> [ad_ids]

    for (const ad of ads) {
      const adId = String(ad.ad_id);

      // Standard video ads have video_id
      if (ad.video_id) {
        videoIds.push(ad.video_id);
        if (!videoAdMap[ad.video_id]) videoAdMap[ad.video_id] = [];
        videoAdMap[ad.video_id].push(adId);
      }

      // Spark Ads (boosted organic posts) use tiktok_item_id
      if (ad.tiktok_item_id) {
        tiktokItemIds.push(ad.tiktok_item_id);
        if (!itemAdMap[ad.tiktok_item_id]) itemAdMap[ad.tiktok_item_id] = [];
        itemAdMap[ad.tiktok_item_id].push(adId);
      }

      // Image ads
      const imageIds = ad.image_ids || [];
      if (Array.isArray(imageIds) && imageIds.length > 0) {
        for (const imgId of imageIds) {
          if (!imageAdMap[imgId]) imageAdMap[imgId] = [];
          imageAdMap[imgId].push(adId);
        }
      }

      // Direct URL fields
      const directUrl = ad.avatar_icon_web_uri || ad.profile_image_url || null;
      if (directUrl && !thumbnails[adId]) {
        thumbnails[adId] = directUrl;
      }
    }

    console.log(`TikTok thumbnails: ${videoIds.length} video_ids, ${tiktokItemIds.length} tiktok_item_ids, ${Object.keys(imageAdMap).length} image_ids, ${Object.keys(thumbnails).length} direct URLs`);

    // For Spark Ads: use TikTok oEmbed API (public, no auth needed)
    // These are boosted organic posts — tiktok_item_id is the organic video ID
    const uniqueItemIds = [...new Set(tiktokItemIds)];
    if (uniqueItemIds.length > 0) {
      const OEMBED_CONCURRENCY = 5;
      for (let i = 0; i < uniqueItemIds.length; i += OEMBED_CONCURRENCY) {
        const batch = uniqueItemIds.slice(i, i + OEMBED_CONCURRENCY);
        const results = await Promise.allSettled(
          batch.map(async (itemId) => {
            try {
              const videoUrl = `https://www.tiktok.com/@/video/${itemId}`;
              const res = await axios.get("https://www.tiktok.com/oembed", {
                params: { url: videoUrl },
                timeout: 5000
              });
              return { itemId, thumbnailUrl: res.data?.thumbnail_url || null };
            } catch (e) {
              return { itemId, thumbnailUrl: null };
            }
          })
        );
        for (const result of results) {
          if (result.status === "fulfilled" && result.value.thumbnailUrl) {
            const { itemId, thumbnailUrl } = result.value;
            if (itemAdMap[itemId]) {
              for (const adId of itemAdMap[itemId]) {
                thumbnails[adId] = thumbnailUrl;
              }
            }
          }
        }
      }
      console.log(`TikTok: after oEmbed Spark Ad lookup, ${Object.keys(thumbnails).length} thumbnails`);
    }

    // Fetch standard video thumbnails (poster_url) in batches of 60
    const uniqueVideoIds = [...new Set(videoIds)];
    for (let i = 0; i < uniqueVideoIds.length; i += 60) {
      const batch = uniqueVideoIds.slice(i, i + 60);
      try {
        const res = await axios.get("https://business-api.tiktok.com/open_api/v1.3/file/video/ad/info/", {
          params: {
            advertiser_id: token.advertiser_id,
            video_ids: JSON.stringify(batch)
          },
          headers: { "Access-Token": token.access_token }
        });
        const videos = res.data?.data?.list || [];
        for (const v of videos) {
          const posterUrl = v.poster_url || v.video_cover_url || null;
          if (posterUrl && videoAdMap[v.video_id]) {
            for (const adId of videoAdMap[v.video_id]) {
              thumbnails[adId] = posterUrl;
            }
          }
        }
      } catch (e) {
        console.warn(`TikTok video info batch failed: ${e.response?.data?.message || e.message}`);
      }
    }

    // Fetch image URLs in batches of 100
    const uniqueImageIds = Object.keys(imageAdMap);
    for (let i = 0; i < uniqueImageIds.length; i += 100) {
      const batch = uniqueImageIds.slice(i, i + 100);
      try {
        const res = await axios.get("https://business-api.tiktok.com/open_api/v1.3/file/image/ad/info/", {
          params: {
            advertiser_id: token.advertiser_id,
            image_ids: JSON.stringify(batch)
          },
          headers: { "Access-Token": token.access_token }
        });
        const images = res.data?.data?.list || [];
        for (const img of images) {
          const imgUrl = img.image_url || img.url || null;
          if (imgUrl && imageAdMap[img.image_id]) {
            for (const adId of imageAdMap[img.image_id]) {
              if (!thumbnails[adId]) thumbnails[adId] = imgUrl;
            }
          }
        }
      } catch (e) {
        console.warn(`TikTok image info batch failed: ${e.response?.data?.message || e.message}`);
      }
    }

    console.log(`TikTok: fetched ${Object.keys(thumbnails).length} total thumbnails`);
  } catch (err) {
    console.warn("TikTok thumbnail fetch failed (non-blocking):", err.message);
  }
  return thumbnails;
}

async function fetchTikTokInsights(token, adIds) {
  if (!token) return [];
  const endDate = new Date().toISOString().split("T")[0];
  const startDate = new Date(Date.now() - 90 * 24 * 60 * 60 * 1000).toISOString().split("T")[0];

  // Extended metrics include engagement + video data
  // Note: "favorites" is NOT a valid TikTok reporting metric (causes error 40002), so we omit it
  const extendedMetrics = ["spend", "impressions", "clicks", "reach", "likes", "shares", "comments", "video_watched_2s", "video_play_actions"];
  const basicMetrics = ["spend", "impressions", "clicks"];

  async function fetchWithMetrics(metricsArray) {
    let allRows = [];
    let page = 1;
    const pageSize = 200;

    while (true) {
      const res = await axios.get("https://business-api.tiktok.com/open_api/v1.3/report/integrated/get/", {
        params: {
          advertiser_id: token.advertiser_id, report_type: "BASIC",
          data_level: "AUCTION_AD", dimensions: JSON.stringify(["ad_id"]),
          metrics: JSON.stringify(metricsArray),
          start_date: startDate, end_date: endDate,
          page_size: pageSize, page: page
        },
        headers: { "Access-Token": token.access_token }
      });

      const code = res.data?.code;
      const list = res.data?.data?.list || [];
      const pageInfo = res.data?.data?.page_info || {};
      console.log(`TikTok insights page ${page}: ${list.length} rows, code: ${code}, total: ${pageInfo.total_number || "?"}`);

      if (code !== 0) throw new Error(`TikTok API code ${code}: ${res.data?.message || "unknown"}`);
      allRows = allRows.concat(list);
      if (page >= (pageInfo.total_page || 1)) break;
      page++;
    }
    return allRows;
  }

  // Try extended metrics first, fall back to basic 3 if it fails
  try {
    console.log(`TikTok insights (extended): ${startDate} to ${endDate}, metrics: ${JSON.stringify(extendedMetrics)}`);
    const rows = await fetchWithMetrics(extendedMetrics);
    if (rows.length > 0) {
      console.log(`✅ TikTok extended metrics SUCCESS: ${rows.length} rows`);
      return rows;
    }
    console.log(`⚠️ TikTok extended metrics returned 0 rows, falling back to basic...`);
  } catch (err) {
    console.log(`⚠️ TikTok extended metrics failed: ${err.message}, falling back to basic...`);
  }

  try {
    console.log(`TikTok insights (basic fallback): metrics: ${JSON.stringify(basicMetrics)}`);
    const rows = await fetchWithMetrics(basicMetrics);
    console.log(`TikTok basic metrics: ${rows.length} rows`);
    return rows;
  } catch (err) {
    console.warn("TikTok insights failed (both extended and basic):", JSON.stringify(err.response?.data || err.message));
    return [];
  }
}

function detectTikTokCampaignType(campaignName = "", objective = "") {
  const obj = objective.toUpperCase();
  // TikTok API objective_type values
  if (obj === "REACH" || obj === "VIDEO_VIEWS" || obj === "RF_REACH") return "awareness";
  if (obj === "TRAFFIC" || obj === "WEBSITE_CONVERSIONS" || obj === "CATALOG_SALES") return "traffic";
  if (obj === "COMMUNITY_INTERACTION" || obj === "ENGAGEMENT" || obj === "LEAD_GENERATION") return "engagement";

  // Fallback: detect from campaign name
  const text = `${campaignName} ${objective}`.toLowerCase();
  if (text.includes("awareness") || text.includes("reach") || text.includes("video_view")) return "awareness";
  if (text.includes("traffic") || text.includes("click") || text.includes("website")) return "traffic";
  if (text.includes("engagement") || text.includes("community") || text.includes("interaction")) return "engagement";

  console.log(`TikTok: unknown objective "${objective}" for campaign "${campaignName}", defaulting to awareness`);
  return "awareness";
}

function processTikTokData(ads, insights, campaignObjectiveMap = {}, campaignStatusMap = {}, thumbnailMap = {}) {
  // Build ad info map from ad/get response
  const adInfoMap = {};
  for (const ad of ads) {
    adInfoMap[String(ad.ad_id)] = {
      ad_name: ad.ad_name || ad.ad_text || "",
      campaign_name: ad.campaign_name || "",
      campaign_id: ad.campaign_id || null,
      adgroup_name: ad.adgroup_name || "",
      objective: campaignObjectiveMap[ad.campaign_id] || ad.objective_type || ad.objective || "",
      campaignStatus: campaignStatusMap[ad.campaign_id] || "",
      adStatus: ad.status || ad.operation_status || "",
      primaryStatus: ad._primary_status || ad.primary_status || "",
      secondaryStatus: ad.secondary_status || "",
      created_time: ad.create_time || null
    };
  }

  const rows = [];

  for (const item of insights) {
    const metrics = item.metrics || {};
    const dimensions = item.dimensions || {};
    const adId = String(dimensions.ad_id || "");
    const adInfo = adInfoMap[adId] || {};

    const spend = safeNumber(metrics.spend);
    const impressions = safeNumber(metrics.impressions);

    // SKIP ads with zero spend AND zero impressions — they never ran
    if (spend === 0 && impressions === 0) {
      console.log(`TikTok ad ${adId}: skipping (no spend, no impressions)`);
      continue;
    }

    const campaignType = detectTikTokCampaignType(adInfo.campaign_name, adInfo.objective);

    // Determine ad status using secondary_status (primary_status is often empty)
    // AD_STATUS_DELIVERY_OK = currently delivering, AD_STATUS_DONE/DISABLE/CAMPAIGN_DISABLE = completed
    const secStatus = (adInfo.secondaryStatus || "").toUpperCase();
    const priStatus = (adInfo.primaryStatus || "").toUpperCase();
    const isActive = secStatus === "AD_STATUS_DELIVERY_OK" || priStatus === "STATUS_DELIVERY_OK" ||
                     secStatus.includes("DELIVERY_OK");
    const adStatus = isActive ? "ACTIVE" : "COMPLETED";

    const clicks = safeNumber(metrics.clicks);

    // Calculate derived metrics — use real reach/engagement if available from extended metrics
    const reach = safeNumber(metrics.reach) || impressions;
    const cpm = impressions > 0 ? round((spend / impressions) * 1000) : 0;
    const cpc = clicks > 0 ? round(spend / clicks) : 0;
    const ctr = impressions > 0 ? round((clicks / impressions) * 100) : 0;
    const frequency = reach > 0 ? round(impressions / reach, 2) : 0;

    // TikTok engagement metrics (available when extended metrics succeed)
    const videoViews = safeNumber(metrics.video_play_actions);
    const video2s = safeNumber(metrics.video_watched_2s); // TikTok uses 2-second views
    const ttLikes = safeNumber(metrics.likes);
    const ttComments = safeNumber(metrics.comments);
    const ttShares = safeNumber(metrics.shares);
    const ttFavorites = 0; // TikTok "favorites" metric is not available in reporting API
    const landingPageViews = safeNumber(metrics.landing_page_view) || clicks;
    const viewRate = impressions > 0 ? round((video2s / impressions) * 100) : 0;
    const lpvr = clicks > 0 ? round(landingPageViews / clicks, 4) : 0;

    console.log(`TikTok ad ${adId}: campaign="${adInfo.campaign_name}", obj="${adInfo.objective}", type=${campaignType}, status=${adStatus}, spend=${spend}, reach=${reach}, likes=${ttLikes}, shares=${ttShares}, favorites=${ttFavorites}`);

    const ttAgeH = adInfo.created_time ? Math.round(hoursAgo(adInfo.created_time)) : 0;
    rows.push({
      captured_at: new Date().toISOString(),
      snapshot_hours: ttAgeH,
      hour_label: computeHourLabel(ttAgeH),
      campaign_type: campaignType,
      campaign_name: adInfo.campaign_name || "",
      campaign_id: adInfo.campaign_id || null,
      adset_name: adInfo.adgroup_name || "",
      ad_name: adInfo.ad_name || "",
      ad_id: adId,
      publisher_platform: "tiktok",
      ad_status: adStatus,
      ad_created_time: adInfo.created_time || null,
      date_start: null,
      date_stop: null,
      impressions,
      reach,
      cpm,
      spend,
      frequency,
      cost_per_click: cpc,
      landing_page_views: landingPageViews,
      lpvr,
      video_3s_views: videoViews,
      video_3s_view_rate: viewRate, // TikTok: 2-second view rate stored here
      likes: ttLikes,
      comments: ttComments,
      shares: ttShares,
      saves: ttFavorites, // TikTok "favorites" = Meta "saves"
      link_clicks: clicks,
      ctr,
      awareness_score: null,
      engagement_score: null,
      traffic_score: null,
      boost_recommendation: null,
      thumbnail_url: thumbnailMap[adId] || null
    });
  }

  console.log(`TikTok: ${rows.length} ads with actual data (filtered from ${insights.length} insight rows)`);
  computeAbsoluteScores(rows);
  return rows;
}

// ====== PROCESS INSIGHTS ROWS ======
// Shared logic for processing Meta insights rows into clean ad data
function processInsightRows(rows, { objectiveMap, statusMap, createdTimeMap, thumbnailMap, campaignBudgets, adsetBudgets, snapshotHours }) {
  // Pre-calculate total spend per campaign_id (across all platforms)
  // This is needed because daily_budget applies to the whole campaign, not per-platform
  const campaignSpendTotals = {};
  for (const item of rows) {
    const cid = item.campaign_id;
    if (cid) {
      campaignSpendTotals[cid] = (campaignSpendTotals[cid] || 0) + safeNumber(item.spend);
    }
  }
  console.log("Campaign spend totals:", JSON.stringify(campaignSpendTotals));

  const grouped = new Map();

  for (const item of rows) {
    const campaign_name = item.campaign_name || "";
    const ad_name = item.ad_name || "";
    const publisher_platform = (item.publisher_platform || "").toLowerCase();

    if (!publisher_platform || !["facebook", "instagram", "messenger", "audience_network"].includes(publisher_platform)) continue;
    if (publisher_platform === "audience_network" || publisher_platform === "messenger") continue;

    const key = `${campaign_name}__${ad_name}__${publisher_platform}`;

    const apiObjective = objectiveMap[campaign_name] || "";
    const campaignType = objectiveToCampaignType(apiObjective) || detectCampaignTypeFallback(campaign_name, ad_name);

    const rawStatus = statusMap[item.ad_id] || "UNKNOWN";
    const spend = safeNumber(item.spend);
    const adCreatedTime = createdTimeMap[item.ad_id] || null;
    const adStatus = mapMetaStatus(rawStatus);

    let costPerClick = 0;
    for (const cpa of (item.cost_per_action_type || [])) {
      if (["link_click", "outbound_click"].includes(cpa.action_type)) {
        costPerClick = safeNumber(cpa.value);
        break;
      }
    }

    const adAgeH = adCreatedTime ? Math.round(hoursAgo(adCreatedTime)) : 0;
    const existing = grouped.get(key) || {
      captured_at: new Date().toISOString(),
      ...(snapshotHours !== undefined ? { snapshot_hours: snapshotHours } : { snapshot_hours: adAgeH }),
      hour_label: computeHourLabel(adAgeH),
      campaign_type: campaignType,
      campaign_name,
      campaign_id: item.campaign_id || null,
      adset_name: item.adset_name || null,
      ad_name,
      ad_id: item.ad_id || null,
      publisher_platform,
      ad_status: adStatus,
      ad_created_time: createdTimeMap[item.ad_id] || null,
      date_start: item.date_start || null,
      date_stop: item.date_stop || null,
      impressions: safeNumber(item.impressions),
      reach: safeNumber(item.reach),
      cpm: safeNumber(item.cpm),
      spend: spend,
      frequency: safeNumber(item.frequency),
      cost_per_click: costPerClick,
      video_3s_views: 0,
      video_3s_view_rate: 0,
      likes: 0,
      comments: 0,
      shares: 0,
      saves: 0,
      link_clicks: 0,
      landing_page_views: 0,
      lpvr: 0,
      ctr: 0,
      awareness_score: null,
      engagement_score: null,
      traffic_score: null,
      boost_recommendation: null,
      thumbnail_url: thumbnailMap[item.ad_id] || null
    };

    const parsed = parseActions(item.actions || []);
    existing.video_3s_views += parsed.video3sViews;
    existing.likes += parsed.likes;
    existing.comments += parsed.comments;
    existing.shares += parsed.shares;
    existing.saves += parsed.saves;
    existing.link_clicks += parsed.linkClicks;
    existing.landing_page_views += parsed.landingPageViews;
    existing.impressions = safeNumber(item.impressions);
    existing.reach = safeNumber(item.reach);
    existing.cpm = safeNumber(item.cpm);
    existing.spend = spend;
    existing.frequency = safeNumber(item.frequency);

    grouped.set(key, existing);
  }

  const cleanRows = [...grouped.values()];

  for (const row of cleanRows) {
    row.video_3s_view_rate =
      row.impressions > 0 ? round((row.video_3s_views / row.impressions) * 100) : 0;
    row.ctr =
      row.impressions > 0 ? round((row.link_clicks / row.impressions) * 100) : 0;
    // Calculate CPC from spend / link_clicks (more accurate than cost_per_action_type)
    row.cost_per_click =
      row.link_clicks > 0 ? round(row.spend / row.link_clicks, 4) : 0;
    // LPVR: landing page views / link clicks (as decimal, e.g. 0.70 = 70%)
    row.lpvr =
      row.link_clicks > 0 ? round(row.landing_page_views / row.link_clicks, 4) : 0;
  }

  computeAbsoluteScores(cleanRows);
  return cleanRows;
}

// Add new/pending ads that have no insights data yet (e.g. just launched)
function addNewAdsWithoutInsights(cleanRows, { statusMap, createdTimeMap, thumbnailMap, objectiveMap, campaignBudgets, adsetBudgets }) {
  const existingAdIds = new Set(cleanRows.map(r => r.ad_id));

  for (const [adId, status] of Object.entries(statusMap)) {
    if (existingAdIds.has(adId)) continue; // already in results
    if (status !== "ACTIVE") continue; // only show active ads with no data

    // This ad is ACTIVE but has no insights — it's newly launched
    cleanRows.push({
      captured_at: new Date().toISOString(),
      campaign_type: "unknown",
      campaign_name: "",
      campaign_id: null,
      adset_name: null,
      ad_name: adId, // we only have the ID, name will come from ad details
      ad_id: adId,
      publisher_platform: "pending",
      ad_status: "NEW",
      ad_created_time: createdTimeMap[adId] || null,
      date_start: null,
      date_stop: null,
      impressions: 0, reach: 0, cpm: 0, spend: 0, frequency: 0, cost_per_click: 0,
      video_3s_views: 0, video_3s_view_rate: 0,
      likes: 0, comments: 0, shares: 0, saves: 0, link_clicks: 0, landing_page_views: 0, ctr: 0, lpvr: 0,
      awareness_score: null, engagement_score: null, traffic_score: null,
      boost_recommendation: "new",
      thumbnail_url: thumbnailMap[adId] || null,
      ai_insight: "Just launched — no performance data yet. Check back soon."
    });
  }
}

// ====== CAPTURE ENDPOINT ======

app.get("/capture", async (req, res) => {
  try {
    const snapshotHours = safeNumber(req.query.snapshot_hours || 1);

    const [rows, adDetails, campaignInfo, adsetBudgets] = await Promise.all([
      fetchMetaInsights(),
      fetchAdDetails(),
      fetchCampaignInfo(),
      fetchAdsetBudgets()
    ]);
    const { thumbnails: thumbnailMap, statuses: statusMap, createdTimes: createdTimeMap } = adDetails;
    const { objectives: objectiveMap, budgets: campaignBudgets } = campaignInfo;

    const cleanRows = processInsightRows(rows, {
      objectiveMap, statusMap, createdTimeMap, thumbnailMap,
      campaignBudgets, adsetBudgets, snapshotHours
    });

    // Also capture TikTok data (same system as Meta)
    try {
      const tikTokToken = await getTikTokToken();
      if (tikTokToken) {
        const [ttCampaignData, tikTokAds] = await Promise.all([
          fetchTikTokCampaigns(tikTokToken),
          fetchTikTokAds(tikTokToken)
        ]);
        const { campaignObjectiveMap = {}, campaignStatusMap = {} } = ttCampaignData;
        if (tikTokAds.length > 0) {
          const tikTokAdIds = tikTokAds.map(a => a.ad_id);
          const [tikTokInsights, tikTokThumbs] = await Promise.all([
            fetchTikTokInsights(tikTokToken, tikTokAdIds),
            fetchTikTokThumbnails(tikTokToken, tikTokAds)
          ]);
          const tikTokRows = processTikTokData(tikTokAds, tikTokInsights, campaignObjectiveMap, campaignStatusMap, tikTokThumbs);
          cleanRows.push(...tikTokRows);
          console.log(`Capture: added ${tikTokRows.length} TikTok ads`);
        }
      }
    } catch (ttErr) {
      console.warn("TikTok capture skipped:", ttErr.message);
    }

    await generateAdInsights(cleanRows);

    const { error } = await supabase.from("ad_snapshots").insert(cleanRows);
    if (error) throw error;

    res.json({ ok: true, inserted: cleanRows.length });
  } catch (error) {
    res.status(500).json({
      error: "Capture failed",
      details: error.response?.data || error.message
    });
  }
});

// ====== TIKTOK DEBUG ENDPOINT ======
app.get("/tiktok-debug", async (req, res) => {
  const log = [];
  try {
    log.push("Step 1: Getting TikTok token from Supabase...");
    const token = await getTikTokToken();
    if (!token) {
      log.push("ERROR: No TikTok token found in Supabase. Have you authorized TikTok?");
      return res.json({ ok: false, log });
    }
    log.push(`Token found for advertiser_id: ${token.advertiser_id}, expires: ${token.token_expires_at}`);

    log.push("\nStep 2: Fetching campaigns...");
    const { campaignObjectiveMap, campaignStatusMap } = await fetchTikTokCampaigns(token);
    log.push(`Campaigns loaded: ${Object.keys(campaignObjectiveMap).length}`);
    for (const [cid, obj] of Object.entries(campaignObjectiveMap)) {
      log.push(`  Campaign ${cid}: objective=${obj}, status=${campaignStatusMap[cid]}`);
    }

    log.push("\nStep 3: Fetching ads...");
    const ads = await fetchTikTokAds(token);
    log.push(`Ads found: ${ads.length}`);
    // Log first ad's raw keys and creative fields
    if (ads.length > 0) {
      log.push(`  Raw ad keys: ${Object.keys(ads[0]).join(', ')}`);
      log.push(`  First ad creative fields: video_id=${ads[0].video_id}, image_ids=${JSON.stringify(ads[0].image_ids)}, avatar=${ads[0].avatar_icon_web_uri?.substring(0,60) || 'none'}, profile_img=${ads[0].profile_image_url?.substring(0,60) || 'none'}`);
    }
    for (const ad of ads.slice(0, 10)) {
      log.push(`  Ad ${ad.ad_id}: "${(ad.ad_name||ad.ad_text||'').substring(0,40)}", video_id=${ad.video_id || 'none'}, tiktok_item_id=${ad.tiktok_item_id || 'none'}, identity_id=${ad.identity_id || 'none'}`);
    }
    if (ads.length > 10) log.push(`  ... and ${ads.length - 10} more ads`);

    if (ads.length === 0) {
      log.push("\nNo ads found — nothing to fetch insights for.");
      return res.json({ ok: true, log, ads: [], insights: [], processed: [] });
    }

    log.push("\nStep 4: Fetching insights (raw API test)...");
    // Raw API test first — minimal call to see what TikTok returns
    try {
      const endDate = new Date().toISOString().split("T")[0];
      const startDate = new Date(Date.now() - 90 * 24 * 60 * 60 * 1000).toISOString().split("T")[0];
      const rawRes = await axios.get("https://business-api.tiktok.com/open_api/v1.3/report/integrated/get/", {
        params: {
          advertiser_id: token.advertiser_id,
          report_type: "BASIC",
          data_level: "AUCTION_AD",
          dimensions: JSON.stringify(["ad_id"]),
          metrics: JSON.stringify(["spend", "impressions", "clicks"]),
          start_date: startDate,
          end_date: endDate,
          page_size: 200
        },
        headers: { "Access-Token": token.access_token }
      });
      log.push(`Raw API response — code: ${rawRes.data?.code}, message: "${rawRes.data?.message}", rows: ${rawRes.data?.data?.list?.length || 0}, page_info: ${JSON.stringify(rawRes.data?.data?.page_info || {})}`);
      if (rawRes.data?.code !== 0) log.push(`Full error: ${JSON.stringify(rawRes.data)}`);
      const rawList = rawRes.data?.data?.list || [];
      for (const item of rawList.slice(0, 5)) {
        log.push(`  Raw: ad_id=${item.dimensions?.ad_id}, spend=${item.metrics?.spend}, impressions=${item.metrics?.impressions}`);
      }
    } catch (rawErr) {
      log.push(`Raw API error: ${JSON.stringify(rawErr.response?.data || rawErr.message)}`);
    }

    log.push("\nStep 4b: Full insights fetch...");
    const adIds = ads.map(a => a.ad_id);
    const insights = await fetchTikTokInsights(token, adIds);
    log.push(`Insight rows: ${insights.length}`);
    for (const item of insights) {
      const m = item.metrics || {};
      const d = item.dimensions || {};
      log.push(`  Ad ${d.ad_id}: spend=${m.spend}, impressions=${m.impressions}, reach=${m.reach}, clicks=${m.clicks}, ctr=${m.ctr}, cpc=${m.cpc}, lpv=${m.landing_page_view}`);
    }

    log.push("\nStep 4c: Testing oEmbed for first Spark Ad...");
    const firstSparkAd = ads.find(a => a.tiktok_item_id);
    if (firstSparkAd) {
      const testItemId = firstSparkAd.tiktok_item_id;
      log.push(`  Testing tiktok_item_id: ${testItemId}`);
      for (const urlFormat of [
        `https://www.tiktok.com/@/video/${testItemId}`,
        `https://www.tiktok.com/video/${testItemId}`,
        `https://m.tiktok.com/v/${testItemId}.html`
      ]) {
        try {
          const oembedRes = await axios.get("https://www.tiktok.com/oembed", {
            params: { url: urlFormat },
            timeout: 5000
          });
          log.push(`  oEmbed URL "${urlFormat}" → status=${oembedRes.status}, thumbnail=${oembedRes.data?.thumbnail_url ? 'YES' : 'NO'}, title="${(oembedRes.data?.title || '').substring(0,50)}"`);
          if (oembedRes.data?.thumbnail_url) {
            log.push(`    thumbnail_url: ${oembedRes.data.thumbnail_url.substring(0, 120)}`);
          }
        } catch (oErr) {
          log.push(`  oEmbed URL "${urlFormat}" → FAILED: ${oErr.response?.status || oErr.message}`);
        }
      }
    } else {
      log.push("  No Spark Ads found to test");
    }

    log.push("\nStep 4d: Fetching all thumbnails...");
    const thumbs = await fetchTikTokThumbnails(token, ads);
    log.push(`Thumbnails found: ${Object.keys(thumbs).length}`);
    for (const [adId, url] of Object.entries(thumbs).slice(0, 5)) {
      log.push(`  Ad ${adId}: ${url.substring(0, 80)}...`);
    }

    log.push("\nStep 5: Processing data...");
    const rows = processTikTokData(ads, insights, campaignObjectiveMap, campaignStatusMap, thumbs);
    log.push(`Processed rows (with data): ${rows.length}`);
    for (const r of rows) {
      log.push(`  ${r.ad_name.substring(0,40)} | ${r.publisher_platform} | type=${r.campaign_type} | status=${r.ad_status} | spend=${r.spend} | thumb=${r.thumbnail_url ? 'yes' : 'no'} | score=${r.awareness_score??r.engagement_score??r.traffic_score}`);
    }

    res.json({ ok: true, log, adsCount: ads.length, insightsCount: insights.length, processedCount: rows.length, processed: rows });
  } catch (err) {
    log.push(`\nERROR: ${err.message}`);
    log.push(JSON.stringify(err.response?.data || {}));
    res.json({ ok: false, log, error: err.message });
  }
});

// ====== LIVE ENDPOINT (real-time from Meta, no save) ======

app.get("/live", async (req, res) => {
  try {
    const [rows, adDetails, campaignInfo, adsetBudgets] = await Promise.all([
      fetchMetaInsights(),
      fetchAdDetails(),
      fetchCampaignInfo(),
      fetchAdsetBudgets()
    ]);
    const { thumbnails: thumbnailMap, statuses: statusMap, createdTimes: createdTimeMap } = adDetails;
    const { objectives: objectiveMap, budgets: campaignBudgets } = campaignInfo;

    const cleanRows = processInsightRows(rows, {
      objectiveMap, statusMap, createdTimeMap, thumbnailMap,
      campaignBudgets, adsetBudgets
    });

    // Add new ads that have no insights yet (just launched)
    addNewAdsWithoutInsights(cleanRows, {
      statusMap, createdTimeMap, thumbnailMap, objectiveMap,
      campaignBudgets, adsetBudgets
    });

    await generateAdInsights(cleanRows);

    // Fetch TikTok data (non-blocking if not configured)
    try {
      const tikTokToken = await getTikTokToken();
      if (tikTokToken) {
        // Fetch campaigns (for objectives + statuses) and ads in parallel
        const [ttCampaignData, tikTokAds] = await Promise.all([
          fetchTikTokCampaigns(tikTokToken),
          fetchTikTokAds(tikTokToken)
        ]);
        const { campaignObjectiveMap = {}, campaignStatusMap = {} } = ttCampaignData;

        if (tikTokAds.length > 0) {
          const tikTokAdIds = tikTokAds.map(a => a.ad_id);
          const [tikTokInsights, tikTokThumbs] = await Promise.all([
            fetchTikTokInsights(tikTokToken, tikTokAdIds),
            fetchTikTokThumbnails(tikTokToken, tikTokAds)
          ]);
          const tikTokRows = processTikTokData(tikTokAds, tikTokInsights, campaignObjectiveMap, campaignStatusMap, tikTokThumbs);
          cleanRows.push(...tikTokRows);
          console.log(`TikTok: added ${tikTokRows.length} ads with spend/data`);
        }
      }
    } catch (ttErr) {
      console.warn("TikTok data fetch skipped:", ttErr.message);
    }

    console.log(`Live endpoint: returning ${cleanRows.length} ads (not saved to DB)`);
    res.json({ data: cleanRows });
  } catch (error) {
    res.status(500).json({
      error: "Live fetch failed",
      details: error.response?.data || error.message
    });
  }
});

// ====== SNAPSHOT QUERY ENDPOINT ======
// Returns the closest snapshot to N hours ago from Supabase

app.get("/snapshot/:hours", async (req, res) => {
  try {
    const hoursAgo = parseInt(req.params.hours) || 1;
    const targetTime = new Date(Date.now() - hoursAgo * 60 * 60 * 1000);

    // Get all snapshots, find the batch closest to the target time
    const { data, error } = await supabase
      .from("ad_snapshots")
      .select("*")
      .order("captured_at", { ascending: false });

    if (error) throw error;

    if (!data || data.length === 0) {
      return res.json({ data: [], target_time: targetTime.toISOString(), message: "No snapshots available" });
    }

    // Find the unique captured_at timestamp closest to our target time
    const timestamps = [...new Set(data.map(d => d.captured_at))];
    let closestTimestamp = timestamps[0];
    let closestDiff = Math.abs(new Date(timestamps[0]) - targetTime);

    for (const ts of timestamps) {
      const diff = Math.abs(new Date(ts) - targetTime);
      if (diff < closestDiff) {
        closestDiff = diff;
        closestTimestamp = ts;
      }
    }

    // Return all rows from that snapshot batch
    const snapshotData = data.filter(d => d.captured_at === closestTimestamp);

    res.json({
      data: snapshotData,
      target_time: targetTime.toISOString(),
      actual_time: closestTimestamp,
      hours_requested: hoursAgo
    });
  } catch (error) {
    res.status(500).json({
      error: "Snapshot fetch failed",
      details: error.message
    });
  }
});

// ====== SMART CAPTURE ENDPOINT ======
// Intelligent cron endpoint: captures only ads that NEED a snapshot right now.
// Logic: ads < 12 hours old → capture every hour, ads >= 12 hours old → capture every 24 hours.
// Also skips BUDGET_SPENT ads entirely (no point capturing dead ads).
// Call this from cron-job.org every 1 hour.

app.get("/smart-capture", async (req, res) => {
  try {
    const [rows, adDetails, campaignInfo, adsetBudgets] = await Promise.all([
      fetchMetaInsights(),
      fetchAdDetails(),
      fetchCampaignInfo(),
      fetchAdsetBudgets()
    ]);
    const { thumbnails: thumbnailMap, statuses: statusMap, createdTimes: createdTimeMap } = adDetails;
    const { objectives: objectiveMap, budgets: campaignBudgets } = campaignInfo;

    // Check last capture time per ad from Supabase
    const { data: recentCaptures, error: rcError } = await supabase
      .from("ad_snapshots")
      .select("ad_id, captured_at")
      .order("captured_at", { ascending: false });

    if (rcError) console.warn("Could not fetch recent captures:", rcError.message);

    const lastCaptureMap = {};
    for (const row of (recentCaptures || [])) {
      if (row.ad_id && !lastCaptureMap[row.ad_id]) {
        lastCaptureMap[row.ad_id] = new Date(row.captured_at);
      }
    }

    // Pre-calculate total spend per campaign_id (same as processInsightRows)
    const campaignSpendTotals = {};
    for (const item of rows) {
      const cid = item.campaign_id;
      if (cid) {
        campaignSpendTotals[cid] = (campaignSpendTotals[cid] || 0) + safeNumber(item.spend);
      }
    }

    const now = new Date();
    const grouped = new Map();
    let skippedCount = 0;
    let budgetSpentCount = 0;

    for (const item of rows) {
      const campaign_name = item.campaign_name || "";
      const ad_name = item.ad_name || "";
      const publisher_platform = (item.publisher_platform || "").toLowerCase();

      if (!publisher_platform || !["facebook", "instagram"].includes(publisher_platform)) continue;

      const adId = item.ad_id || null;
      const spend = safeNumber(item.spend);
      const totalCampaignSpend = campaignSpendTotals[item.campaign_id] || spend;
      const adCreatedTime = createdTimeMap[adId] || null;

      // Check Meta's effective_status — skip non-active ads
      const metaStatus = mapMetaStatus(statusMap[adId] || "UNKNOWN");
      if (metaStatus !== "ACTIVE") {
        budgetSpentCount++;
        continue;
      }

      // Skip ads with zero spend AND zero impressions — no real data yet
      const impressions = safeNumber(item.impressions);
      if (spend === 0 && impressions === 0) {
        skippedCount++;
        continue;
      }

      // Throttle: capture every hour for first 12h of data, then every 24h
      const lastCapture = lastCaptureMap[adId];
      if (lastCapture) {
        const hoursSinceLastCapture = (now - lastCapture) / (1000 * 60 * 60);
        // For smart-capture (manual trigger), use 1h interval
        if (hoursSinceLastCapture < 0.8) {
          skippedCount++;
          continue;
        }
      }

      const key = `${campaign_name}__${ad_name}__${publisher_platform}`;
      const apiObjective = objectiveMap[campaign_name] || "";
      const campaignType = objectiveToCampaignType(apiObjective) || detectCampaignTypeFallback(campaign_name, ad_name);

      let costPerClick = 0;
      for (const cpa of (item.cost_per_action_type || [])) {
        if (["link_click", "outbound_click"].includes(cpa.action_type)) {
          costPerClick = safeNumber(cpa.value);
          break;
        }
      }

      const existing = grouped.get(key) || {
        captured_at: now.toISOString(),
        snapshot_hours: Math.round(adAgeHours),
        hour_label: computeHourLabel(Math.round(adAgeHours)),
        campaign_type: campaignType,
        campaign_name,
        campaign_id: item.campaign_id || null,
        adset_name: item.adset_name || null,
        ad_name,
        ad_id: adId,
        publisher_platform,
        ad_status: metaStatus,
        ad_created_time: createdTimeMap[adId] || null,
        date_start: item.date_start || null,
        date_stop: item.date_stop || null,
        impressions: safeNumber(item.impressions),
        reach: safeNumber(item.reach),
        cpm: safeNumber(item.cpm),
        spend: spend,
        frequency: safeNumber(item.frequency),
        cost_per_click: costPerClick,
        video_3s_views: 0,
        video_3s_view_rate: 0,
        likes: 0, comments: 0, shares: 0, saves: 0, link_clicks: 0, landing_page_views: 0, ctr: 0, lpvr: 0,
        awareness_score: null, engagement_score: null, traffic_score: null,
        boost_recommendation: null,
        thumbnail_url: thumbnailMap[adId] || null
      };

      const parsed = parseActions(item.actions || []);
      existing.video_3s_views += parsed.video3sViews;
      existing.likes += parsed.likes;
      existing.comments += parsed.comments;
      existing.shares += parsed.shares;
      existing.saves += parsed.saves;
      existing.link_clicks += parsed.linkClicks;
    existing.landing_page_views += parsed.landingPageViews;
      existing.impressions = safeNumber(item.impressions);
      existing.reach = safeNumber(item.reach);
      existing.cpm = safeNumber(item.cpm);
      existing.spend = spend;
      existing.frequency = safeNumber(item.frequency);

      grouped.set(key, existing);
    }

    const cleanRows = [...grouped.values()];

    for (const row of cleanRows) {
      row.video_3s_view_rate =
        row.impressions > 0 ? round((row.video_3s_views / row.impressions) * 100) : 0;
      row.ctr =
        row.impressions > 0 ? round((row.link_clicks / row.impressions) * 100) : 0;
      row.cost_per_click =
        row.link_clicks > 0 ? round(row.spend / row.link_clicks, 4) : 0;
      row.lpvr =
        row.link_clicks > 0 ? round(row.landing_page_views / row.link_clicks, 4) : 0;
    }

    computeAbsoluteScores(cleanRows);

    // Also capture TikTok data in smart-capture
    let tikTokCaptured = 0;
    try {
      const tikTokToken = await getTikTokToken();
      if (tikTokToken) {
        const [ttCampaignData, tikTokAds] = await Promise.all([
          fetchTikTokCampaigns(tikTokToken),
          fetchTikTokAds(tikTokToken)
        ]);
        const { campaignObjectiveMap = {}, campaignStatusMap = {} } = ttCampaignData;
        if (tikTokAds.length > 0) {
          const tikTokAdIds = tikTokAds.map(a => a.ad_id);
          const [tikTokInsights, tikTokThumbs] = await Promise.all([
            fetchTikTokInsights(tikTokToken, tikTokAdIds),
            fetchTikTokThumbnails(tikTokToken, tikTokAds)
          ]);
          const tikTokRows = processTikTokData(tikTokAds, tikTokInsights, campaignObjectiveMap, campaignStatusMap, tikTokThumbs);
          cleanRows.push(...tikTokRows);
          tikTokCaptured = tikTokRows.length;
          console.log(`Smart capture: added ${tikTokRows.length} TikTok ads`);
        }
      }
    } catch (ttErr) {
      console.warn("TikTok smart-capture skipped:", ttErr.message);
    }

    await generateAdInsights(cleanRows);

    if (cleanRows.length > 0) {
      const { error } = await supabase.from("ad_snapshots").insert(cleanRows);
      if (error) throw error;
    }

    console.log(`Smart capture: ${cleanRows.length} captured (${tikTokCaptured} TikTok), ${skippedCount} skipped (too recent), ${budgetSpentCount} skipped (not active)`);
    res.json({
      ok: true,
      captured: cleanRows.length,
      tiktok_captured: tikTokCaptured,
      skipped: skippedCount,
      not_active_skipped: budgetSpentCount
    });
  } catch (error) {
    res.status(500).json({
      error: "Smart capture failed",
      details: error.response?.data || error.message
    });
  }
});

// ====== DASHBOARD DATA ENDPOINT ======

app.get("/dashboard", async (req, res) => {
  try {
    const { data, error } = await supabase
      .from("ad_snapshots")
      .select("*")
      .order("captured_at", { ascending: false });

    if (error) throw error;
    res.json({ data });
  } catch (error) {
    res.status(500).json({
      error: "Dashboard fetch failed",
      details: error.message
    });
  }
});

// ====== AD HISTORY ENDPOINT ======

app.get("/ad-history/:adId", async (req, res) => {
  try {
    const adId = req.params.adId;
    const platform = req.query.platform || null;

    let query = supabase
      .from("ad_snapshots")
      .select("*")
      .eq("ad_id", adId)
      .order("captured_at", { ascending: true });

    if (platform) {
      query = query.eq("publisher_platform", platform);
    }

    const { data, error } = await query;
    if (error) throw error;

    // Also fetch tracking info
    const { data: tracking } = await supabase
      .from("ad_tracking")
      .select("*")
      .eq("ad_id", adId)
      .single();

    res.json({
      ad_id: adId,
      snapshots: data || [],
      tracking: tracking || null,
      snapshot_count: (data || []).length
    });
  } catch (error) {
    res.status(500).json({
      error: "History fetch failed",
      details: error.message
    });
  }
});

// ====== REPORT GENERATION ENDPOINT ======

app.post("/generate-report", async (req, res) => {
  if (!ANTHROPIC_API_KEY) {
    return res.status(500).json({ error: "Anthropic API key not configured" });
  }

  try {
    const { datePreset } = req.body;
    // Fetch fresh data from Meta for the requested date range
    const [rows, campaignInfo, adDetails] = await Promise.all([
      fetchMetaInsights(datePreset || "last_30d"),
      fetchCampaignInfo(),
      fetchAdDetails()
    ]);

    const { objectives: objectiveMap, budgets: campaignBudgets, adsetBudgets } = campaignInfo;
    const { thumbnails: thumbnailMap, statuses: statusMap, createdTimes: createdTimeMap } = adDetails;

    // Process rows into clean ad data
    const grouped = new Map();
    for (const item of rows) {
      const campaign_name = item.campaign_name || "";
      const ad_name = item.ad_name || "";
      const publisher_platform = (item.publisher_platform || "").toLowerCase();
      if (!publisher_platform || !["facebook", "instagram"].includes(publisher_platform)) continue;

      const key = `${campaign_name}__${ad_name}__${publisher_platform}`;
      const apiObjective = objectiveMap[campaign_name] || "";
      const campaignType = objectiveToCampaignType(apiObjective) || detectCampaignTypeFallback(campaign_name, ad_name);
      const rawStatus = statusMap[item.ad_id] || "UNKNOWN";
      const adStatus = mapMetaStatus(rawStatus);
      const spend = safeNumber(item.spend);

      const existing = grouped.get(key) || {
        campaign_type: campaignType, campaign_name, ad_name, ad_id: item.ad_id || null,
        publisher_platform, ad_status: adStatus,
        impressions: safeNumber(item.impressions), reach: safeNumber(item.reach),
        cpm: safeNumber(item.cpm), spend, frequency: safeNumber(item.frequency),
        video_3s_views: 0, video_3s_view_rate: 0,
        likes: 0, comments: 0, shares: 0, saves: 0, link_clicks: 0, landing_page_views: 0, ctr: 0, lpvr: 0,
        cost_per_click: 0
      };

      const parsed = parseActions(item.actions || []);
      existing.video_3s_views += parsed.video3sViews;
      existing.likes += parsed.likes;
      existing.comments += parsed.comments;
      existing.shares += parsed.shares;
      existing.saves += parsed.saves;
      existing.link_clicks += parsed.linkClicks;
    existing.landing_page_views += parsed.landingPageViews;
      existing.impressions = safeNumber(item.impressions);
      existing.reach = safeNumber(item.reach);
      existing.cpm = safeNumber(item.cpm);
      existing.spend = spend;
      grouped.set(key, existing);
    }

    const cleanRows = [...grouped.values()];
    for (const row of cleanRows) {
      row.video_3s_view_rate = row.impressions > 0 ? round((row.video_3s_views / row.impressions) * 100) : 0;
      row.ctr = row.impressions > 0 ? round((row.link_clicks / row.impressions) * 100) : 0;
      row.cost_per_click = row.link_clicks > 0 ? round(row.spend / row.link_clicks, 4) : 0;
      row.lpvr = row.link_clicks > 0 ? round(row.landing_page_views / row.link_clicks, 4) : 0;
    }
    computeAbsoluteScores(cleanRows);

    // Separate by campaign type and status
    const activeAds = cleanRows.filter(r => r.ad_status === 'ACTIVE');
    const completedAds = cleanRows.filter(r => r.ad_status !== 'ACTIVE');
    const awareness = cleanRows.filter(r => r.campaign_type === 'awareness');
    const engagement = cleanRows.filter(r => r.campaign_type === 'engagement');
    const traffic = cleanRows.filter(r => r.campaign_type === 'traffic');

    // Build data summary for Claude
    const totalSpend = cleanRows.reduce((s, d) => s + (d.spend || 0), 0);
    const totalReach = cleanRows.reduce((s, d) => s + (d.reach || 0), 0);
    const totalClicks = cleanRows.reduce((s, d) => s + (d.link_clicks || 0), 0);
    const totalImpressions = cleanRows.reduce((s, d) => s + (d.impressions || 0), 0);
    const avgCTR = totalImpressions > 0 ? (totalClicks / totalImpressions * 100).toFixed(2) : 0;
    const avgCPM = cleanRows.length > 0 ? (cleanRows.reduce((s, d) => s + (d.cpm || 0), 0) / cleanRows.length).toFixed(3) : 0;

    const dateLabel = {
      'last_7d': 'Last 7 Days', 'last_14d': 'Last 14 Days', 'last_30d': 'Last 30 Days',
      'this_month': 'This Month', 'last_month': 'Last Month', 'maximum': 'All Time'
    }[datePreset] || datePreset;

    // Build per-ad details
    const adDetails_str = cleanRows.map(ad => {
      const score = ad.awareness_score ?? ad.engagement_score ?? ad.traffic_score ?? 0;
      return `- "${ad.ad_name}" (${ad.publisher_platform}, ${ad.campaign_type}, ${ad.ad_status}): Score=${score}, Reach=${ad.reach}, Impressions=${ad.impressions}, Spend=\u20ac${ad.spend.toFixed(2)}, CPM=\u20ac${ad.cpm.toFixed(3)}, CTR=${ad.ctr}%, Clicks=${ad.link_clicks}, Shares=${ad.shares}, Saves=${ad.saves}, Comments=${ad.comments}, Likes=${ad.likes}, CPC=\u20ac${ad.cost_per_click}, Frequency=${ad.frequency}`;
    }).join('\n');

    const prompt = `You are an expert social media advertising analyst for FibreGuard (stain-resistant textile brand). Generate a comprehensive performance report.

DATE RANGE: ${dateLabel}

OVERVIEW:
- Total Ads: ${cleanRows.length} (${activeAds.length} active, ${completedAds.length} completed)
- Total Spend: $${totalSpend.toFixed(2)}
- Total Reach: ${totalReach.toLocaleString()}
- Total Impressions: ${totalImpressions.toLocaleString()}
- Total Link Clicks: ${totalClicks.toLocaleString()}
- Average CTR: ${avgCTR}%
- Average CPM: $${avgCPM}

CAMPAIGN BREAKDOWN:
- Awareness: ${awareness.length} ads, Spend: $${awareness.reduce((s,d)=>s+d.spend,0).toFixed(2)}, Reach: ${awareness.reduce((s,d)=>s+d.reach,0).toLocaleString()}
- Engagement: ${engagement.length} ads, Spend: $${engagement.reduce((s,d)=>s+d.spend,0).toFixed(2)}, Shares: ${engagement.reduce((s,d)=>s+d.shares,0)}, Saves: ${engagement.reduce((s,d)=>s+d.saves,0)}
- Traffic: ${traffic.length} ads, Spend: $${traffic.reduce((s,d)=>s+d.spend,0).toFixed(2)}, Clicks: ${traffic.reduce((s,d)=>s+d.link_clicks,0)}

PER-AD DATA:
${adDetails_str}

Generate the report as a JSON object with this structure:
{
  "title": "FibreGuard Ad Performance Report",
  "dateRange": "${dateLabel}",
  "executiveSummary": "2-3 sentence overview of overall performance",
  "highlights": ["highlight 1", "highlight 2", "highlight 3"],
  "concerns": ["concern 1", "concern 2"],
  "sections": [
    {
      "title": "Section title",
      "content": "Analysis paragraph (2-4 sentences)",
      "metrics": [{"label": "Metric Name", "value": "formatted value", "trend": "up|down|neutral"}]
    }
  ],
  "topPerformers": [{"name": "ad name", "platform": "instagram|facebook", "reason": "why it's top"}],
  "recommendations": [{"priority": "high|medium|low", "action": "what to do", "reason": "why"}],
  "platformComparison": {
    "summary": "1-2 sentence comparison",
    "instagram": {"strength": "...", "weakness": "..."},
    "facebook": {"strength": "...", "weakness": "..."}
  }
}

Rules:
- Be specific with numbers and percentages
- Keep sections focused and actionable
- Include 3-5 sections covering: Overall Performance, Awareness Analysis, Engagement Analysis, Traffic Analysis, Platform Comparison
- Recommendations should be concrete and actionable
- Reference specific ads by name when relevant`;

    const response = await axios.post('https://api.anthropic.com/v1/messages', {
      model: 'claude-haiku-4-5-20251001',
      max_tokens: 4096,
      messages: [{ role: 'user', content: prompt }]
    }, {
      headers: {
        'x-api-key': ANTHROPIC_API_KEY,
        'anthropic-version': '2023-06-01',
        'content-type': 'application/json'
      }
    });

    const text = response.data.content[0].text;
    const jsonMatch = text.match(/\{[\s\S]*\}/);
    if (jsonMatch) {
      const report = JSON.parse(jsonMatch[0]);
      // Attach raw chart data for the frontend
      report.chartData = {
        spendByType: [
          { label: 'Awareness', value: round(awareness.reduce((s,d)=>s+d.spend,0)) },
          { label: 'Engagement', value: round(engagement.reduce((s,d)=>s+d.spend,0)) },
          { label: 'Traffic', value: round(traffic.reduce((s,d)=>s+d.spend,0)) }
        ],
        reachByPlatform: [
          { label: 'Instagram', value: cleanRows.filter(r=>r.publisher_platform==='instagram').reduce((s,d)=>s+d.reach,0) },
          { label: 'Facebook', value: cleanRows.filter(r=>r.publisher_platform==='facebook').reduce((s,d)=>s+d.reach,0) },
          { label: 'TikTok', value: cleanRows.filter(r=>r.publisher_platform==='tiktok').reduce((s,d)=>s+d.reach,0) }
        ],
        topAdsScores: cleanRows
          .map(r => ({ name: r.ad_name.substring(0,30), score: r.awareness_score ?? r.engagement_score ?? r.traffic_score ?? 0, type: r.campaign_type, platform: r.publisher_platform }))
          .sort((a,b) => b.score - a.score)
          .slice(0, 8),
        overview: { totalSpend: round(totalSpend), totalReach, totalClicks, totalImpressions, avgCTR: parseFloat(avgCTR), avgCPM: parseFloat(avgCPM), activeAds: activeAds.length, completedAds: completedAds.length }
      };
      res.json(report);
    } else {
      res.status(500).json({ error: "Could not parse report from AI response" });
    }
  } catch (error) {
    console.error("Report generation failed:", error.message);
    res.status(500).json({ error: "Report generation failed", details: error.message });
  }
});

// ====== HEALTH / KEEP-ALIVE ENDPOINT ======
// Lightweight endpoint for waking the server from cold start.
// Other devices hit this first to ensure the server is ready before fetching /live.
app.get("/health", (req, res) => {
  res.json({ ok: true, uptime: process.uptime(), ts: new Date().toISOString() });
});

// Migration check: test required columns exist
app.get("/migrate", async (req, res) => {
  try {
    const checks = {};

    // Check hour_label on ad_snapshots
    const { data: snapTest, error: snapErr } = await supabase.from("ad_snapshots").select("hour_label").limit(1);
    if (snapErr && (snapErr.message || "").includes("hour_label")) {
      checks.hour_label = { ok: false, action: "ALTER TABLE ad_snapshots ADD COLUMN hour_label TEXT;" };
    } else {
      checks.hour_label = { ok: true };
    }

    // Check first_data_at on ad_tracking
    const { data: trackTest, error: trackErr } = await supabase.from("ad_tracking").select("first_data_at").limit(1);
    if (trackErr && (trackErr.message || "").includes("first_data_at")) {
      checks.first_data_at = { ok: false, action: "ALTER TABLE ad_tracking ADD COLUMN first_data_at TIMESTAMPTZ;" };
    } else {
      checks.first_data_at = { ok: true };
    }

    const allOk = Object.values(checks).every(c => c.ok);
    const actions = Object.entries(checks)
      .filter(([, c]) => !c.ok)
      .map(([name, c]) => `${name}: ${c.action}`);

    res.json({
      ok: allOk,
      checks,
      ...(actions.length > 0 ? { action_needed: actions.join("\n") } : { message: "All columns exist and are working!" })
    });
  } catch (err) {
    res.json({ ok: false, error: err.message });
  }
});

// ====== META DEBUG ENDPOINT ======
// Diagnostic endpoint to troubleshoot Meta API data issues
app.get("/debug/meta", async (req, res) => {
  const debug = { timestamp: new Date().toISOString(), steps: [] };
  try {
    // Step 1: Fetch all ad details
    const adDetails = await fetchAdDetails();
    const { statuses: statusMap, createdTimes: createdTimeMap } = adDetails;
    const allAdIds = Object.keys(statusMap);
    const activeAds = Object.entries(statusMap).filter(([, s]) => s === "ACTIVE");
    const completedAds = Object.entries(statusMap).filter(([, s]) => s === "COMPLETED");

    debug.steps.push({
      step: "ad_details",
      total_ads: allAdIds.length,
      active: activeAds.length,
      completed: completedAds.length,
      status_breakdown: Object.entries(statusMap).reduce((acc, [, s]) => { acc[s] = (acc[s] || 0) + 1; return acc; }, {})
    });

    // Step 2: Fetch insights (now with pagination)
    const campaignInfo = await fetchCampaignInfo();
    const { objectives: objectiveMap } = campaignInfo;
    const rows = await fetchMetaInsights();

    // Breakdown by platform
    const platformCounts = {};
    const platformSpend = {};
    for (const r of rows) {
      const p = (r.publisher_platform || "unknown").toLowerCase();
      platformCounts[p] = (platformCounts[p] || 0) + 1;
      platformSpend[p] = (platformSpend[p] || 0) + safeNumber(r.spend);
    }

    // Unique ad_ids in insights
    const insightAdIds = [...new Set(rows.map(r => r.ad_id))];

    debug.steps.push({
      step: "insights",
      total_rows: rows.length,
      unique_ads: insightAdIds.length,
      platform_row_counts: platformCounts,
      platform_spend: platformSpend
    });

    // Step 3: Campaign type breakdown
    const campaignTypes = {};
    for (const r of rows) {
      const p = (r.publisher_platform || "").toLowerCase();
      if (!["facebook", "instagram"].includes(p)) continue;
      const objective = objectiveMap[r.campaign_name] || "";
      const type = objectiveToCampaignType(objective) || detectCampaignTypeFallback(r.campaign_name, r.ad_name);
      const key = `${type}_${p}`;
      if (!campaignTypes[key]) campaignTypes[key] = { count: 0, spend: 0, ads: [] };
      campaignTypes[key].count++;
      campaignTypes[key].spend += safeNumber(r.spend);
      if (campaignTypes[key].ads.length < 5) {
        campaignTypes[key].ads.push({
          ad_name: (r.ad_name || "").substring(0, 60),
          campaign_name: r.campaign_name,
          objective: objective,
          spend: r.spend,
          platform: p
        });
      }
    }

    debug.steps.push({ step: "campaign_types", breakdown: campaignTypes });

    // Step 4: Show active ads with their campaign objectives
    const activeAdDetails = activeAds.slice(0, 20).map(([adId, status]) => {
      const insightRow = rows.find(r => r.ad_id === adId);
      return {
        ad_id: adId,
        status,
        ad_name: insightRow?.ad_name || "no insights",
        campaign_name: insightRow?.campaign_name || "unknown",
        objective: objectiveMap[insightRow?.campaign_name] || "unknown",
        platform: insightRow?.publisher_platform || "unknown",
        spend: insightRow?.spend || "0"
      };
    });

    debug.steps.push({ step: "active_ad_details", ads: activeAdDetails });

    // Step 5: Check Supabase for Instagram vs Facebook snapshots
    const { data: igSnaps, error: igErr } = await supabase
      .from("ad_snapshots")
      .select("ad_id, ad_name, campaign_name, campaign_type, publisher_platform, spend, impressions, ad_status, captured_at")
      .eq("publisher_platform", "instagram")
      .order("captured_at", { ascending: false })
      .limit(10);

    const { data: fbSnaps, error: fbErr } = await supabase
      .from("ad_snapshots")
      .select("ad_id, ad_name, campaign_name, campaign_type, publisher_platform, spend, impressions, ad_status, captured_at")
      .eq("publisher_platform", "facebook")
      .order("captured_at", { ascending: false })
      .limit(10);

    debug.steps.push({
      step: "supabase_snapshots",
      instagram: { count: (igSnaps || []).length, error: igErr?.message, recent: (igSnaps || []).slice(0, 5) },
      facebook: { count: (fbSnaps || []).length, error: fbErr?.message, recent: (fbSnaps || []).slice(0, 5) }
    });

    res.json(debug);
  } catch (err) {
    debug.steps.push({ step: "fatal_error", message: err.message });
    res.json(debug);
  }
});

// ====== TIKTOK DEBUG ENDPOINT ======
// Diagnostic endpoint to troubleshoot TikTok API data issues
app.get("/debug/tiktok", async (req, res) => {
  const debug = { timestamp: new Date().toISOString(), steps: [] };
  try {
    // Step 1: Check token
    const token = await getTikTokToken();
    if (!token) {
      debug.steps.push({ step: "token", status: "FAIL", message: "No TikTok token found in Supabase" });
      return res.json(debug);
    }
    debug.steps.push({
      step: "token", status: "OK",
      advertiser_id: token.advertiser_id,
      token_preview: token.access_token ? token.access_token.substring(0, 20) + "..." : "missing"
    });

    // Step 2: Fetch campaigns
    const campaignData = await fetchTikTokCampaigns(token);
    const { campaignObjectiveMap = {}, campaignStatusMap = {} } = campaignData;
    const campaignList = Object.entries(campaignObjectiveMap).map(([id, obj]) => ({
      campaign_id: id, objective: obj, status: campaignStatusMap[id] || "unknown"
    }));
    debug.steps.push({ step: "campaigns", status: "OK", count: campaignList.length, campaigns: campaignList });

    // Step 3: Fetch ads
    const ads = await fetchTikTokAds(token);
    const adSummary = ads.slice(0, 30).map(a => ({
      ad_id: a.ad_id, ad_name: a.ad_name || a.ad_text || "",
      campaign_name: a.campaign_name || "", campaign_id: a.campaign_id || "",
      primary_status: a._primary_status || a.primary_status || "",
      secondary_status: a.secondary_status || "",
      operation_status: a.operation_status || ""
    }));
    debug.steps.push({ step: "ads", status: "OK", total: ads.length, sample: adSummary });

    // Step 4: Search for the specific missing ads
    const searchTerms = ["juice", "sofa", "spring", "morning", "chenelle", "performance"];
    const matchingAds = ads.filter(a => {
      const text = `${a.ad_name || ""} ${a.ad_text || ""} ${a.campaign_name || ""}`.toLowerCase();
      return searchTerms.some(t => text.includes(t));
    }).map(a => ({
      ad_id: a.ad_id, ad_name: a.ad_name || a.ad_text || "",
      campaign_name: a.campaign_name || "", campaign_id: a.campaign_id || "",
      primary_status: a._primary_status || a.primary_status || "",
      secondary_status: a.secondary_status || "",
      create_time: a.create_time || ""
    }));
    debug.steps.push({ step: "search_specific_ads", found: matchingAds.length, ads: matchingAds });

    // Step 5: Fetch insights and check for non-zero spend
    const adIds = ads.map(a => a.ad_id);
    const endDate = new Date().toISOString().split("T")[0];
    const startDate = new Date(Date.now() - 90 * 24 * 60 * 60 * 1000).toISOString().split("T")[0];

    // Try basic metrics directly
    let insightRows = [];
    try {
      const insightRes = await axios.get("https://business-api.tiktok.com/open_api/v1.3/report/integrated/get/", {
        params: {
          advertiser_id: token.advertiser_id, report_type: "BASIC",
          data_level: "AUCTION_AD", dimensions: JSON.stringify(["ad_id"]),
          metrics: JSON.stringify(["spend", "impressions", "clicks"]),
          start_date: startDate, end_date: endDate,
          page_size: 200, page: 1
        },
        headers: { "Access-Token": token.access_token }
      });
      insightRows = res.data?.data?.list || [];
      const code = insightRes.data?.code;
      const total = insightRes.data?.data?.page_info?.total_number;
      insightRows = insightRes.data?.data?.list || [];

      // Find rows with non-zero spend
      const withSpend = insightRows.filter(r => safeNumber(r.metrics?.spend) > 0);
      const withImpressions = insightRows.filter(r => safeNumber(r.metrics?.impressions) > 0);

      debug.steps.push({
        step: "insights_basic", status: code === 0 ? "OK" : "ERROR",
        api_code: code, api_message: insightRes.data?.message,
        date_range: `${startDate} to ${endDate}`,
        total_rows: total, returned_rows: insightRows.length,
        rows_with_spend: withSpend.length,
        rows_with_impressions: withImpressions.length,
        spend_samples: withSpend.slice(0, 10).map(r => ({
          ad_id: r.dimensions?.ad_id, spend: r.metrics?.spend,
          impressions: r.metrics?.impressions, clicks: r.metrics?.clicks
        }))
      });
    } catch (insightErr) {
      debug.steps.push({
        step: "insights_basic", status: "ERROR",
        message: insightErr.message,
        response_data: insightErr.response?.data
      });
    }

    // Step 6: Try extended metrics
    try {
      const extRes = await axios.get("https://business-api.tiktok.com/open_api/v1.3/report/integrated/get/", {
        params: {
          advertiser_id: token.advertiser_id, report_type: "BASIC",
          data_level: "AUCTION_AD", dimensions: JSON.stringify(["ad_id"]),
          metrics: JSON.stringify(["spend", "impressions", "clicks", "reach", "likes", "shares", "comments", "video_watched_2s", "video_play_actions"]),
          start_date: startDate, end_date: endDate,
          page_size: 200, page: 1
        },
        headers: { "Access-Token": token.access_token }
      });
      debug.steps.push({
        step: "insights_extended", status: extRes.data?.code === 0 ? "OK" : "ERROR",
        api_code: extRes.data?.code, api_message: extRes.data?.message,
        returned_rows: (extRes.data?.data?.list || []).length
      });
    } catch (extErr) {
      debug.steps.push({
        step: "insights_extended", status: "ERROR",
        message: extErr.message,
        response_data: extErr.response?.data
      });
    }

    // Step 7: Check Supabase for any existing TikTok snapshots
    const { data: ttSnapshots, error: snapErr } = await supabase
      .from("ad_snapshots")
      .select("ad_id, ad_name, campaign_name, campaign_type, spend, impressions, ad_status, captured_at, publisher_platform")
      .eq("publisher_platform", "tiktok")
      .order("captured_at", { ascending: false })
      .limit(20);
    debug.steps.push({
      step: "supabase_tiktok_snapshots", status: snapErr ? "ERROR" : "OK",
      error: snapErr?.message,
      count: (ttSnapshots || []).length,
      snapshots: (ttSnapshots || []).slice(0, 20)
    });

    // Step 8: Check tiktok_tokens table
    const { data: tokenData, error: tokenErr } = await supabase
      .from("tiktok_tokens")
      .select("*")
      .limit(5);
    debug.steps.push({
      step: "tiktok_tokens_table", status: tokenErr ? "ERROR" : "OK",
      count: (tokenData || []).length,
      tokens: (tokenData || []).map(t => ({
        advertiser_id: t.advertiser_id,
        created_at: t.created_at,
        updated_at: t.updated_at,
        expires_at: t.expires_at,
        token_preview: t.access_token ? t.access_token.substring(0, 20) + "..." : "missing"
      }))
    });

    res.json(debug);
  } catch (err) {
    debug.steps.push({ step: "fatal_error", message: err.message, stack: err.stack?.split("\n").slice(0, 3) });
    res.json(debug);
  }
});

app.listen(PORT, () => {
  console.log(`Server running on http://localhost:${PORT}`);
});
