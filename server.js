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
  res.sendFile(path.join(__dirname, "dashboard.html"));
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

// ====== UTILITY FUNCTIONS ======

function safeNumber(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : 0;
}

function round(value, decimals = 2) {
  return Number(safeNumber(value).toFixed(decimals));
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
// Returns { objectives: { campaign_name -> objective }, budgets: { campaign_id -> { daily_budget, lifetime_budget } } }
async function fetchCampaignInfo() {
  const objectiveMap = {};  // campaign_name -> objective string
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
        campaignBudgets[campaign.id] = {
          daily_budget: safeNumber(campaign.daily_budget) / 100, // Meta returns cents
          lifetime_budget: safeNumber(campaign.lifetime_budget) / 100
        };
        console.log(`Campaign: "${campaign.name}" → objective: ${campaign.objective}, status: ${campaign.status}, lifetime_budget: ${campaign.lifetime_budget}, daily_budget: ${campaign.daily_budget}`);
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
  return { objectives: objectiveMap, budgets: campaignBudgets };
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

  const { data } = await axios.get(url, {
    params: {
      access_token: META_ACCESS_TOKEN,
      fields: "campaign_name,campaign_id,adset_name,ad_name,ad_id,impressions,reach,cpm,spend,frequency,actions,cost_per_action_type,date_start,date_stop",
      breakdowns: "publisher_platform",
      action_breakdowns: "action_type",
      level: "ad",
      date_preset: datePreset
    }
  });

  return data.data || [];
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

async function fetchTikTokAds(token) {
  if (!token) return [];
  try {
    const res = await axios.get("https://business-api.tiktok.com/open_api/v1.3/ad/get/", {
      params: {
        advertiser_id: token.advertiser_id,
        page_size: 200,
        filtering: JSON.stringify({ status: "AD_STATUS_DELIVERY_OK" })
      },
      headers: { "Access-Token": token.access_token }
    });
    return res.data?.data?.list || [];
  } catch (err) {
    console.warn("TikTok ad fetch failed:", err.message);
    return [];
  }
}

async function fetchTikTokInsights(token, adIds) {
  if (!token || adIds.length === 0) return [];
  try {
    // Get today's date and 90 days ago for reporting
    const endDate = new Date().toISOString().split("T")[0];
    const startDate = new Date(Date.now() - 90 * 24 * 60 * 60 * 1000).toISOString().split("T")[0];

    const res = await axios.get("https://business-api.tiktok.com/open_api/v1.3/report/integrated/get/", {
      params: {
        advertiser_id: token.advertiser_id,
        report_type: "BASIC",
        data_level: "AUCTION_AD",
        dimensions: JSON.stringify(["ad_id"]),
        metrics: JSON.stringify([
          "spend", "impressions", "reach", "cpm", "cpc", "ctr",
          "clicks", "video_play_actions", "video_watched_6s",
          "likes", "comments", "shares", "follows", "frequency",
          "campaign_name", "adgroup_name", "ad_name"
        ]),
        start_date: startDate,
        end_date: endDate,
        filtering: JSON.stringify({ ad_ids: adIds }),
        page_size: 200
      },
      headers: { "Access-Token": token.access_token }
    });
    return res.data?.data?.list || [];
  } catch (err) {
    console.warn("TikTok insights fetch failed:", err.message);
    return [];
  }
}

function detectTikTokCampaignType(campaignName = "", objective = "") {
  const text = `${campaignName} ${objective}`.toLowerCase();
  if (text.includes("awareness") || text.includes("reach") || text.includes("video_views") || text.includes("entertaining")) return "awareness";
  if (text.includes("traffic") || text.includes("clicks") || text.includes("website_visits")) return "traffic";
  if (text.includes("engagement") || text.includes("community") || text.includes("education") || text.includes("educational")) return "engagement";
  return "awareness"; // default
}

function processTikTokData(ads, insights) {
  // Build ad info map
  const adInfoMap = {};
  for (const ad of ads) {
    adInfoMap[ad.ad_id] = {
      ad_name: ad.ad_name || ad.ad_text || "",
      campaign_name: ad.campaign_name || "",
      campaign_id: ad.campaign_id || null,
      adgroup_name: ad.adgroup_name || "",
      objective: ad.objective_type || ad.objective || "",
      created_time: ad.create_time || null,
      status: "ACTIVE"
    };
  }

  const rows = [];
  for (const item of insights) {
    const metrics = item.metrics || {};
    const dimensions = item.dimensions || {};
    const adId = dimensions.ad_id || null;
    const adInfo = adInfoMap[adId] || {};

    const campaignType = detectTikTokCampaignType(adInfo.campaign_name || metrics.campaign_name, adInfo.objective);
    const spend = safeNumber(metrics.spend);
    const impressions = safeNumber(metrics.impressions);
    const reach = safeNumber(metrics.reach);
    const clicks = safeNumber(metrics.clicks);
    const videoViews = safeNumber(metrics.video_play_actions);
    const video6s = safeNumber(metrics.video_watched_6s);
    const likes = safeNumber(metrics.likes);
    const comments = safeNumber(metrics.comments);
    const shares = safeNumber(metrics.shares);
    const frequency = safeNumber(metrics.frequency);

    const cpm = safeNumber(metrics.cpm);
    const cpc = safeNumber(metrics.cpc);
    const ctr = safeNumber(metrics.ctr);
    const viewRate = impressions > 0 ? round((video6s / impressions) * 100) : 0;

    rows.push({
      captured_at: new Date().toISOString(),
      campaign_type: campaignType,
      campaign_name: adInfo.campaign_name || metrics.campaign_name || "",
      campaign_id: adInfo.campaign_id || null,
      adset_name: adInfo.adgroup_name || metrics.adgroup_name || "",
      ad_name: adInfo.ad_name || metrics.ad_name || "",
      ad_id: adId,
      publisher_platform: "tiktok",
      ad_status: "ACTIVE",
      ad_created_time: adInfo.created_time || null,
      date_start: null,
      date_stop: null,
      impressions,
      reach,
      cpm,
      spend,
      frequency,
      cost_per_click: cpc,
      landing_page_views: 0,
      lpvr: 0,
      video_3s_views: videoViews,
      video_3s_view_rate: viewRate,
      likes,
      comments,
      shares,
      saves: 0, // TikTok API doesn't expose saves
      link_clicks: clicks,
      ctr,
      awareness_score: null,
      engagement_score: null,
      traffic_score: null,
      boost_recommendation: null,
      thumbnail_url: null
    });
  }

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

    const existing = grouped.get(key) || {
      captured_at: new Date().toISOString(),
      ...(snapshotHours !== undefined ? { snapshot_hours: snapshotHours } : {}),
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

    // Fetch TikTok data in parallel (non-blocking if not configured)
    try {
      const tikTokToken = await getTikTokToken();
      if (tikTokToken) {
        const tikTokAds = await fetchTikTokAds(tikTokToken);
        if (tikTokAds.length > 0) {
          const tikTokAdIds = tikTokAds.map(a => a.ad_id);
          const tikTokInsights = await fetchTikTokInsights(tikTokToken, tikTokAdIds);
          const tikTokRows = processTikTokData(tikTokAds, tikTokInsights);
          cleanRows.push(...tikTokRows);
          console.log(`TikTok: added ${tikTokRows.length} ads`);
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

      // Determine ad age from created_time
      const adCreatedDate = adCreatedTime ? new Date(adCreatedTime) : null;
      const adAgeHours = adCreatedDate ? (now - adCreatedDate) / (1000 * 60 * 60) : Infinity;
      const captureIntervalHours = adAgeHours < 12 ? 1 : 24;

      const lastCapture = lastCaptureMap[adId];
      if (lastCapture) {
        const hoursSinceLastCapture = (now - lastCapture) / (1000 * 60 * 60);
        if (hoursSinceLastCapture < captureIntervalHours * 0.8) {
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
    await generateAdInsights(cleanRows);

    if (cleanRows.length > 0) {
      const { error } = await supabase.from("ad_snapshots").insert(cleanRows);
      if (error) throw error;
    }

    console.log(`Smart capture: ${cleanRows.length} captured, ${skippedCount} skipped (too recent), ${budgetSpentCount} skipped (not active)`);
    res.json({
      ok: true,
      captured: cleanRows.length,
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

app.listen(PORT, () => {
  console.log(`Server running on http://localhost:${PORT}`);
});
