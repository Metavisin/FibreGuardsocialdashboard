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

// Serve the dashboard at root URL
app.get("/", (req, res) => {
  res.sendFile(path.join(__dirname, "dashboard.html"));
});

const META_ACCESS_TOKEN = process.env.META_ACCESS_TOKEN;
const META_AD_ACCOUNT_ID = process.env.META_AD_ACCOUNT_ID;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY;
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
  let likes = 0, comments = 0, shares = 0, saves = 0, video3sViews = 0, linkClicks = 0;

  for (const action of actions) {
    const type = action.action_type;
    const value = safeNumber(action.value);
    if (type === "video_view") video3sViews += value;
    if (["like", "post_reaction", "onsite_post_reaction"].includes(type)) likes += value;
    if (["comment", "post_comment"].includes(type)) comments += value;
    if (["share", "post_share"].includes(type)) shares += value;
    if (["save", "post_save"].includes(type)) saves += value;
    if (["link_click", "outbound_click"].includes(type)) linkClicks += value;
  }

  return { likes, comments, shares, saves, video3sViews, linkClicks };
}

// ====== NORMALIZED 0-100 SCORING ======
// Uses min-max normalization within the group, then applies weights.
// For "higher is better" metrics: (value - min) / (max - min) * 100
// For "lower is better" metrics (CPM): (max - value) / (max - min) * 100
// If all values are the same, score is 50 (neutral).

function normalizeHigher(value, min, max) {
  if (max === min) return 50;
  return ((value - min) / (max - min)) * 100;
}

function normalizeLower(value, min, max) {
  if (max === min) return 50;
  return ((max - value) / (max - min)) * 100;
}

function computeNormalizedScores(rows) {
  // Separate by campaign type
  const awareness = rows.filter(r => r.campaign_type === "awareness");
  const engagement = rows.filter(r => r.campaign_type === "engagement");

  // Score awareness ads
  if (awareness.length > 0) {
    const reachVals = awareness.map(r => r.reach);
    const cpmVals = awareness.map(r => r.cpm);
    const vrVals = awareness.map(r => r.video_3s_view_rate);

    const reachMin = Math.min(...reachVals), reachMax = Math.max(...reachVals);
    const cpmMin = Math.min(...cpmVals), cpmMax = Math.max(...cpmVals);
    const vrMin = Math.min(...vrVals), vrMax = Math.max(...vrVals);

    for (const row of awareness) {
      const reachScore = normalizeHigher(row.reach, reachMin, reachMax);
      const cpmScore = normalizeLower(row.cpm, cpmMin, cpmMax);
      const vrScore = normalizeHigher(row.video_3s_view_rate, vrMin, vrMax);

      row.awareness_score = round(reachScore * 0.4 + cpmScore * 0.4 + vrScore * 0.2);
      row.engagement_score = null;
    }
  }

  // Score engagement ads
  if (engagement.length > 0) {
    const shareVals = engagement.map(r => r.shares);
    const saveVals = engagement.map(r => r.saves);
    const commentVals = engagement.map(r => r.comments);
    const likeVals = engagement.map(r => r.likes);

    const shareMin = Math.min(...shareVals), shareMax = Math.max(...shareVals);
    const saveMin = Math.min(...saveVals), saveMax = Math.max(...saveVals);
    const commentMin = Math.min(...commentVals), commentMax = Math.max(...commentVals);
    const likeMin = Math.min(...likeVals), likeMax = Math.max(...likeVals);

    for (const row of engagement) {
      const shareScore = normalizeHigher(row.shares, shareMin, shareMax);
      const saveScore = normalizeHigher(row.saves, saveMin, saveMax);
      const commentScore = normalizeHigher(row.comments, commentMin, commentMax);
      const likeScore = normalizeHigher(row.likes, likeMin, likeMax);

      row.engagement_score = round(shareScore * 0.4 + saveScore * 0.3 + commentScore * 0.2 + likeScore * 0.1);
      row.awareness_score = null;
      row.traffic_score = null;
    }
  }

  // Score traffic ads: CTR 40% + Link Clicks 30% + CPC (inverse CPM as proxy) 20% + Reach 10%
  const traffic = rows.filter(r => r.campaign_type === "traffic");
  if (traffic.length > 0) {
    const ctrVals = traffic.map(r => r.ctr);
    const clickVals = traffic.map(r => r.link_clicks);
    const cpmVals = traffic.map(r => r.cpm);
    const reachVals = traffic.map(r => r.reach);

    const ctrMin = Math.min(...ctrVals), ctrMax = Math.max(...ctrVals);
    const clickMin = Math.min(...clickVals), clickMax = Math.max(...clickVals);
    const cpmMin = Math.min(...cpmVals), cpmMax = Math.max(...cpmVals);
    const reachMin = Math.min(...reachVals), reachMax = Math.max(...reachVals);

    for (const row of traffic) {
      const ctrScore = normalizeHigher(row.ctr, ctrMin, ctrMax);
      const clickScore = normalizeHigher(row.link_clicks, clickMin, clickMax);
      const cpmScore = normalizeLower(row.cpm, cpmMin, cpmMax); // lower CPM = better
      const reachScore = normalizeHigher(row.reach, reachMin, reachMax);

      row.traffic_score = round(ctrScore * 0.4 + clickScore * 0.3 + cpmScore * 0.2 + reachScore * 0.1);
      row.awareness_score = null;
      row.engagement_score = null;
    }
  }

  // Boost recommendations based on normalized score (out of 100)
  for (const row of rows) {
    const score = row.awareness_score ?? row.engagement_score ?? row.traffic_score ?? 0;
    if (score >= 70) row.boost_recommendation = "boost";
    else if (score >= 40) row.boost_recommendation = "monitor";
    else row.boost_recommendation = "no boost";
  }
}

// ====== META API FUNCTIONS ======

// Fetch campaign objectives from Meta — returns map of campaign_name -> objective
// Fetches ALL campaigns (not just active) so older insights data can be matched too
async function fetchCampaignObjectives() {
  const objectiveMap = {}; // campaign_name -> objective string
  try {
    let url = `https://graph.facebook.com/v25.0/act_${META_AD_ACCOUNT_ID}/campaigns`;
    let hasMore = true;

    while (hasMore) {
      const { data } = await axios.get(url, {
        params: {
          access_token: META_ACCESS_TOKEN,
          fields: "name,objective,status",
          limit: 200
        }
      });

      for (const campaign of (data.data || [])) {
        objectiveMap[campaign.name] = campaign.objective || "";
        console.log(`Campaign: "${campaign.name}" → objective: ${campaign.objective}, status: ${campaign.status}`);
      }

      // Handle pagination — Meta returns campaigns in pages
      if (data.paging?.next) {
        url = data.paging.next;
      } else {
        hasMore = false;
      }
    }

    console.log(`Loaded objectives for ${Object.keys(objectiveMap).length} campaigns`);
  } catch (err) {
    console.warn("Failed to fetch campaign objectives (non-blocking):", err.message);
  }
  return objectiveMap;
}

async function fetchMetaInsights(datePreset = "last_7d") {
  const url = `https://graph.facebook.com/v25.0/act_${META_AD_ACCOUNT_ID}/insights`;

  const { data } = await axios.get(url, {
    params: {
      access_token: META_ACCESS_TOKEN,
      fields: "campaign_name,ad_name,ad_id,impressions,reach,cpm,actions",
      breakdowns: "publisher_platform",
      action_breakdowns: "action_type",
      level: "ad",
      date_preset: datePreset
    }
  });

  return data.data || [];
}

// Fetch ad creative thumbnails from Meta — keyed by ad_id for reliable matching
// Uses two passes: first get ad->creative mapping, then fetch hi-res thumbnails
async function fetchAdThumbnails() {
  const map = {}; // ad_id -> thumbnail_url
  try {
    // Step 1: Get ads with their creative IDs
    const adsUrl = `https://graph.facebook.com/v25.0/act_${META_AD_ACCOUNT_ID}/ads`;
    const { data: adsData } = await axios.get(adsUrl, {
      params: {
        access_token: META_ACCESS_TOKEN,
        fields: "id,name,creative{id}",
        limit: 100
      }
    });

    const ads = adsData.data || [];
    console.log(`Found ${ads.length} ads from Meta`);

    // Build creative_id -> [ad_ids] mapping
    const creativeToAds = {};
    for (const ad of ads) {
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
          map[adId] = thumbUrl;
          console.log(`Ad ${adId}: hi-res thumb=YES`);
        }
      }
    }

    console.log(`Mapped ${Object.keys(map).length} ad thumbnails (hi-res)`);
  } catch (err) {
    console.warn("Could not fetch ad thumbnails:", err.message);
  }
  return map;
}

// Debug endpoint to check what Meta returns for creatives
// Debug endpoint: see campaign objectives from Meta
app.get("/debug-campaigns", async (req, res) => {
  try {
    const objectiveMap = await fetchCampaignObjectives();
    res.json(objectiveMap);
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

// ====== CAPTURE ENDPOINT ======

app.get("/capture", async (req, res) => {
  try {
    const snapshotHours = safeNumber(req.query.snapshot_hours || 1);

    // Fetch insights, thumbnails, and campaign objectives in parallel
    const [rows, thumbnailMap, objectiveMap] = await Promise.all([
      fetchMetaInsights(),
      fetchAdThumbnails(),
      fetchCampaignObjectives()
    ]);

    const grouped = new Map();

    for (const item of rows) {
      const campaign_name = item.campaign_name || "";
      const ad_name = item.ad_name || "";
      const publisher_platform = (item.publisher_platform || "").toLowerCase();

      // Skip rows without a recognized platform (aggregated/audience_network rows)
      if (!publisher_platform || !["facebook", "instagram", "messenger", "audience_network"].includes(publisher_platform)) continue;
      // Also skip audience_network — not useful for dashboard
      if (publisher_platform === "audience_network" || publisher_platform === "messenger") continue;

      const key = `${campaign_name}__${ad_name}__${publisher_platform}`;

      // Determine campaign type from API objective first, fallback to keyword detection
      const apiObjective = objectiveMap[campaign_name] || "";
      const campaignType = objectiveToCampaignType(apiObjective) || detectCampaignTypeFallback(campaign_name, ad_name);

      const existing = grouped.get(key) || {
        captured_at: new Date().toISOString(),
        snapshot_hours: snapshotHours,
        campaign_type: campaignType,
        campaign_name,
        ad_name,
        ad_id: item.ad_id || null,
        publisher_platform,
        impressions: safeNumber(item.impressions),
        reach: safeNumber(item.reach),
        cpm: safeNumber(item.cpm),
        video_3s_views: 0,
        video_3s_view_rate: 0,
        likes: 0,
        comments: 0,
        shares: 0,
        saves: 0,
        link_clicks: 0,
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
      existing.impressions = safeNumber(item.impressions);
      existing.reach = safeNumber(item.reach);
      existing.cpm = safeNumber(item.cpm);

      grouped.set(key, existing);
    }

    const cleanRows = [...grouped.values()];

    // Compute 3s view rates and CTR
    for (const row of cleanRows) {
      row.video_3s_view_rate =
        row.impressions > 0 ? round((row.video_3s_views / row.impressions) * 100) : 0;
      row.ctr =
        row.impressions > 0 ? round((row.link_clicks / row.impressions) * 100) : 0;
    }

    // Calculate normalized 0-100 scores
    computeNormalizedScores(cleanRows);

    // Generate AI insights (non-blocking — capture succeeds even if AI fails)
    await generateAdInsights(cleanRows);

    const { error } = await supabase.from("ad_snapshots").insert(cleanRows);
    if (error) throw error;

    res.json({ inserted: cleanRows.length, data: cleanRows });
  } catch (error) {
    res.status(500).json({
      error: "Capture failed",
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

app.listen(PORT, () => {
  console.log(`Server running on http://localhost:${PORT}`);
});
