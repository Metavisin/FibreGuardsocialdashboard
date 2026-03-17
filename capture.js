#!/usr/bin/env node
/**
 * FibreGuard Ad Snapshot Capture
 *
 * Standalone script that runs via GitHub Actions every 30 minutes.
 * - Polls Meta API for ads with delivery status ACTIVE
 * - Tracks newly active ads in Supabase `ad_tracking` table
 * - Captures hourly snapshots for the first 12 hours of an ad being live
 * - After 12 hours, captures every 6 hours
 * - After 48 hours, captures every 24 hours
 * - Stores all snapshots in Supabase `ad_snapshots` table
 */

import axios from "axios";
import { createClient } from "@supabase/supabase-js";

// ====== CONFIG ======
const META_ACCESS_TOKEN = process.env.META_ACCESS_TOKEN;
const META_AD_ACCOUNT_ID = process.env.META_AD_ACCOUNT_ID;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!META_ACCESS_TOKEN || !META_AD_ACCOUNT_ID || !SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.error("Missing required environment variables");
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

// ====== UTILITIES ======

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

function objectiveToCampaignType(objective = "") {
  const obj = objective.toUpperCase();
  if (obj.includes("AWARENESS") || obj.includes("REACH") || obj.includes("VIDEO_VIEWS") || obj.includes("BRAND_AWARENESS")) return "awareness";
  if (obj.includes("TRAFFIC") || obj.includes("LINK_CLICKS") || obj.includes("OUTCOME_TRAFFIC") || obj.includes("OUTCOME_LEADS")) return "traffic";
  if (obj.includes("ENGAGEMENT") || obj.includes("POST_ENGAGEMENT") || obj.includes("CONVERSIONS") || obj.includes("MESSAGES")) return "engagement";
  return null;
}

function detectCampaignTypeFallback(campaignName = "", adName = "") {
  const text = `${campaignName} ${adName}`.toLowerCase();
  if (text.includes("awareness") || text.includes("entertaining")) return "awareness";
  if (text.includes("engagement") || text.includes("education") || text.includes("educational")) return "engagement";
  if (text.includes("traffic")) return "traffic";
  return "awareness";
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

// ====== TARGET-BASED SCORING (Variant B) ======
// Each component capped at 2.0, max score = 200 pts
const TARGETS = {
  awareness: { reach: 400000, cpm: 0.07, viewRate: 15 },
  engagement: { shareRate: 2.0, saveRate: 3.0, commentRate: 1.5, likeRate: 20.0 },
  traffic: { ctr: 1.0, cpc: 0.007, lpvr: 0.70, frequency: 2.0 }
};

function computeAbsoluteScores(rows) {
  for (const row of rows) {
    if (row.campaign_type === "awareness") {
      const reachRatio = Math.min(TARGETS.awareness.reach > 0 ? row.reach / TARGETS.awareness.reach : 0, 2.0);
      const cpmRatio = Math.min(row.cpm > 0 ? TARGETS.awareness.cpm / row.cpm : 0, 2.0);
      const viewRatio = Math.min(TARGETS.awareness.viewRate > 0 ? row.video_3s_view_rate / TARGETS.awareness.viewRate : 0, 2.0);
      row.awareness_score = round((0.40 * reachRatio + 0.40 * cpmRatio + 0.20 * viewRatio) * 100);
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
      row.traffic_score = round((0.40 * ctrRatio + 0.30 * cpcRatio + 0.20 * lpvrRatio + 0.10 * freqRatio) * 100);
      row.awareness_score = null;
      row.engagement_score = null;
    }
    const score = row.awareness_score ?? row.engagement_score ?? row.traffic_score ?? 0;
    if (score >= 100) row.boost_recommendation = "boost";
    else if (score >= 70) row.boost_recommendation = "monitor";
    else row.boost_recommendation = "no boost";
  }
}

// ====== META API ======

async function fetchActiveAds() {
  console.log("Fetching ads from Meta API...");
  const url = `https://graph.facebook.com/v25.0/act_${META_AD_ACCOUNT_ID}/ads`;
  const { data } = await axios.get(url, {
    params: {
      access_token: META_ACCESS_TOKEN,
      fields: "id,name,effective_status,configured_status,created_time,creative{id},campaign{id,name,objective,effective_status,budget_remaining,lifetime_budget,daily_budget,stop_time},adset{id,name,effective_status,budget_remaining,lifetime_budget,daily_budget,end_time}",
      limit: 200
    }
  });

  const ads = data.data || [];
  const now = new Date();
  const activeAds = [];

  for (const ad of ads) {
    const adEffective = ad.effective_status || "UNKNOWN";
    const campaignEffective = ad.campaign?.effective_status || "UNKNOWN";
    const adsetEffective = ad.adset?.effective_status || "UNKNOWN";
    const campaignBudgetRemaining = parseFloat(ad.campaign?.budget_remaining || "-1");
    const adsetBudgetRemaining = parseFloat(ad.adset?.budget_remaining || "-1");
    const campaignLifetime = parseFloat(ad.campaign?.lifetime_budget || "0");
    const adsetLifetime = parseFloat(ad.adset?.lifetime_budget || "0");
    const campaignStopTime = ad.campaign?.stop_time ? new Date(ad.campaign.stop_time) : null;
    const adsetEndTime = ad.adset?.end_time ? new Date(ad.adset.end_time) : null;

    let isActive = adEffective === "ACTIVE" &&
      campaignEffective === "ACTIVE" &&
      adsetEffective === "ACTIVE";

    // Check budget exhaustion
    if (isActive && campaignLifetime > 0 && campaignBudgetRemaining === 0) isActive = false;
    if (isActive && adsetLifetime > 0 && adsetBudgetRemaining === 0) isActive = false;
    if (isActive && campaignStopTime && campaignStopTime < now) isActive = false;
    if (isActive && adsetEndTime && adsetEndTime < now) isActive = false;

    if (isActive) {
      activeAds.push({
        ad_id: ad.id,
        ad_name: ad.name,
        created_time: ad.created_time,
        campaign_id: ad.campaign?.id,
        campaign_name: ad.campaign?.name || "",
        campaign_objective: ad.campaign?.objective || "",
        adset_name: ad.adset?.name || ""
      });
    }
  }

  console.log(`Found ${ads.length} total ads, ${activeAds.length} actively delivering`);
  return activeAds;
}

async function fetchInsightsForAds(adIds) {
  if (adIds.length === 0) return [];

  console.log(`Fetching insights for ${adIds.length} ads...`);
  const url = `https://graph.facebook.com/v25.0/act_${META_AD_ACCOUNT_ID}/insights`;
  const { data } = await axios.get(url, {
    params: {
      access_token: META_ACCESS_TOKEN,
      fields: "campaign_name,campaign_id,adset_name,ad_name,ad_id,impressions,reach,cpm,spend,frequency,actions,cost_per_action_type,date_start,date_stop",
      breakdowns: "publisher_platform",
      action_breakdowns: "action_type",
      level: "ad",
      date_preset: "maximum",
      filtering: JSON.stringify([{ field: "ad.id", operator: "IN", value: adIds }])
    }
  });

  return data.data || [];
}

async function fetchThumbnails(adIds) {
  const thumbnails = {};
  try {
    const url = `https://graph.facebook.com/v25.0/act_${META_AD_ACCOUNT_ID}/ads`;
    const { data } = await axios.get(url, {
      params: {
        access_token: META_ACCESS_TOKEN,
        fields: "id,creative{id}",
        filtering: JSON.stringify([{ field: "id", operator: "IN", value: adIds }]),
        limit: 200
      }
    });

    const creativeToAds = {};
    for (const ad of (data.data || [])) {
      const cid = ad.creative?.id;
      if (cid) {
        if (!creativeToAds[cid]) creativeToAds[cid] = [];
        creativeToAds[cid].push(ad.id);
      }
    }

    const creativesUrl = `https://graph.facebook.com/v25.0/act_${META_AD_ACCOUNT_ID}/adcreatives`;
    const { data: cData } = await axios.get(creativesUrl, {
      params: {
        access_token: META_ACCESS_TOKEN,
        fields: "id,thumbnail_url,image_url",
        thumbnail_width: 600,
        thumbnail_height: 600,
        limit: 100
      }
    });

    for (const creative of (cData.data || [])) {
      const thumbUrl = creative.thumbnail_url || creative.image_url || null;
      for (const adId of (creativeToAds[creative.id] || [])) {
        if (thumbUrl) thumbnails[adId] = thumbUrl;
      }
    }
  } catch (err) {
    console.warn("Could not fetch thumbnails:", err.message);
  }
  return thumbnails;
}

// ====== TRACKING LOGIC ======

async function getTrackedAds() {
  const { data, error } = await supabase
    .from("ad_tracking")
    .select("*")
    .eq("is_active", true);

  if (error) throw error;
  return data || [];
}

async function upsertTracking(adId, adName, campaignName) {
  const { data: existing } = await supabase
    .from("ad_tracking")
    .select("id")
    .eq("ad_id", adId)
    .single();

  if (existing) {
    // Reactivate if it was previously marked inactive
    await supabase
      .from("ad_tracking")
      .update({ is_active: true, updated_at: new Date().toISOString() })
      .eq("ad_id", adId);
  } else {
    // New ad — start tracking
    await supabase.from("ad_tracking").insert({
      ad_id: adId,
      ad_name: adName,
      campaign_name: campaignName,
      first_seen_active: new Date().toISOString(),
      is_active: true,
      snapshot_count: 0
    });
    console.log(`🆕 New active ad detected: "${adName}" (${adId})`);
  }
}

async function markInactive(adIds) {
  if (adIds.length === 0) return;
  await supabase
    .from("ad_tracking")
    .update({ is_active: false, updated_at: new Date().toISOString() })
    .in("ad_id", adIds);
}

async function getLastSnapshot(adId) {
  const { data } = await supabase
    .from("ad_snapshots")
    .select("captured_at")
    .eq("ad_id", adId)
    .order("captured_at", { ascending: false })
    .limit(1)
    .single();

  return data?.captured_at || null;
}

async function incrementSnapshotCount(adId) {
  const { data } = await supabase
    .from("ad_tracking")
    .select("snapshot_count")
    .eq("ad_id", adId)
    .single();

  await supabase
    .from("ad_tracking")
    .update({
      snapshot_count: (data?.snapshot_count || 0) + 1,
      last_snapshot_at: new Date().toISOString(),
      updated_at: new Date().toISOString()
    })
    .eq("ad_id", adId);
}

// ====== MAIN CAPTURE LOGIC ======

async function run() {
  console.log(`\n${"=".repeat(60)}`);
  console.log(`FibreGuard Capture — ${new Date().toISOString()}`);
  console.log(`${"=".repeat(60)}\n`);

  // 1. Get currently active ads from Meta
  const activeAds = await fetchActiveAds();
  const activeAdIds = activeAds.map(a => a.ad_id);

  if (activeAds.length === 0) {
    console.log("No active ads found. Nothing to capture.");
    return;
  }

  // 2. Update tracking table — register new ads, reactivate returning ones
  for (const ad of activeAds) {
    await upsertTracking(ad.ad_id, ad.ad_name, ad.campaign_name);
  }

  // 3. Mark ads that are no longer active
  const trackedAds = await getTrackedAds();
  const nowInactiveIds = trackedAds
    .filter(t => !activeAdIds.includes(t.ad_id))
    .map(t => t.ad_id);
  await markInactive(nowInactiveIds);
  if (nowInactiveIds.length > 0) {
    console.log(`Marked ${nowInactiveIds.length} ads as inactive`);
  }

  // 4. Determine which ads need a snapshot right now
  const adsToCaptureIds = [];
  const refreshedTracking = await getTrackedAds();

  for (const tracked of refreshedTracking) {
    if (!activeAdIds.includes(tracked.ad_id)) continue;

    const hoursSinceFirstSeen = hoursAgo(tracked.first_seen_active);
    const lastSnapshotAt = tracked.last_snapshot_at || tracked.first_seen_active;
    const hoursSinceLastSnapshot = hoursAgo(lastSnapshotAt);

    // Determine capture interval based on age
    let captureIntervalHours;
    if (hoursSinceFirstSeen <= 12) {
      captureIntervalHours = 1;  // Every hour for first 12 hours
    } else if (hoursSinceFirstSeen <= 48) {
      captureIntervalHours = 6;  // Every 6 hours for next 36 hours
    } else {
      captureIntervalHours = 24; // Daily after 48 hours
    }

    if (hoursSinceLastSnapshot >= captureIntervalHours * 0.8) {
      adsToCaptureIds.push(tracked.ad_id);
      console.log(`📸 Capturing "${tracked.ad_name}" — ${round(hoursSinceFirstSeen, 1)}h old, interval: ${captureIntervalHours}h, last snapshot: ${round(hoursSinceLastSnapshot, 1)}h ago`);
    } else {
      console.log(`⏭️  Skipping "${tracked.ad_name}" — ${round(hoursSinceFirstSeen, 1)}h old, next capture in ${round(captureIntervalHours - hoursSinceLastSnapshot, 1)}h`);
    }
  }

  if (adsToCaptureIds.length === 0) {
    console.log("\nAll ads are up-to-date. No snapshots needed.");
    return;
  }

  // 5. Fetch insights and thumbnails for ads that need capturing
  const [insightsRows, thumbnails] = await Promise.all([
    fetchInsightsForAds(adsToCaptureIds),
    fetchThumbnails(adsToCaptureIds)
  ]);

  // 6. Build campaign objective map from active ads
  const objectiveMap = {};
  for (const ad of activeAds) {
    objectiveMap[ad.campaign_name] = ad.campaign_objective;
  }

  // 7. Process insights into snapshot rows
  const grouped = new Map();
  for (const item of insightsRows) {
    const platform = (item.publisher_platform || "").toLowerCase();
    if (!["facebook", "instagram"].includes(platform)) continue;

    const key = `${item.campaign_name}__${item.ad_name}__${platform}`;
    const apiObjective = objectiveMap[item.campaign_name] || "";
    const campaignType = objectiveToCampaignType(apiObjective) || detectCampaignTypeFallback(item.campaign_name, item.ad_name);
    const spend = safeNumber(item.spend);
    const adCreated = activeAds.find(a => a.ad_id === item.ad_id)?.created_time || null;
    const adAgeHours = adCreated ? Math.round(hoursAgo(adCreated)) : 0;

    const existing = grouped.get(key) || {
      captured_at: new Date().toISOString(),
      snapshot_hours: adAgeHours,
      campaign_type: campaignType,
      campaign_name: item.campaign_name,
      campaign_id: item.campaign_id || null,
      adset_name: item.adset_name || null,
      ad_name: item.ad_name,
      ad_id: item.ad_id || null,
      publisher_platform: platform,
      ad_status: "ACTIVE",
      ad_created_time: adCreated,
      date_start: item.date_start || null,
      date_stop: item.date_stop || null,
      impressions: safeNumber(item.impressions),
      reach: safeNumber(item.reach),
      cpm: safeNumber(item.cpm),
      spend,
      frequency: safeNumber(item.frequency),
      cost_per_click: 0,
      landing_page_views: 0,
      lpvr: 0,
      video_3s_views: 0,
      video_3s_view_rate: 0,
      likes: 0, comments: 0, shares: 0, saves: 0, link_clicks: 0, ctr: 0,
      awareness_score: null, engagement_score: null, traffic_score: null,
      boost_recommendation: null,
      thumbnail_url: thumbnails[item.ad_id] || null
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
    row.video_3s_view_rate = row.impressions > 0 ? round((row.video_3s_views / row.impressions) * 100) : 0;
    row.ctr = row.impressions > 0 ? round((row.link_clicks / row.impressions) * 100) : 0;
    row.cost_per_click = row.link_clicks > 0 ? round(row.spend / row.link_clicks, 4) : 0;
    row.lpvr = row.link_clicks > 0 ? round(row.landing_page_views / row.link_clicks, 4) : 0;
  }

  computeAbsoluteScores(cleanRows);

  // 8. Insert into Supabase
  if (cleanRows.length > 0) {
    const { error } = await supabase.from("ad_snapshots").insert(cleanRows);
    if (error) throw error;

    // Update snapshot counts
    const capturedAdIds = [...new Set(cleanRows.map(r => r.ad_id))];
    for (const adId of capturedAdIds) {
      await incrementSnapshotCount(adId);
    }
  }

  console.log(`\n✅ Captured ${cleanRows.length} snapshot rows for ${adsToCaptureIds.length} ads`);

  // 9. Summary
  console.log("\n--- Summary ---");
  console.log(`Active ads: ${activeAds.length}`);
  console.log(`Snapshots captured: ${cleanRows.length}`);
  console.log(`Ads skipped (up-to-date): ${refreshedTracking.length - adsToCaptureIds.length}`);
  console.log(`Ads marked inactive: ${nowInactiveIds.length}`);
}

// Run
run()
  .then(() => {
    console.log("\nDone.");
    process.exit(0);
  })
  .catch(err => {
    console.error("\n❌ Capture failed:", err.message);
    process.exit(1);
  });
