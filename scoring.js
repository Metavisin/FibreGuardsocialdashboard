/**
 * FibreGuard Ad Scoring Module
 *
 * Implements peer-relative scoring with min-max normalization.
 * Shared by capture.js (real-time scoring after insert) and server.js (/score endpoint for backfill).
 *
 * Four scoring buckets:
 *   A — TikTok reach:        reach (40%), CPM inverted (40%), 2s view rate (20%)
 *   B — TikTok community:    share rate (40%), favourites rate (30%), comment rate (20%), like rate (10%)
 *   C — Instagram awareness:  reach (40%), CPM inverted (40%), 3s view rate (20%)
 *   D — Instagram engagement: share rate (40%), save rate (30%), comment rate (20%), like rate (10%)
 *
 * Facebook rows are left unscored (NULL).
 */

// ====== BUCKET DEFINITIONS ======

const BUCKETS = {
  "tiktok__reach": {
    platform: "tiktok",
    campaignType: "reach",
    metrics: [
      { name: "reach", source: (r) => r.reach, weight: 0.40, invert: false },
      { name: "cpm", source: (r) => r.cpm, weight: 0.40, invert: true },
      {
        name: "video_2s_view_rate",
        source: (r) => {
          if (r.video_2s_view_rate != null && r.video_2s_view_rate > 0) return r.video_2s_view_rate;
          // Fallback: compute from video_2s_views / impressions × 100
          if (r.impressions > 0 && r.video_2s_views > 0) return (r.video_2s_views / r.impressions) * 100;
          return 0;
        },
        weight: 0.20,
        invert: false
      }
    ],
    usesRates: false // reach + cpm are raw values, not impression-based rates
  },
  "tiktok__community": {
    platform: "tiktok",
    campaignType: "community",
    metrics: [
      { name: "share_rate", source: (r) => r.impressions > 0 ? (r.shares / r.impressions) * 1000 : 0, weight: 0.40, invert: false },
      { name: "favourites_rate", source: (r) => r.impressions > 0 ? (r.saves / r.impressions) * 1000 : 0, weight: 0.30, invert: false },
      { name: "comment_rate", source: (r) => r.impressions > 0 ? (r.comments / r.impressions) * 1000 : 0, weight: 0.20, invert: false },
      { name: "like_rate", source: (r) => r.impressions > 0 ? (r.likes / r.impressions) * 1000 : 0, weight: 0.10, invert: false }
    ],
    usesRates: true
  },
  "instagram__awareness": {
    platform: "instagram",
    campaignType: "awareness",
    metrics: [
      { name: "reach", source: (r) => r.reach, weight: 0.40, invert: false },
      { name: "cpm", source: (r) => r.cpm, weight: 0.40, invert: true },
      {
        name: "video_3s_view_rate",
        source: (r) => {
          if (r.video_3s_view_rate != null && r.video_3s_view_rate > 0) return r.video_3s_view_rate;
          // Fallback: compute from video_3s_views / impressions × 100
          if (r.impressions > 0 && r.video_3s_views > 0) return (r.video_3s_views / r.impressions) * 100;
          return 0;
        },
        weight: 0.20,
        invert: false
      }
    ],
    usesRates: false
  },
  "instagram__engagement": {
    platform: "instagram",
    campaignType: "engagement",
    metrics: [
      { name: "share_rate", source: (r) => r.impressions > 0 ? (r.shares / r.impressions) * 1000 : 0, weight: 0.40, invert: false },
      { name: "save_rate", source: (r) => r.impressions > 0 ? (r.saves / r.impressions) * 1000 : 0, weight: 0.30, invert: false },
      { name: "comment_rate", source: (r) => r.impressions > 0 ? (r.comments / r.impressions) * 1000 : 0, weight: 0.20, invert: false },
      { name: "like_rate", source: (r) => r.impressions > 0 ? (r.likes / r.impressions) * 1000 : 0, weight: 0.10, invert: false }
    ],
    usesRates: true
  }
};

// ====== HELPER FUNCTIONS ======

/**
 * Look up which bucket a row belongs to.
 * Returns null for unsupported combos (Facebook, traffic, etc.)
 */
export function lookupBucket(platform, campaignType) {
  const key = `${(platform || "").toLowerCase()}__${(campaignType || "").toLowerCase()}`;
  return BUCKETS[key] || null;
}

/**
 * Compute the 70th percentile using linear interpolation.
 * Matches numpy.percentile(arr, 70) / pandas.Series.quantile(0.70).
 */
export function percentile70(values) {
  if (!values || values.length === 0) return null;
  if (values.length === 1) return values[0];

  const sorted = [...values].sort((a, b) => a - b);
  const n = sorted.length;
  // Linear interpolation method (matches numpy default)
  const idx = 0.70 * (n - 1);
  const lo = Math.floor(idx);
  const hi = Math.ceil(idx);
  const frac = idx - lo;

  if (lo === hi) return sorted[lo];
  return sorted[lo] + frac * (sorted[hi] - sorted[lo]);
}

/**
 * Min-max normalize a value. For inverted metrics (CPM), lower is better.
 */
function minMaxNormalize(value, min, max, invert = false) {
  if (max === min) return 0.5;
  let n = (value - min) / (max - min);
  if (invert) n = 1 - n;
  return Math.max(0, Math.min(1, n)); // Clip to [0, 1]
}

// ====== CORE SCORING FUNCTION ======

/**
 * Score a single snapshot row R against its peer pool.
 *
 * @param {Object} R - The snapshot row to score (must have all metric columns)
 * @param {Array}  P - The peer pool rows (up to 15, same bucket & snapshot_hours, excluding R's ad_id)
 * @returns {{ score: number|null, benchmark: number|null, boost: boolean|null }}
 */
export function scoreRow(R, P) {
  const bucket = lookupBucket(R.publisher_platform, R.campaign_type);

  // Unsupported bucket (Facebook, traffic, etc.) → leave NULL
  if (!bucket) {
    return { score: null, benchmark: null, boost: null };
  }

  // If bucket uses rates and R has no impressions → cannot compute
  if (bucket.usesRates && (!R.impressions || R.impressions === 0)) {
    return { score: null, benchmark: null, boost: null };
  }

  // Filter out peers with 0 impressions for rate-based buckets
  let validPeers = P;
  if (bucket.usesRates) {
    validPeers = P.filter(p => p.impressions && p.impressions > 0);
  }

  // The pool for normalization is P ∪ {R}
  const pool = [...validPeers, R];

  // Step 4.3 + 4.4: Compute raw values and normalize each metric
  const metricNormalized = {}; // metricName → { rowId: normalizedValue }

  for (const metric of bucket.metrics) {
    // Compute raw values for the entire pool
    const rawValues = pool.map(row => metric.source(row));

    const minM = Math.min(...rawValues);
    const maxM = Math.max(...rawValues);

    // Normalize each row
    for (let i = 0; i < pool.length; i++) {
      const row = pool[i];
      const rowKey = row === R ? "__R__" : `peer_${i}`;
      if (!metricNormalized[rowKey]) metricNormalized[rowKey] = {};
      metricNormalized[rowKey][metric.name] = minMaxNormalize(rawValues[i], minM, maxM, metric.invert);
    }
  }

  // Step 4.5: Weighted sum for each row in pool
  const scores = {};
  for (const [rowKey, normalizedMetrics] of Object.entries(metricNormalized)) {
    let weightedSum = 0;
    for (const metric of bucket.metrics) {
      weightedSum += metric.weight * (normalizedMetrics[metric.name] || 0);
    }
    scores[rowKey] = Math.round(weightedSum * 100 * 100) / 100; // Round to 2 decimals
  }

  const rScore = scores["__R__"];

  // Step 4.6: Benchmark = 70th percentile of peer scores (not R)
  const peerScores = Object.entries(scores)
    .filter(([key]) => key !== "__R__")
    .map(([, s]) => s);

  let benchmark = null;
  let boost = null;

  if (peerScores.length > 0) {
    benchmark = Math.round(percentile70(peerScores) * 100) / 100;
    boost = rScore > benchmark; // Strict greater-than
  }
  // If P is empty: score is computed (will be 50.0), benchmark and boost stay NULL

  return { score: rScore, benchmark, boost };
}

// ====== BATCH SCORING FOR SUPABASE ======

/**
 * Score all unscored rows in a Supabase table.
 * Groups by (platform, campaign_type, snapshot_hours) for efficiency.
 *
 * @param {Object} supabase - Supabase client
 * @param {Object} options - { dryRun: false, limit: 5000 }
 * @returns {{ scored: number, skipped: number, errors: number, warnings: string[] }}
 */
export async function scoreUnscoredRows(supabase, options = {}) {
  const { dryRun = false, limit = 5000 } = options;
  const warnings = [];
  let scored = 0;
  let skipped = 0;
  let errors = 0;

  // Fetch all unscored rows for TikTok and Instagram
  let unscoredRows = [];
  let from = 0;
  const pageSize = 1000;

  while (true) {
    const { data, error } = await supabase
      .from("ad_snapshots")
      .select("id, ad_id, captured_at, snapshot_hours, publisher_platform, campaign_type, impressions, reach, cpm, video_2s_views, video_2s_view_rate, video_3s_views, video_3s_view_rate, likes, comments, shares, saves")
      .is("score", null)
      .in("publisher_platform", ["tiktok", "instagram"])
      .order("captured_at", { ascending: true })
      .range(from, from + pageSize - 1);

    if (error) {
      console.error("Failed to fetch unscored rows:", error.message);
      return { scored: 0, skipped: 0, errors: 1, warnings: [error.message] };
    }

    unscoredRows = unscoredRows.concat(data || []);
    if (!data || data.length < pageSize) break;
    from += pageSize;
  }

  console.log(`Found ${unscoredRows.length} unscored rows to process`);
  if (unscoredRows.length === 0) return { scored: 0, skipped: 0, errors: 0, warnings: [] };

  // Cap at limit
  if (unscoredRows.length > limit) {
    unscoredRows = unscoredRows.slice(0, limit);
    warnings.push(`Processing capped at ${limit} rows`);
  }

  // Group by (platform, campaign_type, snapshot_hours) for efficient peer pool loading
  const groups = {};
  for (const row of unscoredRows) {
    const key = `${row.publisher_platform}__${row.campaign_type}__${row.snapshot_hours}`;
    if (!groups[key]) groups[key] = [];
    groups[key].push(row);
  }

  console.log(`Grouped into ${Object.keys(groups).length} bucket/hour combos`);

  for (const [groupKey, rows] of Object.entries(groups)) {
    const [platform, campaignType, snapshotHoursStr] = groupKey.split("__");
    const snapshotHours = parseInt(snapshotHoursStr, 10);
    const bucket = lookupBucket(platform, campaignType);

    if (!bucket) {
      skipped += rows.length;
      continue;
    }

    // Load ALL potential peers for this bucket+hour (we'll filter per-row)
    const { data: allPeersRaw, error: peerErr } = await supabase
      .from("ad_snapshots")
      .select("id, ad_id, captured_at, snapshot_hours, publisher_platform, campaign_type, impressions, reach, cpm, video_2s_views, video_2s_view_rate, video_3s_views, video_3s_view_rate, likes, comments, shares, saves")
      .eq("publisher_platform", platform)
      .eq("campaign_type", campaignType)
      .eq("snapshot_hours", snapshotHours)
      .order("captured_at", { ascending: false })
      .limit(500); // Generous limit to cover all ads

    if (peerErr) {
      console.error(`Peer pool fetch failed for ${groupKey}:`, peerErr.message);
      errors += rows.length;
      continue;
    }

    const allPeers = allPeersRaw || [];

    // Score each row in this group
    for (const R of rows) {
      try {
        // Build the peer pool for this specific row:
        // 1. Exclude R's own ad_id
        // 2. Keep only the most recent snapshot per ad_id
        // 3. Take top 15 by captured_at
        const peersByAd = {};
        for (const peer of allPeers) {
          if (peer.ad_id === R.ad_id) continue;
          if (peer.id === R.id) continue;
          // Keep the most recent snapshot per ad_id
          if (!peersByAd[peer.ad_id] || new Date(peer.captured_at) > new Date(peersByAd[peer.ad_id].captured_at)) {
            peersByAd[peer.ad_id] = peer;
          }
        }

        // Sort by captured_at descending, take top 15
        const P = Object.values(peersByAd)
          .sort((a, b) => new Date(b.captured_at) - new Date(a.captured_at))
          .slice(0, 15);

        if (P.length < 10) {
          warnings.push(`Small peer pool (${P.length}) for ad ${R.ad_id} at hour ${snapshotHours} in ${platform}/${campaignType}`);
        }

        const { score, benchmark, boost } = scoreRow(R, P);

        if (score === null) {
          skipped++;
          continue;
        }

        if (!dryRun) {
          const { error: updateErr } = await supabase
            .from("ad_snapshots")
            .update({ score, benchmark, boost })
            .eq("id", R.id);

          if (updateErr) {
            console.error(`Failed to update row ${R.id}:`, updateErr.message);
            errors++;
          } else {
            scored++;
          }
        } else {
          scored++;
        }
      } catch (err) {
        console.error(`Error scoring row ${R.id}:`, err.message);
        errors++;
      }
    }
  }

  console.log(`Scoring complete: ${scored} scored, ${skipped} skipped, ${errors} errors`);
  if (warnings.length > 0 && warnings.length <= 20) {
    for (const w of warnings) console.warn(`⚠️  ${w}`);
  } else if (warnings.length > 20) {
    console.warn(`⚠️  ${warnings.length} warnings (showing first 5):`);
    for (const w of warnings.slice(0, 5)) console.warn(`   ${w}`);
  }

  return { scored, skipped, errors, warnings };
}

/**
 * Score a single newly-inserted row (for use in capture.js after insert).
 * Fetches peers from Supabase and computes score/benchmark/boost.
 *
 * @param {Object} supabase - Supabase client
 * @param {Object} row - The row that was just inserted (must have id, ad_id, etc.)
 * @returns {{ score: number|null, benchmark: number|null, boost: boolean|null }}
 */
export async function scoreNewRow(supabase, row) {
  const bucket = lookupBucket(row.publisher_platform, row.campaign_type);
  if (!bucket) return { score: null, benchmark: null, boost: null };

  // Cap at 48 hours
  if (row.snapshot_hours > 48) return { score: null, benchmark: null, boost: null };

  // Cannot compute rates without impressions
  if (bucket.usesRates && (!row.impressions || row.impressions === 0)) {
    return { score: null, benchmark: null, boost: null };
  }

  // Fetch peers: same bucket, same snapshot_hours, excluding this ad_id
  // Get more than 15 to ensure we can deduplicate by ad_id
  const { data: peersRaw, error } = await supabase
    .from("ad_snapshots")
    .select("id, ad_id, captured_at, snapshot_hours, publisher_platform, campaign_type, impressions, reach, cpm, video_2s_views, video_2s_view_rate, video_3s_views, video_3s_view_rate, likes, comments, shares, saves")
    .eq("publisher_platform", row.publisher_platform)
    .eq("campaign_type", row.campaign_type)
    .eq("snapshot_hours", row.snapshot_hours)
    .neq("ad_id", row.ad_id)
    .order("captured_at", { ascending: false })
    .limit(200);

  if (error) {
    console.error(`Peer pool fetch failed for scoring:`, error.message);
    return { score: null, benchmark: null, boost: null };
  }

  // Deduplicate: one row per ad_id (most recent)
  const peersByAd = {};
  for (const peer of (peersRaw || [])) {
    if (!peersByAd[peer.ad_id] || new Date(peer.captured_at) > new Date(peersByAd[peer.ad_id].captured_at)) {
      peersByAd[peer.ad_id] = peer;
    }
  }

  // Top 15 by captured_at
  const P = Object.values(peersByAd)
    .sort((a, b) => new Date(b.captured_at) - new Date(a.captured_at))
    .slice(0, 15);

  if (P.length < 10) {
    console.warn(`⚠️  Small peer pool (${P.length}) for ad ${row.ad_id} at hour ${row.snapshot_hours} in ${row.publisher_platform}/${row.campaign_type}`);
  }

  const result = scoreRow(row, P);

  // Write back to Supabase if we have an id
  if (row.id && result.score !== null) {
    const { error: updateErr } = await supabase
      .from("ad_snapshots")
      .update({ score: result.score, benchmark: result.benchmark, boost: result.boost })
      .eq("id", row.id);

    if (updateErr) {
      console.error(`Failed to update score for row ${row.id}:`, updateErr.message);
    }
  }

  return result;
}
