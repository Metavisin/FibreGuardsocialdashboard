-- Migration: Create ad_tracking table for smart capture scheduling
-- Run this in your Supabase SQL Editor

CREATE TABLE IF NOT EXISTS ad_tracking (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  ad_id text NOT NULL UNIQUE,
  ad_name text,
  campaign_name text,
  first_seen_active timestamptz NOT NULL DEFAULT now(),
  last_snapshot_at timestamptz,
  snapshot_count integer DEFAULT 0,
  is_active boolean DEFAULT true,
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now()
);

-- Index for quick lookups
CREATE INDEX IF NOT EXISTS idx_ad_tracking_ad_id ON ad_tracking(ad_id);
CREATE INDEX IF NOT EXISTS idx_ad_tracking_active ON ad_tracking(is_active);
