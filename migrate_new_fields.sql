-- Migration: Add new Meta API fields to ad_snapshots table
-- Run this in your Supabase SQL Editor

-- Campaign & ad set identifiers
ALTER TABLE ad_snapshots ADD COLUMN IF NOT EXISTS campaign_id text;
ALTER TABLE ad_snapshots ADD COLUMN IF NOT EXISTS adset_name text;

-- Ad launch date (from Meta's created_time)
ALTER TABLE ad_snapshots ADD COLUMN IF NOT EXISTS ad_created_time timestamptz;

-- Reporting date range from Meta insights
ALTER TABLE ad_snapshots ADD COLUMN IF NOT EXISTS date_start date;
ALTER TABLE ad_snapshots ADD COLUMN IF NOT EXISTS date_stop date;

-- Financial metrics
ALTER TABLE ad_snapshots ADD COLUMN IF NOT EXISTS spend numeric DEFAULT 0;
ALTER TABLE ad_snapshots ADD COLUMN IF NOT EXISTS frequency numeric DEFAULT 0;
ALTER TABLE ad_snapshots ADD COLUMN IF NOT EXISTS cost_per_click numeric DEFAULT 0;
