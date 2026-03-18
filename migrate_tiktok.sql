-- TikTok token storage table
CREATE TABLE IF NOT EXISTS tiktok_tokens (
  id serial PRIMARY KEY,
  advertiser_id text NOT NULL,
  advertiser_name text,
  access_token text NOT NULL,
  refresh_token text,
  token_expires_at timestamptz,
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now()
);
