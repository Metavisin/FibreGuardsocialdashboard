# FibreGuard Dashboard — Railway Deployment Guide

## Step 1: Push to GitHub

Open terminal in your project folder and run:

```bash
git init
git add -A
git commit -m "FibreGuard ad performance dashboard"
```

Then create a repo on GitHub (https://github.com/new) — name it something like `fibreguard-dashboard`, set it to **Private**, and push:

```bash
git remote add origin https://github.com/YOUR_USERNAME/fibreguard-dashboard.git
git branch -M main
git push -u origin main
```

## Step 2: Deploy on Railway

1. Go to https://railway.app and sign in with GitHub
2. Click **"New Project"** → **"Deploy from GitHub Repo"**
3. Select your `fibreguard-dashboard` repo
4. Railway will auto-detect Node.js and start building

### Add Environment Variables

In your Railway project, go to **Variables** tab and add these 4 variables:

| Variable                   | Value                          |
|---------------------------|--------------------------------|
| `META_ACCESS_TOKEN`        | Your Meta token                |
| `META_AD_ACCOUNT_ID`       | `400249422770334`              |
| `SUPABASE_URL`             | `https://kulalbfnfugauwpjukze.supabase.co` |
| `SUPABASE_SERVICE_ROLE_KEY`| Your Supabase service role key |

> Do NOT add PORT — Railway sets this automatically.

### Generate a Domain

Go to **Settings** → **Networking** → **Generate Domain**
You'll get a URL like: `fibreguard-dashboard-production.up.railway.app`

This is your live dashboard URL!

## Step 3: Set Up Cron Jobs (cron-job.org)

1. Go to https://cron-job.org and create a free account
2. Create **5 cron jobs** — one for each checkpoint:

| Title              | URL                                                                 | Schedule         |
|-------------------|---------------------------------------------------------------------|-----------------|
| FG Capture 1h     | `https://YOUR-RAILWAY-URL/capture?snapshot_hours=1`                 | Every 6 hours   |
| FG Capture 4h     | `https://YOUR-RAILWAY-URL/capture?snapshot_hours=4`                 | Every 6 hours   |
| FG Capture 8h     | `https://YOUR-RAILWAY-URL/capture?snapshot_hours=8`                 | Every 12 hours  |
| FG Capture 24h    | `https://YOUR-RAILWAY-URL/capture?snapshot_hours=24`                | Once daily       |
| FG Capture 48h    | `https://YOUR-RAILWAY-URL/capture?snapshot_hours=48`                | Once daily       |

### Recommended cron schedules:

- **1h capture**: `0 */6 * * *` (every 6 hours: midnight, 6am, noon, 6pm)
- **4h capture**: `15 */6 * * *` (same but offset by 15 min)
- **8h capture**: `30 */12 * * *` (every 12 hours)
- **24h capture**: `45 8 * * *` (once daily at 8:45am)
- **48h capture**: `0 9 * * *` (once daily at 9:00am)

> Note: These capture "snapshots" of current performance. The `snapshot_hours` parameter
> tags the data so you can compare how ads perform at different time intervals.
> Over time, this builds the dataset to find the optimal boost window.

## Step 4: Update Dashboard URL

After deployment, open `dashboard.html` and update the Supabase URL if needed.
The dashboard fetches directly from Supabase, so it will work from any domain.

## That's it!

Your dashboard will be live at your Railway URL, and cron jobs will automatically
capture ad performance data at each checkpoint interval. After ~1 month of data,
you'll have enough to analyze optimal boost timing.
