#!/bin/bash
# =============================================================
# ITF Combined Scraper — Server Run Commands
# =============================================================
# Scrapes EVERYTHING in one pass per match page:
#   - Home/Away verification
#   - Tiebreak loser scores
#   - Match time + set times
#   - Date/time
#
# =============================================================
# 1. Pull the latest code and data:
#
#    cd ~/itf-match-record-share
#    git pull origin main
#
# 2. Install deps (if needed):
#
#    pip install pandas selenium webdriver-manager
#
# =============================================================
# 3. Run all 4 shards (one per terminal/tmux pane):

# --- Shard 0 ---
python itf_combined_scraper.py --shard 0 --total-shards 4 --resume

# --- Shard 1 ---
python itf_combined_scraper.py --shard 1 --total-shards 4 --resume

# --- Shard 2 ---
python itf_combined_scraper.py --shard 2 --total-shards 4 --resume

# --- Shard 3 ---
python itf_combined_scraper.py --shard 3 --total-shards 4 --resume

# =============================================================
# 4. After ALL 4 shards finish, combine:
#
#    python itf_combined_scraper.py --combine
#
# 5. Apply scraped data to the ITF CSV:
#
#    python itf_combined_scraper.py --apply
#
# =============================================================
# Or run all 4 in background at once:
#
#    python itf_combined_scraper.py --shard 0 --total-shards 4 --resume &
#    python itf_combined_scraper.py --shard 1 --total-shards 4 --resume &
#    python itf_combined_scraper.py --shard 2 --total-shards 4 --resume &
#    python itf_combined_scraper.py --shard 3 --total-shards 4 --resume &
#    wait
#    python itf_combined_scraper.py --combine
#    python itf_combined_scraper.py --apply
#
# =============================================================
# Output files:
#    itf_combined_scrape_shard0of4.csv  (raw shard outputs)
#    itf_combined_scrape_shard1of4.csv
#    itf_combined_scrape_shard2of4.csv
#    itf_combined_scrape_shard3of4.csv
#    itf_combined_scrape_combined.csv   (after --combine)
#    Updated ITF CSV                    (after --apply)
# =============================================================
