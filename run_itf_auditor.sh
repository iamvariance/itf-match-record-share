#!/bin/bash
# =============================================================
# ITF Home/Away Auditor — Server Run Commands
# =============================================================
# 1. Pull the latest code and data:
#
#    cd ~/itf-match-record-share
#    git pull origin main
#
# 2. Run all 4 shards (copy-paste each into a separate terminal/tmux pane):

# --- Shard 0 ---
python itf_home_away_auditor.py --shard 0 --total-shards 4 --resume

# --- Shard 1 ---
python itf_home_away_auditor.py --shard 1 --total-shards 4 --resume

# --- Shard 2 ---
python itf_home_away_auditor.py --shard 2 --total-shards 4 --resume

# --- Shard 3 ---
python itf_home_away_auditor.py --shard 3 --total-shards 4 --resume

# =============================================================
# 3. After ALL 4 shards finish, combine results:
#
#    python itf_home_away_auditor.py --combine
#
# =============================================================
# Or run all 4 in background at once:
#
#    python itf_home_away_auditor.py --shard 0 --total-shards 4 --resume &
#    python itf_home_away_auditor.py --shard 1 --total-shards 4 --resume &
#    python itf_home_away_auditor.py --shard 2 --total-shards 4 --resume &
#    python itf_home_away_auditor.py --shard 3 --total-shards 4 --resume &
#    wait
#    python itf_home_away_auditor.py --combine
#
# =============================================================
# Dependencies (install if needed):
#
#    pip install pandas selenium webdriver-manager
#
# =============================================================
# Output files:
#    itf_home_away_audit_shard0of4.csv
#    itf_home_away_audit_shard1of4.csv
#    itf_home_away_audit_shard2of4.csv
#    itf_home_away_audit_shard3of4.csv
#    itf_home_away_audit_combined.csv  (after --combine)
# =============================================================
