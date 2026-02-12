"""
ITF Home/Away Auditor — Verifies player home/away assignment by scraping Flashscore pages.

The match_url player ordering is NOT reliable.
The Flashscore PAGE is the source of truth:
  - Top player (duelParticipant__home) = player_home
  - Bottom player (duelParticipant__away) = player_away

This script visits each match URL and compares the page's home/away
against the CSV's home/away. Produces a corrections report.

Usage (4 parallel shards):
    python itf_home_away_auditor.py --shard 0 --total-shards 4 --resume
    python itf_home_away_auditor.py --shard 1 --total-shards 4 --resume
    python itf_home_away_auditor.py --shard 2 --total-shards 4 --resume
    python itf_home_away_auditor.py --shard 3 --total-shards 4 --resume

After all shards finish, combine and apply:
    python itf_home_away_auditor.py --combine
"""

import os
import re
import sys
import time
import signal
import random
import argparse
import pandas as pd
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
from webdriver_manager.chrome import ChromeDriverManager


# ============================================
# CONFIG
# ============================================

INPUT_FILE = "ITF_Flashscore_2019on_MatchRecord_FIXED_with_scores_and_sets.csv"
OUTPUT_BASE = "itf_home_away_audit"

DELAY_BETWEEN_MATCHES = (0.4, 0.8)
SAVE_EVERY = 50
HEADLESS = True
MAX_RETRIES = 3

# Graceful shutdown
SHUTDOWN_REQUESTED = False

def signal_handler(sig, frame):
    global SHUTDOWN_REQUESTED
    print("\n[SIGNAL] Shutdown requested. Finishing current match...")
    SHUTDOWN_REQUESTED = True

signal.signal(signal.SIGINT, signal_handler)
if hasattr(signal, 'SIGTERM'):
    signal.signal(signal.SIGTERM, signal_handler)


def log(msg):
    ts = datetime.now().strftime('%H:%M:%S')
    print(f"[{ts}] {msg}")


# ============================================
# SELENIUM SETUP
# ============================================

COOKIE_ACCEPTED = False

def create_driver():
    options = webdriver.ChromeOptions()
    if HEADLESS:
        options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    options.add_experimental_option("excludeSwitches", ["enable-logging"])
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    driver.set_page_load_timeout(30)
    return driver


def accept_cookies(driver):
    global COOKIE_ACCEPTED
    if COOKIE_ACCEPTED:
        return
    candidates = [
        (By.ID, "onetrust-accept-btn-handler"),
        (By.CSS_SELECTOR, '[aria-label="Accept all"]'),
        (By.XPATH, '//button[contains(text(),"Accept")]'),
    ]
    for by, sel in candidates:
        try:
            btn = WebDriverWait(driver, 2).until(EC.element_to_be_clickable((by, sel)))
            driver.execute_script("arguments[0].click();", btn)
            COOKIE_ACCEPTED = True
            log("Cookie consent accepted")
            return
        except:
            continue
    COOKIE_ACCEPTED = True  # Don't retry


def safe_get(driver, url, retries=MAX_RETRIES):
    """Load URL with retries, recreating driver on failure."""
    global COOKIE_ACCEPTED
    for attempt in range(retries):
        try:
            driver.get(url)
            time.sleep(1.5 + random.uniform(0, 0.5))
            accept_cookies(driver)
            return driver
        except Exception as e:
            log(f"  safe_get attempt {attempt+1}/{retries} failed: {e}")
            if attempt < retries - 1:
                try:
                    driver.quit()
                except:
                    pass
                driver = create_driver()
                COOKIE_ACCEPTED = False
                time.sleep(2)
    return driver


def extract_id_from_href(href):
    """Extract player ID from href like /player/name/ID/"""
    if not href:
        return None
    match = re.search(r'/player/[^/]+/([A-Za-z0-9]+)/?', href)
    return match.group(1) if match else None


# ============================================
# SCRAPING
# ============================================

def scrape_home_away(driver, url):
    """Scrape correct home/away from the Flashscore page DOM."""
    result = {
        'page_home_name': None,
        'page_home_id': None,
        'page_away_name': None,
        'page_away_id': None,
        'list_date_time': None,
        'error': None,
    }

    try:
        driver = safe_get(driver, url)

        # HOME player (top - duelParticipant__home)
        try:
            home_div = driver.find_element(By.CSS_SELECTOR, 'div.duelParticipant__home')
            home_name_el = home_div.find_element(By.CSS_SELECTOR, 
                'a.participant__participantName, div.participant__participantName')
            result['page_home_name'] = home_name_el.text.strip()
            try:
                home_link = home_div.find_element(By.CSS_SELECTOR, 'a.participant__participantLink')
                result['page_home_id'] = extract_id_from_href(home_link.get_attribute('href'))
            except:
                pass  # Some ITF players may not have profile links
        except Exception as e:
            result['error'] = f"Home extraction failed: {e}"
            return result, driver

        # AWAY player (bottom - duelParticipant__away)
        try:
            away_div = driver.find_element(By.CSS_SELECTOR, 'div.duelParticipant__away')
            away_name_el = away_div.find_element(By.CSS_SELECTOR, 
                'a.participant__participantName, div.participant__participantName')
            result['page_away_name'] = away_name_el.text.strip()
            try:
                away_link = away_div.find_element(By.CSS_SELECTOR, 'a.participant__participantLink')
                result['page_away_id'] = extract_id_from_href(away_link.get_attribute('href'))
            except:
                pass
        except Exception as e:
            result['error'] = f"Away extraction failed: {e}"
            return result, driver

        # Date/time (bonus — fill missing list_date_time)
        try:
            time_div = driver.find_element(By.CSS_SELECTOR, 'div.duelParticipant__startTime div')
            result['list_date_time'] = time_div.text.strip()
        except:
            pass

    except Exception as e:
        result['error'] = str(e)

    return result, driver


def determine_status(csv_home_id, csv_away_id, page_home_id, page_away_id,
                     csv_home_name, csv_away_name, page_home_name, page_away_name):
    """
    Determine if home/away assignment is correct, swapped, or unknown.
    Uses player IDs as primary comparison, falls back to name matching.
    """
    # Try ID-based comparison first
    if page_home_id and page_away_id and csv_home_id and csv_away_id:
        if csv_home_id == page_home_id and csv_away_id == page_away_id:
            return 'correct', 'id_match'
        if csv_home_id == page_away_id and csv_away_id == page_home_id:
            return 'swapped', 'id_match'
    
    # Fall back to name-based comparison
    def normalize(name):
        if not name:
            return ""
        return re.sub(r'[^a-z]', '', name.lower().strip())
    
    csv_h = normalize(csv_home_name)
    csv_a = normalize(csv_away_name)
    page_h = normalize(page_home_name)
    page_a = normalize(page_away_name)
    
    if csv_h and page_h:
        # Check if names match (allowing for truncation, init differences)
        # Use surname as primary match signal
        csv_h_surname = csv_home_name.split()[-1].lower() if csv_home_name else ""
        csv_a_surname = csv_away_name.split()[-1].lower() if csv_away_name else ""
        page_h_surname = page_home_name.split()[-1].lower() if page_home_name else ""
        page_a_surname = page_away_name.split()[-1].lower() if page_away_name else ""
        
        if csv_h_surname == page_h_surname and csv_a_surname == page_a_surname:
            return 'correct', 'name_match'
        if csv_h_surname == page_a_surname and csv_a_surname == page_h_surname:
            return 'swapped', 'name_match'
    
    return 'unknown', 'no_match'


# ============================================
# COMBINE MODE
# ============================================

def combine_shards(output_base):
    """Combine all shard outputs into a single file."""
    import glob
    pattern = f"{output_base}_shard*of*.csv"
    shard_files = sorted(glob.glob(pattern))
    
    if not shard_files:
        log(f"No shard files found matching {pattern}")
        return
    
    log(f"Found {len(shard_files)} shard files:")
    dfs = []
    for f in shard_files:
        df = pd.read_csv(f)
        log(f"  {f}: {len(df)} rows")
        dfs.append(df)
    
    combined = pd.concat(dfs, ignore_index=True)
    combined = combined.drop_duplicates(subset='match_uid', keep='last')
    
    out_file = f"{output_base}_combined.csv"
    combined.to_csv(out_file, index=False)
    
    # Summary
    total = len(combined)
    correct = (combined['status'] == 'correct').sum()
    swapped = (combined['status'] == 'swapped').sum()
    unknown = (combined['status'] == 'unknown').sum()
    errors = combined['error'].notna().sum()
    
    log(f"\n{'='*60}")
    log(f"  COMBINED AUDIT RESULTS")
    log(f"{'='*60}")
    log(f"  Total matches audited: {total:,}")
    log(f"  Correct:  {correct:,} ({correct/total*100:.1f}%)")
    log(f"  Swapped:  {swapped:,} ({swapped/total*100:.1f}%)")
    log(f"  Unknown:  {unknown:,} ({unknown/total*100:.1f}%)")
    log(f"  Errors:   {errors:,} ({errors/total*100:.1f}%)")
    log(f"  Saved to: {out_file}")


# ============================================
# MAIN
# ============================================

def main():
    parser = argparse.ArgumentParser(description="ITF Home/Away Auditor")
    parser.add_argument("--input", default=INPUT_FILE)
    parser.add_argument("--output-base", default=OUTPUT_BASE)
    parser.add_argument("--shard", type=int, default=0, help="Shard index (0-based)")
    parser.add_argument("--total-shards", type=int, default=1, help="Total shards")
    parser.add_argument("--resume", action="store_true", help="Skip already-audited matches")
    parser.add_argument("--limit", type=int, default=0, help="Max matches (0=unlimited)")
    parser.add_argument("--combine", action="store_true", help="Combine shard outputs")
    args = parser.parse_args()

    if args.combine:
        combine_shards(args.output_base)
        return

    output_file = f"{args.output_base}_shard{args.shard}of{args.total_shards}.csv"

    log(f"\n{'='*60}")
    log(f"  ITF HOME/AWAY AUDITOR")
    log(f"  Shard {args.shard}/{args.total_shards}")
    log(f"  Input: {args.input}")
    log(f"  Output: {output_file}")
    log(f"{'='*60}\n")

    # Validate input
    if not os.path.exists(args.input):
        log(f"ERROR: Input file not found: {args.input}")
        sys.exit(1)

    df = pd.read_csv(args.input, low_memory=False)
    required = ['match_uid', 'match_url', 'player_home', 'player_away', 'player_home_id', 'player_away_id']
    missing = [c for c in required if c not in df.columns]
    if missing:
        log(f"ERROR: Missing required columns: {missing}")
        sys.exit(1)

    log(f"Loaded {len(df):,} matches")

    # Shard
    df = df.iloc[args.shard::args.total_shards].copy().reset_index(drop=True)
    log(f"This shard: {len(df):,} matches")

    # Resume
    existing_uids = set()
    if args.resume and os.path.exists(output_file):
        try:
            existing = pd.read_csv(output_file)
            existing_uids = set(existing['match_uid'].dropna().astype(str))
            log(f"Resuming: {len(existing_uids):,} already audited, skipping")
        except:
            pass

    # Limit
    if args.limit > 0:
        df = df.head(args.limit)
        log(f"Limited to {args.limit} matches")

    # Create driver
    driver = create_driver()

    results = []
    processed = 0
    correct = 0
    swapped = 0
    unknown = 0
    errors = 0

    try:
        for idx, row in df.iterrows():
            if SHUTDOWN_REQUESTED:
                log("Shutdown requested. Stopping...")
                break

            match_uid = str(row['match_uid'])
            if match_uid in existing_uids:
                continue

            url = row['match_url']
            csv_home_name = str(row['player_home'])
            csv_away_name = str(row['player_away'])
            csv_home_id = str(row['player_home_id'])
            csv_away_id = str(row['player_away_id'])

            processed += 1
            remaining = len(df) - len(existing_uids) - processed
            
            # Scrape
            info, driver = scrape_home_away(driver, url)

            if info['error']:
                errors += 1
                status = 'error'
                match_method = None
                log(f"  [{processed}] {match_uid} ERROR: {info['error']}")
            else:
                status, match_method = determine_status(
                    csv_home_id, csv_away_id, info['page_home_id'], info['page_away_id'],
                    csv_home_name, csv_away_name, info['page_home_name'], info['page_away_name']
                )
                if status == 'correct':
                    correct += 1
                elif status == 'swapped':
                    swapped += 1
                else:
                    unknown += 1
                
                if status != 'correct':
                    log(f"  [{processed}] {match_uid} {status.upper()} ({match_method}) | "
                        f"CSV: {csv_home_name} vs {csv_away_name} | "
                        f"Page: {info['page_home_name']} vs {info['page_away_name']}")

            results.append({
                'match_uid': match_uid,
                'status': status,
                'match_method': match_method,
                'csv_home_name': csv_home_name,
                'csv_home_id': csv_home_id,
                'csv_away_name': csv_away_name,
                'csv_away_id': csv_away_id,
                'page_home_name': info['page_home_name'],
                'page_home_id': info['page_home_id'],
                'page_away_name': info['page_away_name'],
                'page_away_id': info['page_away_id'],
                'list_date_time': info['list_date_time'],
                'error': info['error'],
            })

            # Periodic save
            if len(results) >= SAVE_EVERY:
                save_results(output_file, results)
                log(f"  Saved {len(results)} results | C:{correct} S:{swapped} U:{unknown} E:{errors} | ~{remaining} left")
                results = []

            # Progress log
            if processed % 100 == 0:
                log(f"  Progress: {processed:,} done | C:{correct} S:{swapped} U:{unknown} E:{errors}")

            time.sleep(random.uniform(*DELAY_BETWEEN_MATCHES))

    except KeyboardInterrupt:
        log("Interrupted — saving progress...")

    finally:
        if results:
            save_results(output_file, results)
            log(f"Saved final {len(results)} results")

        try:
            driver.quit()
        except:
            pass

        log(f"\n{'='*60}")
        log(f"  SUMMARY — Shard {args.shard}/{args.total_shards}")
        log(f"{'='*60}")
        log(f"  Processed: {processed:,}")
        log(f"  Correct:   {correct:,}")
        log(f"  Swapped:   {swapped:,}")
        log(f"  Unknown:   {unknown:,}")
        log(f"  Errors:    {errors:,}")
        log(f"  Output:    {output_file}")


def save_results(output_file, results):
    """Append results to CSV."""
    df_new = pd.DataFrame(results)
    if os.path.exists(output_file):
        df_new.to_csv(output_file, mode='a', index=False, header=False)
    else:
        df_new.to_csv(output_file, index=False)


if __name__ == "__main__":
    main()
