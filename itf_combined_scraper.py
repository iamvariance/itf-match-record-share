"""
ITF Combined Page Scraper — One pass to grab everything we need.

Visits each ITF match page ONCE and extracts:
  1. Home/Away verification (duelParticipant__home/away vs CSV)
  2. Tiebreak loser scores (from <sup> elements in score box)
  3. Match time + set times (from smh__time elements)
  4. Date/time (from duelParticipant__startTime)
  5. Court surface + indoor/outdoor (from overline header + infoBox)

This replaces the standalone itf_home_away_auditor.py. No need for
separate TB and time scrapers — everything lives on the same page.

Usage (4 parallel shards on server):
    python itf_combined_scraper.py --shard 0 --total-shards 4 --resume
    python itf_combined_scraper.py --shard 1 --total-shards 4 --resume
    python itf_combined_scraper.py --shard 2 --total-shards 4 --resume
    python itf_combined_scraper.py --shard 3 --total-shards 4 --resume

After all shards finish:
    python itf_combined_scraper.py --combine

Apply results to the ITF CSV:
    python itf_combined_scraper.py --apply
"""

import os
import re
import sys
import time
import signal
import random
import argparse
import pandas as pd
import numpy as np
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager


# ============================================
# CONFIG
# ============================================

INPUT_FILE = "ITF_Flashscore_2019on_MatchRecord_FIXED_with_scores_and_sets.csv"
OUTPUT_BASE = "itf_combined_scrape"

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
# SCRAPING — ALL DATA FROM ONE PAGE
# ============================================

def scrape_match_page(driver, url):
    """
    Visit one match page and extract everything:
      - Home/away player names + IDs
      - Tiebreak scores (from <sup> in score box)
      - Match time + set times
      - Date/time
    """
    result = {
        # Home/Away
        'page_home_name': None,
        'page_home_id': None,
        'page_away_name': None,
        'page_away_id': None,
        # TB scores (loser's TB score, the <sup> number)
        'page_set1_tb_home': None,
        'page_set1_tb_away': None,
        'page_set2_tb_home': None,
        'page_set2_tb_away': None,
        'page_set3_tb_home': None,
        'page_set3_tb_away': None,
        # Set scores from page (for cross-validation)
        'page_set1_home': None, 'page_set1_away': None,
        'page_set2_home': None, 'page_set2_away': None,
        'page_set3_home': None, 'page_set3_away': None,
        # Times
        'page_time_overall': None,
        'page_time_set1': None,
        'page_time_set2': None,
        'page_time_set3': None,
        # Date/time
        'page_date_time': None,
        # Surface
        'page_court_type': None,
        # Error
        'error': None,
    }

    try:
        driver = safe_get(driver, url)

        # ---- HOME PLAYER ----
        try:
            home_div = driver.find_element(By.CSS_SELECTOR, 'div.duelParticipant__home')
            home_name_el = home_div.find_element(By.CSS_SELECTOR,
                'a.participant__participantName, div.participant__participantName')
            result['page_home_name'] = home_name_el.text.strip()
            try:
                home_link = home_div.find_element(By.CSS_SELECTOR, 'a.participant__participantLink')
                result['page_home_id'] = extract_id_from_href(home_link.get_attribute('href'))
            except NoSuchElementException:
                pass
        except Exception as e:
            result['error'] = f"Home player extraction failed: {e}"
            return result, driver

        # ---- AWAY PLAYER ----
        try:
            away_div = driver.find_element(By.CSS_SELECTOR, 'div.duelParticipant__away')
            away_name_el = away_div.find_element(By.CSS_SELECTOR,
                'a.participant__participantName, div.participant__participantName')
            result['page_away_name'] = away_name_el.text.strip()
            try:
                away_link = away_div.find_element(By.CSS_SELECTOR, 'a.participant__participantLink')
                result['page_away_id'] = extract_id_from_href(away_link.get_attribute('href'))
            except NoSuchElementException:
                pass
        except Exception as e:
            result['error'] = f"Away player extraction failed: {e}"
            return result, driver

        # ---- SCORE BOX: Set scores + Tiebreak scores ----
        for set_num in range(1, 4):  # Sets 1–3
            for side, label in [('home', 'home'), ('away', 'away')]:
                try:
                    el = driver.find_element(By.CSS_SELECTOR,
                        f'div.smh__part.smh__{side}.smh__part--{set_num}')

                    # Get TB score from <sup> first
                    tb_score = None
                    try:
                        sup_el = el.find_element(By.CSS_SELECTOR, 'sup')
                        sup_text = sup_el.text.strip()
                        if sup_text:
                            tb_score = sup_text
                            result[f'page_set{set_num}_tb_{label}'] = tb_score
                    except NoSuchElementException:
                        pass

                    # Get set score (text node only, excluding <sup>)
                    full_text = el.text.strip()
                    if tb_score and full_text.endswith(tb_score):
                        score_text = full_text[:-len(tb_score)].strip()
                    elif tb_score:
                        score_text = full_text.replace(tb_score, "").strip()
                    else:
                        score_text = full_text

                    if score_text:
                        result[f'page_set{set_num}_{label}'] = score_text

                except NoSuchElementException:
                    pass  # Set doesn't exist (e.g. no Set 3)

        # ---- TIMES ----
        # Overall match time
        try:
            time_el = driver.find_element(By.CSS_SELECTOR, 'div.smh__time.smh__time--overall')
            result['page_time_overall'] = time_el.text.strip()
        except NoSuchElementException:
            pass

        # Per-set times (0-indexed: --0 = Set 1, --1 = Set 2, --2 = Set 3)
        for i in range(3):
            try:
                time_el = driver.find_element(By.CSS_SELECTOR, f'div.smh__time.smh__time--{i}')
                text = time_el.text.strip()
                if text:
                    result[f'page_time_set{i+1}'] = text
            except NoSuchElementException:
                pass

        # ---- DATE/TIME ----
        try:
            dt_el = driver.find_element(By.CSS_SELECTOR, 'div.duelParticipant__startTime div')
            result['page_date_time'] = dt_el.text.strip()
        except NoSuchElementException:
            pass

        # ---- SURFACE + INDOOR/OUTDOOR ----
        # Surface from overline header: "Tournament, SURFACE - Round"
        # Some tournaments have commas in name (e.g. "Raleigh, NC, HARD - QF")
        # so we take the LAST comma-segment before " - "
        surface = None
        try:
            spans = driver.find_elements(By.CSS_SELECTOR, 'span[data-testid="wcl-scores-overline-03"]')
            for span in spans:
                text = span.text.strip()
                if ',' in text and ' - ' in text:
                    before_dash = text.split(' - ', 1)[0].strip()
                    surface = before_dash.rsplit(',', 1)[-1].strip().upper()
                    break
        except:
            pass

        # Indoor detection from infoBox
        played_indoor = False
        try:
            info_boxes = driver.find_elements(By.CSS_SELECTOR, 'div.infoBox__info')
            for box in info_boxes:
                box_text = (box.text or '').strip().lower()
                if 'played indoor' in box_text:
                    played_indoor = True
                    break
        except:
            pass

        # Format: outdoor = "HARD", indoor = "HARD (indoor)"
        if surface:
            if played_indoor:
                # Title-case for readability: "Hard (indoor)", "Clay (indoor)"
                result['page_court_type'] = f"{surface.title()} (indoor)"
            else:
                result['page_court_type'] = surface

    except Exception as e:
        result['error'] = str(e)

    return result, driver


# ============================================
# HOME/AWAY COMPARISON
# ============================================

def determine_home_away_status(csv_home_id, csv_away_id, page_home_id, page_away_id,
                                csv_home_name, csv_away_name, page_home_name, page_away_name):
    """Determine if home/away assignment is correct, swapped, or unknown."""
    # ID-based (most reliable)
    if page_home_id and page_away_id and csv_home_id and csv_away_id:
        if csv_home_id == page_home_id and csv_away_id == page_away_id:
            return 'correct', 'id_match'
        if csv_home_id == page_away_id and csv_away_id == page_home_id:
            return 'swapped', 'id_match'

    # Name-based fallback (surname matching)
    def surname(name):
        return name.split()[-1].lower().strip() if name else ""

    csv_h = surname(csv_home_name)
    csv_a = surname(csv_away_name)
    page_h = surname(page_home_name)
    page_a = surname(page_away_name)

    if csv_h and page_h:
        if csv_h == page_h and csv_a == page_a:
            return 'correct', 'name_match'
        if csv_h == page_a and csv_a == page_h:
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
    correct = (combined['ha_status'] == 'correct').sum()
    swapped = (combined['ha_status'] == 'swapped').sum()
    unknown = (combined['ha_status'] == 'unknown').sum()
    errors = combined['error'].notna().sum()

    has_tb = combined[[c for c in combined.columns if c.startswith('page_set') and '_tb_' in c]].notna().any(axis=1).sum()
    has_time = combined['page_time_overall'].notna().sum()
    has_surface = combined['page_court_type'].notna().sum() if 'page_court_type' in combined.columns else 0

    log(f"\n{'='*60}")
    log(f"  COMBINED RESULTS")
    log(f"{'='*60}")
    log(f"  Total matches:  {total:,}")
    log(f"  Home/Away — Correct: {correct:,}  Swapped: {swapped:,}  Unknown: {unknown:,}")
    log(f"  Matches with TB data scraped: {has_tb:,}")
    log(f"  Matches with time data scraped: {has_time:,}")
    log(f"  Matches with surface scraped: {has_surface:,}")
    log(f"  Errors: {errors:,}")
    log(f"  Saved to: {out_file}")

    if has_surface > 0:
        surface_dist = combined['page_court_type'].value_counts()
        log(f"  Surface distribution:")
        for srf, cnt in surface_dist.items():
            log(f"    {srf}: {cnt:,}")


# ============================================
# APPLY MODE — Update the ITF CSV with scraped data
# ============================================

def apply_results(input_file, output_base):
    """Apply scraped TB scores, times, and home/away corrections to the ITF CSV."""
    combined_file = f"{output_base}_combined.csv"
    if not os.path.exists(combined_file):
        log(f"ERROR: {combined_file} not found. Run --combine first.")
        sys.exit(1)

    log(f"Loading {input_file}...")
    df = pd.read_csv(input_file, low_memory=False)
    scrape = pd.read_csv(combined_file)
    log(f"  ITF: {len(df):,} rows | Scraped: {len(scrape):,} rows")

    # Index by match_uid for fast lookup
    scrape = scrape.set_index('match_uid')
    changes = {'tb_filled': 0, 'time_filled': 0, 'ha_swapped': 0, 'datetime_filled': 0}

    for idx, row in df.iterrows():
        uid = row['match_uid']
        if uid not in scrape.index:
            continue
        s = scrape.loc[uid]

        # ---- Fill TB scores ----
        for set_n in [1, 2, 3]:
            for side in ['home', 'away']:
                tb_col = f'{side}_set{set_n}_tb'
                page_col = f'page_set{set_n}_tb_{side}'
                if tb_col in df.columns and page_col in scrape.columns:
                    page_val = s.get(page_col)
                    if pd.notna(page_val) and pd.isna(row.get(tb_col)):
                        df.at[idx, tb_col] = page_val
                        changes['tb_filled'] += 1

        # ---- Fill times ----
        time_map = {
            'time_overall': 'page_time_overall',
            'time_set1': 'page_time_set1',
            'time_set2': 'page_time_set2',
            'time_set3': 'page_time_set3',
        }
        for csv_col, page_col in time_map.items():
            if csv_col in df.columns and page_col in scrape.columns:
                page_val = s.get(page_col)
                if pd.notna(page_val) and (pd.isna(row.get(csv_col)) or str(row.get(csv_col)).strip() == ''):
                    df.at[idx, csv_col] = page_val
                    changes['time_filled'] += 1

        # ---- Fill date/time ----
        if pd.notna(s.get('page_date_time')) and (pd.isna(row.get('list_date_time')) or str(row.get('list_date_time')).strip() == ''):
            df.at[idx, 'list_date_time'] = s['page_date_time']
            changes['datetime_filled'] += 1

        # ---- Fill surface ----
        if 'page_court_type' in scrape.columns and pd.notna(s.get('page_court_type')):
            if 'court_type' not in df.columns:
                df['court_type'] = np.nan
            if pd.isna(row.get('court_type')) or str(row.get('court_type')).strip() == '':
                df.at[idx, 'court_type'] = s['page_court_type']
                changes['surface_filled'] = changes.get('surface_filled', 0) + 1

        # ---- Home/Away swap ----
        if s.get('ha_status') == 'swapped':
            # Swap player names
            df.at[idx, 'player_home'], df.at[idx, 'player_away'] = row['player_away'], row['player_home']
            df.at[idx, 'player_home_id'], df.at[idx, 'player_away_id'] = row['player_away_id'], row['player_home_id']

            # Swap all home_* <-> away_* stat columns
            home_cols = [c for c in df.columns if c.startswith('home_')]
            for hc in home_cols:
                ac = hc.replace('home_', 'away_', 1)
                if ac in df.columns:
                    df.at[idx, hc], df.at[idx, ac] = row[ac], row[hc]

            # Swap match_score (e.g., "2-0" → "0-2")
            ms = str(row.get('match_score', ''))
            if '-' in ms:
                parts = ms.split('-')
                df.at[idx, 'match_score'] = f"{parts[1]}-{parts[0]}"

            # Swap time columns (not side-specific, no swap needed)
            changes['ha_swapped'] += 1

    log(f"\n{'='*60}")
    log(f"  APPLY SUMMARY")
    log(f"{'='*60}")
    log(f"  TB scores filled:      {changes['tb_filled']:,}")
    log(f"  Time values filled:    {changes['time_filled']:,}")
    log(f"  Date/time filled:      {changes['datetime_filled']:,}")
    log(f"  Surface filled:        {changes.get('surface_filled', 0):,}")
    log(f"  Home/away swapped:     {changes['ha_swapped']:,}")

    # Backup and save
    backup = input_file.replace('.csv', f'_backup_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv')
    import shutil
    shutil.copy2(input_file, backup)
    log(f"  Backup: {backup}")

    df.to_csv(input_file, index=False)
    log(f"  Written: {input_file}")


# ============================================
# MAIN
# ============================================

def main():
    parser = argparse.ArgumentParser(description="ITF Combined Page Scraper")
    parser.add_argument("--input", default=INPUT_FILE)
    parser.add_argument("--output-base", default=OUTPUT_BASE)
    parser.add_argument("--shard", type=int, default=0, help="Shard index (0-based)")
    parser.add_argument("--total-shards", type=int, default=1, help="Total number of shards")
    parser.add_argument("--resume", action="store_true", help="Skip already-scraped matches")
    parser.add_argument("--limit", type=int, default=0, help="Max matches to process (0=unlimited)")
    parser.add_argument("--combine", action="store_true", help="Combine shard outputs")
    parser.add_argument("--apply", action="store_true", help="Apply scraped data to ITF CSV")
    args = parser.parse_args()

    if args.combine:
        combine_shards(args.output_base)
        return

    if args.apply:
        apply_results(args.input, args.output_base)
        return

    output_file = f"{args.output_base}_shard{args.shard}of{args.total_shards}.csv"

    log(f"\n{'='*60}")
    log(f"  ITF COMBINED SCRAPER")
    log(f"  Shard {args.shard}/{args.total_shards}")
    log(f"  Input: {args.input}")
    log(f"  Output: {output_file}")
    log(f"{'='*60}\n")

    if not os.path.exists(args.input):
        log(f"ERROR: Input file not found: {args.input}")
        sys.exit(1)

    df = pd.read_csv(args.input, low_memory=False)
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
            log(f"Resuming: {len(existing_uids):,} already scraped, skipping")
        except:
            pass

    # Limit
    if args.limit > 0:
        df = df.head(args.limit)
        log(f"Limited to {args.limit} matches")

    # Calculate how many we actually need to process
    total_to_process = len(df) - len(existing_uids)
    log(f"Matches to process: {total_to_process:,}")

    # Create driver
    driver = create_driver()

    results = []
    processed = 0
    stats = {'correct': 0, 'swapped': 0, 'unknown': 0, 'errors': 0,
             'tb_found': 0, 'time_found': 0, 'surface_found': 0}

    try:
        for idx, row in df.iterrows():
            if SHUTDOWN_REQUESTED:
                log("Shutdown requested. Stopping...")
                break

            match_uid = str(row['match_uid'])
            if match_uid in existing_uids:
                continue

            url = row['match_url']
            csv_home_name = str(row.get('player_home', ''))
            csv_away_name = str(row.get('player_away', ''))
            csv_home_id = str(row.get('player_home_id', ''))
            csv_away_id = str(row.get('player_away_id', ''))

            processed += 1

            # Scrape everything from this page
            info, driver = scrape_match_page(driver, url)

            pct = (processed / total_to_process * 100) if total_to_process > 0 else 0

            if info['error']:
                stats['errors'] += 1
                ha_status = 'error'
                ha_method = None
                log(f"  [{processed}/{total_to_process}] ({pct:.1f}%) ERROR: {info['error']}")
            else:
                ha_status, ha_method = determine_home_away_status(
                    csv_home_id, csv_away_id, info['page_home_id'], info['page_away_id'],
                    csv_home_name, csv_away_name, info['page_home_name'], info['page_away_name']
                )
                if ha_status == 'correct':
                    stats['correct'] += 1
                elif ha_status == 'swapped':
                    stats['swapped'] += 1
                else:
                    stats['unknown'] += 1

                # Count TB and time finds
                if any(info.get(f'page_set{n}_tb_{s}') for n in [1,2,3] for s in ['home','away']):
                    stats['tb_found'] += 1
                if info.get('page_time_overall'):
                    stats['time_found'] += 1
                if info.get('page_court_type'):
                    stats['surface_found'] += 1

                log(f"  [{processed}/{total_to_process}] ({pct:.1f}%) {ha_status.upper()} | "
                    f"{csv_home_name} vs {csv_away_name}")

            # Build result row
            result_row = {
                'match_uid': match_uid,
                'ha_status': ha_status,
                'ha_method': ha_method,
                'csv_home_name': csv_home_name,
                'csv_home_id': csv_home_id,
                'csv_away_name': csv_away_name,
                'csv_away_id': csv_away_id,
            }
            # Add all page_ fields
            for k, v in info.items():
                result_row[k] = v

            results.append(result_row)

            # Periodic save
            if len(results) >= SAVE_EVERY:
                save_results(output_file, results)
                log(f"  ** SAVED {SAVE_EVERY} | C:{stats['correct']} S:{stats['swapped']} "
                    f"U:{stats['unknown']} E:{stats['errors']} | "
                    f"TB:{stats['tb_found']} T:{stats['time_found']} Srf:{stats['surface_found']} | {pct:.1f}% done **")
                results = []

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
        log(f"  Processed:    {processed:,}")
        log(f"  H/A Correct:  {stats['correct']:,}")
        log(f"  H/A Swapped:  {stats['swapped']:,}")
        log(f"  H/A Unknown:  {stats['unknown']:,}")
        log(f"  TB found:     {stats['tb_found']:,}")
        log(f"  Times found:  {stats['time_found']:,}")
        log(f"  Surface found:{stats['surface_found']:,}")
        log(f"  Errors:       {stats['errors']:,}")
        log(f"  Output:       {output_file}")


def save_results(output_file, results):
    """Append results to CSV."""
    df_new = pd.DataFrame(results)
    if os.path.exists(output_file):
        df_new.to_csv(output_file, mode='a', index=False, header=False)
    else:
        df_new.to_csv(output_file, index=False)


if __name__ == "__main__":
    main()
