# ITF Combined Scraper — Handoff Notes

## What Was Done

A full run of `itf_combined_scraper.py` was executed across 4 shards on a secondary PC, scraping Flashscore match pages for ~33,992 ITF tennis matches. The scraper visits each match URL and extracts:

1. **Home/Away verification** — confirms whether the CSV's home/away player assignment matches the Flashscore page (via player ID matching)
2. **Tiebreak loser scores** — the losing side's tiebreak score for each set (sets 1–3)
3. **Match & set durations** — overall match time plus per-set times
4. **Date/time** of the match
5. **Court surface** — Hard, Clay, Grass + indoor/outdoor indicator

## Shard Status

| Shard | Rows | Status | Notes |
|-------|------|--------|-------|
| 0 | 8,498 | ✅ Complete | Zero errors |
| 1 | 8,498 | ✅ Complete | Zero errors |
| 2 | 7,650 | ⚠️ INCOMPLETE | Crashed at ~90.5% — `OSError: No space left on device`. **848 matches missing.** |
| 3 | 8,498 | ✅ Complete | Zero errors |

**Total scraped: 33,144 / 33,992 (97.5%)**

## What Needs to Be Done

### 1. Complete Shard 2 (848 missing matches)
Run this command — the `--resume` flag will skip the 7,650 already-scraped matches and pick up where it left off:

```bash
python itf_combined_scraper.py --shard 2 --total-shards 4 --resume
```

### 2. Combine All Shards
After shard 2 finishes:

```bash
python itf_combined_scraper.py --combine
```

This merges all 4 shard CSVs into `itf_combined_scrape_combined.csv`.

### 3. Apply Results to Main CSV
```bash
python itf_combined_scraper.py --apply
```

This writes the scraped data back into the main `ITF_Flashscore_2019on_MatchRecord_FIXED_with_scores_and_sets.csv`.

## Key Findings from Completed Shards

### Home/Away Accuracy: 100% Correct
- **33,143 / 33,143 CORRECT** (the 1 remaining row was a network error, not a mismatch)
- **Zero swaps detected** — every home/away assignment in the CSV already matches Flashscore
- All verified via `id_match` method (player IDs compared, not just names)

### Match Times: 99.3% Coverage
- Overall match time found for **32,902 / 33,144** matches (99.3%)
- Set 1 time: 99.2% | Set 2 time: 98.2% | Set 3 time: 29.2% (only ~29% of matches go to 3 sets)
- **242 matches** missing overall time

### Tiebreak Scores: 18.8% of Matches
- **6,245 matches** had at least one tiebreak
- Set 1 TBs: 3,036 | Set 2 TBs: 2,769 | Set 3 TBs: 914

### Surface: 100% Coverage (with some cleanup needed)
- **33,143 / 33,144** have a court type value
- Clean values: **HARD** 18,477 (55.7%) | **CLAY** 14,108 (42.6%) | **GRASS** 330 (1.0%)
- **228 dirty surface values** that need cleanup:
  - `GA` (58) — likely misparsed tournament name
  - Tournament names scraped instead of surface: `W100 VITORIA` (31), `W15 CASTELLON` (31), `W75 VITORIA` (29), `W75 DOKSY` (27)
  - Indoor markers (valid but may need normalizing): `Clay (indoor)` (30), `Hard (indoor)` (20), `Grass (indoor)` (2)

## The 1 Error Row
- **match_uid**: `0ddiKM05` (Daavettila S. vs Moon J.)
- Cause: `ConnectionResetError` — network timeout during page load
- The player IDs on the page DO match the CSV, so the data is fine; it just couldn't complete the full scrape for that page
- This match is in shard 1 and will NOT be retried by `--resume` since it was already recorded. You can manually re-scrape it or ignore it.

## Modified Files
- `itf_combined_scraper.py` was modified locally to add **verbose per-match logging** (percentage progress for every match instead of only logging errors and every 200th match). This change is included in this push. It does not affect scrape behavior, only logging output.

## Output Columns in Shard CSVs
```
match_uid, ha_status, ha_method, csv_home_name, csv_home_id, csv_away_name, csv_away_id,
page_home_name, page_home_id, page_away_name, page_away_id,
page_set1_tb_home, page_set1_tb_away, page_set2_tb_home, page_set2_tb_away,
page_set3_tb_home, page_set3_tb_away,
page_set1_home, page_set1_away, page_set2_home, page_set2_away, page_set3_home, page_set3_away,
page_time_overall, page_time_set1, page_time_set2, page_time_set3,
page_date_time, page_court_type, error
```

## Environment
- Python 3.13.9 (venv)
- Dependencies: `pandas`, `selenium`, `webdriver-manager`
- Chrome headless mode
- Scrape rate: ~6–10 seconds per match with random delay (0.4–0.8s between matches)
