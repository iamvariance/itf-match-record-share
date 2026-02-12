"""
ITF Match Record: Fix 0 → NaN for semantically missing data.

The ITF file has ~1.84 million cells that are 0 but should be NaN.
This script applies the same NaN logic proven in pbp_stats_calculator.py
and wta_main_pbp_stats2.1.csv.

Rules applied:
  1. All s3 columns → NaN for 2-set matches (match_score ∈ {2-0, 0-2})
  2. All per-set TB columns → NaN for non-tiebreak sets (score ≠ 7-6/6-7)
  3. Overall TB columns → NaN for matches with zero tiebreaks
  4. s1_mp_* → always NaN (match point impossible in Set 1)
  5. s2_mp logic (loser of s1 can't have MP opps in s2; winner can't face MP)
  6. bp/sp/mp saved/lost → NaN when faced == 0
  7. bp/sp/mp converted → NaN when opportunities == 0
  8. Incomplete matches (1-0, 0-1, 0-0, 1-1) → unplayed set stats → NaN

Usage:
    python itf_fix_nan.py
    python itf_fix_nan.py --input ITF_file.csv --output ITF_file_fixed.csv
    python itf_fix_nan.py --dry-run   # Report only, don't write
"""

import os
import argparse
import pandas as pd
import numpy as np
from datetime import datetime


# ============================================
# COLUMN DEFINITIONS
# ============================================

# All stat suffixes that exist per-set
# 20 stat suffixes per side per set (verified from actual ITF CSV headers)
PER_SET_STATS = [
    'service_pts_won', 'service_pts_played',
    'return_pts_won', 'return_pts_played',
    'service_games_won', 'service_games_played',
    'return_games_won', 'return_games_played',
    'tb_serve_pts_won', 'tb_serve_pts_played',
    'tb_return_pts_won', 'tb_return_pts_played',
    'bp_saved', 'bp_faced', 'bp_converted', 'bp_opportunities',
    'sp_saved', 'sp_faced',
    'mp_saved', 'mp_faced',
]

TB_STATS = ['tb_serve_pts_won', 'tb_serve_pts_played', 'tb_return_pts_won', 'tb_return_pts_played']

# Conditional NaN rules: saved → NaN when faced == 0, converted → NaN when opportunities == 0
# Note: ITF only has saved/faced for SP and MP (no converted/opportunities)
CONDITIONAL_NAN_PAIRS = {
    'bp': {'saved_col': 'bp_saved', 'faced_col': 'bp_faced',
            'conv_col': 'bp_converted', 'opp_col': 'bp_opportunities'},
    'sp': {'saved_col': 'sp_saved', 'faced_col': 'sp_faced'},
    'mp': {'saved_col': 'mp_saved', 'faced_col': 'mp_faced'},
}

SIDES = ['home', 'away']


def log(msg):
    ts = datetime.now().strftime('%H:%M:%S')
    print(f"[{ts}] {msg}")


def count_nans(df, columns):
    """Count NaN cells in specified columns."""
    return df[columns].isna().sum().sum()


def main():
    parser = argparse.ArgumentParser(description="Fix 0 → NaN in ITF match record")
    parser.add_argument("--input", default="ITF_Flashscore_2019on_MatchRecord_FIXED_with_scores_and_sets.csv")
    parser.add_argument("--output", default=None, help="Output file (default: overwrite input with backup)")
    parser.add_argument("--dry-run", action="store_true", help="Report changes without writing")
    args = parser.parse_args()

    log(f"Loading {args.input}...")
    df = pd.read_csv(args.input, low_memory=False)
    log(f"Loaded {len(df):,} matches, {len(df.columns)} columns")

    # Track changes
    total_cells_fixed = 0
    change_log = {}

    # Identify all stat columns (cols 28+)
    stat_cols = list(df.columns[28:])
    initial_zeros = (df[stat_cols] == 0).sum().sum()
    initial_nans = df[stat_cols].isna().sum().sum()
    log(f"Before fix: {initial_zeros:,} zeros, {initial_nans:,} NaNs in stat columns")

    # ========================================
    # RULE 1: s3 columns → NaN for matches without Set 3
    # ========================================
    log("Rule 1: Setting s3 stats to NaN for matches without Set 3...")
    
    # Matches without a complete Set 3
    no_s3 = df['home_set3'].isna() | (df['match_score'].isin(['2-0', '0-2']))
    # Also handle incomplete matches where Set 3 wasn't played
    no_s3 = no_s3 | (df['match_score'].isin(['1-0', '0-1', '0-0']))
    # 1-1 matches might need special handling - check if Set 3 was actually played
    mask_11 = df['match_score'] == '1-1'
    no_s3_11 = mask_11 & df['home_set3'].isna()
    no_s3 = no_s3 | no_s3_11

    s3_cols = [c for c in stat_cols if '_s3_' in c]
    cells_before = count_nans(df, s3_cols)
    for col in s3_cols:
        df.loc[no_s3, col] = np.nan
    cells_after = count_nans(df, s3_cols)
    fixed = cells_after - cells_before
    total_cells_fixed += fixed
    change_log['Rule 1: s3 for non-3-set matches'] = fixed
    log(f"  → {fixed:,} cells fixed ({no_s3.sum():,} matches affected)")

    # ========================================
    # RULE 1b: s2 columns → NaN for matches without Set 2
    # ========================================
    log("Rule 1b: Setting s2 stats to NaN for matches without Set 2...")
    no_s2 = df['home_set2'].isna() | (df['match_score'].isin(['0-0']))
    # 1-0 or 0-1 with no set2 data
    mask_10_01 = df['match_score'].isin(['1-0', '0-1'])
    no_s2_partial = mask_10_01 & df['home_set2'].isna()
    no_s2 = no_s2 | no_s2_partial

    s2_cols = [c for c in stat_cols if '_s2_' in c]
    cells_before = count_nans(df, s2_cols)
    for col in s2_cols:
        df.loc[no_s2, col] = np.nan
    cells_after = count_nans(df, s2_cols)
    fixed = cells_after - cells_before
    total_cells_fixed += fixed
    change_log['Rule 1b: s2 for non-2-set matches'] = fixed
    log(f"  → {fixed:,} cells fixed ({no_s2.sum():,} matches affected)")

    # ========================================
    # RULE 2: Per-set TB columns → NaN for non-TB sets
    # ========================================
    log("Rule 2: Setting per-set TB stats to NaN for non-tiebreak sets...")

    tb_per_set_cols = {}
    for set_n in [1, 2, 3]:
        tb_per_set_cols[set_n] = [f'{side}_s{set_n}_{stat}' 
                                   for side in SIDES for stat in TB_STATS
                                   if f'{side}_s{set_n}_{stat}' in df.columns]

    rule2_fixed = 0
    for set_n in [1, 2, 3]:
        h_col, a_col = f'home_set{set_n}', f'away_set{set_n}'
        if h_col not in df.columns:
            continue
        
        # Set was played but NOT a tiebreak (not 7-6 or 6-7)
        set_played = df[h_col].notna() & df[a_col].notna()
        is_tb = ((df[h_col] == 7) & (df[a_col] == 6)) | ((df[h_col] == 6) & (df[a_col] == 7))
        not_tb = set_played & ~is_tb

        cols = tb_per_set_cols.get(set_n, [])
        if cols:
            cells_before = count_nans(df, cols)
            for col in cols:
                df.loc[not_tb, col] = np.nan
            cells_after = count_nans(df, cols)
            fixed = cells_after - cells_before
            rule2_fixed += fixed
            log(f"  Set {set_n}: {fixed:,} cells fixed ({not_tb.sum():,} non-TB sets)")

    total_cells_fixed += rule2_fixed
    change_log['Rule 2: per-set TB for non-TB sets'] = rule2_fixed

    # ========================================
    # RULE 3: Overall TB columns → NaN if no TB in entire match
    # ========================================
    log("Rule 3: Setting overall TB stats to NaN for matches with no tiebreaks...")

    # A match has a TB if any set is 7-6 or 6-7
    has_tb_s1 = ((df['home_set1'] == 7) & (df['away_set1'] == 6)) | ((df['home_set1'] == 6) & (df['away_set1'] == 7))
    has_tb_s2 = ((df['home_set2'] == 7) & (df['away_set2'] == 6)) | ((df['home_set2'] == 6) & (df['away_set2'] == 7))
    has_tb_s3 = False  # Default
    if 'home_set3' in df.columns:
        has_tb_s3 = ((df['home_set3'] == 7) & (df['away_set3'] == 6)) | ((df['home_set3'] == 6) & (df['away_set3'] == 7))
    
    # Fill NaN in bool series with False
    has_tb_s1 = has_tb_s1.fillna(False)
    has_tb_s2 = has_tb_s2.fillna(False)
    if isinstance(has_tb_s3, pd.Series):
        has_tb_s3 = has_tb_s3.fillna(False)

    no_tb_match = ~(has_tb_s1 | has_tb_s2 | has_tb_s3)

    overall_tb_cols = [f'{side}_{stat}' for side in SIDES for stat in TB_STATS
                       if f'{side}_{stat}' in df.columns]
    
    cells_before = count_nans(df, overall_tb_cols)
    for col in overall_tb_cols:
        df.loc[no_tb_match, col] = np.nan
    cells_after = count_nans(df, overall_tb_cols)
    fixed = cells_after - cells_before
    total_cells_fixed += fixed
    change_log['Rule 3: overall TB for no-TB matches'] = fixed
    log(f"  → {fixed:,} cells fixed ({no_tb_match.sum():,} matches)")

    # ========================================
    # RULE 4: s1_mp_* → ALWAYS NaN (match point impossible in Set 1)
    # ========================================
    log("Rule 4: Setting s1_mp to NaN (match point impossible in Set 1)...")

    s1_mp_cols = [f'{side}_s1_mp_{stat}' for side in SIDES 
                  for stat in ['saved', 'faced', 'converted', 'opportunities']
                  if f'{side}_s1_mp_{stat}' in df.columns]

    cells_before = count_nans(df, s1_mp_cols)
    for col in s1_mp_cols:
        df[col] = np.nan
    cells_after = count_nans(df, s1_mp_cols)
    fixed = cells_after - cells_before
    total_cells_fixed += fixed
    change_log['Rule 4: s1_mp always NaN'] = fixed
    log(f"  → {fixed:,} cells fixed")

    # ========================================
    # RULE 5: s2_mp logic based on Set 1 winner
    # ========================================
    log("Rule 5: Setting s2_mp based on Set 1 winner/loser...")

    rule5_fixed = 0
    for idx in df.index:
        h_s1 = df.at[idx, 'home_set1']
        a_s1 = df.at[idx, 'away_set1']
        
        if pd.isna(h_s1) or pd.isna(a_s1):
            continue
        
        try:
            h_s1, a_s1 = int(h_s1), int(a_s1)
        except (ValueError, TypeError):
            continue
        
        if h_s1 > a_s1:
            set1_winner, set1_loser = 'home', 'away'
        elif a_s1 > h_s1:
            set1_winner, set1_loser = 'away', 'home'
        else:
            continue  # Tied set shouldn't happen in completed matches
        
        # Match point logic in best-of-3 Set 2:
        # Set 1 WINNER leads 1-0. In Set 2, they can earn match points (one set from winning).
        # Set 1 LOSER trails 0-1. Even winning Set 2 only gives 1-1. CANNOT earn match points.
        #
        # Therefore:
        # - set1_winner's mp_faced/mp_saved in S2 → NaN
        #   (loser is down 0-1, can't create match points against winner)
        # - set1_loser's mp_faced/mp_saved in S2 → VALID
        #   (winner IS up 1-0, CAN create match points the loser must face/save)
        for stat in ['mp_faced', 'mp_saved']:
            col = f'{set1_winner}_s2_{stat}'
            if col in df.columns and df.at[idx, col] == 0:
                df.at[idx, col] = np.nan
                rule5_fixed += 1

    total_cells_fixed += rule5_fixed
    change_log['Rule 5: s2_mp winner/loser logic'] = rule5_fixed
    log(f"  → {rule5_fixed:,} cells fixed")

    # ========================================
    # RULE 6: saved → NaN when faced == 0
    # RULE 7: converted → NaN when opportunities == 0
    # (Only BP has converted/opportunities; SP and MP only have saved/faced)
    # ========================================
    log("Rules 6-7: Conditional NaN for bp/sp/mp saved/converted...")

    rule67_fixed = 0
    for side in SIDES:
        for pt_type, cols in CONDITIONAL_NAN_PAIRS.items():
            # Overall: saved → NaN if faced == 0
            faced_col = f'{side}_{cols["faced_col"]}'
            saved_col = f'{side}_{cols["saved_col"]}'
            if faced_col in df.columns and saved_col in df.columns:
                mask = (df[faced_col] == 0) & (df[saved_col] == 0) & df[saved_col].notna()
                count = mask.sum()
                df.loc[mask, saved_col] = np.nan
                rule67_fixed += count

            # Overall: converted → NaN if opportunities == 0 (BP only)
            if 'conv_col' in cols and 'opp_col' in cols:
                opp_col = f'{side}_{cols["opp_col"]}'
                conv_col = f'{side}_{cols["conv_col"]}'
                if opp_col in df.columns and conv_col in df.columns:
                    mask = (df[opp_col] == 0) & (df[conv_col] == 0) & df[conv_col].notna()
                    count = mask.sum()
                    df.loc[mask, conv_col] = np.nan
                    rule67_fixed += count

            # Per-set
            for set_n in [1, 2, 3]:
                prefix = f's{set_n}'
                faced_col = f'{side}_{prefix}_{cols["faced_col"]}'
                saved_col = f'{side}_{prefix}_{cols["saved_col"]}'
                if faced_col in df.columns and saved_col in df.columns:
                    mask = df[faced_col].notna() & (df[faced_col] == 0) & df[saved_col].notna() & (df[saved_col] == 0)
                    count = mask.sum()
                    df.loc[mask, saved_col] = np.nan
                    rule67_fixed += count

                if 'conv_col' in cols and 'opp_col' in cols:
                    opp_col = f'{side}_{prefix}_{cols["opp_col"]}'
                    conv_col = f'{side}_{prefix}_{cols["conv_col"]}'
                    if opp_col in df.columns and conv_col in df.columns:
                        mask = df[opp_col].notna() & (df[opp_col] == 0) & df[conv_col].notna() & (df[conv_col] == 0)
                        count = mask.sum()
                        df.loc[mask, conv_col] = np.nan
                        rule67_fixed += count

    total_cells_fixed += rule67_fixed
    change_log['Rules 6-7: conditional bp/sp/mp NaN'] = rule67_fixed
    log(f"  → {rule67_fixed:,} cells fixed")

    # ========================================
    # SUMMARY
    # ========================================
    final_zeros = (df[stat_cols].fillna(-999) == 0).sum().sum()  # Don't count NaN as zero
    final_nans = df[stat_cols].isna().sum().sum()

    log(f"\n{'='*60}")
    log(f"  FIX SUMMARY")
    log(f"{'='*60}")
    log(f"  Total cells fixed (0 → NaN): {total_cells_fixed:,}")
    log(f"  Before: {initial_zeros:,} zeros, {initial_nans:,} NaNs")
    log(f"  After:  {final_zeros:,} zeros, {final_nans:,} NaNs")
    log(f"")
    for rule, count in change_log.items():
        log(f"  {rule}: {count:,}")

    # ========================================
    # VALIDATION CHECKS
    # ========================================
    log(f"\n{'='*60}")
    log(f"  VALIDATION")
    log(f"{'='*60}")

    # Check: no s3 stats should be non-NaN for 2-set matches
    two_set_mask = df['match_score'].isin(['2-0', '0-2'])
    s3_residual = df.loc[two_set_mask, s3_cols].notna().sum().sum()
    log(f"  s3 non-NaN in 2-set matches: {s3_residual} (should be 0)")

    # Check: s1_mp should all be NaN
    s1_mp_residual = df[s1_mp_cols].notna().sum().sum()
    log(f"  s1_mp non-NaN values: {s1_mp_residual} (should be 0)")

    # Check: overall TB non-NaN in no-TB matches
    tb_residual = df.loc[no_tb_match, overall_tb_cols].notna().sum().sum()
    log(f"  Overall TB non-NaN in no-TB matches: {tb_residual} (should be ~0, may have anomalies)")

    # ========================================
    # WRITE OUTPUT
    # ========================================
    if args.dry_run:
        log("\n  DRY RUN — no file written.")
    else:
        output_file = args.output or args.input
        if output_file == args.input:
            # Create backup
            backup = args.input.replace('.csv', f'_backup_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv')
            log(f"\n  Creating backup: {backup}")
            import shutil
            shutil.copy2(args.input, backup)
        
        log(f"  Writing to {output_file}...")
        df.to_csv(output_file, index=False)
        log(f"  ✅ Done. {len(df):,} rows, {len(df.columns)} columns written.")


if __name__ == "__main__":
    main()
