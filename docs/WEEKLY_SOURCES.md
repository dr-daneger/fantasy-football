Weekly Projections Sources and Verification

- Source library: R `ffpros` (`fp_projections`) with `page = <pos>`, `year = <YEAR>`, `week = <WEEK>`, `scoring = <SCORING>`.
- FantasyPros pages (manually verifiable):
  - QB:  https://www.fantasypros.com/nfl/projections/qb.php?week=<WEEK>&scoring=<SCORING>
  - RB:  https://www.fantasypros.com/nfl/projections/rb.php?week=<WEEK>&scoring=<SCORING>
  - WR:  https://www.fantasypros.com/nfl/projections/wr.php?week=<WEEK>&scoring=<SCORING>
  - TE:  https://www.fantasypros.com/nfl/projections/te.php?week=<WEEK>&scoring=<SCORING>
  - K:   https://www.fantasypros.com/nfl/projections/k.php?week=<WEEK>&scoring=<SCORING>
  - DST: https://www.fantasypros.com/nfl/projections/dst.php?week=<WEEK>&scoring=<SCORING>

How it maps into JSON
- The `projections.weekly` object is built from `data/staging/fp_weekly.parquet` (written by `r/ffpros_ingest.R`).
- Fields:
  - `fantasy_points`: `weekly_avg` from ingestor (from FP points when provided; else midpoint of `weekly_floor`/`weekly_ceiling`).
  - `rank_overall`, `rank_pos`: computed by sorting on `weekly_avg` overall and by position.
  - `stats`: numeric stat columns per position (e.g., `passing_att`, `passing_yds`, `rushing_att`, `receiving_rec`, etc.).
  - Note: Weekly ECR and uncertainty (`sd`, `floor`, `ceiling`) are intentionally not included in the JSON.

Verification quick steps
1) Run: `FP_YEAR=2025 FP_WEEK=1 FP_SCORING=PPR Rscript r/ffpros_ingest.R`
2) Inspect: `data/staging/fp_weekly.parquet` (or `fp_weekly.csv`) for a player and position.
3) Cross‑check the player’s `fantasypts`/`weekly_avg` and fractional stats with the corresponding projections page above (same week & scoring).
