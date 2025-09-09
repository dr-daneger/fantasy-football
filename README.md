Unified Fantasy Pipeline: Sleeper + Yahoo + FantasyPros

Overview
- Goal: Produce `data/out/projections.json` with FantasyPros weekly/ROS projections plus per‑league ownership for Sleeper and Yahoo.
- Sources:
  - Sleeper: league rosters via public API
  - Yahoo: league rosters via yahoo_fantasy_api (OAuth2)
  - FantasyPros: weekly and ROS exports produced by `r/ffpros_ingest.R`

Quick Start
- FantasyPros data:
  - Ensure `data/staging/fp_weekly.parquet` and `data/staging/fp_ros.parquet` exist.
  - Or run `Rscript r/ffpros_ingest.R` (requires R + packages used by the script).
- Sleeper (recommended):
  - One‑time pull and cache league rosters by username + league name:
    - `python main.py --sleeper-username <user> --sleeper-league "<League Name>" --season 2025 --run-r-ingestor`
  - The script writes `data/staging/sleeper_rosters.parquet` for reuse.
- Yahoo (optional):
  - Create `oauth2.json` in repo root with `consumer_key` and `consumer_secret`.
  - First run pops a browser for auth. Example:
    - `python main.py --yahoo-league-url https://football.fantasysports.yahoo.com/f1/<leagueId> --season 2025`
  - Writes `data/staging/yahoo_rosters.parquet` for reuse.
- Final JSON:
  - If rosters were previously staged, you can simply run:
    - `python main.py --season 2025`
  - Output: `data/out/projections.json` (one object per FP player, with ownership status per platform).

Ownership Semantics
- For each player, two platform blocks are returned under `ownership`: `sleeper` and `yahoo`.
- The object is never null. It always contains at least `status`:
  - `OWNED`: player appears on a team roster for that platform. Includes `team_name`, `owner` (if available), and `eligible_positions`.
  - `FA`: league data is present but player is not on any roster.
  - `unavailable`: platform data was not fetched/loaded; ownership unknown, but not null.

Crosswalk (id_map) and Keys
- The crosswalk `data/staging/id_map.parquet` ties FantasyPros players to platform player IDs.
- The key used across frames is `fp_key = "<slug>|<POS>|<team>"` where:
  - `POS` is canonicalized (FantasyPros DST/D/ST -> DEF)
  - `team` is lowercase; when missing/NA it becomes `fa` (to match R output)
- Past runs may have created blank‑team keys ending with a trailing `|`. The assembler tolerates this by also looking up the alternate key (translating between `|` and `|fa`).

What Changed In This Iteration
- Fixed fp_key handling in the Python crosswalk so it no longer overwrites the canonical R key.
- Added robust ownership objects with `status` so Sleeper/Yahoo are never null in JSON.
- Added fallback lookups to reconcile `|fa` vs `|` blank‑team key mismatches.
- Added debug prints: crosswalk row counts and ownership coverage summary.
- `main()` now auto‑loads cached roster parquet files when CLI args are omitted.

Troubleshooting Guide
- Validate inputs exist:
  - `data/staging/fp_weekly.parquet` and `data/staging/fp_ros.parquet` must exist (or use `--run-r-ingestor`).
  - `data/staging/sleeper_rosters.parquet` and/or `data/staging/yahoo_rosters.parquet` should exist, or pass CLI args to pull fresh.
- Debug prints to watch for:
  - `[Crosswalk] Wrote/updated ... (N rows) — updates=..., inserts=..., no-owner-hits=...` (if you see `no-owner-hits` trending high, check name/pos/team normalization below).
  - `[Assemble] Ownership coverage — Sleeper OWNED: X, Yahoo OWNED: Y` (sanity check coverage; should be near roster counts from staging files).
- Common mismatch causes:
  - Team defenses: positions are normalized to `DEF` and defense name slugs strip `dst/defense/def` tokens before matching.
  - Missing team in FP: use of `fa` (free agent) for the team component in `fp_key`. The assembler will fall back between `|fa` and `|`.
  - Yahoo owners missing: the Yahoo API sometimes lacks `nickname`; we still populate `team_name`, and `owner` may be null.
- Regenerate crosswalk from scratch (optional):
  - Delete `data/staging/id_map.parquet` and re‑run `python main.py ...` to rebuild using the latest logic.

Data Flow Details
1) Pull Sleeper/Yahoo rosters -> write parquet under `data/staging` (idempotent caches).
2) Run R ingestor -> writes `fp_weekly.parquet` and `fp_ros.parquet`.
3) Python crosswalk -> aligns FP rows with platform IDs, writes `id_map.parquet`.
4) Assembler -> merges weekly/ROS with ownership and writes `data/out/projections.json`.

CLI Examples
- Sleeper only: `python main.py --sleeper-username <user> --sleeper-league "<League Name>" --season 2025`
- Yahoo only: `python main.py --yahoo-league-url https://football.fantasysports.yahoo.com/f1/<leagueId> --season 2025`
- Both + run R ingestor: `python main.py --sleeper-username <user> --sleeper-league "<League Name>" --yahoo-league-key <gameId>.l.<leagueId> --season 2025 --week 1 --run-r-ingestor`

Breadcrumbs for Future Agents
- Start with staged parquet files in `data/staging/` to avoid redundant network calls.
- Inspect `data/staging/id_map.parquet` to confirm keys include `|fa` when FP team is missing; search for a couple players (e.g., Mahomes, Steelers DEF).
- If ownership drops to `FA` unexpectedly, check the slug/pos/team normalization and the defense‑name slug cleaning.
- If JSON write fails with `ndarray not JSON serializable`, ensure any list‑like fields (eligible_positions) are converted to Python lists before dumping.

Notes
- Yahoo OAuth file can be pointed to via `YAHOO_OAUTH_JSON` env var; default is `oauth2.json` in repo root.
- Sleeper players blob cache is `players_nfl.json` and is safe to reuse.

**FantasyPros Data Sources**
- Library: R package `ffpros` (ffverse) which scrapes FantasyPros.com and returns tidy data frames. See https://ffpros.ffverse.com and https://github.com/DynastyProcess/ffpros.
- Script: `r/ffpros_ingest.R` calls two functions per position (QB,RB,WR,TE,K,DST):
  - `fp_projections(page=<pos>, year=<YEAR>, week=<WEEK>, scoring=<SCORING>)` → weekly projections (stats + fantasy points). Used to populate `projections.weekly` in JSON.
  - `fp_rankings(page=<pos>, year=<YEAR>, week=<WEEK>)` → weekly ECR ranks/dispersion. Attached to the weekly record when available.
  - `fp_rankings(page=paste0('ros-', <pos>), year=<YEAR>)` → rest‑of‑season (ROS) rankings. Used to populate `projections.ros` in JSON (ROS points are not provided by FP for NFL).
- Parameterization (via env vars; defaults in parentheses):
  - `FP_YEAR` (2025), `FP_WEEK` (1), `FP_SCORING` (PPR), `FP_POS` (qb,rb,wr,te,k,dst)
  - Set `FP_DEBUG=true` to print mapping diagnostics (which columns were picked for player/team/points etc.).
- Canonical key logic in the R script:
  - `fp_player_slug = slugify(player)`
  - `team_clean = tolower(team)`; if missing/blank/NA → `fa`
  - `fp_key = "<fp_player_slug>|<POS>|<team_clean>"`
  - `weekly_avg = fantasypts` when present; otherwise midpoint of `weekly_floor`/`weekly_ceiling`.

**FantasyPros Pages To Verify Manually**
- Projections (what the pipeline ingests):
  - QB: `https://www.fantasypros.com/nfl/projections/qb.php?week=<WEEK>&scoring=<SCORING>`
  - RB: `https://www.fantasypros.com/nfl/projections/rb.php?week=<WEEK>&scoring=<SCORING>`
  - WR: `https://www.fantasypros.com/nfl/projections/wr.php?week=<WEEK>&scoring=<SCORING>`
  - …and so on for TE/K/DST by replacing `<pos>`. These tables correspond to `fp_projections` and produce fractional “expected value” stats and fantasy points.
- Rankings “Stats (Avg.)” (what you see in screenshots with rounded lines):
  - QB: `https://www.fantasypros.com/nfl/rankings/qb.php?week=<WEEK>&scoring=STD&view=stats`
  - RB: `https://www.fantasypros.com/nfl/rankings/rb.php?week=<WEEK>&scoring=STD&view=stats`
  - These are driven by the ECR views, often rounded to integers, and will not match projections one‑for‑one. Our pipeline does not ingest these rows by default (only the ECR rank/dispersion, not the rounded stats).

**Independent Verification Steps**
- End‑to‑end (CLI):
  1) `FP_YEAR=2025 FP_WEEK=1 FP_SCORING=PPR Rscript r/ffpros_ingest.R`
  2) Inspect `data/staging/fp_weekly.parquet` or `fp_weekly.csv` for the player of interest:
     - Filter on `fp_player_slug` (e.g., `lamar-jackson`) and position.
  3) Confirm `fantasypts`/`weekly_avg` and the fractional stats match the projections page for the same week/scoring.
  4) Run `python main.py --season 2025` to build `data/out/projections.json` and verify the same numbers under `projections.weekly`.
- Browser spot‑check (example for Lamar Jackson, Week 1):
  - Projections page (PPR): `https://www.fantasypros.com/nfl/projections/qb.php?week=1&scoring=PPR` → values align with fractional stats in our JSON.
  - Rankings “Stats (Avg.)” (STD/ECR): `https://www.fantasypros.com/nfl/rankings/qb.php?week=1&scoring=STD&view=stats` → rounded line (e.g., att 19, yds 209) and a higher fantasy point total (e.g., ~29.4 in STD scoring). This is a different view than projections and is expected to differ.

**Authenticity/Provenance Visible In JSON**
- Each JSON record includes:
  - `metadata.source = "fantasypros"`, `metadata.pulled_at` (UTC)
  - `projections.context.scoring` and `season`
  - Weekly fields sourced from `fp_weekly.parquet`: `fantasy_points` (aka `weekly_avg`), ranks, and the fractional `stats` blob.
  - ROS fields sourced from `fp_ros.parquet`: `rank`, `pos_rank`, dispersion fields, and ROS `pulled_at`.
- To verify a single player:
  - Look up their row in `data/staging/fp_weekly.csv` and compare to the corresponding projections page for the same week/scoring.
  - If desired, recompute fantasy points locally from the `stats` using your own league rules and compare to JSON `fantasy_points`.

**Important Distinction: Projections vs ECR Stats (Avg.)**
- Projections are fractional expected values; ECR “Stats (Avg.)” are a different site view commonly shown as rounded integers. Mixing these can appear “off”. Our pipeline purposely ingests projections (plus ECR ranks), not the rounded ECR stats, to preserve expected‑value arithmetic and consistency across scoring systems.

**Reproducing the Exact Pull**
- Run with R logging:
  - `FP_DEBUG=true FP_YEAR=2025 FP_WEEK=1 FP_SCORING=PPR Rscript r/ffpros_ingest.R`
  - The script logs which columns it mapped to `player`, `team`, `id`, and `points` for each position.
- The parquet/CSV in `data/staging/` are the verbatim data used by `main.py` to assemble `data/out/projections.json`.

If a week returns no projections
- The R script now preserves the previous `fp_weekly.parquet/csv` when the current pull has 0 rows (e.g., FantasyPros has not published that week yet or the endpoint changed).
- You’ll see `[ffpros][warn] Weekly projections are empty` in logs. In that case either:
  - Re‑run for a different week (e.g., `FP_WEEK=1`), or
  - Try again later when FP publishes the week’s projections.

Updates
- Weekly JSON: `projections.weekly` excludes ECR fields and uncertainty (`sd`, `floor`, `ceiling`). Weekly ranks are points‑based from projections (`rank_overall`, `rank_pos`). See `docs/WEEKLY_SOURCES.md`.
- ROS JSON: When the ROS tables include projected points (e.g., a `Proj. FPTS` column), these are captured as `projections.ros.fantasy_points`. We also capture `projections.ros.ecr_vs_adp` when available. See `docs/ROS_SOURCES.md`.
