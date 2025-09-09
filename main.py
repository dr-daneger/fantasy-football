#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Unified Fantasy Pipeline — Sleeper + Yahoo + FantasyPros (via R) → JSON

Usage (examples):
  # Sleeper only
  python main.py --sleeper-username DBoiii --sleeper-league "The Degenerates" --season 2025

  # Yahoo only (pick one of the three)
  python main.py --yahoo-league-key 461.l.1264351 --season 2025
  python main.py --yahoo-league-id 1264351 --season 2025
  python main.py --yahoo-league-url "https://football.fantasysports.yahoo.com/f1/1264351" --season 2025

  # Pull both, run the R ingestor, then build JSON:
  python main.py --sleeper-username DBoiii --sleeper-league "The Degenerates" \
                 --yahoo-league-key 461.l.1264351 \
                 --season 2025 --week 1 --run-r-ingestor

Outputs:
  - data/staging/sleeper_rosters.parquet
  - data/staging/yahoo_rosters.parquet
  - data/staging/id_map.parquet              (persistent crosswalk FP↔Sleeper↔Yahoo)
  - data/staging/fp_weekly.parquet           (from R)
  - data/staging/fp_ros.parquet              (from R)
  - data/out/projections.json                (final merged weekly+ROS+ownership)

Notes:
  - Yahoo OAuth: expects oauth2.json in CWD (or set env YAHOO_OAUTH_JSON). First run opens a browser.
  - Sleeper players blob cached as players_nfl.json (same as your original).
  - The script is idempotent; re-running updates id_map with new FP players as needed.
"""

import argparse
import json
import os
import re
import subprocess
import sys
import unicodedata
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests

# --- Optional deps (only needed if you pull Yahoo) ---
try:
    from yahoo_oauth import OAuth2
    from yahoo_fantasy_api import game as ygame
    from yahoo_fantasy_api import team as yteam
except Exception:
    OAuth2 = None
    ygame = None
    yteam = None

# --------------------------
# General constants/paths
# --------------------------
BASE_SLEEPER = "https://api.sleeper.app/v1"
PLAYERS_CACHE = "players_nfl.json"
STAGING_DIR = "data/staging"
OUT_DIR = "data/out"
os.makedirs(STAGING_DIR, exist_ok=True)
os.makedirs(OUT_DIR, exist_ok=True)

PREFERRED_POS_ORDER = ['QB', 'RB', 'WR', 'TE', 'K', 'DEF']

# Hard-coded mapping to align Yahoo D/ST display names to FantasyPros team slugs
# This helps attach FP ids for Yahoo defenses whose player names are city-only.
YAHOO_DST_NAME_TO_FP_SLUG = {
    'Arizona': 'arizona-cardinals',
    'Atlanta': 'atlanta-falcons',
    'Baltimore': 'baltimore-ravens',
    'Buffalo': 'buffalo-bills',
    'Carolina': 'carolina-panthers',
    'Chicago': 'chicago-bears',
    'Cincinnati': 'cincinnati-bengals',
    'Cleveland': 'cleveland-browns',
    'Dallas': 'dallas-cowboys',
    'Denver': 'denver-broncos',
    'Detroit': 'detroit-lions',
    'Green Bay': 'green-bay-packers',
    'Houston': 'houston-texans',
    'Indianapolis': 'indianapolis-colts',
    'Jacksonville': 'jacksonville-jaguars',
    'Kansas City': 'kansas-city-chiefs',
    'Las Vegas': 'las-vegas-raiders',
    # Los Angeles is ambiguous in Yahoo (Chargers or Rams). Add specific variants if present.
    'Los Angeles Chargers': 'los-angeles-chargers',
    'LA Chargers': 'los-angeles-chargers',
    'Chargers': 'los-angeles-chargers',
    'Los Angeles Rams': 'los-angeles-rams',
    'LA Rams': 'los-angeles-rams',
    'Rams': 'los-angeles-rams',
    'Miami': 'miami-dolphins',
    'Minnesota': 'minnesota-vikings',
    'New England': 'new-england-patriots',
    'New Orleans': 'new-orleans-saints',
    'New York Giants': 'new-york-giants',
    'NY Giants': 'new-york-giants',
    'Giants': 'new-york-giants',
    'New York Jets': 'new-york-jets',
    'NY Jets': 'new-york-jets',
    'Jets': 'new-york-jets',
    'Philadelphia': 'philadelphia-eagles',
    'Pittsburgh': 'pittsburgh-steelers',
    'San Francisco': 'san-francisco-49ers',
    'Seattle': 'seattle-seahawks',
    'Tampa Bay': 'tampa-bay-buccaneers',
    'Tennessee': 'tennessee-titans',
    'Washington': 'washington-commanders',
}

# Columns to exclude from `weekly.stats` because they are bubbled up elsewhere
POINT_COLS = {
    "fantasypts", "fantasypoints", "fantasy_points",
    "fpts", "fpts_ppr", "misc_fpts", "weekly_avg"
}
WEEKLY_EXCLUDE = POINT_COLS | {
    "player", "fp_player_name", "fp_player_slug", "fp_player_id",
    # ID/identity variants that must never leak into stats:
    "fantasypros_id", "fantasyprosid", "player_id", "playerid", "fp_id", "id",
    "pos", "position", "team", "nfl_team", "year", "week",
    "__fp_key", "source", "pulled_at",
    "weekly_sd", "weekly_floor", "weekly_ceiling",
    "weekly_points_overall_rank", "weekly_points_pos_rank",
    "weekly_ecr_rank", "weekly_ecr_best_rank", "weekly_ecr_worst_rank",
    "weekly_ecr_avg_rank", "weekly_ecr_sd_rank"
}

# =====================================================
# Utility helpers
# =====================================================
def _get(url: str):
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return r.json()

def _normalize_name(n: str) -> str:
    """Lowercase, ASCII, remove punctuation and generational suffixes, collapse spaces."""
    if not n:
        return ""
    s = unicodedata.normalize("NFKD", n).encode("ascii", "ignore").decode("ascii")
    s = re.sub(r"[^\w\s-]", "", s).lower().strip()
    s = re.sub(r"\b(jr|sr|ii|iii|iv|v)\b", "", s).strip()
    s = re.sub(r"\s+", " ", s)
    return s

def _clean_team(t: Optional[str]) -> Optional[str]:
    if t is None:
        return None
    s = str(t).strip()
    if s == "" or s.lower() in {"na", "none", "null", "character(0)"}:
        return None
    return s.upper()

def _slug_no_suffix(name: str) -> str:
    """Suffixless slug used to match names across sources."""
    s = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode("ascii").lower()
    s = re.sub(r"\b(jr|sr|ii|iii|iv|v)\b\.?", "", s)
    s = re.sub(r"[^a-z0-9]+", "-", s).strip("-")
    return s

def _safe_to_csv(df: pd.DataFrame, path: str, **kwargs) -> str:
    """Write CSV; if locked, write to '<name>_new.csv'. Returns the path used."""
    try:
        df.to_csv(path, **kwargs)
        return path
    except PermissionError:
        alt = path[:-4] + "_new.csv" if path.lower().endswith(".csv") else path + ".new"
        print(f"[Export] '{path}' is locked. Writing to '{alt}' instead.")
        df.to_csv(alt, **kwargs)
        return alt

# =====================================================
# Sleeper helpers (based on your working script)
# =====================================================
def slp_get_user(username: str) -> Dict:
    return _get(f"{BASE_SLEEPER}/user/{username}")

def slp_get_user_leagues(user_id: str, season: int) -> List[Dict]:
    return _get(f"{BASE_SLEEPER}/user/{user_id}/leagues/nfl/{season}")

def slp_pick_league(leagues: List[Dict], league_name: str) -> Dict:
    if not leagues:
        raise SystemExit("No Sleeper leagues found for this user/season.")
    exact = [lg for lg in leagues if (lg.get("name") or "").lower() == league_name.lower()]
    matches = exact or [lg for lg in leagues if league_name.lower() in (lg.get("name") or "").lower()]
    if not matches:
        names = [lg.get("name") for lg in leagues]
        raise SystemExit(f'No Sleeper league named like "{league_name}". Available: {names}')
    matches.sort(key=lambda lg: lg.get("created", 0), reverse=True)
    return matches[0]

def slp_get_league_users(league_id: str) -> List[Dict]:
    return _get(f"{BASE_SLEEPER}/league/{league_id}/users")

def slp_get_league_rosters(league_id: str) -> List[Dict]:
    return _get(f"{BASE_SLEEPER}/league/{league_id}/rosters")

def slp_get_players() -> Dict:
    if os.path.exists(PLAYERS_CACHE):
        with open(PLAYERS_CACHE, "r", encoding="utf-8") as f:
            return json.load(f)
    data = _get(f"{BASE_SLEEPER}/players/nfl")
    with open(PLAYERS_CACHE, "w", encoding="utf-8") as f:
        json.dump(data, f)
    return data

def slp_build_roster_df(rosters: List[Dict[str, Any]],
                        users: List[Dict[str, Any]],
                        players_blob: Dict[str, Any]) -> pd.DataFrame:
    # map owner_id -> display name and fantasy team name
    user_name = {}
    user_teamname = {}
    for u in users:
        uid = u.get("user_id")
        display = (u.get("display_name") or u.get("username") or u.get("nickname"))
        meta = u.get("metadata") or {}
        team_name = meta.get("team_name") or meta.get("team_name_update") or display
        if uid:
            user_name[uid] = display
            user_teamname[uid] = team_name
    rows = []
    for r in rosters:
        oid = r.get("owner_id")
        owner = user_name.get(oid)
        fantasy_team = user_teamname.get(oid) or owner
        for pid in (r.get("players") or []):
            p = players_blob.get(pid) or {}
            name = (p.get("full_name") or f"{p.get('first_name','')} {p.get('last_name','')}".strip()).strip()
            pos  = _canonical_pos(p.get("position") or "")
            team = _clean_team(p.get("team") or p.get("team_abbr") or "")
            name_for_slug = re.sub(r"\b(d/?st|dst|defense|def)\b", "", name, flags=re.I).strip() if pos == "DEF" else name
            # Eligible positions: prefer fantasy_positions; fall back to position
            raw_elig = p.get("fantasy_positions")
            if not isinstance(raw_elig, list) or not raw_elig:
                rp = p.get("position")
                raw_elig = [rp] if isinstance(rp, str) and rp else (rp if isinstance(rp, list) else [])
            elig = _augment_flex_eligibility(raw_elig, pos)
            rows.append({
                "platform": "sleeper",
                "player_id": str(pid),
                "player_key": str(pid),
                "player": name,
                "team": team or "",  # NFL team
                "team_name": fantasy_team or "",  # Fantasy team name
                "owner": owner,
                "eligible_positions": elig,
                "position": pos or None,
                "slug_no_suffix": _slug_no_suffix(name_for_slug),
            })
    return pd.DataFrame(rows)


def normalize_player_record(p: Dict[str, Any]) -> Dict[str, Any]:
    player_id = str(p.get("player_id") or "")
    name = (p.get("name") or "").strip()
    team = _clean_team(p.get("editorial_team_full_name") or p.get("editorial_team_abbr") or p.get("team_name") or "")
    owner = p.get("owner") or None
    elig = p.get("eligible_positions") or []
    # Choose a primary position (first eligible), then canonicalize
    raw_pos = str(elig[0]).upper() if elig else (p.get("primary_position") or p.get("position") or "").upper()
    pos = _canonical_pos(raw_pos)

    # For defenses, strip D/ST tokens before slugging so slugs match FP team slugs
    name_for_slug = name
    if pos == "DEF":
        name_for_slug = re.sub(r"\b(d/?st|dst|defense|def)\b", "", name_for_slug, flags=re.I).strip()

    return {
        "player_id": player_id,
        "player": name,
        "team": team or "",
        "team_name": p.get("editorial_team_full_name") or p.get("team_name") or "",
        "owner": owner,
        "eligible_positions": elig,
        "position": pos,
        "slug_no_suffix": _slug_no_suffix(name_for_slug),
    }

def pull_sleeper(username: Optional[str], league_name: Optional[str], season: int) -> Optional[pd.DataFrame]:
    """
    Resolve `username` -> user_id -> user's leagues (season) -> pick target league by name ->
    fetch users/rosters/players -> build roster DataFrame and persist it.
    """
    if not username or not league_name:
        return None

    # 1) Get the Sleeper user record so we have a user_id
    user = slp_get_user(username)  # must return a dict with "user_id"
    if not user or not user.get("user_id"):
        raise SystemExit(f"[Sleeper] Could not resolve user_id for username '{username}'.")

    # 2) List that user's leagues for the season
    leagues = slp_get_user_leagues(user["user_id"], season)
    if not leagues:
        raise SystemExit(f"[Sleeper] No leagues found for user '{username}' in season {season}.")

    # 3) Pick the specific league by human-readable league name
    league = slp_pick_league(leagues, league_name)
    if not league:
        raise SystemExit(f"[Sleeper] League named '{league_name}' not found for user '{username}' in {season}.")

    league_id = league["league_id"]
    print(f'[Sleeper] Using league: {league.get("name")} (league_id={league_id})')

    # 4) Fetch league users, rosters, and the players blob
    users = slp_get_league_users(league_id)
    rosters = slp_get_league_rosters(league_id)
    players = slp_get_players()

    # 5) Build a per-player roster dataframe (owner, team, eligible positions, etc.)
    df = slp_build_roster_df(rosters, users, players)

    # 6) Persist
    df.to_parquet(os.path.join(STAGING_DIR, "sleeper_rosters.parquet"), index=False)
    print(f"[Sleeper] Wrote {STAGING_DIR}/sleeper_rosters.parquet")
    return df


# =====================================================
# Yahoo helpers (based on your working script)
# =====================================================
def parse_league_id(league_url: Optional[str], league_id: Optional[str]) -> str:
    if league_id:
        return str(league_id)
    if league_url:
        m = re.search(r"/f1/(\d+)", league_url)
        if not m:
            raise SystemExit("Could not parse league_id from the provided URL. Expected .../f1/<league_id>")
        return m.group(1)
    raise SystemExit("Provide --yahoo-league-id or --yahoo-league-url or --yahoo-league-key")

def ensure_oauth() -> "OAuth2":
    cfg_path = os.environ.get("YAHOO_OAUTH_JSON", "oauth2.json")
    if not os.path.exists(cfg_path):
        raise SystemExit(f"{cfg_path} not found. Create it with 'consumer_key' and 'consumer_secret'.")
    sc = OAuth2(None, None, from_file=cfg_path)
    if not sc.token_is_valid():
        sc.refresh_access_token()
    return sc

def resolve_league_key(gm: "ygame.Game", raw_id: str, season: Optional[int]) -> str:
    if ".l." in raw_id:
        return raw_id
    try:
        keys = gm.league_ids(season)
    except TypeError:
        try:
            keys = gm.league_ids(str(season))
        except Exception:
            keys = gm.league_ids()
    for lk in keys:
        if lk.endswith(f".l.{raw_id}"):
            return lk
    raise SystemExit(f"Could not resolve league_key for league id {raw_id} in season {season}. Found: {keys}")

def build_team_meta_map(lg) -> Dict[str, Dict[str, Optional[str]]]:
    meta: Dict[str, Dict[str, Optional[str]]] = {}

    def absorb(entry: dict):
        tk = entry.get("team_key")
        if not tk:
            return
        managers = entry.get("managers") or []
        owner = None
        if managers and isinstance(managers, list) and isinstance(managers[0], dict):
            owner = managers[0].get("nickname") or managers[0].get("guid")
        name = entry.get("name")
        cur = meta.get(tk, {})
        if name:
            cur["team_name"] = name
        if owner:
            cur["owner"] = owner
        meta[tk] = cur

    try:
        st = lg.standings()
        if isinstance(st, list):
            for item in st:
                if isinstance(item, dict):
                    if "team_key" in item:
                        absorb(item)
                    elif "team" in item and isinstance(item["team"], dict):
                        absorb(item["team"])
    except Exception:
        pass

    try:
        tlist = lg.teams()
        if isinstance(tlist, list) and tlist and not isinstance(tlist[0], str):
            for t in tlist:
                if isinstance(t, dict):
                    absorb(t)
        elif isinstance(tlist, dict):
            for v in tlist.values():
                if isinstance(v, dict):
                    absorb(v)
    except Exception:
        pass

    return meta

def get_team_keys(lg) -> List[str]:
    keys = set()
    try:
        st = lg.standings()
        if isinstance(st, list):
            for item in st:
                if isinstance(item, dict):
                    if "team_key" in item:
                        keys.add(item["team_key"])
                    elif "team" in item and isinstance(item["team"], dict) and "team_key" in item["team"]:
                        keys.add(item["team"]["team_key"])
    except Exception:
        pass

    try:
        tlist = lg.teams()
        if isinstance(tlist, list):
            if tlist:
                if isinstance(tlist[0], str):
                    keys.update(tlist)
                else:
                    for t in tlist:
                        if isinstance(t, dict) and t.get("team_key"):
                            keys.add(t["team_key"])
        elif isinstance(tlist, dict):
            for k, v in tlist.items():
                if isinstance(k, str) and ".t." in k:
                    keys.add(k)
                if isinstance(v, dict) and v.get("team_key"):
                    keys.add(v["team_key"])
    except Exception:
        pass

    keys_list = sorted(keys)
    if not keys_list:
        raise SystemExit("Could not extract any team keys from league.standings() or league.teams().")
    return keys_list

def choose_primary_from_eligible(eligible: Any) -> Optional[str]:
    if not isinstance(eligible, list):
        return None
    candidates = [pos for pos in eligible if pos in PREFERRED_POS_ORDER]
    if candidates:
        for pref in PREFERRED_POS_ORDER:
            if pref in candidates:
                return pref
    for pos in eligible:
        if pos and pos not in ("W/R/T", "W/T", "R/W", "WR/RB", "RB/WR", "TE/W", "BN"):
            return pos
    return None

def _canonical_pos(p: Optional[str]) -> Optional[str]:
    """
    Canonicalize positions across sources.
    FantasyPros uses 'DST' (and sometimes 'D/ST'), while Sleeper/Yahoo use 'DEF'.
    """
    if not p:
        return None
    u = str(p).upper()
    return "DEF" if u in {"DST", "D/ST"} else u

def _augment_flex_eligibility(elig: Any, primary_pos: Optional[str]) -> List[str]:
    """Return a cleaned eligible_positions list and append 'W/R/T' for RB/WR/TE.

    - Accepts list or scalar; returns list[str]
    - Canonicalizes DST/D/ST -> DEF
    - Preserves order while de-duplicating
    - Adds 'W/R/T' for RB/WR/TE if not present
    """
    # Normalize to list
    if isinstance(elig, list):
        raw_list = elig
    elif hasattr(elig, "tolist"):
        # numpy arrays / pandas arrays
        try:
            raw_list = list(elig.tolist())
        except Exception:
            raw_list = list(elig)
    elif isinstance(elig, str) and elig:
        raw_list = [elig]
    else:
        raw_list = []

    # De-duplicate while preserving order and canonicalize DEF
    out: List[str] = []
    for e in raw_list:
        u = str(e).upper()
        if u in {"DST", "D/ST"}:
            u = "DEF"
        if u and u not in out:
            out.append(u)

    posU = _canonical_pos(primary_pos)
    if posU in {"RB", "WR", "TE"} and "W/R/T" not in out:
        out.append("W/R/T")
    return out

def _best_owner_hit(
    slug: str,
    pos: Optional[str],
    team: Optional[str],
    pool: pd.DataFrame,
    id_col: str
) -> Optional[str]:
    """Flexible matcher for ownership mapping: try (slug+pos+team) → (slug+pos) → (slug)."""
    if pool is None or pool.empty:
        return None
    posU = _canonical_pos(pos) or ""
    teamN = _clean_team(team) or ""

    # 1) slug + pos + team
    m = pool[(pool["slug_no_suffix"] == slug) &
             (pool["position"].astype(str).str.upper() == posU) &
             (pool["team"].fillna("").map(_clean_team) == teamN)]
    if not m.empty:
        return str(m.iloc[0][id_col])

    # 2) slug + pos
    m = pool[(pool["slug_no_suffix"] == slug) &
             (pool["position"].astype(str).str.upper() == posU)]
    if not m.empty:
        return str(m.iloc[0][id_col])

    # 3) slug only
    m = pool[(pool["slug_no_suffix"] == slug)]
    if not m.empty:
        return str(m.iloc[0][id_col])

    return None

# =====================================================
# FantasyPros (R) ingestor orchestration
# =====================================================
def run_r_ingestor(week: int = 1) -> None:
    """
    Calls: Rscript r/ffpros_ingest.R with specified week
    Expects the R script to write fp_weekly.parquet and fp_ros.parquet
    """
    # Set environment variable for R script to use
    os.environ["FP_WEEK"] = str(week)
    cmd = ["Rscript", "r/ffpros_ingest.R"]
    print(f"[R] Running: {' '.join(cmd)} (week={week})")
    try:
        subprocess.run(cmd, check=True)
        print("[R] Ingest complete.")
    except FileNotFoundError:
        print("[R] Rscript not found on PATH. Skipping.")
    except subprocess.CalledProcessError as e:
        print(f"[R] Ingestor returned non-zero exit code: {e.returncode}. Skipping.")

# =====================================================
# Crosswalk + merge logic
# =====================================================
def load_parquet_optional(path: str) -> Optional[pd.DataFrame]:
    if os.path.exists(path):
        return pd.read_parquet(path)
    return None

def _build_key(slug: str, pos: Optional[str], team: Optional[str]) -> str:
    return f"{slug}|{(pos or '').upper()}|{(team or '' ).lower()}"

def build_or_update_id_map(
    weekly_fp: pd.DataFrame,
    sleeper_df: Optional[pd.DataFrame],
    yahoo_df: Optional[pd.DataFrame]
) -> pd.DataFrame:
    id_map_path = os.path.join(STAGING_DIR, "id_map.parquet")
    if os.path.exists(id_map_path):
        id_map = pd.read_parquet(id_map_path)
    else:
        id_map = pd.DataFrame(columns=[
            "fp_key", "canonical_player_id",
            "fp_player_id", "fp_slug",
            "sleeper_player_id", "yahoo_player_id",
            "name_norm", "pos", "team"
        ])

    # Prefer the R-computed join key if present
    if "fp_key" in weekly_fp.columns:
        wk = weekly_fp.copy()
    elif "__fp_key" in weekly_fp.columns:            # rare alt
        wk = weekly_fp.assign(fp_key=weekly_fp["__fp_key"])
    else:
    # Fallback: slug|POS|team (note: keep consistent with R: missing team -> "fa")
        slug = weekly_fp.get("fp_player_slug", weekly_fp.get("player", pd.Series([""] * len(weekly_fp)))).fillna("").map(_slug_no_suffix)
        pos  = weekly_fp.get("pos", pd.Series([""] * len(weekly_fp))).fillna("").str.upper().replace({"DST": "DEF", "D/ST": "DEF"})
        team_raw = weekly_fp.get("team", weekly_fp.get("nfl_team", pd.Series([""] * len(weekly_fp)))).fillna("")
        team = team_raw.apply(lambda t: (t or "").lower() if (t and t.lower() not in {"na","none","null","character(0)"}) else "fa")
        wk = weekly_fp.assign(fp_key=slug + "|" + pos + "|" + team)
    # Note: do not overwrite wk again; we want the canonical key from R if present

    # Normalize for matching/backfilling
    if len(wk) == 0:
        for c in ["player", "fp_player_name", "pos", "team", "nfl_team", "fp_player_id"]:
            if c not in wk.columns:
                wk[c] = pd.Series(dtype="object")
    name_series = wk["player"] if "player" in wk.columns else (wk["fp_player_name"] if "fp_player_name" in wk.columns else pd.Series([""] * len(wk)))
    wk["name_norm"] = name_series.fillna("").map(_normalize_name)
    # Canonicalize DST→DEF so ownership matching is consistent
    pos_series = wk["pos"] if "pos" in wk.columns else pd.Series([""] * len(wk))
    wk["pos"] = pos_series.fillna("").str.upper().replace({"DST": "DEF", "D/ST": "DEF"})
    team_series = wk["team"] if "team" in wk.columns else (wk["nfl_team"] if "nfl_team" in wk.columns else pd.Series([""] * len(wk)))
    wk["team"] = team_series.fillna("")
    if "fp_player_id" not in wk.columns:
        wk["fp_player_id"] = pd.Series([None] * len(wk))

    # Build ownership pools with slug_no_suffix, cleaning defense names before slugging
    slp_pool = None
    yah_pool = None
    if sleeper_df is not None and not sleeper_df.empty:
        slp_pool = sleeper_df.copy()
        name_for_slug = slp_pool["player"].fillna("")
        if "position" in slp_pool.columns:
            m = slp_pool["position"].astype(str).str.upper().eq("DEF")
            name_for_slug.loc[m] = name_for_slug.loc[m].str.replace(r"\b(d/?st|dst|defense|def)\b", "", regex=True).str.strip()
        slp_pool["slug_no_suffix"] = name_for_slug.map(_slug_no_suffix)

    if yahoo_df is not None and not yahoo_df.empty:
        yah_pool = yahoo_df.copy()
        name_for_slug = yah_pool["player"].fillna("")
        if "position" in yah_pool.columns:
            m = yah_pool["position"].astype(str).str.upper().eq("DEF")
            name_for_slug.loc[m] = name_for_slug.loc[m].str.replace(r"\b(d/?st|dst|defense|def)\b", "", regex=True).str.strip()
        yah_pool["slug_no_suffix"] = name_for_slug.map(_slug_no_suffix)

    id_map_idx = id_map.set_index("fp_key") if not id_map.empty else None
    new_rows = []

    for _, row in wk.iterrows():
        key = row["fp_key"]

        # Prepare inputs used for both update and insert
        name_norm = row["name_norm"]
        pos = str(row.get("pos") or "")
        team = _clean_team(row.get("team")) or ""
        slug = _slug_no_suffix(str(row.get("player") or ""))
        fp_pid = int(row["fp_player_id"]) if "fp_player_id" in row and pd.notnull(row["fp_player_id"]) else None

        # Try flexible ownership match now (we store only platform IDs here)
        slp_match = _best_owner_hit(slug, pos, team, slp_pool, "player_id") if slp_pool is not None else None
        yah_match = _best_owner_hit(slug, pos, team, yah_pool, "player_id") if yah_pool is not None else None

        if id_map_idx is not None and key in id_map_idx.index:
            # REFRESH: backfill any missing platform IDs and fp_player_id
            idx = (id_map["fp_key"] == key)
            if slp_match and (id_map.loc[idx, "sleeper_player_id"].isna().all() | (id_map.loc[idx, "sleeper_player_id"] == "").all()):
                id_map.loc[idx, "sleeper_player_id"] = slp_match
            if yah_match and (id_map.loc[idx, "yahoo_player_id"].isna().all() | (id_map.loc[idx, "yahoo_player_id"] == "").all()):
                id_map.loc[idx, "yahoo_player_id"] = yah_match
            if fp_pid is not None and id_map.loc[idx, "fp_player_id"].isna().all():
                id_map.loc[idx, "fp_player_id"] = fp_pid
            # Keep auxiliary fields fresh (no-ops if unchanged)
            id_map.loc[idx, "name_norm"] = name_norm
            id_map.loc[idx, "pos"] = pos
            id_map.loc[idx, "team"] = team
            continue

        # INSERT new row
        canon = f"canon:{name_norm}:{team}:{pos}"
        new_rows.append({
            "fp_key": key,
            "canonical_player_id": canon,
            "fp_player_id": fp_pid,
            "fp_slug": row.get("fp_player_slug") if "fp_player_slug" in row else slug,
            "sleeper_player_id": slp_match,
            "yahoo_player_id": yah_match,
            "name_norm": name_norm,
            "pos": pos,
            "team": team,
        })

    if new_rows:
        id_map = pd.concat([id_map, pd.DataFrame(new_rows)], ignore_index=True)
        id_map.drop_duplicates(subset=["fp_key"], keep="last", inplace=True)

    id_map.to_parquet(id_map_path, index=False)
    print(f"[Crosswalk] Wrote/updated {id_map_path} ({len(id_map)} rows)")
    return id_map
    id_map_path = os.path.join(STAGING_DIR, "id_map.parquet")
    if os.path.exists(id_map_path):
        id_map = pd.read_parquet(id_map_path)
    else:
        id_map = pd.DataFrame(columns=[
            "fp_key", "canonical_player_id",
            "fp_player_id", "fp_slug",
            "sleeper_player_id", "yahoo_player_id",
            "name_norm", "pos", "team"
        ])

    # Prefer the R-computed join key if present
    if "__fp_key" in weekly_fp.columns:
        wk = weekly_fp.copy()
        wk["fp_key"] = weekly_fp["__fp_key"]
    else:
        # Fallback: slug|POS|team
        slug = weekly_fp.get("fp_player_slug", weekly_fp.get("player", pd.Series([""] * len(weekly_fp)))).fillna("").map(_slug_no_suffix)
        pos = weekly_fp.get("pos", pd.Series([""] * len(weekly_fp))).fillna("").str.upper()
        team = weekly_fp.get("team", weekly_fp.get("nfl_team", pd.Series([""] * len(weekly_fp)))).fillna("").str.lower()
        wk = weekly_fp.assign(fp_key=slug + "|" + pos + "|" + team)

    # Normalize for matching/backfilling
    wk["name_norm"] = wk.get("player", wk.get("fp_player_name", "")).fillna("").map(_normalize_name)
    # Canonicalize DST→DEF for the crosswalk so ownership matching is consistent
    wk["pos"] = wk.get("pos", "").fillna("").str.upper().replace({"DST": "DEF", "D/ST": "DEF"})
    wk["team"] = wk.get("team", wk.get("nfl_team", "")).fillna("")
    wk["fp_player_id"] = wk.get("fp_player_id", None)

    # Build ownership pools with slug_no_suffix, and clean defense names so slugs match FP team slugs
    slp_pool = None
    yah_pool = None
    if sleeper_df is not None and not sleeper_df.empty:
        slp_pool = sleeper_df.copy()
        name_for_slug = slp_pool["player"].fillna("")
        if "position" in slp_pool.columns:
            m = slp_pool["position"].astype(str).str.upper().eq("DEF")
            name_for_slug.loc[m] = name_for_slug.loc[m].str.replace(r"\b(d/?st|dst|defense|def)\b", "", regex=True).str.strip()
        slp_pool["slug_no_suffix"] = name_for_slug.map(_slug_no_suffix)

    if yahoo_df is not None and not yahoo_df.empty:
        yah_pool = yahoo_df.copy()
        name_for_slug = yah_pool["player"].fillna("")
        if "position" in yah_pool.columns:
            m = yah_pool["position"].astype(str).str.upper().eq("DEF")
            name_for_slug.loc[m] = name_for_slug.loc[m].str.replace(r"\b(d/?st|dst|defense|def)\b", "", regex=True).str.strip()
        yah_pool["slug_no_suffix"] = name_for_slug.map(_slug_no_suffix)

    id_map_idx = id_map.set_index("fp_key") if not id_map.empty else None
    new_rows = []

    for _, row in wk.iterrows():
        key = row["fp_key"]

        # Prepare inputs used for both update and insert
        name_norm = row["name_norm"]
        pos = str(row.get("pos") or "")
        team = _clean_team(row.get("team")) or ""
        slug = _slug_no_suffix(str(row.get("player") or ""))
        fp_pid = int(row["fp_player_id"]) if "fp_player_id" in row and pd.notnull(row["fp_player_id"]) else None

        # Try flexible ownership match now (we store only platform IDs here)
        slp_match = _best_owner_hit(slug, pos, team, slp_pool, "player_id") if slp_pool is not None else None
        yah_match = _best_owner_hit(slug, pos, team, yah_pool, "player_id") if yah_pool is not None else None

        if id_map_idx is not None and key in id_map_idx.index:
            # REFRESH existing row: backfill any missing platform IDs and fp_player_id
            idx = (id_map["fp_key"] == key)
            # Sleeper
            if slp_match and (id_map.loc[idx, "sleeper_player_id"].isna().all() | (id_map.loc[idx, "sleeper_player_id"] == "").all()):
                id_map.loc[idx, "sleeper_player_id"] = slp_match
            # Yahoo
            if yah_match and (id_map.loc[idx, "yahoo_player_id"].isna().all() | (id_map.loc[idx, "yahoo_player_id"] == "").all()):
                id_map.loc[idx, "yahoo_player_id"] = yah_match
            # FP id
            if fp_pid is not None and id_map.loc[idx, "fp_player_id"].isna().all():
                id_map.loc[idx, "fp_player_id"] = fp_pid
            # Keep auxiliary fields fresh (safe no-ops if unchanged)
            id_map.loc[idx, "name_norm"] = name_norm
            id_map.loc[idx, "pos"] = pos
            id_map.loc[idx, "team"] = team
            continue

        # INSERT new row
        canon = f"canon:{name_norm}:{team}:{pos}"
        new_rows.append({
            "fp_key": key,
            "canonical_player_id": canon,
            "fp_player_id": fp_pid,
            "fp_slug": row.get("fp_player_slug") if "fp_player_slug" in row else slug,
            "sleeper_player_id": slp_match,
            "yahoo_player_id": yah_match,
            "name_norm": name_norm,
            "pos": pos,
            "team": team,
        })

    if new_rows:
        id_map = pd.concat([id_map, pd.DataFrame(new_rows)], ignore_index=True)
        id_map.drop_duplicates(subset=["fp_key"], keep="last", inplace=True)

    id_map.to_parquet(id_map_path, index=False)
    print(f"[Crosswalk] Wrote/updated {id_map_path} ({len(id_map)} rows)")
    return id_map


def assemble_json(
    fp_weekly: pd.DataFrame,
    fp_ros: pd.DataFrame,
    id_map: pd.DataFrame,
    sleeper_df: Optional[pd.DataFrame],
    yahoo_df: Optional[pd.DataFrame],
    season_for_context: Optional[int] = None,
    scoring_for_context: str = "PPR",
) -> List[Dict[str, Any]]:
    # Ensure FP key present in both frames
    def ensure_key(df: pd.DataFrame) -> pd.DataFrame:
        if "fp_key" in df.columns:
            return df
        if "__fp_key" in df.columns:
            return df.assign(fp_key=df["__fp_key"])
        # Otherwise, synthesize it from slug/pos/team.
        slug = df.get("fp_player_slug", df.get("player", pd.Series([""] * len(df)))).fillna("").map(_slug_no_suffix)
        pos = df.get("pos", pd.Series([""] * len(df))).fillna("").str.upper()
        team = df.get("team", df.get("nfl_team", pd.Series([""] * len(df)))).fillna("").str.lower()
        return df.assign(fp_key=slug + "|" + pos + "|" + team)

    fp_weekly = ensure_key(fp_weekly)
    fp_ros = ensure_key(fp_ros)

    id_map_idx = id_map.set_index("fp_key") if not id_map.empty else pd.DataFrame()

    # Ownership lookup tables keyed by platform id → (team_name, owner, eligible_positions)
    slp_own = {}
    if sleeper_df is not None and not sleeper_df.empty:
        slp_own = (sleeper_df[["player_id", "team_name", "owner", "eligible_positions"]]
                   .groupby("player_id").agg(lambda x: x.iloc[0]).to_dict(orient="index"))

    yah_own = {}
    if yahoo_df is not None and not yahoo_df.empty:
        yah_own = (yahoo_df[["player_id", "team_name", "owner", "eligible_positions"]]
                   .groupby("player_id").agg(lambda x: x.iloc[0]).to_dict(orient="index"))

    # Fast ROS index
    ros_idx = fp_ros.set_index("fp_key") if "fp_key" in fp_ros.columns else pd.DataFrame()
    ros_slugpos = {}
    ros_by_slug = {}
    if not fp_ros.empty and "fp_player_slug" in fp_ros.columns:
        # Normalize POS once
        _fp_ros_posU = fp_ros.get("pos", "").fillna("").str.upper()
        for i, r in fp_ros.assign(_posU=_fp_ros_posU).iterrows():
            slug = r.get("fp_player_slug")
            posU = r.get("_posU")
            if isinstance(slug, str) and slug:
                if isinstance(posU, str) and posU:
                    ros_slugpos[(slug, posU)] = r
                # keep first by slug as a last-resort fallback
                ros_by_slug.setdefault(slug, r)

    
    out: List[Dict[str, Any]] = []
    owned_slp = 0
    owned_yah = 0
    for _, row in fp_weekly.iterrows():
        key = row["fp_key"]
        im = id_map_idx.loc[key] if key in getattr(id_map_idx, "index", []) else None
        # Fallback: harmonize blank-team vs 'fa' suffix in keys
        if im is None and isinstance(key, str):
            if key.endswith("|fa") and (key[:-2] in getattr(id_map_idx, "index", [])):
                im = id_map_idx.loc[key[:-2]]
            elif key.endswith("|") and ((key + "fa") in getattr(id_map_idx, "index", [])):
                im = id_map_idx.loc[key + "fa"]

        # Identity fields
        name = row.get("player") or row.get("fp_player_name")
        pos = _canonical_pos(row.get("pos") or row.get("position"))
        team = _clean_team(row.get("team") or row.get("nfl_team"))
        year = int(row.get("year")) if "year" in row and pd.notnull(row["year"]) else None
        week = int(row.get("week")) if "week" in row and pd.notnull(row["week"]) else None
        fp_player_id = int(row["fp_player_id"]) if "fp_player_id" in row and pd.notnull(row["fp_player_id"]) else None

        # Weekly points: prefer weekly_avg (computed in R from points/floor/ceiling logic)
        weekly_pts = float(row["weekly_avg"]) if "weekly_avg" in row and pd.notnull(row["weekly_avg"]) else None

        # Weekly ranks (points-based)
        rank_overall = int(row["weekly_points_overall_rank"]) if "weekly_points_overall_rank" in row and pd.notnull(row["weekly_points_overall_rank"]) else None
        rank_pos     = int(row["weekly_points_pos_rank"])     if "weekly_points_pos_rank"     in row and pd.notnull(row["weekly_points_pos_rank"])     else None

        # We no longer emit weekly ECR or uncertainty (sd/floor/ceiling) in JSON.

        # Weekly stats blob: numeric columns excluding known ID/points/rank/uncertainty fields
        weekly_stats: Dict[str, float] = {}
        for c in row.index:
            lc = c.lower()
            if lc in WEEKLY_EXCLUDE:
                continue
            v = row[c]
            if pd.notnull(v):
                try:
                    weekly_stats[c] = float(v)
                except Exception:
                    pass

        # ROS attachment
        ros_row = None
        if hasattr(ros_idx, "index") and key in ros_idx.index:
            ros_row = ros_idx.loc[key]
        else:
            slug = row.get("fp_player_slug") or _slug_no_suffix(str(row.get("player") or ""))
            posU = _canonical_pos(row.get("pos") or row.get("position")) or ""
            # 1) slug+pos
            if ros_row is None and (slug, posU) in ros_slugpos:
                ros_row = ros_slugpos[(slug, posU)]
            # 2) slug only
            if ros_row is None and slug in ros_by_slug:
                ros_row = ros_by_slug[slug]
        ros_points   = float(ros_row["ros_points"]) if (ros_row is not None and "ros_points" in ros_row and pd.notnull(ros_row["ros_points"])) else None
        ros_rank     = int(ros_row["ros_rank"])     if (ros_row is not None and "ros_rank"   in ros_row and pd.notnull(ros_row["ros_rank"]))   else None
        ros_pos_rank = int(ros_row["ros_pos_rank"]) if (ros_row is not None and "ros_pos_rank" in ros_row and pd.notnull(ros_row["ros_pos_rank"])) else None
        ros_best_rank = int(ros_row["ros_best_rank"]) if (ros_row is not None and "ros_best_rank" in ros_row and pd.notnull(ros_row["ros_best_rank"])) else None
        ros_worst_rank= int(ros_row["ros_worst_rank"])if (ros_row is not None and "ros_worst_rank"in ros_row and pd.notnull(ros_row["ros_worst_rank"])) else None
        ros_avg_rank  = float(ros_row["ros_avg_rank"])if (ros_row is not None and "ros_avg_rank"  in ros_row and pd.notnull(ros_row["ros_avg_rank"]))  else None
        ros_sd_rank   = float(ros_row["ros_sd_rank"]) if (ros_row is not None and "ros_sd_rank"   in ros_row and pd.notnull(ros_row["ros_sd_rank"]))   else None
        ros_pulled_at = str(ros_row["pulled_at"])     if (ros_row is not None and "pulled_at"     in ros_row and pd.notnull(ros_row["pulled_at"]))     else None
        ros_ecr_vs_adp = float(ros_row["ros_ecr_vs_adp"]) if (ros_row is not None and "ros_ecr_vs_adp" in ros_row and pd.notnull(ros_row["ros_ecr_vs_adp"])) else None

        # ownership (via id_map → platform ids)
        own_slp = None
        own_yah = None
        # Always produce non-null ownership objects; default to FA/unavailable
        slp_has_data = sleeper_df is not None and not sleeper_df.empty
        yah_has_data = yahoo_df is not None and not yahoo_df.empty
        own_slp = {"status": ("FA" if slp_has_data else "unavailable")}
        own_yah = {"status": ("FA" if yah_has_data else "unavailable")}
        if im is not None:
            slp_id = im["sleeper_player_id"] if "sleeper_player_id" in im else None
            yah_id = im["yahoo_player_id"] if "yahoo_player_id" in im else None
            if slp_id:
                if slp_id in slp_own:
                    o = slp_own[slp_id]
                    ep = o.get("eligible_positions")
                    if hasattr(ep, "tolist"):
                        ep = ep.tolist()
                    own_slp = {"status": "OWNED", "team_name": o.get("team_name"), "owner": o.get("owner"), "eligible_positions": ep}
                    owned_slp += 1
                else:
                    own_slp = {"status": "FA"}
            if yah_id:
                if yah_id in yah_own:
                    o = yah_own[yah_id]
                    ep = o.get("eligible_positions")
                    if hasattr(ep, "tolist"):
                        ep = ep.tolist()
                    own_yah = {"status": "OWNED", "team_name": o.get("team_name"), "owner": o.get("owner"), "eligible_positions": ep}
                    owned_yah += 1
                else:
                    own_yah = {"status": "FA"}

        obj = {
            "player_id": (im["canonical_player_id"] if im is not None
                          else f"canon:{_normalize_name(str(name))}:{team or ''}:{pos or ''}"),
            "player": {
                "name": name,
                "fp_key": key,
                "fp_player_id": fp_player_id,
                "team": team,
                "position": pos,
            },
            "ownership": {
                "sleeper": own_slp,
                "yahoo": own_yah,
            },
            "projections": {
                "context": { "scoring": scoring_for_context, "season": season_for_context or year },
                "weekly": {
                    "year": year,
                    "week": week,
                    "fantasy_points": weekly_pts,
                    "rank_overall": rank_overall,
                    "rank_pos": rank_pos,
                    "stats": weekly_stats or None
                },
                "ros": {
                    "year": year,
                    "fantasy_points": ros_points,
                    "rank": ros_rank,
                    "pos_rank": ros_pos_rank,
                    "best_rank": ros_best_rank,
                    "worst_rank": ros_worst_rank,
                    "avg_rank": ros_avg_rank,
                    "sd_rank": ros_sd_rank,
                    "ecr_vs_adp": ros_ecr_vs_adp,
                    "pulled_at": ros_pulled_at
                }
            },
            "metadata": { "source": "fantasypros", "pulled_at": str(row.get("pulled_at") or "") }
        }
        out.append(obj)
    
    print(f"[Assemble] Ownership coverage — Sleeper OWNED: {owned_slp}, Yahoo OWNED: {owned_yah}")
    return out

def export_roster_csvs(sleeper_df: Optional[pd.DataFrame], yahoo_df: Optional[pd.DataFrame], id_map: Optional[pd.DataFrame] = None) -> None:
    os.makedirs(OUT_DIR, exist_ok=True)
    if sleeper_df is not None and not sleeper_df.empty:
        sdf = sleeper_df.copy()
        if "platform" not in sdf.columns:
            sdf["platform"] = "sleeper"
        if "player_key" not in sdf.columns:
            sdf["player_key"] = sdf.get("player_id", "")
        # Attach FantasyPros ids via id_map on Sleeper player_id
        if id_map is not None and not id_map.empty:
            im_cols = [c for c in ["sleeper_player_id", "fp_player_id", "canonical_player_id", "fp_key", "pos", "team"] if c in id_map.columns]
            if "sleeper_player_id" in im_cols:
                im = id_map[im_cols].copy()
                # Deduplicate by platform id
                im = im.drop_duplicates(subset=["sleeper_player_id"], keep="first")
                im["sleeper_player_id"] = im["sleeper_player_id"].astype(str)
                sdf["player_id"] = sdf["player_id"].astype(str)
                sdf = sdf.merge(im, how="left", left_on="player_id", right_on="sleeper_player_id")
                if "sleeper_player_id" in sdf.columns:
                    sdf.drop(columns=["sleeper_player_id"], inplace=True)
                # restore NFL team column if merge created team_x/team_y
                if "team" not in sdf.columns:
                    if "team_x" in sdf.columns:
                        sdf["team"] = sdf["team_x"]
                    elif "team_y" in sdf.columns:
                        sdf["team"] = sdf["team_y"]
        # Present NFL team as nfl_team to avoid confusion with fantasy team_name
        sdf_export = sdf.rename(columns={"team": "nfl_team"})
        cols = [c for c in ["platform","player_id","player_key","fp_player_id","canonical_player_id","fp_key","player","position","nfl_team","team_name","owner","eligible_positions"] if c in sdf_export.columns]
        used = _safe_to_csv(sdf_export, os.path.join(OUT_DIR, "sleeper_rosters.csv"), index=False, columns=cols, encoding="utf-8-sig")
        print(f"[Export] Wrote {used} ({len(sleeper_df)} rows)")
    if yahoo_df is not None and not yahoo_df.empty:
        ydf = yahoo_df.copy()
        if "platform" not in ydf.columns:
            ydf["platform"] = "yahoo"
        if "player_key" not in ydf.columns:
            ydf["player_key"] = ydf.get("player_id", "")
        # Attach FantasyPros ids via id_map on Yahoo player_id
        if id_map is not None and not id_map.empty:
            im_cols = [c for c in ["yahoo_player_id", "fp_player_id", "canonical_player_id", "fp_key", "pos", "team"] if c in id_map.columns]
            if "yahoo_player_id" in im_cols:
                im = id_map[im_cols].copy()
                im = im.drop_duplicates(subset=["yahoo_player_id"], keep="first")
                im["yahoo_player_id"] = im["yahoo_player_id"].astype(str)
                ydf["player_id"] = ydf["player_id"].astype(str)
                ydf = ydf.merge(im, how="left", left_on="player_id", right_on="yahoo_player_id")
                if "yahoo_player_id" in ydf.columns:
                    ydf.drop(columns=["yahoo_player_id"], inplace=True)
                if "team" not in ydf.columns:
                    if "team_x" in ydf.columns:
                        ydf["team"] = ydf["team_x"]
                    elif "team_y" in ydf.columns:
                        ydf["team"] = ydf["team_y"]
        # Hard-coded D/ST fallback by Yahoo city name -> FP slug (fills FP ids if still missing)
        if ("position" in ydf.columns) and (id_map is not None) and (not id_map.empty):
            try:
                if "fp_player_id" not in ydf.columns:
                    ydf["fp_player_id"] = pd.NA
                if "canonical_player_id" not in ydf.columns:
                    ydf["canonical_player_id"] = pd.NA
                if "fp_key" not in ydf.columns:
                    ydf["fp_key"] = pd.NA
                need_mask = ydf["position"].astype(str).str.upper().eq("DEF") & ydf["fp_player_id"].isna()
                if need_mask.any():
                    im2 = id_map.copy()
                    if "pos" in im2.columns:
                        im2_pos = im2["pos"].astype(str).str.upper().replace({"DST": "DEF", "D/ST": "DEF"})
                        im2 = im2[im2_pos == "DEF"].copy()
                    if "fp_slug" in im2.columns:
                        im2["_slug"] = im2["fp_slug"].fillna("")
                    else:
                        im2["_slug"] = im2["fp_key"].astype(str).str.split("|").str[0]
                    im2 = im2[im2["_slug"] != ""].drop_duplicates(subset=["_slug"], keep="first")
                    slug_to_fp_id = {}
                    slug_to_canon = {}
                    slug_to_key = {}
                    for _, r in im2.iterrows():
                        slug = r["_slug"]
                        if ("fp_player_id" in im2.columns) and pd.notnull(r.get("fp_player_id")):
                            slug_to_fp_id[slug] = r.get("fp_player_id")
                        if ("canonical_player_id" in im2.columns) and pd.notnull(r.get("canonical_player_id")):
                            slug_to_canon[slug] = r.get("canonical_player_id")
                        if ("fp_key" in im2.columns) and pd.notnull(r.get("fp_key")):
                            slug_to_key[slug] = r.get("fp_key")
                    for idx in ydf.index[need_mask]:
                        nm = str(ydf.at[idx, "player"]).strip()
                        slug = YAHOO_DST_NAME_TO_FP_SLUG.get(nm)
                        if slug and slug in slug_to_fp_id:
                            ydf.at[idx, "fp_player_id"] = slug_to_fp_id.get(slug)
                            if slug in slug_to_canon:
                                ydf.at[idx, "canonical_player_id"] = slug_to_canon.get(slug)
                            if slug in slug_to_key:
                                ydf.at[idx, "fp_key"] = slug_to_key.get(slug)
            except Exception as e:
                print(f"[Export] WARN: DST fallback mapping failed: {e}")
        ydf_export = ydf.rename(columns={"team": "nfl_team"})
        cols = [c for c in ["platform","player_id","player_key","fp_player_id","canonical_player_id","fp_key","player","position","nfl_team","team_name","owner","eligible_positions"] if c in ydf_export.columns]
        used = _safe_to_csv(ydf_export, os.path.join(OUT_DIR, "yahoo_rosters.csv"), index=False, columns=cols, encoding="utf-8-sig")
        print(f"[Export] Wrote {used} ({len(yahoo_df)} rows)")
def export_roster_diffs(sleeper_df: Optional[pd.DataFrame], yahoo_df: Optional[pd.DataFrame]) -> None:
    """Write CSV reports of players rostered in one league but not the other (based on normalized slug + pos)."""
    os.makedirs(OUT_DIR, exist_ok=True)
    def norm(df: Optional[pd.DataFrame]) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame(columns=["key","player","position","team","team_name","owner"])    
        d = df.copy()
        d["position"] = d.get("position", "").astype(str).str.upper().replace({"DST":"DEF","D/ST":"DEF"})
        if "slug_no_suffix" not in d.columns:
            d["slug_no_suffix"] = d["player"].fillna("").map(_slug_no_suffix)
        d["key"] = d["slug_no_suffix"] + "|" + d["position"]
        return d

    s = norm(sleeper_df)
    y = norm(yahoo_df)
    s_keys = set(s["key"]) if not s.empty else set()
    y_keys = set(y["key"]) if not y.empty else set()

    s_only_keys = sorted(s_keys - y_keys)
    y_only_keys = sorted(y_keys - s_keys)

    s_only = s[s["key"].isin(s_only_keys)][["player","position","team","team_name","owner"]]
    y_only = y[y["key"].isin(y_only_keys)][["player","position","team","team_name","owner"]]

    s_out = os.path.join(OUT_DIR, "roster_diff_sleeper_only.csv")
    y_out = os.path.join(OUT_DIR, "roster_diff_yahoo_only.csv")
    s_used = _safe_to_csv(s_only, s_out, index=False, encoding="utf-8-sig")
    y_used = _safe_to_csv(y_only, y_out, index=False, encoding="utf-8-sig")

    print(f"[Export] Wrote {s_used} ({len(s_only)} players)")
    print(f"[Export] Wrote {y_used} ({len(y_only)} players)")

def export_roster_summaries(sleeper_df: Optional[pd.DataFrame], yahoo_df: Optional[pd.DataFrame]) -> None:
    os.makedirs(OUT_DIR, exist_ok=True)
    rows = []
    if sleeper_df is not None and not sleeper_df.empty:
        grp = sleeper_df.groupby("team_name").size().reset_index(name="count")
        for _, r in grp.iterrows():
            rows.append({"platform":"sleeper","team_name": r["team_name"], "count": int(r["count"])})
    if yahoo_df is not None and not yahoo_df.empty:
        grp = yahoo_df.groupby("team_name").size().reset_index(name="count")
        for _, r in grp.iterrows():
            rows.append({"platform":"yahoo","team_name": r["team_name"], "count": int(r["count"])})
    if rows:
        df = pd.DataFrame(rows)
        outp = os.path.join(OUT_DIR, "roster_summary_counts.csv")
        used = _safe_to_csv(df, outp, index=False, encoding="utf-8-sig")
        print(f"[Export] Wrote {used}")

def pull_yahoo(league_key: Optional[str],
               league_id: Optional[str],
               league_url: Optional[str],
               season: int,
               week: Optional[int]) -> Optional[pd.DataFrame]:
    if not (league_key or league_id or league_url):
        return None
    if OAuth2 is None or ygame is None or yteam is None:
        raise SystemExit("Install yahoo_oauth and yahoo_fantasy_api to use --yahoo-* options.")

    sc = ensure_oauth()
    gm = ygame.Game(sc, 'nfl')
    lk = league_key or resolve_league_key(gm, parse_league_id(league_url, league_id), season)
    lg = gm.to_league(lk)
    meta = build_team_meta_map(lg)
    rows = []
    for tk in get_team_keys(lg):
        tm = yteam.Team(sc, tk)
        roster = tm.roster(week=week) if week else tm.roster()
        owner = (meta.get(tk) or {}).get("owner")
        team_name = (meta.get(tk) or {}).get("team_name")
        for p in roster:
            elig = p.get("eligible_positions") or []
            pos = _canonical_pos(choose_primary_from_eligible(elig) or p.get("primary_position"))
            name = (p.get("name") or p.get("full_name") or "").strip()
            pid = str(p.get("player_id") or p.get("player_key") or "")
            pkey = str(p.get("player_key") or "")
            t = _clean_team(p.get("editorial_team_abbr") or p.get("editorial_team_full_name"))
            name_for_slug = re.sub(r"\b(d/?st|dst|defense|def)\b", "", name, flags=re.I).strip() if pos == "DEF" else name
            rows.append({
                "platform": "yahoo",
                "player_id": pid,
                "player_key": pkey,
                "player": name,
                "team": t or "",
                "team_name": team_name or "",
                "owner": owner,
                "eligible_positions": elig,
                "position": pos,
                "slug_no_suffix": _slug_no_suffix(name_for_slug),
            })
    df = pd.DataFrame(rows)
    df.to_parquet(os.path.join(STAGING_DIR, "yahoo_rosters.parquet"), index=False)
    print(f"[Yahoo] Wrote {STAGING_DIR}/yahoo_rosters.parquet")
    return df

# =====================================================
# CLI + Orchestration
# =====================================================
def main():
    ap = argparse.ArgumentParser(description="Unified Fantasy pipeline: Sleeper + Yahoo + FantasyPros → JSON")
    # Sleeper args
    ap.add_argument("--sleeper-username", help="Sleeper username (e.g., DBoiii)")
    ap.add_argument("--sleeper-league", help='Sleeper league name to match (case-insensitive), e.g., "The Degenerates"')
    # Yahoo args (pick one)
    ap.add_argument("--yahoo-league-key", help="Full Yahoo league key, e.g., 461.l.1264351")
    ap.add_argument("--yahoo-league-id", help="Numeric Yahoo league id, e.g., 1264351")
    ap.add_argument("--yahoo-league-url", help="Full Yahoo league URL, e.g., https://football.fantasysports.yahoo.com/f1/1264351")
    # Common
    ap.add_argument("--season", type=int, default=2025, help="Season year (default 2025)")
    ap.add_argument("--week", type=int, default=None, help="NFL week number (omit to use current Yahoo roster view)")
    # R ingestor toggle
    ap.add_argument("--run-r-ingestor", action="store_true", help="Call Rscript r/ffpros_ingest.R before merging")
    args = ap.parse_args()

    # 1) Pull leagues (or fall back to existing staging files)
    sleeper_df = pull_sleeper(args.sleeper_username, args.sleeper_league, args.season)
    yahoo_df = pull_yahoo(args.yahoo_league_key, args.yahoo_league_id, args.yahoo_league_url, args.season, args.week)
    if sleeper_df is None:
        sleeper_df = load_parquet_optional(os.path.join(STAGING_DIR, "sleeper_rosters.parquet"))
        if sleeper_df is not None:
            print(f"[Sleeper] Using cached {STAGING_DIR}/sleeper_rosters.parquet ({len(sleeper_df)} rows)")
    if yahoo_df is None:
        yahoo_df = load_parquet_optional(os.path.join(STAGING_DIR, "yahoo_rosters.parquet"))
        if yahoo_df is not None:
            print(f"[Yahoo] Using cached {STAGING_DIR}/yahoo_rosters.parquet ({len(yahoo_df)} rows)")

    # Normalize/augment eligible_positions for Sleeper so RB/WR/TE include W/R/T
    if sleeper_df is not None and not sleeper_df.empty:
        try:
            sleeper_df["eligible_positions"] = sleeper_df.apply(
                lambda r: _augment_flex_eligibility(r.get("eligible_positions"), r.get("position")), axis=1
            )
        except Exception:
            pass

    # 2) Run R ingestor if requested
    if args.run_r_ingestor:
        run_r_ingestor(args.week)

    # 3) Load FantasyPros parquet exports (must exist either from R run, or pre-supplied)
    fp_weekly_path = os.path.join(STAGING_DIR, "fp_weekly.parquet")
    fp_ros_path = os.path.join(STAGING_DIR, "fp_ros.parquet")
    fp_weekly = load_parquet_optional(fp_weekly_path)
    fp_ros = load_parquet_optional(fp_ros_path)
    if fp_weekly is None or fp_ros is None:
        missing = [p for p, df in [(fp_weekly_path, fp_weekly), (fp_ros_path, fp_ros)] if df is None]
        raise SystemExit(f"Missing FantasyPros parquet(s): {missing}. Run with --run-r-ingestor or provide these files.")

    # 4) Crosswalk / id_map
    id_map = build_or_update_id_map(fp_weekly, sleeper_df, yahoo_df)

    # 5) Assemble JSON
    payload = assemble_json(fp_weekly, fp_ros, id_map, sleeper_df, yahoo_df, season_for_context=args.season, scoring_for_context="PPR")

    # 5b) Export roster CSVs and cross-league diffs for sanity checks
    export_roster_csvs(sleeper_df, yahoo_df, id_map)
    export_roster_summaries(sleeper_df, yahoo_df)
    export_roster_diffs(sleeper_df, yahoo_df)

    # 6) Write JSON
    out_path = os.path.join(OUT_DIR, "projections.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    print(f"[Output] Wrote {out_path} ({len(payload)} players)")

if __name__ == "__main__":
    main()
