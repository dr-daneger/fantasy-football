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

# Columns to exclude from `weekly.stats` because they are bubbled up elsewhere
POINT_COLS = {
    "fantasypts", "fantasypoints", "fantasy_points",
    "fpts", "fpts_ppr", "misc_fpts", "weekly_avg"
}
WEEKLY_EXCLUDE = POINT_COLS | {
    "player", "fp_player_name", "fp_player_slug", "fp_player_id",
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

def slp_build_roster_df(rosters: List[Dict], users: List[Dict], players: Dict) -> pd.DataFrame:
    user_map = {
        u.get("user_id"): {
            "owner": u.get("display_name") or u.get("username"),
            "team_name": (u.get("metadata") or {}).get("team_name") or u.get("display_name") or u.get("username"),
        }
        for u in users
    }

    rows = []
    for r in rosters:
        owner_meta = user_map.get(r.get("owner_id"), {})
        starters = set(r.get("starters") or [])
        reserve = set(r.get("reserve") or [])
        for pid in (r.get("players") or []):
            p = players.get(pid, {})
            name = p.get("full_name") or " ".join(filter(None, [p.get("first_name"), p.get("last_name")])).strip()
            rows.append({
                "platform": "sleeper",
                "team_key": str(r.get("roster_id")),
                "team_name": owner_meta.get("team_name"),
                "owner": owner_meta.get("owner"),
                "player_id": pid,
                "player": name,
                "pos": p.get("position"),
                "nfl_team": p.get("team"),
                "eligible_positions": None,  # Sleeper not exposed here
                "is_starter": pid in starters,
                "is_reserve": pid in reserve,
                "slug_no_suffix": _slug_no_suffix(name),
            })

    df = pd.DataFrame(rows)
    pos_order = {"QB": 1, "RB": 2, "WR": 3, "TE": 4, "FLEX": 5, "K": 6, "DEF": 7, "IDP": 8, None: 99}
    df["pos_sort"] = df["pos"].map(pos_order).fillna(50)
    df = df.sort_values(["owner", "pos_sort", "player"]).drop(columns=["pos_sort"]).reset_index(drop=True)
    return df

def pull_sleeper(username: Optional[str], league_name: Optional[str], season: int) -> Optional[pd.DataFrame]:
    if not username or not league_name:
        return None
    user = slp_get_user(username)
    leagues = slp_get_user_leagues(user["user_id"], season)
    league = slp_pick_league(leagues, league_name)
    league_id = league["league_id"]
    print(f'[Sleeper] Using league: {league.get("name")} (league_id={league_id})')
    users = slp_get_league_users(league_id)
    rosters = slp_get_league_rosters(league_id)
    players = slp_get_players()
    df = slp_build_roster_df(rosters, users, players)
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

def normalize_player_record(p: Dict[str, Any]) -> Dict[str, Any]:
    pid = p.get("player_id") or (p.get("player") or {}).get("player_id")

    name_field = p.get("name")
    if isinstance(name_field, str):
        name = name_field
    else:
        name = None
        if isinstance(name_field, dict):
            name = name_field.get("full")
        if not name and isinstance(p.get("player"), dict):
            n = p["player"].get("name") or {}
            name = n.get("full")

    selected = p.get("selected_position")
    pos = None
    if isinstance(selected, str) and selected and selected not in ("BN", "W/R/T", "W/T", "R/W"):
        pos = selected
    if not pos:
        pos = choose_primary_from_eligible(p.get("eligible_positions"))
    if not pos:
        pt = p.get("position_type")
        pos = "DEF" if pt == "DT" else (pt or None)

    is_starter = False
    if isinstance(selected, str):
        is_starter = selected != "BN"
    else:
        sel = p.get("selected_position")
        if isinstance(sel, list):
            is_starter = any(sp and sp != "BN" for sp in sel)
        elif isinstance(sel, dict):
            is_starter = sel.get("position") not in (None, "BN")

    player_dict = p.get("player") if isinstance(p.get("player"), dict) else {}
    nfl_team = p.get("editorial_team_abbr") or player_dict.get("editorial_team_abbr")

    return {
        "player_id": pid,
        "player": name,
        "pos": pos,
        "nfl_team": nfl_team,
        "eligible_positions": p.get("eligible_positions"),
        "is_starter": bool(is_starter),
        "slug_no_suffix": _slug_no_suffix(name or ""),
    }

def pull_yahoo(league_key: Optional[str], league_id: Optional[str], league_url: Optional[str],
               season: int, week: Optional[int]) -> Optional[pd.DataFrame]:
    if not any([league_key, league_id, league_url]):
        return None
    if OAuth2 is None:
        raise SystemExit("yahoo-fantasy-api/yahoo_oauth not installed. pip install yahoo_fantasy_api yahoo_oauth")

    sc = ensure_oauth()
    gm = ygame.Game(sc, "nfl")

    if league_key and re.match(r"^\d+\.1\.\d+$", league_key):
        print("Note: correcting Yahoo league key '.1.' to '.l.'")
        league_key = re.sub(r"^(\d+)\.1\.(\d+)$", r"\1.l.\2", league_key)

    if league_key:
        lk = league_key
    else:
        raw = parse_league_id(league_url, league_id)
        lk = resolve_league_key(gm, raw, season)

    print(f"[Yahoo] Using league_key: {lk}")
    lg = gm.to_league(lk)

    team_meta = build_team_meta_map(lg)
    team_keys = get_team_keys(lg)
    print(f"[Yahoo] Found {len(team_keys)} team(s)")

    rows: List[Dict] = []
    for tk in team_keys:
        meta = team_meta.get(tk, {})
        team_name = meta.get("team_name") or tk
        owner = meta.get("owner") or team_name
        tm = yteam.Team(sc, tk)
        roster = tm.roster(week=week)
        roster_iter = roster.get("players") if isinstance(roster, dict) else roster
        for p in (roster_iter or []):
            flat = normalize_player_record(p)
            rows.append({
                "platform": "yahoo",
                "team_key": tk,
                "team_name": team_name,
                "owner": owner,
                **flat,
            })

    df = pd.DataFrame(rows).sort_values(["team_name", "owner", "pos", "player"]).reset_index(drop=True)
    df.to_parquet(os.path.join(STAGING_DIR, "yahoo_rosters.parquet"), index=False)
    print(f"[Yahoo] Wrote {STAGING_DIR}/yahoo_rosters.parquet")
    return df

# =====================================================
# FantasyPros (R) ingestor orchestration
# =====================================================
def run_r_ingestor() -> None:
    """
    Calls: Rscript r/ffpros_ingest.R
    Expects the R script to write fp_weekly.parquet and fp_ros.parquet
    """
    cmd = ["Rscript", "r/ffpros_ingest.R"]
    print(f"[R] Running: {' '.join(cmd)}")
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
    posU = (pos or "").upper()
    teamN = _clean_team(team) or ""
    # 1) slug + pos + team
    hit = pool[(pool["slug_no_suffix"] == slug) &
               ((pool["pos"].fillna("").str.upper()) == posU) &
               ((pool["nfl_team"].fillna("").str.upper()) == teamN)]
    if not hit.empty:
        return str(hit.iloc[0][id_col])
    # 2) slug + pos
    hit = pool[(pool["slug_no_suffix"] == slug) &
               ((pool["pos"].fillna("").str.upper()) == posU)]
    if not hit.empty:
        return str(hit.iloc[0][id_col])
    # 3) slug only
    hit = pool[(pool["slug_no_suffix"] == slug)]
    if not hit.empty:
        return str(hit.iloc[0][id_col])
    return None

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
    if "__fp_key" in weekly_fp.columns:
        wk = weekly_fp.copy()
        wk["fp_key"] = weekly_fp["__fp_key"]
    else:
        # Fallback: slug|POS|team
        slug = weekly_fp.get("fp_player_slug", weekly_fp.get("player", pd.Series([""] * len(weekly_fp)))).fillna("").map(_slug_no_suffix)
        pos = weekly_fp.get("pos", pd.Series([""] * len(weekly_fp))).fillna("").str.upper()
        team = weekly_fp.get("team", weekly_fp.get("nfl_team", pd.Series([""] * len(weekly_fp)))).fillna("").str.lower()
        wk = weekly_fp.assign(fp_key=slug + "|" + pos + "|" + team)

    wk["name_norm"] = wk.get("player", wk.get("fp_player_name", "")).fillna("").map(_normalize_name)
    wk["pos"] = wk.get("pos", "").fillna("")
    wk["team"] = wk.get("team", wk.get("nfl_team", "")).fillna("")
    wk["fp_player_id"] = wk.get("fp_player_id", None)

    # Build ownership pools with slug_no_suffix
    slp_pool = None
    yah_pool = None
    if sleeper_df is not None and not sleeper_df.empty:
        slp_pool = sleeper_df.copy()
        if "slug_no_suffix" not in slp_pool.columns:
            slp_pool["slug_no_suffix"] = slp_pool["player"].fillna("").map(_slug_no_suffix)
    if yahoo_df is not None and not yahoo_df.empty:
        yah_pool = yahoo_df.copy()
        if "slug_no_suffix" not in yah_pool.columns:
            yah_pool["slug_no_suffix"] = yah_pool["player"].fillna("").map(_slug_no_suffix)

    id_map_idx = id_map.set_index("fp_key") if not id_map.empty else None
    new_rows = []
    for _, row in wk.iterrows():
        key = row["fp_key"]
        if id_map_idx is not None and key in id_map_idx.index:
            # We could refresh fp_player_id if newly available
            continue

        name_norm = row["name_norm"]
        pos = str(row.get("pos") or "")
        team = _clean_team(row.get("team")) or ""
        slug = _slug_no_suffix(str(row.get("player") or ""))

        # Try flexible ownership match now (we still store only IDs here)
        slp_match = _best_owner_hit(slug, pos, team, slp_pool, "player_id") if slp_pool is not None else None
        yah_match = _best_owner_hit(slug, pos, team, yah_pool, "player_id") if yah_pool is not None else None

        canon = f"canon:{name_norm}:{team}:{pos}"
        new_rows.append({
            "fp_key": key,
            "canonical_player_id": canon,
            "fp_player_id": int(row["fp_player_id"]) if "fp_player_id" in row and pd.notnull(row["fp_player_id"]) else None,
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
        if "__fp_key" in df.columns:
            return df
        slug = df.get("fp_player_slug", df.get("player", pd.Series([""] * len(df)))).fillna("").map(_slug_no_suffix)
        pos = df.get("pos", pd.Series([""] * len(df))).fillna("").str.upper()
        team = df.get("team", df.get("nfl_team", pd.Series([""] * len(df)))).fillna("").str.lower()
        return df.assign(__fp_key=slug + "|" + pos + "|" + team)

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
    ros_idx = fp_ros.set_index("__fp_key") if "__fp_key" in fp_ros.columns else pd.DataFrame()

    out: List[Dict[str, Any]] = []
    for _, row in fp_weekly.iterrows():
        key = row["__fp_key"]
        im = id_map_idx.loc[key] if key in getattr(id_map_idx, "index", []) else None

        # Identity fields
        name = row.get("player") or row.get("fp_player_name")
        pos = (row.get("pos") or row.get("position") or "").upper() or None
        team = _clean_team(row.get("team") or row.get("nfl_team"))
        year = int(row.get("year")) if "year" in row and pd.notnull(row["year"]) else None
        week = int(row.get("week")) if "week" in row and pd.notnull(row["week"]) else None
        fp_player_id = int(row["fp_player_id"]) if "fp_player_id" in row and pd.notnull(row["fp_player_id"]) else None

        # Weekly points: prefer weekly_avg (computed in R from points/floor/ceiling logic)
        weekly_pts = float(row["weekly_avg"]) if "weekly_avg" in row and pd.notnull(row["weekly_avg"]) else None

        # Weekly ranks (points-based)
        rank_overall = int(row["weekly_points_overall_rank"]) if "weekly_points_overall_rank" in row and pd.notnull(row["weekly_points_overall_rank"]) else None
        rank_pos     = int(row["weekly_points_pos_rank"])     if "weekly_points_pos_rank"     in row and pd.notnull(row["weekly_points_pos_rank"])     else None

        # Weekly ECR dispersion
        ecr_rank       = int(row["weekly_ecr_rank"])        if "weekly_ecr_rank"        in row and pd.notnull(row["weekly_ecr_rank"])        else None
        ecr_best_rank  = int(row["weekly_ecr_best_rank"])   if "weekly_ecr_best_rank"   in row and pd.notnull(row["weekly_ecr_best_rank"])   else None
        ecr_worst_rank = int(row["weekly_ecr_worst_rank"])  if "weekly_ecr_worst_rank"  in row and pd.notnull(row["weekly_ecr_worst_rank"])  else None
        ecr_avg_rank   = float(row["weekly_ecr_avg_rank"])  if "weekly_ecr_avg_rank"    in row and pd.notnull(row["weekly_ecr_avg_rank"])    else None
        ecr_sd_rank    = float(row["weekly_ecr_sd_rank"])   if "weekly_ecr_sd_rank"     in row and pd.notnull(row["weekly_ecr_sd_rank"])     else None

        # Weekly uncertainty (from projections)
        w_sd      = float(row["weekly_sd"])      if "weekly_sd"      in row and pd.notnull(row["weekly_sd"])      else None
        w_floor   = float(row["weekly_floor"])   if "weekly_floor"   in row and pd.notnull(row["weekly_floor"])   else None
        w_ceiling = float(row["weekly_ceiling"]) if "weekly_ceiling" in row and pd.notnull(row["weekly_ceiling"]) else None

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
        ros_row = ros_idx.loc[key] if (hasattr(ros_idx, "index") and key in ros_idx.index) else None
        ros_points   = float(ros_row["ros_points"]) if (ros_row is not None and "ros_points" in ros_row and pd.notnull(ros_row["ros_points"])) else None
        ros_rank     = int(ros_row["ros_rank"])     if (ros_row is not None and "ros_rank"   in ros_row and pd.notnull(ros_row["ros_rank"]))   else None
        ros_pos_rank = int(ros_row["ros_pos_rank"]) if (ros_row is not None and "ros_pos_rank" in ros_row and pd.notnull(ros_row["ros_pos_rank"])) else None
        ros_best_rank = int(ros_row["ros_best_rank"]) if (ros_row is not None and "ros_best_rank" in ros_row and pd.notnull(ros_row["ros_best_rank"])) else None
        ros_worst_rank= int(ros_row["ros_worst_rank"])if (ros_row is not None and "ros_worst_rank"in ros_row and pd.notnull(ros_row["ros_worst_rank"])) else None
        ros_avg_rank  = float(ros_row["ros_avg_rank"])if (ros_row is not None and "ros_avg_rank"  in ros_row and pd.notnull(ros_row["ros_avg_rank"]))  else None
        ros_sd_rank   = float(ros_row["ros_sd_rank"]) if (ros_row is not None and "ros_sd_rank"   in ros_row and pd.notnull(ros_row["ros_sd_rank"]))   else None
        ros_pulled_at = str(ros_row["pulled_at"])     if (ros_row is not None and "pulled_at"     in ros_row and pd.notnull(ros_row["pulled_at"]))     else None

        # ownership (via id_map → platform ids)
        own_slp = None
        own_yah = None
        if im is not None:
            slp_id = im["sleeper_player_id"] if "sleeper_player_id" in im else None
            yah_id = im["yahoo_player_id"] if "yahoo_player_id" in im else None
            if slp_id in slp_own:
                o = slp_own[slp_id]
                own_slp = {"team_name": o.get("team_name"), "owner": o.get("owner"), "eligible_positions": o.get("eligible_positions")}
            if yah_id in yah_own:
                o = yah_own[yah_id]
                own_yah = {"team_name": o.get("team_name"), "owner": o.get("owner"), "eligible_positions": o.get("eligible_positions")}

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
                    "ecr_rank": ecr_rank,
                    "ecr_best_rank": ecr_best_rank,
                    "ecr_worst_rank": ecr_worst_rank,
                    "ecr_avg_rank": ecr_avg_rank,
                    "ecr_sd_rank": ecr_sd_rank,
                    "sd": w_sd,
                    "floor": w_floor,
                    "ceiling": w_ceiling,
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
                    "pulled_at": ros_pulled_at
                }
            },
            "metadata": { "source": "fantasypros", "pulled_at": str(row.get("pulled_at") or "") }
        }
        out.append(obj)

    return out

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

    # 1) Pull leagues
    sleeper_df = pull_sleeper(args.sleeper_username, args.sleeper_league, args.season)
    yahoo_df = pull_yahoo(args.yahoo_league_key, args.yahoo_league_id, args.yahoo_league_url, args.season, args.week)

    # 2) Run R ingestor if requested
    if args.run_r_ingestor:
        run_r_ingestor()

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

    # 6) Write JSON
    out_path = os.path.join(OUT_DIR, "projections.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    print(f"[Output] Wrote {out_path} ({len(payload)} players)")

if __name__ == "__main__":
    main()
