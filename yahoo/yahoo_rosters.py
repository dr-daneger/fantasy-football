#!/usr/bin/env python3
# Yahoo Fantasy NFL roster exporter (read-only) — v4
# - Handles roster JSON where fields are: name (str), player_id (int), position_type (str),
#   eligible_positions (list[str]), selected_position (str), status (str)
# - Picks granular positions (QB/RB/WR/TE/K/DEF) instead of 'O'
# - Fills owner with team_name when missing so summary never collapses
# - Robust to different outputs of league.teams() / league.standings()
# - Optional --debug-json dumps first raw roster payload

import argparse
import json
import os
import re
from typing import Dict, List, Optional, Any

import pandas as pd
from yahoo_oauth import OAuth2
from yahoo_fantasy_api import game as ygame
from yahoo_fantasy_api import team as yteam

PREFERRED_POS_ORDER = ['QB', 'RB', 'WR', 'TE', 'K', 'DEF']

def parse_league_id(league_url: Optional[str], league_id: Optional[str]) -> str:
    if league_id:
        return str(league_id)
    if league_url:
        m = re.search(r"/f1/(\d+)", league_url)
        if not m:
            raise SystemExit("Could not parse league_id from the provided URL. Expected .../f1/<league_id>")
        return m.group(1)
    raise SystemExit("Provide --league-id or --league-url")

def ensure_oauth() -> OAuth2:
    cfg_path = os.environ.get("YAHOO_OAUTH_JSON", "oauth2.json")
    if not os.path.exists(cfg_path):
        raise SystemExit(f"{cfg_path} not found. Create it with 'consumer_key' and 'consumer_secret'.")
    sc = OAuth2(None, None, from_file=cfg_path)
    if not sc.token_is_valid():
        sc.refresh_access_token()
    return sc

def get_game(sc: OAuth2, season: int) -> ygame.Game:
    return ygame.Game(sc, "nfl")

def resolve_league_key(gm: ygame.Game, raw_id: str, season: Optional[int]) -> str:
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
        "is_starter": bool(is_starter),
    }

def fetch_league_teams_and_rosters(sc: OAuth2, gm: ygame.Game, league_key: str, week: Optional[int], debug_json: bool = False) -> pd.DataFrame:
    lg = gm.to_league(league_key)

    team_meta = build_team_meta_map(lg)
    team_keys = get_team_keys(lg)
    print(f"Found {len(team_keys)} team(s)")

    rows: List[Dict] = []
    dumped_debug = False
    for team_key in team_keys:
        meta = team_meta.get(team_key, {})
        team_name = meta.get("team_name") or team_key
        owner = meta.get("owner") or team_name

        tm = yteam.Team(sc, team_key)
        roster = tm.roster(week=week)
        roster_iter = roster.get("players") if isinstance(roster, dict) else roster

        if debug_json and not dumped_debug:
            try:
                with open("yahoo_debug_first_roster.json", "w", encoding="utf-8") as f:
                    json.dump(roster, f, ensure_ascii=False, indent=2, default=str)
                dumped_debug = True
                print("Wrote yahoo_debug_first_roster.json")
            except Exception:
                pass

        for p in (roster_iter or []):
            flat = normalize_player_record(p)
            rows.append({
                "team_key": team_key,
                "team_name": team_name,
                "owner": owner,
                **flat,
            })

    df = pd.DataFrame(rows)
    df = df.sort_values(["team_name", "owner", "pos", "player"]).reset_index(drop=True)
    return df

def summarize(df: pd.DataFrame) -> pd.DataFrame:
    df2 = df.copy()
    df2["owner"] = df2["owner"].fillna(df2["team_name"]).fillna("Unknown")

    pivot = df2.pivot_table(
        index=["team_name", "owner"],
        columns="pos",
        values="player_id",
        aggfunc="count",
        fill_value=0,
    )
    pivot.columns = [f"cnt_{str(c)}" for c in pivot.columns]

    starters = (
        df2[df2["is_starter"] == True]
        .groupby(["team_name", "owner"])
        .size()
        .rename("starters_count")
    )
    summary = pivot.join(starters, how="left").fillna({"starters_count": 0}).reset_index()
    return summary

def main():
    ap = argparse.ArgumentParser(description="Export Yahoo Fantasy NFL league rosters to CSV/XLSX.")
    ap.add_argument("--league-id", help="Numeric Yahoo league id (e.g., 1264351)")
    ap.add_argument("--league-url", help="Full Yahoo league URL (e.g., https://football.fantasysports.yahoo.com/f1/1264351)")
    ap.add_argument("--league-key", help="Full Yahoo league key, e.g., 461.l.1264351 (skips discovery)", default=None)
    ap.add_argument("--season", type=int, default=2025, help="Season year (default 2025)")
    ap.add_argument("--week", type=int, default=None, help="NFL week number (omit to use current roster)")
    ap.add_argument("--debug-json", action="store_true", help="Dump first raw roster JSON to yahoo_debug_first_roster.json")
    args = ap.parse_args()

    sc = ensure_oauth()
    gm = get_game(sc, args.season)

    if args.league_key and re.match(r"^\d+\.1\.\d+$", args.league_key):
        print("Note: correcting league key '.1.' to '.l.'")
        args.league_key = re.sub(r"^(\d+)\.1\.(\d+)$", r"\1.l.\2", args.league_key)

    if args.league_key:
        league_key = args.league_key
    else:
        league_id = parse_league_id(args.league_url, args.league_id)
        league_key = resolve_league_key(gm, league_id, args.season)

    print(f"Using league_key: {league_key}")

    df = fetch_league_teams_and_rosters(sc, gm, league_key, args.week, args.debug_json)
    df.to_csv("yahoo_rosters.csv", index=False)
    try:
        df.to_excel("yahoo_rosters.xlsx", index=False)
    except Exception as e:
        print(f"Note: could not write Excel (install openpyxl): {e}")

    summary = summarize(df)
    summary.to_csv("yahoo_roster_summary.csv", index=False)

    print("Wrote yahoo_rosters.csv, yahoo_rosters.xlsx, yahoo_roster_summary.csv")
    print("Tip: First run opens a browser for Yahoo OAuth2; tokens are cached in oauth2.json (or YAHOO_OAUTH_JSON).")

if __name__ == "__main__":
    main()
