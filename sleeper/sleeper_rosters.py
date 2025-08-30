#!/usr/bin/env python3
"""
Sleeper league roster puller by username + league name.

Usage:
  python sleeper_rosters.py --username DBoiii --league "The Degenerates" --season 2025

Outputs:
  - sleeper_rosters.csv          (all players per roster with owner + starter flag)
  - sleeper_rosters.xlsx         (same as Excel; requires openpyxl)
  - sleeper_roster_summary.csv   (one row per roster with counts by position)

Endpoints used (public, read-only):
  - GET /v1/user/{username}
  - GET /v1/user/{user_id}/leagues/nfl/{season}
  - GET /v1/league/{league_id}
  - GET /v1/league/{league_id}/users
  - GET /v1/league/{league_id}/rosters
  - GET /v1/players/nfl

Notes:
  - Caches the players dictionary to disk (players_nfl.json) to avoid re-downloading
    the large blob every run.
  - Picks the league whose name matches case-insensitively; if multiple, chooses
    the most recently created.
"""

import argparse
import json
import os
from typing import Dict, List

import pandas as pd
import requests

BASE = "https://api.sleeper.app/v1"
PLAYERS_CACHE = "players_nfl.json"


def _get(url: str):
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return r.json()


def get_user(username: str) -> Dict:
    return _get(f"{BASE}/user/{username}")


def get_user_leagues(user_id: str, season: int) -> List[Dict]:
    return _get(f"{BASE}/user/{user_id}/leagues/nfl/{season}")


def pick_league(leagues: List[Dict], league_name: str) -> Dict:
    if not leagues:
        raise SystemExit("No leagues found for this user/season.")

    exact = [lg for lg in leagues if (lg.get("name") or "").lower() == league_name.lower()]
    if exact:
        matches = exact
    else:
        matches = [lg for lg in leagues if league_name.lower() in (lg.get("name") or "").lower()]

    if not matches:
        names = [lg.get("name") for lg in leagues]
        raise SystemExit(f'No league named like "{league_name}". Available: {names}')

    # If multiple matches, pick the most recently created
    matches.sort(key=lambda lg: lg.get("created", 0), reverse=True)
    return matches[0]


def get_league_users(league_id: str) -> List[Dict]:
    return _get(f"{BASE}/league/{league_id}/users")


def get_league_rosters(league_id: str) -> List[Dict]:
    return _get(f"{BASE}/league/{league_id}/rosters")


def get_players() -> Dict:
    if os.path.exists(PLAYERS_CACHE):
        with open(PLAYERS_CACHE, "r", encoding="utf-8") as f:
            return json.load(f)
    data = _get(f"{BASE}/players/nfl")
    with open(PLAYERS_CACHE, "w", encoding="utf-8") as f:
        json.dump(data, f)
    return data


def build_roster_df(rosters: List[Dict], users: List[Dict], players: Dict) -> pd.DataFrame:
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
            name = p.get("full_name") or " ".join(
                filter(None, [p.get("first_name"), p.get("last_name")])
            ).strip()
            rows.append({
                "roster_id": r.get("roster_id"),
                "owner": owner_meta.get("owner"),
                "team_name": owner_meta.get("team_name"),
                "player_id": pid,
                "player": name,
                "pos": p.get("position"),
                "nfl_team": p.get("team"),
                "is_starter": pid in starters,
                "is_reserve": pid in reserve,
            })

    df = pd.DataFrame(rows)
    pos_order = {"QB": 1, "RB": 2, "WR": 3, "TE": 4, "FLEX": 5, "K": 6, "DEF": 7, "IDP": 8, None: 99}
    df["pos_sort"] = df["pos"].map(pos_order).fillna(50)
    df = df.sort_values(["owner", "pos_sort", "player"]).drop(columns=["pos_sort"]).reset_index(drop=True)
    return df


def summarize_rosters(df: pd.DataFrame) -> pd.DataFrame:
    pivot = df.pivot_table(index=["owner", "team_name", "roster_id"],
                           columns="pos",
                           values="player_id",
                           aggfunc="count",
                           fill_value=0)
    pivot.columns = [f"cnt_{c}" for c in pivot.columns]
    starters = df[df["is_starter"]].groupby(["owner", "team_name", "roster_id"]).size().rename("starters_count")
    summary = pivot.join(starters, how="left").fillna({"starters_count": 0}).reset_index()
    return summary


def main():
    ap = argparse.ArgumentParser(description="Pull Sleeper league rosters into CSV/Excel.")
    ap.add_argument("--username", required=True, help="Sleeper username (e.g., DBoiii)")
    ap.add_argument("--league", required=True, help="League name to match (case-insensitive)")
    ap.add_argument("--season", type=int, default=2025, help="Season year, default 2025")
    args = ap.parse_args()

    user = get_user(args.username)
    leagues = get_user_leagues(user["user_id"], args.season)
    league = pick_league(leagues, args.league)

    league_id = league["league_id"]
    print(f'Using league: {league.get("name")} (league_id={league_id})')

    users = get_league_users(league_id)
    rosters = get_league_rosters(league_id)
    players = get_players()

    df = build_roster_df(rosters, users, players)
    df.to_csv("sleeper_rosters.csv", index=False)
    try:
        df.to_excel("sleeper_rosters.xlsx", index=False)
    except Exception as e:
        print(f"Could not write Excel file (install openpyxl): {e}")

    summary = summarize_rosters(df)
    summary.to_csv("sleeper_roster_summary.csv", index=False)

    print("Wrote: sleeper_rosters.csv, sleeper_rosters.xlsx, sleeper_roster_summary.csv")
    print("Tip: players_nfl.json cached locally to speed future runs.")


if __name__ == "__main__":
    main()
