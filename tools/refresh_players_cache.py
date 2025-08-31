#!/usr/bin/env python3
# tools/refresh_players_cache.py

import json, re, unicodedata, requests
from pathlib import Path
from typing import Any, Dict

OUTFILE = Path("players_nfl.json")

TEAM_ALIASES = {
    "WAS": "WSH", "WFT": "WSH", "WASHINGTON": "WSH",
    "JAX": "JAC",
    "LV": "LVR", "OAK": "LVR",
    "LA": "LAR", "STL": "LAR",
    "SD": "LAC",
}

SUFFIX_RE = re.compile(r"\b(jr|sr|ii|iii|iv|v)\b\.?", re.IGNORECASE)

def strip_suffix(name: str) -> str: return SUFFIX_RE.sub("", name).strip()

def slugify(name: str) -> str:
    base = strip_suffix(name)
    base = unicodedata.normalize("NFKD", base).encode("ascii","ignore").decode("ascii")
    base = re.sub(r"[^a-z0-9]+", "-", base.lower()).strip("-")
    return base

def normalize_team(team: Any) -> str | None:
    if team is None: return None
    s = str(team).strip().upper()
    if s in {"", "NA", "NONE", "NULL"}: return None
    return TEAM_ALIASES.get(s, s)

def main():
    url = "https://api.sleeper.app/v1/players/nfl"
    print(f"[Sleeper] GET {url}")
    data: Dict[str, Dict[str, Any]] = requests.get(url, timeout=60).json()

    out: Dict[str, Any] = {}
    for pid, p in data.items():
        first = (p.get("first_name") or "").strip()
        last  = (p.get("last_name") or "").strip()
        full  = (p.get("full_name") or f"{first} {last}").strip()
        if not full: continue

        out[pid] = {
            "sleeper_player_id": pid,
            "name": strip_suffix(full),
            "slug": slugify(full),
            "position": p.get("position"),
            "team": normalize_team(p.get("team")),
            "bye_week": p.get("bye_week"),
            "status": p.get("status"),
        }

    OUTFILE.write_text(json.dumps(out, ensure_ascii=False))
    print(f"[Sleeper] Wrote {OUTFILE} ({len(out)} players))")

if __name__ == "__main__":
    main()
