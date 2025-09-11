#!/usr/bin/env python3
"""
trade_reco.py — Lineup-aware trade recommendations from unified projections JSON.

Entrypoint:
  python -m trade_reco --input data/out/projections.json --league {yahoo|sleeper} \
    --epsilon 1.5 --top1 30 --top2 10 --max_trades_per_team 5 \
    --shapes 1v1,2v1,1v2 --output json --out data/out/trade_recos.json

Assumptions (per project spec):
  - Use projections.weekly.fantasy_points as-is (no re-scoring).
  - Include K and DEF; FLEX admits {RB,WR,TE} only.
  - ROS ranks are tie-breakers only (not used in valuation).
  - Input JSON is the source of truth; positions and eligible_positions already canonicalized
    (e.g., DEF not DST; FLEX expressed as W/R/T).
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import sys
from dataclasses import dataclass
from itertools import combinations
from typing import Dict, Iterable, List, Optional, Tuple


# ==========================
# Data model (per spec)
# ==========================


@dataclass(frozen=True)
class PlayerKey:
    name: str
    position: str  # QB/RB/WR/TE/K/DEF
    nfl_team: Optional[str]


@dataclass
class PlayerProj:
    next_week: float
    rank_pos_week: Optional[int] = None
    ros_pos_rank: Optional[int] = None


@dataclass
class PlayerOwnership:
    status: str  # OWNED or FREE AGENT (JSON may use "FA")
    team_name: Optional[str]
    eligible_positions: List[str]


@dataclass
class LeaguePlayer:
    key: PlayerKey
    proj: PlayerProj
    ownership: PlayerOwnership  # league-specific view

    def stable_name(self) -> str:
        return self.key.name


@dataclass
class TeamRoster:
    team_name: str
    players: List[LeaguePlayer]


@dataclass
class LeagueState:
    league_id: str
    platform: str  # "yahoo" or "sleeper"
    teams: Dict[str, TeamRoster]  # by team_name
    free_agents: List[LeaguePlayer]
    lineup_slots: Dict[str, int]  # e.g., {"QB":1, "RB":2, ...}


# ==========================
# Utility & config
# ==========================


POSITION_ORDER = ["QB", "RB", "WR", "TE", "K", "DEF"]
FLEX_POOL = {"RB", "WR", "TE"}


def league_default_slots(platform: str) -> Dict[str, int]:
    p = platform.lower()
    if p == "sleeper":
        return {"QB": 1, "WR": 2, "RB": 2, "TE": 1, "FLEX": 2, "K": 1, "DEF": 1}
    if p == "yahoo":
        return {"QB": 1, "WR": 2, "RB": 2, "TE": 1, "FLEX": 1, "K": 1, "DEF": 1}
    raise ValueError(f"Unknown platform: {platform}")


def _canon_pos(pos: Optional[str]) -> Optional[str]:
    if pos is None:
        return None
    p = str(pos).strip().upper()
    # JSON is assumed canonical already; keep simple mapping for safety
    if p in {"DST", "D/ST", "D-STD", "D/S"}:
        return "DEF"
    if p in {"PK"}:
        return "K"
    allowed = {"QB", "RB", "WR", "TE", "K", "DEF"}
    return p if p in allowed else p


def _status_is_owned(status: Optional[str]) -> bool:
    s = (status or "").strip().upper()
    return s == "OWNED"


def _safe_float(x) -> float:
    try:
        if x is None:
            return 0.0
        return float(x)
    except Exception:
        return 0.0


def _sort_key_player(lp: LeaguePlayer) -> Tuple:
    # Stable deterministic ordering
    return (
        -_safe_float(lp.proj.next_week),
        (lp.proj.rank_pos_week or 10**9),
        (lp.proj.ros_pos_rank or 10**9),
        lp.key.name or "",
    )


def _is_flex_eligible(lp: LeaguePlayer) -> bool:
    return any(p in FLEX_POOL for p in lp.ownership.eligible_positions)


def _eligible_for(lp: LeaguePlayer, pos: str) -> bool:
    pos = pos.upper()
    if pos == "FLEX":
        return _is_flex_eligible(lp)
    return pos in lp.ownership.eligible_positions


def _deepcopy_league(league: LeagueState) -> LeagueState:
    teams_copy: Dict[str, TeamRoster] = {}
    for name, tr in league.teams.items():
        teams_copy[name] = TeamRoster(team_name=tr.team_name, players=list(tr.players))
    fa_copy = list(league.free_agents)
    return LeagueState(
        league_id=league.league_id,
        platform=league.platform,
        teams=teams_copy,
        free_agents=fa_copy,
        lineup_slots=dict(league.lineup_slots),
    )


# ==========================
# Loader & league state
# ==========================


def load_players_json(path: str) -> List[dict]:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError("Input JSON must be a list of player objects")
    return data


def build_league_state(
    players: List[dict],
    league_key: str,
    lineup_slots: Dict[str, int],
    league_id: str,
) -> LeagueState:
    league_key = league_key.lower()
    teams: Dict[str, TeamRoster] = {}
    free_agents: List[LeaguePlayer] = []

    for row in players:
        # Required fields (by spec)
        pl = row.get("player", {})
        own = row.get("ownership", {}).get(league_key)
        proj = row.get("projections", {}).get("weekly", {})

        if own is None:
            # Drop rows missing league ownership view
            continue

        name = (pl.get("name") or "").strip()
        pos = _canon_pos(pl.get("position")) or ""
        team = row.get("player", {}).get("team")
        nfl_team = (team if team else None)

        # eligible positions present per user; keep as-is
        eligible = list(own.get("eligible_positions") or [])
        eligible = [(_canon_pos(p) if p != "W/R/T" else "W/R/T") for p in eligible]
        # Fallback: derive eligibility from position if missing/empty
        if not eligible:
            if pos in {"RB", "WR", "TE"}:
                eligible = [pos, "W/R/T"]
            elif pos in {"QB", "K", "DEF"}:
                eligible = [pos]
        # Expand FLEX acceptance for internal checks
        # Keep storage as original tokens; _eligible_for handles FLEX semantics

        next_week = _safe_float(proj.get("fantasy_points"))
        rank_pos_week = proj.get("rank_pos")
        ros_pos_rank = row.get("projections", {}).get("ros", {}).get("pos_rank")

        lp = LeaguePlayer(
            key=PlayerKey(name=name, position=pos, nfl_team=nfl_team),
            proj=PlayerProj(next_week=next_week, rank_pos_week=rank_pos_week, ros_pos_rank=ros_pos_rank),
            ownership=PlayerOwnership(
                status=str(own.get("status") or "").strip(),
                team_name=str(own.get("team_name") or "").strip() or None,
                eligible_positions=eligible,
            ),
        )

        if _status_is_owned(lp.ownership.status):
            team_name = lp.ownership.team_name or "<Unknown Team>"
            if team_name not in teams:
                teams[team_name] = TeamRoster(team_name=team_name, players=[])
            teams[team_name].players.append(lp)
        else:
            free_agents.append(lp)

    # Sort players deterministically within teams and FA
    for tr in teams.values():
        tr.players.sort(key=_sort_key_player)
    free_agents.sort(key=_sort_key_player)

    return LeagueState(
        league_id=league_id,
        platform=league_key,
        teams=teams,
        free_agents=free_agents,
        lineup_slots=lineup_slots,
    )


# ==========================
# Optimizer & scoring
# ==========================


def choose_starters(players: List[LeaguePlayer], slots: Dict[str, int]) -> Tuple[List[LeaguePlayer], float]:
    # Greedy deterministic fill: fixed slots first, then FLEX
    remaining = list(players)
    remaining.sort(key=_sort_key_player)
    starters: List[LeaguePlayer] = []
    total = 0.0

    def take_for(pos: str, n: int):
        nonlocal remaining, starters, total
        if n <= 0:
            return
        filled = 0
        kept: List[LeaguePlayer] = []
        for lp in remaining:
            if _eligible_for(lp, pos):
                kept.append(lp)
                starters.append(lp)
                total += _safe_float(lp.proj.next_week)
                filled += 1
                if filled >= n:
                    break
        # Remove chosen from remaining
        if kept:
            remaining = [p for p in remaining if p not in kept]

    # Fixed order
    for pos in ["QB", "RB", "WR", "TE", "K", "DEF"]:
        take_for(pos, int(slots.get(pos, 0)))

    # FLEX after fixed
    flex_n = int(slots.get("FLEX", 0))
    if flex_n > 0:
        # Among remaining, choose top by next_week where flex-eligible
        flex_pool = [p for p in remaining if _is_flex_eligible(p)]
        flex_pool.sort(key=_sort_key_player)
        chosen = flex_pool[:flex_n]
        for lp in chosen:
            starters.append(lp)
            total += _safe_float(lp.proj.next_week)
        if chosen:
            remaining = [p for p in remaining if p not in chosen]

    return starters, float(total)


def team_lineup_points(team: TeamRoster, slots: Dict[str, int]) -> float:
    _, total = choose_starters(team.players, slots)
    return total


def _bench_players(team: TeamRoster, slots: Dict[str, int]) -> List[LeaguePlayer]:
    starters, _ = choose_starters(team.players, slots)
    bench = [p for p in team.players if p not in starters]
    # worst bench first by next_week ascending
    bench.sort(key=_sort_key_player, reverse=False)
    return bench


def drop_worst_bench(team: TeamRoster, slots: Dict[str, int]) -> Optional[LeaguePlayer]:
    bench = _bench_players(team, slots)
    if not bench:
        return None
    worst = bench[0]
    team.players.remove(worst)
    return worst


def add_best_fa(team: TeamRoster, league: LeagueState) -> Optional[LeaguePlayer]:
    if not league.free_agents:
        return None
    best = league.free_agents[0]
    # Remove from FA pool and add to team
    league.free_agents.pop(0)
    team.players.append(best)
    return best


def add_best_fa_improving(
    team: TeamRoster, league: LeagueState, slots: Dict[str, int], k: int = 30
) -> Optional[LeaguePlayer]:
    """
    Try the top-k free agents and choose the one that most improves the team's
    optimized lineup points (or leaves unchanged if none improve). This avoids
    adding a high-raw-score QB when QB is already capped in starters.
    """
    if not league.free_agents:
        return None
    before = team_lineup_points(team, slots)
    best_pick: Optional[LeaguePlayer] = None
    best_gain = 0.0

    # Consider up to top-k FAs in current sorted order
    pool = league.free_agents[: max(0, k)]
    for fa in pool:
        # Try adding temporarily
        team.players.append(fa)
        after = team_lineup_points(team, slots)
        gain = after - before
        # Revert
        team.players.pop()
        if gain > best_gain + 1e-9:
            best_gain = gain
            best_pick = fa

    if best_pick is None:
        # Fallback to previous simple behavior (add best overall)
        best_pick = league.free_agents[0]
    # Remove chosen FA from pool and add to team
    try:
        league.free_agents.remove(best_pick)
    except ValueError:
        # Shouldn't happen; be resilient
        best_pick = league.free_agents.pop(0)
    team.players.append(best_pick)
    return best_pick


# ==========================
# Candidates & enumeration
# ==========================


def gen_candidates(
    team: TeamRoster, top1: int = 30, top2: int = 10
) -> Tuple[List[LeaguePlayer], List[Tuple[LeaguePlayer, LeaguePlayer]]]:
    singles = list(team.players[: max(0, top1)])
    pairs: List[Tuple[LeaguePlayer, LeaguePlayer]] = []
    two_pool = team.players[: max(0, top2)]
    for a, b in combinations(two_pool, 2):
        pairs.append((a, b))
    return singles, pairs


def enumerate_trades(
    A: TeamRoster,
    B: TeamRoster,
    top1: int = 30,
    top2: int = 10,
    shapes: Iterable[str] = ("1v1", "2v1", "1v2"),
    exclude_kdef_packs: bool = True,
) -> Dict[str, List[Tuple[List[LeaguePlayer], List[LeaguePlayer]]]]:
    a1, a2 = gen_candidates(A, top1=top1, top2=top2)
    b1, b2 = gen_candidates(B, top1=top1, top2=top2)

    out: Dict[str, List[Tuple[List[LeaguePlayer], List[LeaguePlayer]]]] = {"1v1": [], "2v1": [], "1v2": []}

    def is_kdef(lp: LeaguePlayer) -> bool:
        return lp.key.position in {"K", "DEF"}

    if "1v1" in shapes:
        for a in a1:
            for b in b1:
                out["1v1"].append(([a], [b]))

    if "2v1" in shapes:
        for (a, b) in a2:
            if exclude_kdef_packs and is_kdef(a) and is_kdef(b):
                continue
            for c in b1:
                out["2v1"].append(([a, b], [c]))

    if "1v2" in shapes:
        for a in a1:
            for (b, c) in b2:
                if exclude_kdef_packs and is_kdef(b) and is_kdef(c):
                    continue
                out["1v2"].append(([a], [b, c]))

    return out


# ==========================
# Trade simulation & scoring
# ==========================


def apply_trade_and_delta(
    league: LeagueState,
    A: TeamRoster,
    sendA: List[LeaguePlayer],
    B: TeamRoster,
    sendB: List[LeaguePlayer],
    *,
    before_A: Optional[float] = None,
    before_B: Optional[float] = None,
    fa_credit: str = "none",  # none | simple | symmetric
) -> Tuple[float, float]:
    fa_credit = (fa_credit or "none").strip().lower()

    if fa_credit == "symmetric":
        # Compute before with FA improvements for each team independently
        # (using separate league copies to avoid FA-pool coupling)
        LbA = _deepcopy_league(league)
        LbB = _deepcopy_league(league)
        tA_b = LbA.teams[A.team_name]
        tB_b = LbB.teams[B.team_name]
        add_best_fa_improving(tA_b, LbA, LbA.lineup_slots)
        add_best_fa_improving(tB_b, LbB, LbB.lineup_slots)
        before_A_calc = team_lineup_points(tA_b, LbA.lineup_slots)
        before_B_calc = team_lineup_points(tB_b, LbB.lineup_slots)
        if before_A is None:
            before_A = before_A_calc
        if before_B is None:
            before_B = before_B_calc

        # After: perform trade on a fresh copy, then apply FA improvement to both teams
        L = _deepcopy_league(league)
        A2 = L.teams[A.team_name]
        B2 = L.teams[B.team_name]
        # Execute trade
        for p in sendA:
            if p in A2.players:
                A2.players.remove(p)
                B2.players.append(p)
        for p in sendB:
            if p in B2.players:
                B2.players.remove(p)
                A2.players.append(p)
        # Balance only by drops for team receiving more
        if len(sendA) > len(sendB):
            drop_worst_bench(B2, L.lineup_slots)
        elif len(sendB) > len(sendA):
            drop_worst_bench(A2, L.lineup_slots)
        # Symmetric FA improvements for both teams
        add_best_fa_improving(A2, L, L.lineup_slots)
        add_best_fa_improving(B2, L, L.lineup_slots)
        after_A = team_lineup_points(A2, L.lineup_slots)
        after_B = team_lineup_points(B2, L.lineup_slots)
        return (after_A - before_A, after_B - before_B)

    # Default behavior for fa_credit in {"none", "simple"}
    # Work on a copy
    L = _deepcopy_league(league)
    A2 = L.teams[A.team_name]
    B2 = L.teams[B.team_name]

    if before_A is None:
        before_A = team_lineup_points(A2, L.lineup_slots)
    if before_B is None:
        before_B = team_lineup_points(B2, L.lineup_slots)

    # Execute trade
    for p in sendA:
        if p in A2.players:
            A2.players.remove(p)
            B2.players.append(p)
    for p in sendB:
        if p in B2.players:
            B2.players.remove(p)
            A2.players.append(p)

    # Balance roster sizes
    if len(sendA) > len(sendB):
        # A sent more -> B received more -> B drops worst bench
        drop_worst_bench(B2, L.lineup_slots)
        if fa_credit == "simple":
            # prior behavior: grant FA to team that sent more
            add_best_fa_improving(A2, L, L.lineup_slots)
    elif len(sendB) > len(sendA):
        drop_worst_bench(A2, L.lineup_slots)
        if fa_credit == "simple":
            add_best_fa_improving(B2, L, L.lineup_slots)
    # if equal, nothing to do

    after_A = team_lineup_points(A2, L.lineup_slots)
    after_B = team_lineup_points(B2, L.lineup_slots)

    return (after_A - before_A, after_B - before_B)


def passes_epsilon(dA: float, dB: float, eps: float = 1.5) -> bool:
    return dA >= eps and dB >= eps


def _shape_priority(shape: str) -> int:
    # Lower is better
    order = {"1v1": 0, "2v1": 1, "1v2": 1}
    return order.get(shape, 2)


def rank_trades(recs: List[dict]) -> List[dict]:
    # Primary: max min(delta); Secondary: max sum; Tertiary: shape simplicity
    def k(rec: dict):
        dA = float(rec["delta_next"]["A"])
        dB = float(rec["delta_next"]["B"])
        mn = min(dA, dB)
        sm = dA + dB
        return (-mn, -sm, _shape_priority(rec.get("shape", "")))

    return sorted(recs, key=k)


# ==========================
# PR/FR baselines (reporting only)
# ==========================


def _pr_fr(league: LeagueState) -> Tuple[Dict[str, float], float]:
    pr: Dict[str, float] = {}
    for pos in POSITION_ORDER:
        pool = [fa for fa in league.free_agents if _eligible_for(fa, pos)]
        pr[pos] = max((_safe_float(fa.proj.next_week) for fa in pool), default=0.0)
    fr = max(pr.get("RB", 0.0), pr.get("WR", 0.0), pr.get("TE", 0.0))
    return pr, fr


# ==========================
# Positional-need strategy
# ==========================


def choose_starters_detailed(
    players: List[LeaguePlayer], slots: Dict[str, int]
) -> Tuple[Dict[str, List[LeaguePlayer]], float]:
    """Like choose_starters but returns which players filled each slot group.
    Keys: 'QB','RB','WR','TE','K','DEF','FLEX' (lists of players per group)."""
    remaining = list(players)
    remaining.sort(key=_sort_key_player)
    used: Dict[str, List[LeaguePlayer]] = {p: [] for p in ["QB", "RB", "WR", "TE", "K", "DEF", "FLEX"]}
    total = 0.0

    def take_for(pos: str, n: int):
        nonlocal remaining, used, total
        if n <= 0:
            return
        filled = 0
        kept: List[LeaguePlayer] = []
        for lp in remaining:
            if _eligible_for(lp, pos):
                kept.append(lp)
                used[pos].append(lp)
                total += _safe_float(lp.proj.next_week)
                filled += 1
                if filled >= n:
                    break
        if kept:
            remaining = [p for p in remaining if p not in kept]

    for pos in ["QB", "RB", "WR", "TE", "K", "DEF"]:
        take_for(pos, int(slots.get(pos, 0)))

    flex_n = int(slots.get("FLEX", 0))
    if flex_n > 0:
        flex_pool = [p for p in remaining if _is_flex_eligible(p)]
        flex_pool.sort(key=_sort_key_player)
        chosen = flex_pool[:flex_n]
        for lp in chosen:
            used["FLEX"].append(lp)
            total += _safe_float(lp.proj.next_week)
        if chosen:
            remaining = [p for p in remaining if p not in chosen]

    return used, float(total)


def _group_by_pos(players: List[LeaguePlayer]) -> Dict[str, List[LeaguePlayer]]:
    out: Dict[str, List[LeaguePlayer]] = {p: [] for p in POSITION_ORDER}
    for lp in players:
        out.setdefault(lp.key.position, []).append(lp)
    # Sort each group descending by next
    for pos, lst in out.items():
        lst.sort(key=_sort_key_player)
    return out


def compute_positional_snapshot(team: TeamRoster, league: LeagueState) -> dict:
    slots = league.lineup_slots
    used, _ = choose_starters_detailed(team.players, slots)
    bench = [p for p in team.players if all(p not in used[g] for g in used)]
    bench.sort(key=_sort_key_player, reverse=False)  # worst bench first

    pr, fr = _pr_fr(league)

    # Need by fixed positions
    need_by_pos: Dict[str, float] = {}
    for pos in POSITION_ORDER:
        cnt = int(slots.get(pos, 0))
        if cnt <= 0:
            need_by_pos[pos] = 0.0
            continue
        starters = used.get(pos, [])
        gap = 0.0
        for lp in starters:
            gap += max(0.0, _safe_float(pr.get(pos, 0.0)) - _safe_float(lp.proj.next_week))
        need_by_pos[pos] = float(gap)

    # Need for FLEX group
    need_flex = 0.0
    for lp in used.get("FLEX", []):
        need_flex += max(0.0, fr - _safe_float(lp.proj.next_week))

    # Surplus by position (best bench vs replacement)
    bench_by_pos: Dict[str, List[LeaguePlayer]] = _group_by_pos([p for p in team.players if p in bench])
    surplus_by_pos: Dict[str, float] = {}
    for pos in POSITION_ORDER:
        lst = bench_by_pos.get(pos, [])
        if not lst:
            surplus_by_pos[pos] = 0.0
            continue
        top = _safe_float(lst[0].proj.next_week)
        baseline = fr if pos in FLEX_POOL else _safe_float(pr.get(pos, 0.0))
        surplus_by_pos[pos] = max(0.0, top - baseline)

    return {
        "used": used,
        "bench": bench,
        "bench_by_pos": bench_by_pos,
        "need_by_pos": need_by_pos,
        "need_flex": float(need_flex),
        "surplus_by_pos": surplus_by_pos,
        "pr": pr,
        "fr": fr,
    }


def classify_needs_surpluses(
    snap: dict, need_thresh: float = 1.5, surplus_thresh: float = 1.5
) -> Dict[str, set]:
    needs: set = set()
    for pos, val in snap["need_by_pos"].items():
        if val >= need_thresh:
            needs.add(pos)
    # FLEX need propagates to any FLEX-eligible pos
    if snap.get("need_flex", 0.0) >= need_thresh:
        needs.update(FLEX_POOL)

    surplus: set = set()
    for pos, val in snap["surplus_by_pos"].items():
        if val >= surplus_thresh:
            surplus.add(pos)

    return {"needs": needs, "surplus": surplus}


def enumerate_positional_trades(
    A: TeamRoster,
    B: TeamRoster,
    league: LeagueState,
    *,
    pos_top: int = 5,
    need_thresh: float = 1.5,
    surplus_thresh: float = 1.5,
    exclude_kdef_packs: bool = True,
) -> Dict[str, List[Tuple[List[LeaguePlayer], List[LeaguePlayer]]]]:
    """Enumerate targeted trades based on mutual needs/surpluses by position.

    Returns a dict with same shape keys as enumerate_trades: {shape: [([sendA],[sendB]), ...]}
    """
    out: Dict[str, List[Tuple[List[LeaguePlayer], List[LeaguePlayer]]]] = {"1v1": [], "2v1": [], "1v2": []}

    A_snap = compute_positional_snapshot(A, league)
    B_snap = compute_positional_snapshot(B, league)
    A_cls = classify_needs_surpluses(A_snap, need_thresh=need_thresh, surplus_thresh=surplus_thresh)
    B_cls = classify_needs_surpluses(B_snap, need_thresh=need_thresh, surplus_thresh=surplus_thresh)

    # Candidates by pos
    A_bench_pos = A_snap["bench_by_pos"]
    B_bench_pos = B_snap["bench_by_pos"]

    # Helper to check both are K/DEF for packing rule
    def both_kdef(xs: List[LeaguePlayer]) -> bool:
        kinds = {lp.key.position for lp in xs}
        return kinds.issubset({"K", "DEF"}) and len(kinds) == 1

    # 1v1: A gives from A_surplus, B gives from B_surplus, matching the other side's needs
    for posA in sorted(A_cls["surplus"]):
        for posB in sorted(B_cls["surplus"]):
            if posA not in B_cls["needs"] and posB not in A_cls["needs"]:
                continue
            A_pool = A_bench_pos.get(posA, [])[: max(0, pos_top)]
            B_pool = B_bench_pos.get(posB, [])[: max(0, pos_top)]
            for a in A_pool:
                for b in B_pool:
                    out["1v1"].append(([a], [b]))

    # 2v1: A sends two from surplus (can be same pos or mixed), B sends one from surplus that A needs
    # Limit combinations for tractability
    A_pairs: List[Tuple[LeaguePlayer, LeaguePlayer]] = []
    # Build a small pool from A surplus positions
    small_pool_A: List[LeaguePlayer] = []
    for pos in sorted(A_cls["surplus"]):
        small_pool_A += A_bench_pos.get(pos, [])[: max(0, min(3, pos_top))]
    # Deduplicate while preserving order
    seen = set()
    A_small_unique: List[LeaguePlayer] = []
    for p in small_pool_A:
        if id(p) in seen:
            continue
        seen.add(id(p))
        A_small_unique.append(p)
    # Create pairs
    for i in range(len(A_small_unique)):
        for j in range(i + 1, len(A_small_unique)):
            pair = (A_small_unique[i], A_small_unique[j])
            if exclude_kdef_packs and pair[0].key.position in {"K", "DEF"} and pair[1].key.position in {"K", "DEF"}:
                continue
            A_pairs.append(pair)
    # B singles pool
    small_pool_B: List[LeaguePlayer] = []
    for pos in sorted(B_cls["surplus"]):
        if pos in A_cls["needs"]:
            small_pool_B += B_bench_pos.get(pos, [])[: max(0, min(3, pos_top))]
    for (a, b) in A_pairs:
        for c in small_pool_B:
            out["2v1"].append(([a, b], [c]))

    # 1v2: symmetric of above
    B_pairs: List[Tuple[LeaguePlayer, LeaguePlayer]] = []
    small_pool_B2: List[LeaguePlayer] = []
    for pos in sorted(B_cls["surplus"]):
        small_pool_B2 += B_bench_pos.get(pos, [])[: max(0, min(3, pos_top))]
    seen = set()
    B_small_unique: List[LeaguePlayer] = []
    for p in small_pool_B2:
        if id(p) in seen:
            continue
        seen.add(id(p))
        B_small_unique.append(p)
    for i in range(len(B_small_unique)):
        for j in range(i + 1, len(B_small_unique)):
            pair = (B_small_unique[i], B_small_unique[j])
            if exclude_kdef_packs and pair[0].key.position in {"K", "DEF"} and pair[1].key.position in {"K", "DEF"}:
                continue
            B_pairs.append(pair)
    small_pool_A1: List[LeaguePlayer] = []
    for pos in sorted(A_cls["surplus"]):
        if pos in B_cls["needs"]:
            small_pool_A1 += A_bench_pos.get(pos, [])[: max(0, min(3, pos_top))]
    for (b1, b2) in B_pairs:
        for a in small_pool_A1:
            out["1v2"].append(([a], [b1, b2]))

    return out


# ==========================
# End-to-end recommend
# ==========================


def recommend_trades(
    league: LeagueState,
    epsilon: float = 1.5,
    top1: int = 30,
    top2: int = 10,
    shapes: Iterable[str] = ("1v1", "2v1", "1v2"),
    exclude_kdef_packs: bool = True,
    focus_team: Optional[str] = None,
    max_trades_per_team: int = 5,
    *,
    strategy: str = "baseline",
    need_threshold: float = 1.5,
    surplus_threshold: float = 1.5,
    pos_top: int = 5,
    fa_credit: str = "none",
) -> List[dict]:
    team_names = sorted(league.teams)
    recs: List[dict] = []

    # Precompute before-lineup points per team to avoid recomputation inside apply
    before_points = {t: team_lineup_points(league.teams[t], league.lineup_slots) for t in team_names}

    # Enumerate team pairs deterministically (A < B)
    for i, a_name in enumerate(team_names):
        if focus_team and a_name != focus_team:
            continue
        for b_name in team_names[i + 1 :]:
            A = league.teams[a_name]
            B = league.teams[b_name]
            if strategy == "baseline":
                enum = enumerate_trades(
                    A,
                    B,
                    top1=top1,
                    top2=top2,
                    shapes=shapes,
                    exclude_kdef_packs=exclude_kdef_packs,
                )
            elif strategy == "pos-need":
                enum = enumerate_positional_trades(
                    A,
                    B,
                    league,
                    pos_top=pos_top,
                    need_thresh=need_threshold,
                    surplus_thresh=surplus_threshold,
                    exclude_kdef_packs=exclude_kdef_packs,
                )
            elif strategy == "all":
                enum_base = enumerate_trades(
                    A,
                    B,
                    top1=top1,
                    top2=top2,
                    shapes=shapes,
                    exclude_kdef_packs=exclude_kdef_packs,
                )
                enum_pos = enumerate_positional_trades(
                    A,
                    B,
                    league,
                    pos_top=pos_top,
                    need_thresh=need_threshold,
                    surplus_thresh=surplus_threshold,
                    exclude_kdef_packs=exclude_kdef_packs,
                )
                # Merge proposals
                enum = {k: enum_base.get(k, []) + enum_pos.get(k, []) for k in {"1v1", "2v1", "1v2"}}
            else:
                enum = {"1v1": [], "2v1": [], "1v2": []}

            for shape, proposals in enum.items():
                if shape not in shapes:
                    continue
                for (sendA, sendB) in proposals:
                    # Guard against overlapping players (shouldn't happen, but be safe)
                    if any(p in B.players for p in sendA) or any(p in A.players for p in sendB):
                        continue

                    dA, dB = apply_trade_and_delta(
                        league,
                        A,
                        sendA,
                        B,
                        sendB,
                        before_A=before_points[a_name],
                        before_B=before_points[b_name],
                        fa_credit=fa_credit,
                    )
                    if not passes_epsilon(dA, dB, eps=epsilon):
                        continue

                    recs.append(
                        {
                            "league": league.platform,
                            "teams": [A.team_name, B.team_name],
                            "shape": shape,
                            "sendA": [p.stable_name() for p in sendA],
                            "sendB": [p.stable_name() for p in sendB],
                            "delta_next": {"A": round(float(dA), 3), "B": round(float(dB), 3)},
                        }
                    )

    # Rank and optionally cap per team
    recs = rank_trades(recs)
    if max_trades_per_team and max_trades_per_team > 0:
        counts: Dict[str, int] = {}
        filtered: List[dict] = []
        for r in recs:
            A, B = r["teams"]
            if counts.get(A, 0) >= max_trades_per_team and counts.get(B, 0) >= max_trades_per_team:
                continue
            filtered.append(r)
            counts[A] = counts.get(A, 0) + 1
            counts[B] = counts.get(B, 0) + 1
        recs = filtered
    return recs


# ==========================
# CLI & I/O
# ==========================


def _read_file_bytes(path: str) -> bytes:
    with open(path, "rb") as f:
        return f.read()


def _write_output(recs: List[dict], out_fmt: str, out_path: Optional[str]) -> None:
    out_fmt = out_fmt.lower()
    if out_fmt == "jsonl":
        text = "\n".join(json.dumps(r, ensure_ascii=False) for r in recs)
        if out_path:
            with open(out_path, "w", encoding="utf-8") as f:
                f.write(text)
        else:
            print(text)
        return
    if out_fmt == "csv":
        # Flatten a few fields for CSV convenience
        rows = []
        for r in recs:
            rows.append(
                {
                    "league": r.get("league"),
                    "teamA": r.get("teams", [None, None])[0],
                    "teamB": r.get("teams", [None, None])[1],
                    "shape": r.get("shape"),
                    "sendA": ", ".join(r.get("sendA", [])),
                    "sendB": ", ".join(r.get("sendB", [])),
                    "deltaA": r.get("delta_next", {}).get("A"),
                    "deltaB": r.get("delta_next", {}).get("B"),
                }
            )
        if out_path:
            with open(out_path, "w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=list(rows[0].keys()) if rows else [])
                if rows:
                    w.writeheader()
                    w.writerows(rows)
        else:
            w = csv.DictWriter(sys.stdout, fieldnames=list(rows[0].keys()) if rows else [])
            if rows:
                w.writeheader()
                w.writerows(rows)
        return

    # default: json
    payload = {"recommendations": recs}
    if out_path:
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    else:
        print(json.dumps(payload, ensure_ascii=False, indent=2))


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Trade recommendations from projections JSON")
    ap.add_argument("--input", required=True, help="Path to projections.json")
    ap.add_argument("--league", required=True, choices=["yahoo", "sleeper"], help="League platform")
    ap.add_argument("--league-id", default=None, help="Optional league identifier for metadata")
    ap.add_argument("--epsilon", type=float, default=1.5, help="Win-win threshold per team")
    ap.add_argument("--top1", type=int, default=30, help="Top-N singles per team")
    ap.add_argument("--top2", type=int, default=10, help="Top-N pool for two-packs per team")
    ap.add_argument("--max_trades_per_team", type=int, default=5, help="Cap of recs per team in output")
    ap.add_argument(
        "--shapes",
        default="1v1,2v1,1v2",
        help="Comma-separated shapes to consider (subset of 1v1,2v1,1v2)",
    )
    ap.add_argument("--min_fa_points", type=float, default=None, help="Optional FA pruning threshold")
    ap.add_argument(
        "--strategy",
        choices=["baseline", "pos-need", "all"],
        default="baseline",
        help="Trade enumeration strategy: baseline (top-N) or pos-need (positional weaknesses)",
    )
    ap.add_argument("--need-threshold", type=float, default=1.5, help="Points below replacement to flag a need")
    ap.add_argument("--surplus-threshold", type=float, default=1.5, help="Points above replacement for tradeable surplus")
    ap.add_argument("--pos-top", type=int, default=5, help="Top-N bench per position to consider in pos-need mode")
    ap.add_argument(
        "--fa-credit",
        choices=["none", "simple", "symmetric"],
        default="none",
        help="How to account for FA improvements in deltas: none (default), simple (grant FA to team sending more), symmetric (grant FA to both before and after)",
    )
    ap.add_argument("--exclude-kdef-packs", action="store_true", default=True, help="Drop two-packs that are both K/DEF")
    ap.add_argument("--no-exclude-kdef-packs", dest="exclude_kdef_packs", action="store_false")
    ap.add_argument("--focus-team", default=None, help="Only generate recs involving this team")
    ap.add_argument("--output", choices=["json", "jsonl", "csv"], default="json")
    ap.add_argument("--out", default=None, help="Output file path (default: stdout)")
    ap.add_argument("--debug", action="store_true", help="Verbose debug logs")
    args = ap.parse_args(argv)

    players = load_players_json(args.input)

    slots = league_default_slots(args.league)
    league_id = args.league_id or args.league
    league = build_league_state(players, league_key=args.league, lineup_slots=slots, league_id=league_id)

    # FA pruning (optional)
    if args.min_fa_points is not None:
        before = len(league.free_agents)
        league.free_agents = [fa for fa in league.free_agents if _safe_float(fa.proj.next_week) >= args.min_fa_points]
        if args.debug:
            print(f"[debug] FA pruned: {before} -> {len(league.free_agents)} (min_fa_points={args.min_fa_points})")

    pr, fr = _pr_fr(league)

    shapes = [s.strip() for s in args.shapes.split(",") if s.strip()]
    recs = recommend_trades(
        league,
        epsilon=args.epsilon,
        top1=args.top1,
        top2=args.top2,
        shapes=shapes,
        exclude_kdef_packs=args.exclude_kdef_packs,
        focus_team=args.focus_team,
        max_trades_per_team=args.max_trades_per_team,
        strategy=args.strategy,
        need_threshold=args.need_threshold,
        surplus_threshold=args.surplus_threshold,
        pos_top=args.pos_top,
        fa_credit=args.fa_credit,
    )

    # Attach a simple metadata block for transparency
    meta = {
        "league": args.league,
        "league_id": league_id,
        "epsilon": args.epsilon,
        "top1": args.top1,
        "top2": args.top2,
        "shapes": shapes,
        "exclude_kdef_packs": args.exclude_kdef_packs,
        "strategy": args.strategy,
        "fa_credit": args.fa_credit,
        "input_path": os.path.abspath(args.input),
        "input_sha256": hashlib.sha256(_read_file_bytes(args.input)).hexdigest(),
        "pr_next": pr,
        "fr_next": fr,
    }

    # For JSON output, include metadata wrapper
    if args.output == "json":
        payload = {"recommendations": rank_trades(recs), "metadata": meta}
        if args.out:
            with open(args.out, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
        else:
            print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        # For jsonl/csv, emit recs only, and log metadata to stderr in debug
        if args.debug:
            print(json.dumps({"metadata": meta}, ensure_ascii=False, indent=2), file=sys.stderr)
        _write_output(rank_trades(recs), args.output, args.out)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
