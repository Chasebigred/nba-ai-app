from __future__ import annotations

import logging
from datetime import datetime
import os
import re
from typing import Any, Dict, Optional, Tuple

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from mangum import Mangum

from sqlalchemy import desc, func, Float
from sqlalchemy.dialects.postgresql import insert

from db import SessionLocal
from models import Game, Player, PlayerGameStats, StandingsCurrent, Team

# NBA data source used ONLY for refresh/ETL (not for user-facing "live" reads)
from nba_api.stats.endpoints import leaguestandings

# Your ETL job (updates Games/Players/Stats in the warehouse)
from etl.backfill_last_14_days import main as backfill_main

# OpenAI (server-side only)
from pydantic import BaseModel
from openai import OpenAI

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="NBA AI App API")

# -----------------------------------------------------------------------------
# CORS
# -----------------------------------------------------------------------------
FRONTEND_ORIGINS = os.getenv(
    "FRONTEND_ORIGINS",
    "http://localhost:5173"
).split(",")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[o.strip() for o in FRONTEND_ORIGINS],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -----------------------------------------------------------------------------
# HEALTH
# -----------------------------------------------------------------------------
@app.get("/health")
def health():
    """Basic health check for uptime monitoring and local debugging."""
    return {"status": "ok"}


# ---------------------------------------------------------------------------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------------------------------------------------------------------------
def safe_avg(vals):
    """Average that gracefully handles nulls."""
    vals = [v for v in vals if v is not None]
    return round(sum(vals) / len(vals), 3) if vals else None


def minutes_to_float(m):
    """
    Converts "MM:SS" (or "M:SS") to float minutes.
    Returns None if missing/unparseable.
    """
    if not m:
        return None
    if isinstance(m, (int, float)):
        return float(m)

    s = str(m).strip()
    try:
        if ":" in s:
            mm, ss = s.split(":")
            return float(mm) + (float(ss) / 60.0)
        return float(s)
    except Exception:
        return None


# Postgres expression: PlayerGameStats.minutes ("MM:SS") -> float minutes
def minutes_expr_pg():
    mm = func.nullif(func.split_part(PlayerGameStats.minutes, ":", 1), "")
    ss = func.nullif(func.split_part(PlayerGameStats.minutes, ":", 2), "")
    return (
        func.coalesce(func.cast(mm, Float), 0.0)
        + (func.coalesce(func.cast(ss, Float), 0.0) / 60.0)
    )


def resolve_player(db, name: str) -> Optional[Player]:
    """
    Resolve a player by fuzzy-ish name match against Player.full_name.
    """
    n = (name or "").strip()
    if not n:
        return None
    return (
        db.query(Player)
        .filter(func.lower(Player.full_name).like(f"%{n.lower()}%"))
        .order_by(Player.full_name.asc())
        .first()
    )


def clamp_n(n: int, *, default: int, min_n: int = 1, max_n: int = 50) -> int:
    """
    Clamp N to protect DB + OpenAI payload sizes.
    - min_n: minimum window
    - max_n: maximum window (adjust if you want)
    """
    try:
        n_int = int(n)
    except Exception:
        return default
    if n_int < min_n:
        return min_n
    if n_int > max_n:
        return max_n
    return n_int


def parse_last_n_games(q: str) -> Optional[int]:
    """
    Extracts N from phrases like:
      - "last 3 games"
      - "last 10 game(s)"
    Returns int(N) or None.
    """
    m = re.search(r"\blast\s+(\d+)\s+games?\b", q, re.I)
    if not m:
        return None
    try:
        return int(m.group(1))
    except Exception:
        return None


# ---------------------------------------------------------------------------------------------------------------------------------------------
# WAREHOUSE (DB) - READ ENDPOINTS
# ---------------------------------------------------------------------------------------------------------------------------------------------
@app.get("/warehouse/counts")
def warehouse_counts():
    """Quick counts for sanity checking DB state (useful during development)."""
    db = SessionLocal()
    try:
        return {
            "teams": db.query(Team).count(),
            "players": db.query(Player).count(),
            "games": db.query(Game).count(),
            "player_game_stats": db.query(PlayerGameStats).count(),
        }
    finally:
        db.close()


@app.get("/warehouse/players/search")
def search_players(q: str, limit: int = 20):
    """
    Player search against the warehouse.

    Used by the Player tab (type-ahead).
    We do a simple case-insensitive LIKE match and return the top results.
    """
    db = SessionLocal()
    try:
        q_clean = q.strip()
        if not q_clean:
            return {"query": q, "count": 0, "players": []}

        rows = (
            db.query(Player)
            .filter(func.lower(Player.full_name).like(f"%{q_clean.lower()}%"))
            .order_by(Player.full_name.asc())
            .limit(limit)
            .all()
        )

        return {
            "query": q,
            "count": len(rows),
            "players": [
                {
                    "nba_player_id": p.nba_player_id,
                    "full_name": p.full_name,
                    "nba_team_id": p.nba_team_id,
                }
                for p in rows
            ],
        }
    finally:
        db.close()


@app.get("/warehouse/player/{nba_player_id}/last_n")
def player_last_n(nba_player_id: int, season: str = "2025-26", n: int = 10):
    """
    Returns a player's most recent N games from the warehouse + computed averages.

    Notes:
    - Filters to regular-season-style NBA game IDs (002%).
    - Sorts most recent first using Game.game_date.
    - Shooting averages are computed as totals ratio (better than averaging per-game %).
    """
    n = clamp_n(n, default=10, max_n=82)
    db = SessionLocal()
    try:
        rows = (
            db.query(PlayerGameStats, Game)
            .join(Game, PlayerGameStats.nba_game_id == Game.nba_game_id)
            .filter(PlayerGameStats.nba_player_id == nba_player_id)
            .filter(Game.season == season)
            .filter(Game.nba_game_id.like("002%"))
            .order_by(desc(Game.game_date))
            .limit(n)
            .all()
        )

        if not rows:
            return {
                "nba_player_id": nba_player_id,
                "n": n,
                "count": 0,
                "averages": {},
                "games": [],
                "source": "warehouse",
            }

        stats = [r[0] for r in rows]
        games = [r[1] for r in rows]

        out_games = []
        for s, g in zip(stats, games):
            fg_pct_game = (float(s.fgm or 0) / float(s.fga or 0)) if (s.fga or 0) > 0 else None

            out_games.append(
                {
                    "nba_game_id": s.nba_game_id,
                    "game_date": g.game_date.isoformat() + "Z" if g.game_date else None,
                    "nba_team_id": s.nba_team_id,
                    "minutes": s.minutes,
                    "fg_pct": round(fg_pct_game, 3) if fg_pct_game is not None else None,
                    "pts": s.pts,
                    "reb": s.reb,
                    "ast": s.ast,
                    "stl": s.stl,
                    "blk": s.blk,
                    "tov": s.tov,
                    "fg3m": s.fg3m,
                    "fg3a": s.fg3a,
                    "plus_minus": s.plus_minus,
                }
            )

        total_fgm = sum((s.fgm or 0) for s in stats)
        total_fga = sum((s.fga or 0) for s in stats)

        total_fg3m = sum((s.fg3m or 0) for s in stats)
        total_fg3a = sum((s.fg3a or 0) for s in stats)

        total_ftm = sum((s.ftm or 0) for s in stats)
        total_fta = sum((s.fta or 0) for s in stats)

        return {
            "nba_player_id": nba_player_id,
            "n": n,
            "count": len(stats),
            "averages": {
                "pts": safe_avg([s.pts for s in stats]),
                "reb": safe_avg([s.reb for s in stats]),
                "ast": safe_avg([s.ast for s in stats]),
                "stl": safe_avg([s.stl for s in stats]),
                "blk": safe_avg([s.blk for s in stats]),
                "tov": safe_avg([s.tov for s in stats]),
                "min": safe_avg([minutes_to_float(s.minutes) for s in stats]),
                "fg_pct": (round(total_fgm / total_fga, 3) if total_fga > 0 else None),
                "fg3_pct": (round(total_fg3m / total_fg3a, 3) if total_fg3a > 0 else None),
                "ft_pct": (round(total_ftm / total_fta, 3) if total_fta > 0 else None),
            },
            "games": out_games,
            "source": "warehouse",
        }
    finally:
        db.close()


# ---------------------------------------------------------------------------------------------------------------------------------------------
# WAREHOUSE LEADERS (DB) - READ ENDPOINTS
# ---------------------------------------------------------------------------------------------------------------------------------------------
def _base_leaders_query(db, season: str):
    """
    Common base: join stats -> games -> player -> team.
    Filters to regular season-style game IDs (002%).
    """
    return (
        db.query(
            PlayerGameStats.nba_player_id.label("player_id"),
            Player.full_name.label("player_name"),
            Team.abbreviation.label("team_abbreviation"),
            func.count(PlayerGameStats.id).label("gp"),
        )
        .join(Game, PlayerGameStats.nba_game_id == Game.nba_game_id)
        .join(Player, Player.nba_player_id == PlayerGameStats.nba_player_id)
        .outerjoin(Team, Team.nba_team_id == Player.nba_team_id)
        .filter(Game.season == season)
        .filter(Game.nba_game_id.like("002%"))
    )


@app.get("/warehouse/leaders/pts")
def warehouse_leaders_pts(season: str = "2025-26", min_gp: int = 10, limit: int = 25):
    """PPG leaders computed as SUM(pts) / games_played."""
    db = SessionLocal()
    try:
        q = (
            _base_leaders_query(db, season)
            .add_columns(func.sum(PlayerGameStats.pts).label("total"))
            .group_by(PlayerGameStats.nba_player_id, Player.full_name, Team.abbreviation)
        )

        leaders = []
        for r in q.all():
            gp = int(r.gp or 0)
            total = float(r.total or 0)
            if gp < min_gp or gp == 0:
                continue

            leaders.append(
                {
                    "player_id": int(r.player_id),
                    "player_name": r.player_name,
                    "team_abbreviation": r.team_abbreviation,
                    "gp": gp,
                    "value": round(total / gp, 2),
                    "total": int(total),
                }
            )

        leaders.sort(key=lambda x: x["value"], reverse=True)

        return {
            "season": season,
            "min_gp": min_gp,
            "limit": limit,
            "count": len(leaders[:limit]),
            "leaders": leaders[:limit],
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "source": "warehouse",
        }
    finally:
        db.close()


@app.get("/warehouse/leaders/reb")
def warehouse_leaders_reb(season: str = "2025-26", min_gp: int = 10, limit: int = 25):
    """RPG leaders computed as SUM(reb) / games_played."""
    db = SessionLocal()
    try:
        q = (
            _base_leaders_query(db, season)
            .add_columns(func.sum(PlayerGameStats.reb).label("total"))
            .group_by(PlayerGameStats.nba_player_id, Player.full_name, Team.abbreviation)
        )

        leaders = []
        for r in q.all():
            gp = int(r.gp or 0)
            total = float(r.total or 0)
            if gp < min_gp or gp == 0:
                continue

            leaders.append(
                {
                    "player_id": int(r.player_id),
                    "player_name": r.player_name,
                    "team_abbreviation": r.team_abbreviation,
                    "gp": gp,
                    "value": round(total / gp, 2),
                    "total": int(total),
                }
            )

        leaders.sort(key=lambda x: x["value"], reverse=True)

        return {
            "season": season,
            "min_gp": min_gp,
            "limit": limit,
            "count": len(leaders[:limit]),
            "leaders": leaders[:limit],
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "source": "warehouse",
        }
    finally:
        db.close()


@app.get("/warehouse/leaders/ast")
def warehouse_leaders_ast(season: str = "2025-26", min_gp: int = 10, limit: int = 25):
    """APG leaders computed as SUM(ast) / games_played."""
    db = SessionLocal()
    try:
        q = (
            _base_leaders_query(db, season)
            .add_columns(func.sum(PlayerGameStats.ast).label("total"))
            .group_by(PlayerGameStats.nba_player_id, Player.full_name, Team.abbreviation)
        )

        leaders = []
        for r in q.all():
            gp = int(r.gp or 0)
            total = float(r.total or 0)
            if gp < min_gp or gp == 0:
                continue

            leaders.append(
                {
                    "player_id": int(r.player_id),
                    "player_name": r.player_name,
                    "team_abbreviation": r.team_abbreviation,
                    "gp": gp,
                    "value": round(total / gp, 2),
                    "total": int(total),
                }
            )

        leaders.sort(key=lambda x: x["value"], reverse=True)

        return {
            "season": season,
            "min_gp": min_gp,
            "limit": limit,
            "count": len(leaders[:limit]),
            "leaders": leaders[:limit],
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "source": "warehouse",
        }
    finally:
        db.close()


@app.get("/warehouse/leaders/blk")
def warehouse_leaders_blk(season: str = "2025-26", min_gp: int = 10, limit: int = 25):
    """BPG leaders computed as SUM(blk) / games_played."""
    db = SessionLocal()
    try:
        q = (
            _base_leaders_query(db, season)
            .add_columns(func.sum(PlayerGameStats.blk).label("total"))
            .group_by(PlayerGameStats.nba_player_id, Player.full_name, Team.abbreviation)
        )

        leaders = []
        for r in q.all():
            gp = int(r.gp or 0)
            total = float(r.total or 0)
            if gp < min_gp or gp == 0:
                continue

            leaders.append(
                {
                    "player_id": int(r.player_id),
                    "player_name": r.player_name,
                    "team_abbreviation": r.team_abbreviation,
                    "gp": gp,
                    "value": round(total / gp, 2),
                    "total": int(total),
                }
            )

        leaders.sort(key=lambda x: x["value"], reverse=True)

        return {
            "season": season,
            "min_gp": min_gp,
            "limit": limit,
            "count": len(leaders[:limit]),
            "leaders": leaders[:limit],
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "source": "warehouse",
        }
    finally:
        db.close()


@app.get("/warehouse/leaders/fg")
def warehouse_leaders_fg_pct(season: str = "2025-26", min_fga: int = 100, min_gp: int = 10, limit: int = 25):
    """
    FG% leaders computed from warehouse totals:
      fg_pct = SUM(fgm) / SUM(fga)

    Uses min_fga to avoid tiny sample sizes.
    Returns `value` as a 0..1 float.
    """
    db = SessionLocal()
    try:
        q = (
            _base_leaders_query(db, season)
            .add_columns(func.sum(PlayerGameStats.fgm).label("fgm"))
            .add_columns(func.sum(PlayerGameStats.fga).label("fga"))
            .group_by(PlayerGameStats.nba_player_id, Player.full_name, Team.abbreviation)
        )

        leaders = []
        for r in q.all():
            gp = int(r.gp or 0)
            fgm = int(r.fgm or 0)
            fga = int(r.fga or 0)

            if gp < min_gp or fga < min_fga:
                continue

            pct = (fgm / fga) if fga else 0.0
            leaders.append(
                {
                    "player_id": int(r.player_id),
                    "player_name": r.player_name,
                    "team_abbreviation": r.team_abbreviation,
                    "gp": gp,
                    "value": round(pct, 4),
                }
            )

        leaders.sort(key=lambda x: x["value"], reverse=True)

        return {
            "season": season,
            "min_gp": min_gp,
            "min_fga": min_fga,
            "limit": limit,
            "count": len(leaders[:limit]),
            "leaders": leaders[:limit],
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "source": "warehouse",
        }
    finally:
        db.close()


@app.get("/warehouse/leaders/3pt")
def warehouse_leaders_3pt(season: str = "2025-26", min_3pa: int = 50, min_gp: int = 10, limit: int = 25):
    """
    3PT% leaders computed from warehouse totals:
      fg3_pct = SUM(fg3m) / SUM(fg3a)
    """
    db = SessionLocal()
    try:
        q = (
            db.query(
                PlayerGameStats.nba_player_id.label("player_id"),
                Player.full_name.label("player_name"),
                Team.abbreviation.label("team_abbreviation"),
                func.sum(PlayerGameStats.fg3m).label("fg3m"),
                func.sum(PlayerGameStats.fg3a).label("fg3a"),
                func.count(PlayerGameStats.id).label("gp"),
            )
            .join(Game, PlayerGameStats.nba_game_id == Game.nba_game_id)
            .join(Player, Player.nba_player_id == PlayerGameStats.nba_player_id)
            .outerjoin(Team, Team.nba_team_id == Player.nba_team_id)
            .filter(Game.season == season)
            .filter(Game.nba_game_id.like("002%"))
            .group_by(PlayerGameStats.nba_player_id, Player.full_name, Team.abbreviation)
        )

        leaders = []
        for r in q.all():
            fg3a = int(r.fg3a or 0)
            fg3m = int(r.fg3m or 0)
            gp = int(r.gp or 0)

            if gp < min_gp or fg3a < min_3pa:
                continue

            pct = (fg3m / fg3a) if fg3a else 0.0
            leaders.append(
                {
                    "player_id": int(r.player_id),
                    "player_name": r.player_name,
                    "team_abbreviation": r.team_abbreviation,
                    "fg3_pct": round(pct, 4),
                    "fg3m": fg3m,
                    "fg3a": fg3a,
                    "gp": gp,
                }
            )

        leaders.sort(key=lambda x: x["fg3_pct"], reverse=True)

        return {
            "season": season,
            "min_3pa": min_3pa,
            "min_gp": min_gp,
            "limit": limit,
            "count": len(leaders[:limit]),
            "leaders": leaders[:limit],
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "source": "warehouse",
        }
    finally:
        db.close()


@app.get("/warehouse/standings/current")
def warehouse_standings_current(season: str = "2025-26"):
    """
    Read standings from the warehouse.
    The standings table is refreshed by /warehouse/standings/refresh.
    """
    db = SessionLocal()
    try:
        rows = (
            db.query(StandingsCurrent)
            .filter(StandingsCurrent.season == season)
            .order_by(StandingsCurrent.conference.asc(), StandingsCurrent.playoff_rank.asc())
            .all()
        )

        teams = [
            {
                "team_id": r.team_id,
                "team_name": r.team_name,
                "team_city": r.team_city,
                "team_slug": r.team_slug,
                "conference": r.conference,
                "playoff_rank": r.playoff_rank,
                "wins": r.wins,
                "losses": r.losses,
                "win_pct": r.win_pct,
                "home": r.home,
                "road": r.road,
                "l10": r.l10,
                "streak": r.streak,
            }
            for r in rows
        ]

        latest = max((r.updated_at for r in rows), default=None)

        return {
            "season": season,
            "generated_at": (latest.isoformat() + "Z") if latest else None,
            "count": len(teams),
            "teams": teams,
            "source": "warehouse",
        }
    finally:
        db.close()


# ---------------------------------------------------------------------------------------------------------------------------------------------
# AI ENDPOINTS (OpenAI + Warehouse)
# ---------------------------------------------------------------------------------------------------------------------------------------------
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4.1-mini").strip()

_openai_client: Optional[OpenAI] = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None


class AiAskRequest(BaseModel):
    question: str
    season: str = "2025-26"


def summarize_with_openai(question: str, payload: Dict[str, Any]) -> str:
    if not _openai_client:
        raise HTTPException(status_code=500, detail="OPENAI_API_KEY is not configured in this environment.")

    instructions = (
        "You are an NBA stats analyst. "
        "Answer using ONLY the provided JSON data. "
        "If the data is insufficient, say exactly what is missing. "
        "Be concise (max ~10 bullets). Include key numbers."
    )

    resp = _openai_client.responses.create(
        model=OPENAI_MODEL,
        instructions=instructions,
        input=[
            {
                "role": "user",
                "content": f"Question: {question}\n\nDATA(JSON): {payload}",
            }
        ],
    )
    return (getattr(resp, "output_text", "") or "").strip() or "No response text returned from OpenAI."


def parse_compare_players(q: str) -> Optional[Tuple[str, str, int]]:
    # "Compare Stephen Curry and Damian Lillard in the last 5 games."
    m = re.search(r"compare\s+(.+?)\s+and\s+(.+?)\s+in\s+the\s+last\s+(\d+)\s+games?", q, re.I)
    if not m:
        return None
    return m.group(1).strip(), m.group(2).strip(), int(m.group(3))


def parse_summarize_last_n(q: str) -> Optional[Tuple[str, int]]:
    # "Summarize Nikola Jokić’s last 10 games"
    m = re.search(r"summarize\s+(.+?)['’]s\s+last\s+(\d+)\s+games?", q, re.I)
    if not m:
        return None
    return m.group(1).strip(), int(m.group(2))


@app.post("/ai/ask")
def ai_ask(req: AiAskRequest):
    question = (req.question or "").strip()
    if not question:
        raise HTTPException(status_code=400, detail="question is required")

    season = req.season or "2025-26"
    q_lower = question.lower()

    # Generic N extraction for any “last N games” question
    n_from_q = parse_last_n_games(question)

    db = SessionLocal()
    try:
        # ---------------------------------------------------------------------
        # Intent 1: Compare Player A vs Player B in last N games
        # ---------------------------------------------------------------------
        parsed = parse_compare_players(question)
        if parsed:
            p1_name, p2_name, n = parsed
            n = clamp_n(n, default=5, max_n=50)

            p1 = resolve_player(db, p1_name)
            p2 = resolve_player(db, p2_name)
            if not p1 or not p2:
                raise HTTPException(status_code=404, detail="Could not resolve one or both player names.")

            def fetch_last_n(pid: int):
                return (
                    db.query(PlayerGameStats, Game)
                    .join(Game, PlayerGameStats.nba_game_id == Game.nba_game_id)
                    .filter(PlayerGameStats.nba_player_id == pid)
                    .filter(Game.season == season)
                    .filter(Game.nba_game_id.like("002%"))
                    .order_by(desc(Game.game_date))
                    .limit(n)
                    .all()
                )

            def summarize_rows(rows):
                stats = [r[0] for r in rows]
                games = [r[1] for r in rows]
                gp = len(stats) or 1

                total_fgm = sum((s.fgm or 0) for s in stats)
                total_fga = sum((s.fga or 0) for s in stats)

                return {
                    "window_games_requested": n,
                    "gp": len(stats),
                    "avg_pts": round(sum((s.pts or 0) for s in stats) / gp, 2),
                    "avg_reb": round(sum((s.reb or 0) for s in stats) / gp, 2),
                    "avg_ast": round(sum((s.ast or 0) for s in stats) / gp, 2),
                    "fg_pct": round(total_fgm / total_fga, 3) if total_fga else None,
                    "avg_plus_minus": round(safe_avg([s.plus_minus for s in stats]) or 0, 2),
                    "games": [
                        {
                            "date": (g.game_date.isoformat() + "Z") if g.game_date else None,
                            "pts": s.pts,
                            "reb": s.reb,
                            "ast": s.ast,
                            "fgm": s.fgm,
                            "fga": s.fga,
                            "plus_minus": s.plus_minus,
                            "min": s.minutes,
                        }
                        for s, g in zip(stats, games)
                    ],
                }

            data = {
                "season": season,
                "question_type": "compare_players_last_n",
                "n": n,
                "playerA": {
                    "nba_player_id": p1.nba_player_id,
                    "name": p1.full_name,
                    "last_n": summarize_rows(fetch_last_n(p1.nba_player_id)),
                },
                "playerB": {
                    "nba_player_id": p2.nba_player_id,
                    "name": p2.full_name,
                    "last_n": summarize_rows(fetch_last_n(p2.nba_player_id)),
                },
            }

            answer = summarize_with_openai(question, data)
            return {"intent": "compare_players_last_n", "answer": answer, "data": data}

        # ---------------------------------------------------------------------
        # Intent 2: Summarize Player's last N games
        # ---------------------------------------------------------------------
        parsed = parse_summarize_last_n(question)
        if parsed:
            name, n = parsed
            n = clamp_n(n, default=10, max_n=50)
            p = resolve_player(db, name)
            if not p:
                raise HTTPException(status_code=404, detail="Could not resolve player name.")

            rows = (
                db.query(PlayerGameStats, Game)
                .join(Game, PlayerGameStats.nba_game_id == Game.nba_game_id)
                .filter(PlayerGameStats.nba_player_id == p.nba_player_id)
                .filter(Game.season == season)
                .filter(Game.nba_game_id.like("002%"))
                .order_by(desc(Game.game_date))
                .limit(n)
                .all()
            )
            if not rows:
                raise HTTPException(status_code=404, detail="No games found for that player/season.")

            stats = [r[0] for r in rows]
            games = [r[1] for r in rows]
            gp = len(stats) or 1

            total_fgm = sum((s.fgm or 0) for s in stats)
            total_fga = sum((s.fga or 0) for s in stats)

            data = {
                "season": season,
                "question_type": "player_last_n_summary",
                "player": {"nba_player_id": p.nba_player_id, "name": p.full_name},
                "n": n,
                "gp": len(stats),
                "avg_pts": round(sum((s.pts or 0) for s in stats) / gp, 2),
                "avg_reb": round(sum((s.reb or 0) for s in stats) / gp, 2),
                "avg_ast": round(sum((s.ast or 0) for s in stats) / gp, 2),
                "fg_pct": round(total_fgm / total_fga, 3) if total_fga else None,
                "avg_plus_minus": round(safe_avg([s.plus_minus for s in stats]) or 0, 2),
                "games": [
                    {
                        "date": (g.game_date.isoformat() + "Z") if g.game_date else None,
                        "pts": s.pts,
                        "reb": s.reb,
                        "ast": s.ast,
                        "stl": s.stl,
                        "blk": s.blk,
                        "tov": s.tov,
                        "fgm": s.fgm,
                        "fga": s.fga,
                        "fg3m": s.fg3m,
                        "fg3a": s.fg3a,
                        "plus_minus": s.plus_minus,
                        "min": s.minutes,
                    }
                    for s, g in zip(stats, games)
                ],
            }

            answer = summarize_with_openai(question, data)
            return {"intent": "player_last_n_summary", "answer": answer, "data": data}

        # ---------------------------------------------------------------------
        # Intent 3: Most improved scoring over last N games (vs season avg)
        # ---------------------------------------------------------------------
        if "improved" in q_lower and "scoring" in q_lower and "last" in q_lower and "game" in q_lower:
            n = clamp_n(n_from_q or 10, default=10, max_n=50)

            season_sub = (
                db.query(
                    PlayerGameStats.nba_player_id.label("player_id"),
                    Player.full_name.label("player_name"),
                    Team.abbreviation.label("team"),
                    func.count(PlayerGameStats.id).label("gp"),
                    func.sum(PlayerGameStats.pts).label("pts_total"),
                )
                .join(Game, PlayerGameStats.nba_game_id == Game.nba_game_id)
                .join(Player, Player.nba_player_id == PlayerGameStats.nba_player_id)
                .outerjoin(Team, Team.nba_team_id == Player.nba_team_id)
                .filter(Game.season == season)
                .filter(Game.nba_game_id.like("002%"))
                .group_by(PlayerGameStats.nba_player_id, Player.full_name, Team.abbreviation)
            ).subquery()

            rn = func.row_number().over(
                partition_by=PlayerGameStats.nba_player_id,
                order_by=desc(Game.game_date),
            ).label("rn")

            lastn_sub = (
                db.query(
                    PlayerGameStats.nba_player_id.label("player_id"),
                    PlayerGameStats.pts.label("pts"),
                    rn,
                )
                .join(Game, PlayerGameStats.nba_game_id == Game.nba_game_id)
                .filter(Game.season == season)
                .filter(Game.nba_game_id.like("002%"))
            ).subquery()

            lastn_agg = (
                db.query(
                    lastn_sub.c.player_id,
                    func.count().label("gp_lastn"),
                    func.avg(lastn_sub.c.pts).label("ppg_lastn"),
                )
                .filter(lastn_sub.c.rn <= n)
                .group_by(lastn_sub.c.player_id)
            ).subquery()

            rows = (
                db.query(
                    season_sub.c.player_id,
                    season_sub.c.player_name,
                    season_sub.c.team,
                    season_sub.c.gp,
                    (season_sub.c.pts_total / func.nullif(season_sub.c.gp, 0)).label("ppg_season"),
                    lastn_agg.c.ppg_lastn,
                    (lastn_agg.c.ppg_lastn - (season_sub.c.pts_total / func.nullif(season_sub.c.gp, 0))).label("delta"),
                )
                .join(lastn_agg, lastn_agg.c.player_id == season_sub.c.player_id)
                .filter(season_sub.c.gp >= max(10, n))  # needs enough season games to be meaningful
                .filter(lastn_agg.c.gp_lastn >= n)
                .order_by(desc("delta"))
                .limit(25)
                .all()
            )

            data = {
                "season": season,
                "question_type": "top_improved_scoring_lastN_vs_season",
                "n": n,
                "top_25": [
                    {
                        "player_id": int(r.player_id),
                        "player_name": r.player_name,
                        "team": r.team,
                        "gp": int(r.gp or 0),
                        "ppg_season": round(float(r.ppg_season or 0), 2),
                        "ppg_lastN": round(float(r.ppg_lastn or 0), 2),
                        "delta": round(float(r.delta or 0), 2),
                    }
                    for r in rows
                ],
            }

            answer = summarize_with_openai(question, data)
            return {"intent": "top_improved_scoring_lastN_vs_season", "answer": answer, "data": data}

        # ---------------------------------------------------------------------
        # Intent 4: Star players trending up (last N vs season avg)
        # Definition (v1): "star" = season PPG >= 20 and GP >= max(10, N)
        # ---------------------------------------------------------------------
        if "star" in q_lower and "trending" in q_lower and "last" in q_lower and "season" in q_lower and "game" in q_lower:
            n = clamp_n(n_from_q or 10, default=10, max_n=50)

            season_sub = (
                db.query(
                    PlayerGameStats.nba_player_id.label("player_id"),
                    Player.full_name.label("player_name"),
                    Team.abbreviation.label("team"),
                    func.count(PlayerGameStats.id).label("gp"),
                    func.sum(PlayerGameStats.pts).label("pts_total"),
                )
                .join(Game, PlayerGameStats.nba_game_id == Game.nba_game_id)
                .join(Player, Player.nba_player_id == PlayerGameStats.nba_player_id)
                .outerjoin(Team, Team.nba_team_id == Player.nba_team_id)
                .filter(Game.season == season)
                .filter(Game.nba_game_id.like("002%"))
                .group_by(PlayerGameStats.nba_player_id, Player.full_name, Team.abbreviation)
            ).subquery()

            rn = func.row_number().over(
                partition_by=PlayerGameStats.nba_player_id,
                order_by=desc(Game.game_date),
            ).label("rn")

            lastn_sub = (
                db.query(
                    PlayerGameStats.nba_player_id.label("player_id"),
                    PlayerGameStats.pts.label("pts"),
                    rn,
                )
                .join(Game, PlayerGameStats.nba_game_id == Game.nba_game_id)
                .filter(Game.season == season)
                .filter(Game.nba_game_id.like("002%"))
            ).subquery()

            lastn_agg = (
                db.query(
                    lastn_sub.c.player_id,
                    func.count().label("gp_lastn"),
                    func.avg(lastn_sub.c.pts).label("ppg_lastn"),
                )
                .filter(lastn_sub.c.rn <= n)
                .group_by(lastn_sub.c.player_id)
            ).subquery()

            ppg_season_expr = (season_sub.c.pts_total / func.nullif(season_sub.c.gp, 0))
            delta_expr = (lastn_agg.c.ppg_lastn - ppg_season_expr)

            rows = (
                db.query(
                    season_sub.c.player_id,
                    season_sub.c.player_name,
                    season_sub.c.team,
                    season_sub.c.gp,
                    ppg_season_expr.label("ppg_season"),
                    lastn_agg.c.ppg_lastn,
                    delta_expr.label("delta"),
                )
                .join(lastn_agg, lastn_agg.c.player_id == season_sub.c.player_id)
                .filter(season_sub.c.gp >= max(10, n))
                .filter(lastn_agg.c.gp_lastn >= n)
                .filter(ppg_season_expr >= 20)
                .order_by(desc("delta"))
                .limit(15)
                .all()
            )

            data = {
                "season": season,
                "question_type": "stars_trending_up_lastN_vs_season",
                "n": n,
                "definition": {"star": f"season PPG >= 20 and GP >= {max(10, n)}"},
                "top_15": [
                    {
                        "player_id": int(r.player_id),
                        "player_name": r.player_name,
                        "team": r.team,
                        "gp": int(r.gp or 0),
                        "ppg_season": round(float(r.ppg_season or 0), 2),
                        "ppg_lastN": round(float(r.ppg_lastn or 0), 2),
                        "delta": round(float(r.delta or 0), 2),
                    }
                    for r in rows
                ],
            }

            answer = summarize_with_openai(question, data)
            return {"intent": "stars_trending_up_lastN_vs_season", "answer": answer, "data": data}

        # ---------------------------------------------------------------------
        # Intent 5: Top 5 'winning impact' players by avg +/- in last N games
        # ---------------------------------------------------------------------
        if "winning impact" in q_lower and "+/-" in q_lower and "last" in q_lower and "game" in q_lower:
            n = clamp_n(n_from_q or 3, default=3, max_n=25)  # keep this smaller; +/- is noisy & payload grows

            rn = func.row_number().over(
                partition_by=PlayerGameStats.nba_player_id,
                order_by=desc(Game.game_date),
            ).label("rn")

            sub = (
                db.query(
                    PlayerGameStats.nba_player_id.label("player_id"),
                    PlayerGameStats.plus_minus.label("plus_minus"),
                    rn,
                )
                .join(Game, PlayerGameStats.nba_game_id == Game.nba_game_id)
                .filter(Game.season == season)
                .filter(Game.nba_game_id.like("002%"))
            ).subquery()

            agg = (
                db.query(
                    sub.c.player_id,
                    func.count().label("gp_lastn"),
                    func.avg(sub.c.plus_minus).label("avg_plus_minus_lastn"),
                )
                .filter(sub.c.rn <= n)
                .group_by(sub.c.player_id)
                .having(func.count() == n)  # ensure exactly N games available
            ).subquery()

            rows = (
                db.query(
                    agg.c.player_id,
                    Player.full_name.label("player_name"),
                    Team.abbreviation.label("team"),
                    agg.c.avg_plus_minus_lastn,
                )
                .join(Player, Player.nba_player_id == agg.c.player_id)
                .outerjoin(Team, Team.nba_team_id == Player.nba_team_id)
                .order_by(desc(agg.c.avg_plus_minus_lastn))
                .limit(5)
                .all()
            )

            data = {
                "season": season,
                "question_type": "top_winning_impact_avg_plus_minus_lastN",
                "n": n,
                "top_5": [
                    {
                        "player_id": int(r.player_id),
                        "player_name": r.player_name,
                        "team": r.team,
                        "avg_plus_minus_lastN": round(float(r.avg_plus_minus_lastn or 0), 2),
                    }
                    for r in rows
                ],
            }

            answer = summarize_with_openai(question, data)
            return {"intent": "top_winning_impact_avg_plus_minus_lastN", "answer": answer, "data": data}

        # ---------------------------------------------------------------------
        # Intent 6: Best single game stat line so far this season (PTS/REB/AST)
        # Definition: max (PTS + REB + AST) in one game
        # ---------------------------------------------------------------------
        if "best single game stat line" in q_lower or ("best" in q_lower and "single game" in q_lower and "stat line" in q_lower):
            score = (
                func.coalesce(PlayerGameStats.pts, 0)
                + func.coalesce(PlayerGameStats.reb, 0)
                + func.coalesce(PlayerGameStats.ast, 0)
            ).label("pra")

            row = (
                db.query(
                    PlayerGameStats,
                    Game,
                    Player.full_name.label("player_name"),
                    Team.abbreviation.label("team"),
                    score,
                )
                .join(Game, PlayerGameStats.nba_game_id == Game.nba_game_id)
                .join(Player, Player.nba_player_id == PlayerGameStats.nba_player_id)
                .outerjoin(Team, Team.nba_team_id == Player.nba_team_id)
                .filter(Game.season == season)
                .filter(Game.nba_game_id.like("002%"))
                .order_by(desc("pra"))
                .first()
            )

            if not row:
                raise HTTPException(status_code=404, detail="No games found for that season.")

            s, g, player_name, team, pra = row

            data = {
                "season": season,
                "question_type": "best_single_game_stat_line",
                "definition": "max (PTS + REB + AST)",
                "best_game": {
                    "player_name": player_name,
                    "team": team,
                    "nba_game_id": s.nba_game_id,
                    "game_date": g.game_date.isoformat() + "Z" if g.game_date else None,
                    "pts": s.pts,
                    "reb": s.reb,
                    "ast": s.ast,
                    "pra": int(pra or 0),
                },
            }

            answer = summarize_with_openai(question, data)
            return {"intent": "best_single_game_stat_line", "answer": answer, "data": data}

        # ---------------------------------------------------------------------
        # Intent 7: Who has the most minutes? Show avg and total.
        # ---------------------------------------------------------------------
        if "most minutes" in q_lower:
            min_float = minutes_expr_pg().label("min_float")

            sub = (
                db.query(
                    PlayerGameStats.nba_player_id.label("player_id"),
                    func.count(PlayerGameStats.id).label("gp"),
                    func.sum(min_float).label("total_minutes"),
                    func.avg(min_float).label("avg_minutes"),
                )
                .join(Game, PlayerGameStats.nba_game_id == Game.nba_game_id)
                .filter(Game.season == season)
                .filter(Game.nba_game_id.like("002%"))
                .group_by(PlayerGameStats.nba_player_id)
            ).subquery()

            row = (
                db.query(
                    sub.c.player_id,
                    Player.full_name.label("player_name"),
                    Team.abbreviation.label("team"),
                    sub.c.gp,
                    sub.c.total_minutes,
                    sub.c.avg_minutes,
                )
                .join(Player, Player.nba_player_id == sub.c.player_id)
                .outerjoin(Team, Team.nba_team_id == Player.nba_team_id)
                .order_by(desc(sub.c.total_minutes))
                .first()
            )

            if not row:
                raise HTTPException(status_code=404, detail="No minutes data found for that season.")

            data = {
                "season": season,
                "question_type": "most_minutes_total_and_avg",
                "leader": {
                    "player_id": int(row.player_id),
                    "player_name": row.player_name,
                    "team": row.team,
                    "gp": int(row.gp or 0),
                    "total_minutes": round(float(row.total_minutes or 0), 2),
                    "avg_minutes": round(float(row.avg_minutes or 0), 2),
                },
            }

            answer = summarize_with_openai(question, data)
            return {"intent": "most_minutes_total_and_avg", "answer": answer, "data": data}

        # ---------------------------------------------------------------------
        # Intent 8: Best all-around player in the last N games (min 25 MPG)
        # Definition (v1): score = pts + 1.2*reb + 1.5*ast + 2*stl + 2*blk - 2*tov
        # ---------------------------------------------------------------------
        if "best all-around" in q_lower and "last" in q_lower and "game" in q_lower:
            n = clamp_n(n_from_q or 3, default=3, max_n=25)

            rn = func.row_number().over(
                partition_by=PlayerGameStats.nba_player_id,
                order_by=desc(Game.game_date),
            ).label("rn")

            min_float = minutes_expr_pg().label("min_float")
            score = (
                func.coalesce(PlayerGameStats.pts, 0)
                + 1.2 * func.coalesce(PlayerGameStats.reb, 0)
                + 1.5 * func.coalesce(PlayerGameStats.ast, 0)
                + 2.0 * func.coalesce(PlayerGameStats.stl, 0)
                + 2.0 * func.coalesce(PlayerGameStats.blk, 0)
                - 2.0 * func.coalesce(PlayerGameStats.tov, 0)
            ).label("score")

            sub = (
                db.query(
                    PlayerGameStats.nba_player_id.label("player_id"),
                    min_float,
                    score,
                    rn,
                )
                .join(Game, PlayerGameStats.nba_game_id == Game.nba_game_id)
                .filter(Game.season == season)
                .filter(Game.nba_game_id.like("002%"))
            ).subquery()

            agg = (
                db.query(
                    sub.c.player_id,
                    func.count().label("gp_lastn"),
                    func.avg(sub.c.min_float).label("avg_min_lastn"),
                    func.avg(sub.c.score).label("avg_score_lastn"),
                )
                .filter(sub.c.rn <= n)
                .group_by(sub.c.player_id)
                .having(func.count() == n)
                .having(func.avg(sub.c.min_float) >= 25)
            ).subquery()

            row = (
                db.query(
                    agg.c.player_id,
                    Player.full_name.label("player_name"),
                    Team.abbreviation.label("team"),
                    agg.c.avg_min_lastn,
                    agg.c.avg_score_lastn,
                )
                .join(Player, Player.nba_player_id == agg.c.player_id)
                .outerjoin(Team, Team.nba_team_id == Player.nba_team_id)
                .order_by(desc(agg.c.avg_score_lastn))
                .first()
            )

            if not row:
                raise HTTPException(
                    status_code=404,
                    detail=f"No eligible players found (need {n} games and >=25 MPG).",
                )

            data = {
                "season": season,
                "question_type": "best_all_around_lastN_min25mpg",
                "definition": {
                    "min_mpg": 25,
                    "window_games": n,
                    "score_formula": "pts + 1.2*reb + 1.5*ast + 2*stl + 2*blk - 2*tov",
                },
                "best_player": {
                    "player_id": int(row.player_id),
                    "player_name": row.player_name,
                    "team": row.team,
                    "avg_min_lastN": round(float(row.avg_min_lastn or 0), 2),
                    "avg_score_lastN": round(float(row.avg_score_lastn or 0), 2),
                },
            }

            answer = summarize_with_openai(question, data)
            return {"intent": "best_all_around_lastN_min25mpg", "answer": answer, "data": data}

        # ---------------------------------------------------------------------
        # Fallback
        # ---------------------------------------------------------------------
        raise HTTPException(
            status_code=400,
            detail=(
                "Question not supported yet. Try one of:\n"
                "- Compare A and B in the last 5 games\n"
                "- Summarize X's last 10 games\n"
                "- Which players improved their scoring the most over the last N games?\n"
                "- Which star players are trending up? Compare last N games vs season averages.\n"
                "- Show me the top 5 'winning impact' players by average +/- in the last N games.\n"
                "- What is the best single game stat line so far this season?\n"
                "- Who has the most minutes?\n"
                "- Who’s been the best all-around player in the last N games?"
            ),
        )

    finally:
        db.close()


# ---------------------------------------------------------------------------------------------------------------------------------------------
# REFRESH ENDPOINTS (ETL)
# ---------------------------------------------------------------------------------------------------------------------------------------------
@app.post("/warehouse/refresh/last_days")
def refresh_last_days(days: int = 14, season: str = "2025-26", max_games: int = 999999):
    """
    Updates Games/Players/Stats in the warehouse by running your ETL.

    The frontend "Refresh" button calls this, then triggers a standings refresh.
    """
    try:
        backfill_main(days=days, season=season, max_games=max_games, sleep_seconds=0.2)
        return {"ok": True, "season": season, "days": days}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Refresh failed: {e}")


# Accept BOTH POST and GET so the frontend never 405s
@app.post("/warehouse/standings/refresh")
@app.get("/warehouse/standings/refresh")
def refresh_standings_current(season: str = "2025-26"):
    """
    Pull standings from nba_api and upsert into standings_current.

    This is an ETL step:
    - External data source: nba_api
    - Destination: StandingsCurrent warehouse table
    """
    s = leaguestandings.LeagueStandings(season=season, season_type="Regular Season", timeout=30)
    df = s.get_data_frames()[0]

    def safe_int(v):
        try:
            return int(v)
        except Exception:
            return None

    def safe_float(v):
        try:
            return float(v)
        except Exception:
            return None

    db = SessionLocal()
    try:
        upserted = 0
        now = datetime.utcnow()

        for _, r in df.iterrows():
            team_id = safe_int(r.get("TeamID"))
            if team_id is None:
                continue

            payload = {
                "season": season,
                "team_id": team_id,
                "team_name": r.get("TeamName"),
                "team_city": r.get("TeamCity"),
                "team_slug": r.get("TeamSlug"),
                "conference": r.get("Conference"),
                "playoff_rank": safe_int(r.get("PlayoffRank")),
                "wins": safe_int(r.get("WINS")),
                "losses": safe_int(r.get("LOSSES")),
                "win_pct": safe_float(r.get("WinPCT")),
                "home": r.get("HOME"),
                "road": r.get("ROAD"),
                "l10": r.get("L10"),
                "streak": r.get("strCurrentStreak"),
                "updated_at": now,
            }

            stmt = (
                insert(StandingsCurrent)
                .values(**payload)
                .on_conflict_do_update(
                    index_elements=[StandingsCurrent.season, StandingsCurrent.team_id],
                    set_=payload,
                )
            )
            db.execute(stmt)
            upserted += 1

        db.commit()
        return {"ok": True, "season": season, "upserted": upserted, "source": "warehouse"}
    finally:
        db.close()


handler = Mangum(app)
