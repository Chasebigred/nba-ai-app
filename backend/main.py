from __future__ import annotations

import logging
from datetime import datetime

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from mangum import Mangum


from sqlalchemy import desc, func
from sqlalchemy.dialects.postgresql import insert

from db import SessionLocal
from models import Game, Player, PlayerGameStats, StandingsCurrent, Team

# NBA data source used ONLY for refresh/ETL (not for user-facing "live" reads)
from nba_api.stats.endpoints import leaguestandings

# Your ETL job (updates Games/Players/Stats in the warehouse)
from etl.backfill_last_14_days import main as backfill_main

import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="NBA AI App API")

# -----------------------------------------------------------------------------
# CORS
# -----------------------------------------------------------------------------
# Frontend runs on Vite in local dev; allow it to call the API directly.
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


# -----------------------------------------------------------------------------
# WAREHOUSE (DB) - READ ENDPOINTS
# -----------------------------------------------------------------------------
# IMPORTANT:
# - The frontend reads ONLY from the warehouse (SQL DB).
# - External NBA API calls are done only in refresh endpoints (ETL), not on reads.
# -----------------------------------------------------------------------------


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

        stats = [r[0] for r in rows]
        games = [r[1] for r in rows]

        # Build game rows for the UI table
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

        # Totals for "ratio" shooting averages (more accurate than averaging per-game %)
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


# -----------------------------------------------------------------------------
# WAREHOUSE LEADERS (DB) - READ ENDPOINTS
# -----------------------------------------------------------------------------
# These endpoints compute leaderboard stats from stored player_game_stats.
# The UI "Load more" behavior works by increasing the limit parameter.
# -----------------------------------------------------------------------------


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
                    "value": round(total / gp, 2),  # PPG
                    "total": int(total),  # optional (nice for debugging / future UI)
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
                    "value": round(total / gp, 2),  # RPG
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
                    "value": round(total / gp, 2),  # APG
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
                    "value": round(total / gp, 2),  # BPG
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


# -----------------------------------------------------------------------------
# REFRESH ENDPOINTS (ETL)
# -----------------------------------------------------------------------------
# These endpoints are intentionally allowed to call nba_api.
# They update the warehouse so the frontend can do fast DB-only reads.
# -----------------------------------------------------------------------------


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

