from __future__ import annotations

import logging

# Import your refresh functions directly from main.py
from main import refresh_last_days, refresh_standings_current

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def handler(event, context):
    """
    EventBridge -> Lambda entrypoint (NOT HTTP).
    Uses your existing refresh functions from main.py.
    """

    event = event or {}
    season = event.get("season", "2025-26")
    days = int(event.get("days", 1))
    run_standings = bool(event.get("run_standings", True))
    max_games = int(event.get("max_games", 999999))

    logger.info(f"[jobs] starting refresh: season={season} days={days} max_games={max_games} run_standings={run_standings}")

    # Run backfill
    refresh_last_days(days=days, season=season, max_games=max_games)

    # Run standings
    if run_standings:
        refresh_standings_current(season=season)

    logger.info("[jobs] done")
    return {"ok": True, "season": season, "days": days, "ran_standings": run_standings}
