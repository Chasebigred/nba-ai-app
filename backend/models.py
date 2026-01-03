"""
SQLAlchemy ORM models for the NBA Insight data warehouse.

These models define the core database schema used by the application:
- Teams and players (dimension-style tables)
- Games and per-player box score stats (fact-style tables)
- A denormalized standings snapshot for fast reads

All data is populated via ETL jobs and queried by the FastAPI backend.
"""

from datetime import datetime

from sqlalchemy import (
    Column,
    Integer,
    String,
    Float,
    DateTime,
    UniqueConstraint,
)
from db import Base


class Team(Base):
    """
    NBA team reference table.

    Stores relatively static metadata about NBA teams. This table is used
    primarily for joins and display purposes (e.g., team abbreviations in
    leaderboards).
    """

    __tablename__ = "teams"

    id = Column(Integer, primary_key=True, index=True)

    # Stable NBA-provided team identifier
    nba_team_id = Column(Integer, unique=True, index=True, nullable=False)

    name = Column(String, nullable=True)
    abbreviation = Column(String, nullable=True)
    city = Column(String, nullable=True)

    conference = Column(String, nullable=True)
    division = Column(String, nullable=True)

    created_at = Column(DateTime, default=datetime.utcnow, index=True)


class Player(Base):
    """
    NBA player reference table.

    Stores player identity and current team mapping. Per-game statistics
    are stored separately in the PlayerGameStats table.
    """

    __tablename__ = "players"

    id = Column(Integer, primary_key=True, index=True)

    # Stable NBA-provided player identifier
    nba_player_id = Column(Integer, unique=True, index=True, nullable=False)

    full_name = Column(String, nullable=True)

    # Optional: current team mapping (can be null for traded / unsigned players)
    nba_team_id = Column(Integer, nullable=True)

    created_at = Column(DateTime, default=datetime.utcnow, index=True)


class Game(Base):
    """
    NBA game metadata table.

    Represents a single NBA game. Game IDs are strings (as provided by nba_api),
    and are joined against player box score stats in PlayerGameStats.
    """

    __tablename__ = "games"

    id = Column(Integer, primary_key=True, index=True)

    # nba_api uses string IDs like "0022300123"
    nba_game_id = Column(String, unique=True, index=True, nullable=False)

    game_date = Column(DateTime, index=True, nullable=True)
    season = Column(String, index=True, nullable=True)

    home_team_id = Column(Integer, nullable=True)
    away_team_id = Column(Integer, nullable=True)

    home_score = Column(Integer, nullable=True)
    away_score = Column(Integer, nullable=True)

    # Final / Scheduled / etc.
    status = Column(String, nullable=True)

    created_at = Column(DateTime, default=datetime.utcnow, index=True)


class StandingsCurrent(Base):
    """
    Snapshot table for current season standings.

    This table is refreshed periodically and stores a denormalized view of
    standings to allow fast, simple reads from the frontend without complex
    joins or calculations.
    """

    __tablename__ = "standings_current"

    id = Column(Integer, primary_key=True, index=True)

    season = Column(String, index=True, nullable=False)
    team_id = Column(Integer, index=True, nullable=False)

    team_name = Column(String)
    team_city = Column(String, nullable=True)
    team_slug = Column(String, nullable=True)

    conference = Column(String)
    playoff_rank = Column(Integer)

    wins = Column(Integer)
    losses = Column(Integer)
    win_pct = Column(Float)

    home = Column(String, nullable=True)
    road = Column(String, nullable=True)

    l10 = Column(String)
    streak = Column(String)

    # Timestamp of last refresh / upsert
    updated_at = Column(DateTime, default=datetime.utcnow, index=True)

    __table_args__ = (
        # Enforce one standings row per team per season
        UniqueConstraint("season", "team_id", name="uq_standings_current_season_team"),
    )


class PlayerGameStats(Base):
    """
    Per-player, per-game box score statistics.

    This is the primary fact table used for:
    - Player game logs
    - Per-game averages
    - Leaderboard aggregations

    Each row represents a single player's stat line for a single NBA game.
    """

    __tablename__ = "player_game_stats"

    id = Column(Integer, primary_key=True, index=True)

    nba_game_id = Column(String, index=True, nullable=False)
    nba_player_id = Column(Integer, index=True, nullable=False)
    nba_team_id = Column(Integer, index=True, nullable=True)

    # Stored as "MM:SS" string as provided by nba_api
    minutes = Column(String, nullable=True)

    pts = Column(Integer, nullable=True)
    reb = Column(Integer, nullable=True)
    ast = Column(Integer, nullable=True)
    stl = Column(Integer, nullable=True)
    blk = Column(Integer, nullable=True)
    tov = Column(Integer, nullable=True)

    fgm = Column(Integer, nullable=True)
    fga = Column(Integer, nullable=True)
    fg3m = Column(Integer, nullable=True)
    fg3a = Column(Integer, nullable=True)
    ftm = Column(Integer, nullable=True)
    fta = Column(Integer, nullable=True)

    plus_minus = Column(Integer, nullable=True)

    created_at = Column(DateTime, default=datetime.utcnow, index=True)

    __table_args__ = (
        # Prevent duplicate stat lines for the same player/game
        UniqueConstraint("nba_game_id", "nba_player_id", name="uq_player_game"),
    )
