import sys
import os
from datetime import datetime, timedelta
import time
from typing import Optional, Set

# Ensure `backend/` is on the Python path so ETL can be run from `/etl` directly.
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from nba_api.stats.endpoints import leaguegamefinder, boxscoretraditionalv3
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import select

from db import SessionLocal
from models import Team, Player, Game, PlayerGameStats


def upper_cols(df):
    """
    Return a copy of the dataframe with uppercased column names.

    nba_api payloads can vary slightly between endpoints/versions. Normalizing to
    uppercase makes downstream key lookups more consistent (TEAMID vs TeamId, etc.).
    """
    df = df.copy()
    df.columns = [str(c).upper() for c in df.columns]
    return df


def pick(row, *keys):
    """
    Return the first non-null value found in `row` for any of the provided keys.

    This helps the ETL tolerate changing column names across nba_api payloads.
    """
    for k in keys:
        if k in row and row[k] is not None:
            return row[k]
    return None


def to_int(val):
    """
    Convert a value to an integer where possible.
    Returns None for empty / NaN / non-numeric inputs.
    """
    if val is None:
        return None
    s = str(val).strip()
    if s == "" or s.lower() == "nan":
        return None
    if s.lstrip("-").isdigit():
        return int(s)
    return None


def upsert_team(db, nba_team_id: int, name=None, city=None, abbreviation=None, conference=None, division=None):
    """
    Insert or update a Team row keyed by `nba_team_id`.
    """
    stmt = (
        insert(Team)
        .values(
            nba_team_id=nba_team_id,
            name=name,
            city=city,
            abbreviation=abbreviation,
            conference=conference,
            division=division,
        )
        .on_conflict_do_update(
            index_elements=[Team.nba_team_id],
            set_={
                "name": name,
                "city": city,
                "abbreviation": abbreviation,
                "conference": conference,
                "division": division,
            },
        )
    )
    db.execute(stmt)


def upsert_player(db, nba_player_id: int, full_name=None, nba_team_id=None):
    """
    Insert or update a Player row keyed by `nba_player_id`.
    """
    stmt = (
        insert(Player)
        .values(
            nba_player_id=nba_player_id,
            full_name=full_name,
            nba_team_id=nba_team_id,
        )
        .on_conflict_do_update(
            index_elements=[Player.nba_player_id],
            set_={
                "full_name": full_name,
                "nba_team_id": nba_team_id,
            },
        )
    )
    db.execute(stmt)


def upsert_game(
    db,
    nba_game_id: str,
    game_date: Optional[datetime],
    season: str,
    home_team_id: Optional[int],
    away_team_id: Optional[int],
    home_score: Optional[int],
    away_score: Optional[int],
    status: Optional[str],
):
    """
    Insert or update a Game row keyed by `nba_game_id`.
    """
    stmt = (
        insert(Game)
        .values(
            nba_game_id=nba_game_id,
            game_date=game_date,
            season=season,
            home_team_id=home_team_id,
            away_team_id=away_team_id,
            home_score=home_score,
            away_score=away_score,
            status=status,
        )
        .on_conflict_do_update(
            index_elements=[Game.nba_game_id],
            set_={
                "game_date": game_date,
                "season": season,
                "home_team_id": home_team_id,
                "away_team_id": away_team_id,
                "home_score": home_score,
                "away_score": away_score,
                "status": status,
            },
        )
    )
    db.execute(stmt)


def upsert_player_game_stats(db, row: dict):
    """
    Insert or update a PlayerGameStats row keyed by (nba_game_id, nba_player_id).
    """
    stmt = insert(PlayerGameStats).values(**row).on_conflict_do_update(
        constraint="uq_player_game",
        set_={k: row[k] for k in row.keys() if k not in ("nba_game_id", "nba_player_id")},
    )
    db.execute(stmt)


def infer_home_away_from_team_df(teams):
    """
    Infer home/away team IDs from the team dataframe.

    BoxScoreTraditionalV3 does not consistently provide a matchup string the way
    some older endpoints did. For this project, treating the two team rows in
    the returned order as (away, home) is a consistent approximation.
    """
    if len(teams) < 2:
        return None, None
    away_team_id = teams[0]["team_id"]
    home_team_id = teams[1]["team_id"]
    return home_team_id, away_team_id


def get_loaded_game_ids(db, season: str) -> Set[str]:
    """
    Return all game IDs already present in the DB for a given season.

    This allows ETL reruns to be fast/resumable by skipping games already ingested.
    """
    rows = db.execute(select(Game.nba_game_id).where(Game.season == season)).all()
    return {r[0] for r in rows if r and r[0] is not None}


def fetch_boxscore_frames(game_id: str, timeout: int = 60, retries: int = 1, retry_sleep: float = 1.0):
    """
    Fetch box score dataframes for a given game ID.

    Returns:
        (player_df, team_df) on success, or None if data is missing/unavailable.

    nba_api occasionally returns an empty/None response, which can surface as
    AttributeError inside the library. This wrapper treats those cases as a
    "skip and continue" instead of crashing the full backfill.
    """
    for attempt in range(retries + 1):
        try:
            bs = boxscoretraditionalv3.BoxScoreTraditionalV3(game_id=game_id, timeout=timeout)
            frames = bs.get_data_frames()

            if not frames or len(frames) < 2 or frames[0] is None or frames[1] is None:
                return None

            player_df = upper_cols(frames[0])
            team_df = upper_cols(frames[1])
            return player_df, team_df

        except AttributeError as e:
            # Common failure mode when stats.nba.com returns an empty payload.
            msg = str(e)
            if "NoneType" in msg and "get" in msg:
                if attempt < retries:
                    time.sleep(retry_sleep)
                    continue
                return None
            raise

        except Exception:
            # Transient network issues / parsing issues: retry once, else propagate.
            if attempt < retries:
                time.sleep(retry_sleep)
                continue
            raise

    return None


def main(days: int = 14, season: str = "2025-26", max_games: int = 999999, sleep_seconds: float = 0.6):
    """
    Backfill the most recent `days` of NBA regular season games into the warehouse.

    High-level flow:
      1) Pull recent game IDs via LeagueGameFinder
      2) For each game, fetch box score (players + teams) via BoxScoreTraditionalV3
      3) Upsert teams, game metadata, players, and per-player stat lines
      4) Commit per game so the job is resumable and partial progress is preserved
    """
    start_date = (datetime.utcnow() - timedelta(days=days)).strftime("%m/%d/%Y")
    end_date = datetime.utcnow().strftime("%m/%d/%Y")

    print(f"Fetching games from {start_date} to {end_date} ...")

    gf = leaguegamefinder.LeagueGameFinder(
        date_from_nullable=start_date,
        date_to_nullable=end_date,
        league_id_nullable="00",
        season_nullable=season,
        season_type_nullable="Regular Season",
        timeout=60,
    )
    games_df = gf.get_data_frames()[0]

    game_ids = sorted(set(games_df["GAME_ID"].astype(str).tolist()))
    game_ids = game_ids[:max_games]

    print(f"Found {len(game_ids)} games (max_games={max_games})")

    db = SessionLocal()
    try:
        loaded_game_ids = get_loaded_game_ids(db, season)
        print(f"Already loaded games in DB: {len(loaded_game_ids)}")

        fetched = 0
        skipped = 0
        failed = 0

        for i, game_id in enumerate(game_ids, start=1):
            # Skip work if we already have this game in our DB.
            if game_id in loaded_game_ids:
                skipped += 1
                if skipped <= 8 or skipped % 50 == 0:
                    print(f"[{i}/{len(game_ids)}] SKIP (already loaded) GAME_ID={game_id}")
                continue

            print(f"[{i}/{len(game_ids)}] FETCH GAME_ID={game_id}")

            try:
                # Fetch box score (player + team frames). If unavailable, skip gracefully.
                frames = fetch_boxscore_frames(game_id=game_id, timeout=60, retries=1, retry_sleep=1.0)
                if frames is None:
                    skipped += 1
                    print(f"[{i}/{len(game_ids)}] SKIP (boxscore not available yet) GAME_ID={game_id}")
                    continue

                player_df, team_df = frames

                # --------------------
                # Teams (upsert)
                # --------------------
                teams = []
                for _, t in team_df.iterrows():
                    team_id = pick(t, "TEAMID", "TEAM_ID", "TEAMIDHOME", "TEAMIDAWAY")
                    if team_id is None:
                        raise KeyError(f"Could not find TEAMID in team_df columns: {list(team_df.columns)[:40]}")

                    team_abbr = pick(t, "TEAMTRICODE", "TEAM_ABBREVIATION", "TEAM_ABBR")
                    team_name = pick(t, "TEAMNAME", "TEAM_NAME")
                    team_city = pick(t, "TEAMCITY", "TEAM_CITY")
                    pts = pick(t, "POINTS", "PTS")

                    teams.append(
                        {
                            "team_id": int(team_id),
                            "team_abbr": str(team_abbr) if team_abbr is not None else None,
                            "team_name": str(team_name) if team_name is not None else None,
                            "team_city": str(team_city) if team_city is not None else None,
                            "pts": to_int(pts),
                        }
                    )

                for tt in teams:
                    upsert_team(
                        db,
                        nba_team_id=tt["team_id"],
                        name=tt.get("team_name"),
                        city=tt.get("team_city"),
                        abbreviation=tt.get("team_abbr"),
                    )

                # --------------------
                # Home/Away + Scores
                # --------------------
                home_team_id, away_team_id = infer_home_away_from_team_df(teams)
                home_score = next((tt["pts"] for tt in teams if tt["team_id"] == home_team_id), None)
                away_score = next((tt["pts"] for tt in teams if tt["team_id"] == away_team_id), None)

                # --------------------
                # Game date (from LeagueGameFinder)
                # --------------------
                game_date = None
                try:
                    g_row = games_df.loc[games_df["GAME_ID"].astype(str) == game_id].iloc[0]
                    gd = g_row.get("GAME_DATE")
                    if gd is not None and str(gd) != "nan":
                        if hasattr(gd, "to_pydatetime"):
                            game_date = gd.to_pydatetime()
                        else:
                            try:
                                game_date = datetime.fromisoformat(str(gd))
                            except Exception:
                                game_date = None
                except Exception:
                    game_date = None

                upsert_game(
                    db,
                    nba_game_id=game_id,
                    game_date=game_date,
                    season=season,
                    home_team_id=home_team_id,
                    away_team_id=away_team_id,
                    home_score=home_score,
                    away_score=away_score,
                    status="Final" if (home_score is not None and away_score is not None) else None,
                )

                # --------------------
                # Player stats (upsert)
                # --------------------
                for _, p in player_df.iterrows():
                    nba_player_id = pick(p, "PERSONID", "PLAYERID", "PLAYER_ID")
                    nba_team_id = pick(p, "TEAMID", "TEAM_ID")

                    first = pick(p, "FIRSTNAME")
                    last = pick(p, "FAMILYNAME")

                    if first and last:
                        player_name = f"{str(first).strip()} {str(last).strip()}"
                    else:
                        player_name = pick(p, "PLAYERNAME", "NAMEI")

                    if nba_player_id is None:
                        raise KeyError(f"Could not find PERSONID in player_df columns: {list(player_df.columns)[:40]}")

                    nba_player_id = int(nba_player_id)
                    nba_team_id = int(nba_team_id) if nba_team_id is not None and str(nba_team_id).isdigit() else None

                    upsert_player(
                        db,
                        nba_player_id=nba_player_id,
                        full_name=player_name,
                        nba_team_id=nba_team_id,
                    )

                    row = {
                        "nba_game_id": game_id,
                        "nba_player_id": nba_player_id,
                        "nba_team_id": nba_team_id,
                        "minutes": pick(p, "MINUTES", "MIN"),
                        "pts": to_int(pick(p, "POINTS", "PTS")),
                        "reb": to_int(pick(p, "REBOUNDSTOTAL", "REB")),
                        "ast": to_int(pick(p, "ASSISTS", "AST")),
                        "stl": to_int(pick(p, "STEALS", "STL")),
                        "blk": to_int(pick(p, "BLOCKS", "BLK")),
                        "tov": to_int(pick(p, "TURNOVERS", "TO")),
                        "fgm": to_int(pick(p, "FIELDGOALSMADE", "FGM")),
                        "fga": to_int(pick(p, "FIELDGOALSATTEMPTED", "FGA")),
                        "fg3m": to_int(pick(p, "THREEPOINTERSMADE", "FG3M")),
                        "fg3a": to_int(pick(p, "THREEPOINTERSATTEMPTED", "FG3A")),
                        "ftm": to_int(pick(p, "FREETHROWSMADE", "FTM")),
                        "fta": to_int(pick(p, "FREETHROWSATTEMPTED", "FTA")),
                        # Keep raw value. Some payloads represent this as "+12" / "-7".
                        "plus_minus": pick(
                            p,
                            "PLUSMINUS",
                            "PLUSMINUSPOINTS",
                            "PLUS_MINUS",
                            "PLUSMINUSPOINTSDIFFERENTIAL",
                        ),
                    }

                    upsert_player_game_stats(db, row)

                db.commit()
                fetched += 1
                loaded_game_ids.add(game_id)

                # Be polite to stats.nba.com; throttle only after a successful ingest.
                time.sleep(sleep_seconds)

            except KeyboardInterrupt:
                print("\nKeyboardInterrupt: stopping early (data committed up to last successful game).")
                raise
            except Exception as e:
                db.rollback()
                failed += 1
                print(f"FAILED GAME_ID={game_id}: {type(e).__name__}: {e}")

        print("Done.")
        print({"fetched": fetched, "skipped": skipped, "failed": failed})

    finally:
        db.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=14)
    parser.add_argument("--season", type=str, default="2025-26")
    parser.add_argument("--max_games", type=int, default=999999)
    parser.add_argument("--sleep", type=float, default=0.6)
    args = parser.parse_args()

    main(days=args.days, season=args.season, max_games=args.max_games, sleep_seconds=args.sleep)
