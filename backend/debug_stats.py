from db import SessionLocal
from models import PlayerGameStats

NBA_PLAYER_ID = 2544  # LeBron James

db = SessionLocal()

rows = (
    db.query(PlayerGameStats)
    .filter(PlayerGameStats.nba_player_id == NBA_PLAYER_ID)
    .order_by(PlayerGameStats.nba_game_id.desc())
    .limit(10)
    .all()
)

print(f"Last {len(rows)} games for player {NBA_PLAYER_ID}\n")

for r in rows:
    print({
        "game": r.nba_game_id,
        "pts": r.pts,
        "reb": r.reb,
        "ast": r.ast,
        "stl": r.stl,
        "blk": r.blk,
        "tov": r.tov,
        "fg": f"{r.fgm}/{r.fga}",
        "3pt": f"{r.fg3m}/{r.fg3a}",
        "ft": f"{r.ftm}/{r.fta}",
        "+/-": r.plus_minus,
        "min": r.minutes,
    })

db.close()
