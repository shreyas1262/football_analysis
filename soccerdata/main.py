import soccerdata as sd
import pandas as pd

pd.set_option("display.max_columns", None)
pd.set_option("display.width", 200)

fbref = sd.FBref(leagues=["ENG-Premier League"], seasons=["2022"])

if __name__ == "__main__":
    df = fbref.read_team_season_stats(stat_type="standard")
    print(df.head(5).to_string())
