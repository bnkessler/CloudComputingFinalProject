# Necessary Libraries
import pandas as pd
import numpy as np
import sys
import time
import boto3
from datetime import datetime, timedelta
from nba_api.stats.endpoints import AllTimeLeadersGrids, commonplayerinfo, CommonTeamRoster
from nba_api.stats.static import teams
from scipy.stats import chi2_contingency, ttest_ind

def get_team_ids():
    # Retrieve all NBA teams
    nba_teams = teams.get_teams()
    
    team_ids = {}
    # Extract team IDs
    for team in nba_teams:
        team_ids[team["full_name"]] = team['id']
    
    return team_ids
    
# Collects all games for specified teams between a set of yea
def get_all_games(start_year = "2017-9-1", end_year = None, teams = "all"):
    start_year = datetime.strptime(start_year, "%Y-%m-%d")
    team_ids = get_team_ids()
    dfs = []
    if teams == "all":
        for team, team_id in team_ids.items():
            print(f"Attempting to collect {team}")
            gamefinder = leaguegamefinder.LeagueGameFinder(team_id_nullable=team_id, timeout=60)
            games = gamefinder.get_data_frames()[0]
            games["GAME_DATE"] = games["GAME_DATE"].apply(lambda x: datetime.strptime(x, "%Y-%m-%d"))
            games = games[games["GAME_DATE"] >= start_year]
            dfs.append(games)
            print(f"Successfully collected {team}")
            time.sleep(2.5)
        return dfs
    else:
        for team in taems:
            team_id = team_ids[team]
            gamefinder = leaguegamefinder.LeagueGameFinder(team_id_nullable=team_id)
            games = gamefinder.get_data_frames()[0]
            games["GAME_DATE"] = games["GAME_DATE"].apply(lambda x: datetime.strptime(x, "%Y-%m-%d"))
            games = games[games["GAME_DATE"] >= start_year]
            dfs.append(games)
        return dfs
        
# Collecting player birthday data from nba_api
filtered_player_birthdays = read_csv_from_s3("bttj-final-s3", "NBA_DOB_InSeason.csv")
active_player_birthdays = []
count = 0
for player_id in filtered_player_birthdays["id"]:
    count += 1
    if count % 100 == 0:
        print("Starting Sleep")
        time.sleep(300)
    player_info = commonplayerinfo.CommonPlayerInfo(player_id = player_id, timeout=60).get_data_frames()[0]
    time.sleep(.5)
    try:
        if (2017 <= player_info["FROM_YEAR"][0] <= 2024) or (2017 <= player_info["TO_YEAR"][0] <= 2024):
            print(f"Collected {player_info['DISPLAY_FIRST_LAST'][0]}")
            active_player_birthdays.append((player_info["DISPLAY_FIRST_LAST"][0], player_info["TEAM_ABBREVIATION"][0], player_info["BIRTHDATE"][0].split("T")[0], player_info["FROM_YEAR"][0], player_info["TO_YEAR"][0]))
    except TypeError:
        continue
        
print("Completed Collection")

def write_dataframe_to_s3(dataframe, bucket_name, file_name):
    """
    Write Pandas DataFrame to an S3 bucket as a CSV file.

    Parameters:
        dataframe (pandas.DataFrame): The DataFrame to be written.
        bucket_name (str): The name of the S3 bucket.
        file_name (str): The name of the CSV file to be written.

    Returns:
        None
    """
    s3 = boto3.client('s3')
    csv_buffer = StringIO()
    dataframe.to_csv(csv_buffer, index=False)

    s3.put_object(Bucket=bucket_name, Key=file_name, Body=csv_buffer.getvalue())

all_games = get_all_games(start_year = "2017-9-1")
all_games = pd.concat(all_games)
all_games

all_games_filtered_columns = all_games[["TEAM_NAME", "GAME_DATE", "MATCHUP", "WL"]]
all_games_filtered_columns = all_games_filtered_columns.reset_index()
all_games_filtered_columns = all_games_filtered_columns.drop("index", axis = 1)
all_games_filtered_columns["TEAM_NAME"] = all_games_filtered_columns["TEAM_NAME"].apply(lambda x: team_names[x])
all_games_filtered_columns = all_games_filtered_columns[~all_games_filtered_columns["MATCHUP"].isin(drop_games["0"])]
all_games_filtered_columns

# Reading in betting line data from s3
betting_lines = read_csv_from_s3("bttj-final-s3", "api-data.csv")

betting_lines = betting_lines.drop("Unnamed: 0", axis = 1)

betting_lines["game_date"] = pd.to_datetime(betting_lines["game_date"], format='%Y-%m-%d', errors="coerce")

# Cleaning player birthday dataframe
player_birthday_df = pd.DataFrame(active_player_birthdays, columns = ["Name", "Team", "Birthday", "From_Year", "To_Year"])
player_birthday_df = player_birthday_df.merge(filtered_player_birthdays[["Name", "id"]], on = ["Name"])
player_birthday_df = player_birthday_df[player_birthday_df["Team"] != ""]
player_birthday_df = player_birthday_df.reset_index().drop("index", axis = 1)

player_birthday_df

# Merging data to get home and away games for all teams
away_games = pd.merge(betting_lines, all_games_filtered_columns, how = "left", left_on = ["game_date", "visit_team_abbrev"], right_on = ["GAME_DATE", "TEAM_NAME"]).dropna()
home_games = pd.merge(betting_lines, all_games_filtered_columns, how = "left", left_on = ["game_date", "home_team_abbrev"], right_on = ["GAME_DATE", "TEAM_NAME"]).dropna()

merged_data = pd.concat([home_games, away_games]).reset_index().drop("index", axis = 1)
merged_data = merged_data.drop(["GAME_DATE"], axis = 1)

write_dataframe_to_s3(merged_data, "bttj-final-s3", "merged_data.csv")