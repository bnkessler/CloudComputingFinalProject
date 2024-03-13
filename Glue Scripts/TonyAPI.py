import time
import boto3
from datetime import date
import pandas as pd
import numpy as np
import json
import requests as re
from io import StringIO

from nba_api.stats.static import players, teams
from nba_api.stats.endpoints import commonplayerinfo, playercareerstats
from nba_api.stats.endpoints import playergamelog
from nba_api.stats.endpoints import *
from nba_api.stats.endpoints import teamdetails, teamplayerdashboard, commonallplayers, playergamelog, commonteamroster, playercareerstats, playernextngames

# # Pulling list of active players
players.get_active_players()
active_players = players.get_active_players()

# # Convert the list of dictionaries into a pandas DataFrame
active_players = pd.DataFrame(active_players)

next_games = []
for i in active_players["id"]:
    try:
        data = pd.DataFrame(playernextngames.PlayerNextNGames(number_of_games=1, player_id=i, season_all='2023', season_type_all_star='Regular Season').get_data_frames()[0])
        data['id'] = i # adding player to game log
        next_games.append(data)
    except Exception as e:
        print(f"Problem player ID {i}") # players who are not scheduled to play today
    time.sleep(0.2)

all_next_games = pd.concat(next_games, ignore_index=True)



all_next_games['GAME_DATE'] = pd.to_datetime(all_next_games['GAME_DATE'])
Today_games = all_next_games[all_next_games['GAME_DATE'] == datetime.today().strftime('%Y-%m-%d')]

todays_players_ids = Today_games['id'].unique()
todays_players = active_players[active_players['id'].isin(todays_players_ids)]
todays_players

player_stats = pd.DataFrame()

for index, row in todays_players.iterrows():
    player_id = row['id']

    # Only looking at past 5 seasons
    for season in ['2019-20', '2020-21', '2021-22', '2022-23', '2023-24']:
        gamelogs = playergamelog.PlayerGameLog(player_id=player_id, season=season)
        gamelogs_df = gamelogs.get_data_frames()[0]

        gamelogs_df['Season'] = season  # Adding season column
        gamelogs_df['Player ID'] = player_id  # Adding player ID column

        # Append to the main DataFrame
        player_stats = pd.concat([player_stats, gamelogs_df], ignore_index=True)
        time.sleep(0.2)
        
csv_buffer = StringIO()
todays_players.to_csv(csv_buffer)

s3 = boto3.client('s3')

s3.put_object(
    Bucket='bttj-final-s3',  # Replace with your S3 bucket name
    Key='api-call/t-playerstats.csv',  # Replace with desired object key
    Body=csv_buffer.getvalue()
)

