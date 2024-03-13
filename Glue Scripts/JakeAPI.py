import pandas as pd
import numpy as np
from nba_api.stats.endpoints import teamdetails, teamplayerdashboard, commonallplayers, playergamelog, commonteamroster, playercareerstats, playernextngames
from nba_api.stats.static import players, teams
from datetime import timedelta, datetime
import time
from plotnine import *
import boto3

def write_csv_to_s3(df, bucket_name, file_key):
    # Initialize Boto3 S3 client
    s3 = boto3.client('s3')
    try:
        # Write DataFrame to CSV file
        csv_buffer = StringIO()
        df.to_csv(csv_buffer)
        
        # Upload CSV file to S3
        s3.put_object(Body=csv_buffer.getvalue(), Bucket=bucket_name, Key=file_key)
    except Exception as e:
        return None



# #API CALL 1 (inseason_bdays (which is the result of a small amount of filtering of API call1 is used in API call 2)

# #Function to get a list of player's for each year (from Tuukka)
def get_team_ids():
    nba_teams = teams.get_teams()
    team_ids = {}
    for team in nba_teams:
        team_ids[team["full_name"]] = team['id']
    return team_ids

def get_list_players(year):
    api_call = 0
    season = str(year) + '-' + str(year + 1)[2:4]
    player_list = pd.DataFrame()
    for team_id in get_team_ids().values():
        roster = commonteamroster.CommonTeamRoster(team_id, season)
        api_call = api_call+1
        roster_df = roster.get_data_frames()[0]
        player_list = pd.concat([player_list, roster_df], ignore_index = True)
        time.sleep(.2)

    return player_list

# #Complete list of players that were active at any time from 2017-2024
players_list = []
for i in [2017,2018,2019,2020,2021,2022,2023]:
    w = get_list_players(i)
    players_list.append(w)
players_list_all = pd.concat(players_list, ignore_index=True)

# #Filtering the player_list to only include players with a birthday during the NBA regular season
players_list_all['BIRTH_DATE'] = pd.to_datetime(players_list_all['BIRTH_DATE'])
players_list_all['BIRTH_DATE'] = players_list_all['BIRTH_DATE'].dt.strftime('%m-%d')
inseason_bdays = players_list_all[(players_list_all['BIRTH_DATE'] >= '10-24') | (players_list_all['BIRTH_DATE'] <= '04-06')]

# #API CALL 2

# #Function that find the difference between two identically formatted dataframes
def calculate_differences(df1, df2):

    differences = {'Player_ID': []}

    for col in df1.columns:
        if col in df2.columns:

            if df1[col].dtype in ['float64', 'int64']:
                differences[col] = [df1[col].iloc[0]/df1['MIN'].iloc[0] - df2[col].iloc[0]/df2['MIN'].iloc[0]]
            else:
                differences[col] = ['Not numerical']
        else:
            differences[col] = ['Column not found in df2']


    differences['Player_ID'] = [df1['Player_ID'].iloc[0]]

    differences_df = pd.DataFrame(differences)

    return differences_df

# #Function that creates a difference df which is a row with a player's player id along with their statistical differences between the 45 days before their birthday and the seven days after
def get_stat_differences(player_id, season, birthday):
    try:
        x = pd.DataFrame(playergamelog.PlayerGameLog(player_id=player_id, season=season, season_type_all_star='Regular Season').get_data_frames()[0])
    except KeyError as e:
        return None

    if x.empty:
        return None

    x['GAME_DATE'] = pd.to_datetime(x['GAME_DATE'])
    x['GAME_DATE'] = x['GAME_DATE'].astype(str)
    x[['Game Year', 'Game Day']] = x['GAME_DATE'].str.split('-', n=1, expand=True)
    x['Game Day'] = pd.to_datetime(x['Game Day'], format='%m-%d', errors='coerce').dt.strftime('%m-%d')
    x['Game Year'] = x['Game Year'].astype(str)
    x = x[['Player_ID', 'Game_ID', 'WL', 'MIN','REB', 'AST', 'STL', 'BLK', 'TOV', 'PF', 'PTS', 'PLUS_MINUS', "Game Year", "Game Day"]]

    birthday = pd.to_datetime(birthday, format='%m-%d', errors='coerce')
    day_after = birthday + timedelta(1)
    day_after = pd.to_datetime(day_after, format='%m-%d', errors='coerce').strftime('%m-%d')
    week_after = birthday + timedelta(8)
    week_after = pd.to_datetime(week_after, format='%m-%d', errors='coerce').strftime('%m-%d')
    month_hbefore = birthday - timedelta(45)
    month_hbefore = pd.to_datetime(month_hbefore, format='%m-%d', errors='coerce').strftime('%m-%d')
    games_after = x[(x['Game Day']>= day_after) & (x['Game Day'] <= week_after)]
    games_after = pd.DataFrame(games_after.mean()).transpose().drop(['Game_ID', 'Game Year'], axis=1)
    games_after['Player_ID'] = games_after['Player_ID'].astype(str)
    games_before = x[(x['Game Day']<= day_after) & (x['Game Day'] >= month_hbefore)]
    games_before = pd.DataFrame(games_before.mean()).transpose().drop(['Game_ID', 'Game Year'], axis=1)
    differences = calculate_differences(games_after, games_before)
    differences[['Player_id', 'o']]= differences['Player_ID'].str.split('.', n=1, expand=True)
    differences['Season'] = season
    return differences.drop(['Player_ID','o'],axis=1)

# #Function that loops through all players in the player_list and returns the statistiscal differences in one df for all players
def all_diffs(df):
    all_differences = []
    for index, row in df.iterrows():
        try:
            result = get_stat_differences(row['PLAYER_ID'], row['SEASON'], row['BIRTH_DATE'])
            all_differences.append(result)
        except Exception as e:
            print(f"Error occurred at index {index}: {e}")
    return pd.concat(all_differences, ignore_index=True)

diffs = all_diffs(inseason_bdays)


write_csv_to_s3(player_stats, 'bttj-final-s3', 'api-call/diffs_jake.csv')
