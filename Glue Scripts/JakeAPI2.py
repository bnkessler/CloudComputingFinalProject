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

# #getting the yearly per game statistics for each player
base = diffs.copy()
player_stats = []
for index,row in base.iterrows():
    mid = pd.DataFrame(playercareerstats.PlayerCareerStats(per_mode36 = 'PerGame', player_id = row['Player_id']).get_data_frames()[0])
    mid = mid[mid['SEASON_ID'].isin(['2017-18','2018-19','2019-20','2020-21', '2021-22','2022-23', '2023-24'])]
    player_stats.append(mid)
player_stats = pd.concat(player_stats, ignore_index=True)

write_csv_to_s3(player_stats, 'bttj-final-s3', 'api-call/player_stats_jake.csv')
