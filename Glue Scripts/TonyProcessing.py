import sys
from datetime import datetime, timedelta
import time
import pandas as pd
import numpy as np
import boto3
from io import StringIO, BytesIO
from nba_api.stats.static import players
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.backends.backend_pdf import PdfPages

players.get_active_players()
active_players = players.get_active_players()

active_players = pd.DataFrame(active_players)

def read_csv_from_s3(bucket_name, file_key):
    # Initialize Boto3 S3 client
    s3 = boto3.client('s3')
    try:
        # Read CSV file directly into Pandas DataFrame
        response = s3.get_object(Bucket=bucket_name, Key=file_key)
        df = pd.read_csv(response['Body'])
        return df
    except Exception as e:
        return None
        

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
        
# Determine if game is home or away
def determine_type(matchup):
    if '@' in matchup:
        return 'Away'
    elif 'vs.' in matchup:
        return 'Home'
    else:
        return 'Unknown'

# Get location of game
def extract_location(matchup):
    if '@' in matchup:
        # For away games, the location is after '@'
        return matchup.split('@')[1].strip()
    elif 'vs.' in matchup:
        # For home games, the location is before 'vs.'
        return matchup.split('vs.')[0].strip()
    else:
        return 'Unknown'
        
player_stats = read_csv_from_s3('bttj-final-s3', 'api-call/PlayerStats.csv')

player_stats['Type'] = player_stats['MATCHUP'].apply(determine_type)

columns_to_convert = ['FGM', 'FGA', 'FG3M', 'FG3A', 'FTM', 'FTA', 'OREB', 'DREB', 'REB', 'AST', 'STL', 'BLK', 'TOV', 'PTS', 'PLUS_MINUS']

for column in columns_to_convert:
    player_stats[column] = pd.to_numeric(player_stats[column], errors='coerce')

# Getting player who play at least 100 games and 20+ minutes.
player_stats = player_stats.groupby('Player_ID').filter(lambda x: len(x) >= 100)

player_avg_min = player_stats.groupby('Player_ID')['MIN'].mean()
players_over_20_min = player_avg_min[player_avg_min >= 20]
player_stats = player_stats[player_stats['Player_ID'].isin(players_over_20_min.index)]


player_stats['Location'] = player_stats['MATCHUP'].apply(extract_location)

# Getting opponent from matchup column
player_stats['Opponent'] = player_stats['MATCHUP'].apply(lambda x: x.split(' @ ')[-1] if ' @ ' in x else x.split(' vs. ')[-1])

player_stats.to_csv('PlayerStats.csv', index = False)

player_stats = pd.merge(player_stats, active_players[['full_name', 'id']], left_on='Player_ID', right_on='id', how='left')

player_stats.drop('id', axis=1, inplace=True)
player_stats.drop('VIDEO_AVAILABLE', axis = 1, inplace = True)

# Ranking players by statistical category
player_variance = player_stats.groupby(['full_name']).std().reset_index()

top_variance_players = {}
numerical_columns = player_variance.columns[1:]  # Exclude 'Player ID' and 'full_name'

for col in numerical_columns:
    top_variance_players[col] = player_variance.sort_values(by=col, ascending=False)[['full_name', col]]

    
# Sorting the DataFrame by each numerical column for the lowest variance and capturing the entries
lowest_variance_players = {}

for col in numerical_columns:
    lowest_variance_players[col] = player_variance.sort_values(by=col, ascending=True)[['full_name', col]].head(10)

# Create a BytesIO object
pdf_file = BytesIO()

with PdfPages(pdf_file) as pdf:
    for stat, df in lowest_variance_players.items():
        plt.figure(figsize=(16, 8))
        sns.barplot(x=stat, y='full_name', data=df.head(10))
        plt.title(f'Lowest Variance Players by {stat}')
        plt.xlabel(f'Variance in {stat}')
        plt.ylabel('Player')

        pdf.savefig()
        plt.close()

# Seek to the start of the BytesIO object
pdf_file.seek(0)

# Upload the BytesIO object to the S3 bucket
s3.upload_fileobj(pdf_file, 'bttj-final-s3', 'lowest_variance_players_stats.pdf')



