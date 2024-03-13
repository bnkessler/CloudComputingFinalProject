# Necessary Libraries
import pandas as pd
import numpy as np
import sys
import time
import boto3
from datetime import datetime, timedelta
from scipy.stats import chi2_contingency, ttest_ind

wl_dict = {'W': 1, 'L': 0}

team_names = {
    'Atlanta Hawks': 'ATL',
    'Boston Celtics': 'BOS',
    'Cleveland Cavaliers': 'CLE',
    'New Orleans Pelicans': 'NOP',
    'Chicago Bulls': 'CHI',
    'Dallas Mavericks': 'DAL',
    'Denver Nuggets': 'DEN',
    'Golden State Warriors': 'GSW',
    'Houston Rockets': 'HOU',
    'LA Clippers': 'LAC',
    'Los Angeles Lakers': 'LAL',
    'Miami Heat': 'MIA',
    'Milwaukee Bucks': 'MIL',
    'Minnesota Timberwolves': 'MIN',
    'Brooklyn Nets': 'BKN',
    'New York Knicks': 'NYK',
    'Orlando Magic': 'ORL',
    'Indiana Pacers': 'IND',
    'Philadelphia 76ers': 'PHI',
    'Phoenix Suns': 'PHX',
    'Portland Trail Blazers': 'POR',
    'Sacramento Kings': 'SAC',
    'San Antonio Spurs': 'SAS',
    'Oklahoma City Thunder': 'OKC',
    'Toronto Raptors': 'TOR',
    'Utah Jazz': 'UTA',
    'Memphis Grizzlies': 'MEM',
    'Washington Wizards': 'WAS',
    'Detroit Pistons': 'DET',
    'Charlotte Hornets': 'CHA'
}

def read_csv_from_s3(bucket_name, file_key):
    # Initialize Boto3 S3 client
    s3 = boto3.client('s3')
    try:
        # Read CSV file directly into Pandas DataFrame
        response = s3.get_object(Bucket=bucket_name, Key=file_key)
        df = pd.read_csv(response['Body'])
        return df
    except Exception as e:
        print(f"Error reading CSV file from S3: {e}")
        return None
        
def write_tuples_to_s3(tuples_list, bucket_name, file_key):
    # Initialize Boto3 S3 client
    s3 = boto3.client('s3')
    try:
        # Prepare data as string
        data = '\n'.join([' '.join(map(str, tup)) for tup in tuples_list])
        
        # Upload data to S3
        response = s3.put_object(Bucket=bucket_name, Key=file_key, Body=data.encode('utf-8'))
        
        print(f"Data written to s3://{bucket_name}/{file_key} successfully.")
    except Exception as e:
        print(f"Error writing data to S3: {e}")
        
def get_team_ids():
    # Retrieve all NBA teams
    nba_teams = teams.get_teams()
    
    team_ids = {}
    # Extract team IDs
    for team in nba_teams:
        team_ids[team["full_name"]] = team['id']
    
    return team_ids


def mark_birthday_games(games_df, players_df, day_range):
   
    birthday_indexes = []
    games_df["birthday_game"] = 0

    for index, player_row in players_df.iterrows():
        player_team = player_row["Team"]
        player_birthday = datetime.strptime(player_row["Birthday"], "%Y-%m-%d")
        
        birthday_range_start = player_birthday - timedelta(days=day_range)
        birthday_range_end = player_birthday + timedelta(days=day_range)
        
        games_to_check = games_df[(games_df["home_team_abbrev"] == player_team) | (games_df["visit_team_abbrev"] == player_team)]
        birthday_games = games_to_check[(games_to_check["game_date"].dt.month == birthday_range_start.month) & 
                                        (games_to_check['game_date'].dt.day >= birthday_range_start.day) &
                                        (games_to_check['game_date'].dt.day <= birthday_range_end.day)]
        if len(birthday_games.index) >= 1:
            birthday_indexes.append([*birthday_games.index])
    
    for i in birthday_indexes:
        # games_df["birthday_game"].iloc[i] = 1
        games_df.loc[i, "birthday_game"] = 1
        
    return games_df

# Function to mark if a team had covered the spread
def team_covered(games_df, teams):
    
    games_df["team_covered"] = 0
    covered = {}
    
    for team in teams:
        for index, game_row in games_df[games_df["TEAM_NAME"] == team].iterrows():
            if ((game_row["favorite"] == team) and (game_row["favorite_covered"] == 1)) | ((game_row["favorite"] != team) and (game_row["underdog_covered"] == 1)):
                covered[index] = 1
            else:
                covered[index] = 0
    
    games_df["team_covered"] = games_df.index.map(covered)
    
    return games_df   
    
# This is an import from the aws blocked apis
merged_data = read_csv_from_s3("bttj-final-s3", "api-call/MergedData.csv")
player_birthday_df = read_csv_from_s3("bttj-final-s3", "api-call/PlayerBirthdayDF.csv")

# Fix datetime?
merged_data["game_date"] = pd.to_datetime(merged_data["game_date"])
# player_birthday_df["Birthday"] = pd.to_datetime(player_birthday_df["Birthday"])

# Marking birthday games and spread
birthday_games_df = mark_birthday_games(merged_data, player_birthday_df, 1)
birthday_games_df = team_covered(birthday_games_df, pd.unique(birthday_games_df["home_team_abbrev"]))

# Statistics
def chi_square_test_for_team(group):
    contingency_table = pd.crosstab(group['team_covered'], group['birthday_game'])
    chi2, p, dof, expected = chi2_contingency(contingency_table)
    return chi2, p, dof, expected

def t_test_for_team(group):
    birthday_games = group[group['birthday_game'] == 1]['WL']
    birthday_games = birthday_games.apply(lambda x: wl_dict[x])
    non_birthday_games = group[group['birthday_game'] == 0]['WL']
    non_birthday_games = non_birthday_games.apply(lambda x: wl_dict[x])
    
    t_statistic, p_value = ttest_ind(birthday_games, non_birthday_games)
    return t_statistic, p_value

# Create full results list
full_results = []
# Group by 'TEAM_NAME' and apply chi-square test to check significance
results = birthday_games_df.groupby('TEAM_NAME').apply(chi_square_test_for_team)

# Print results for each team
full_results.append(("Chi-Square"))
for team, result in results.items():
    chi2, p, dof, expected = result
    if p <= .1:
        full_results.append((team, "Chi-square Statistic:", chi2, "P-value:", p))
        
# Group by "Team Name" and apply t-test for each team
results = birthday_games_df.groupby('TEAM_NAME').apply(t_test_for_team)

# Print results for each team
full_results.append(("T-Test Team"))
for team, result in results.items():
    t_statistic, p_value = result
    if p_value <= .10:
        full_results.append(("Team:", team, "T-Statistic:", t_statistic, "P-value:", p_value))

# Group by "Team Name" and "Home Team abbrev (city)" and apply t-test for each team
results = birthday_games_df.groupby(['TEAM_NAME', 'home_team_abbrev']).apply(t_test_for_team)

# Print results for each team and city
full_results.append(("T-Test Team and City"))
for (team, city), result in results.items():
    t_statistic, p_value = result
    if (p_value < .05) and (abs(t_statistic) != np.inf):
        full_results.append(("Team:", team, "City:", city, "T-Statistic:", t_statistic, "P-value:", p_value))
        
# Print results to s3
write_tuples_to_s3(full_results, "bttj-final-s3", "Results.txt")