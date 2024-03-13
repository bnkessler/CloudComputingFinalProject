import sys

import pandas as pd
import numpy as np
from datetime import timedelta, datetime
import time
from plotnine import *
import boto3

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
        
def save_plot_to_s3(plot, bucket_name, file_name):
    # Save the plot to a file
    plot.save(filename=file_name, height=5, width=5, units = 'in', dpi=100)

    # Create an S3 client
    s3 = boto3.client('s3')

    # Upload the file to S3
    with open(file_name, "rb") as data:
        s3.upload_fileobj(data, bucket_name, file_name)
        
'bttj-final-s3', 'api-call/PlayerStats.csv'
#######Put csvs here
base = read_csv_from_s3('bttj-final-s3', 'api-call/player_stats_jake.csv')
player_stats = read_csv_from_s3('bttj-final-s3', 'api-call/diff_jake.csv')
#######

base = base.dropna()
base = base[['Player_id', 'Season', 'REB', 'AST', 'STL', 'BLK', 'TOV', 'PF', 'PTS', 'PLUS_MINUS']]
base = base.reset_index().drop(['index'], axis=1)

player_stats_grouped = player_stats.groupby(['PLAYER_ID', 'SEASON_ID']).mean().reset_index()

high_min = player_stats_grouped[player_stats_grouped['MIN'] >= 24]

high_min = high_min[['PLAYER_ID', 'SEASON_ID', 'PLAYER_AGE']]

high_min[['Season','other']] = high_min['SEASON_ID'].str.split('-', n=1, expand=True)

base['Player_id'] = base['Player_id'].astype(str)

high_min['PLAYER_ID'] = high_min['PLAYER_ID'].astype(str)

base['Season'] = base['Season'].astype(str)

high_min['Season'] = high_min['Season'].astype(str)

merged = base.merge(high_min, how = 'inner', left_on = ['Player_id', 'Season'], right_on = ['PLAYER_ID', 'Season'])

merged_young = merged[merged['PLAYER_AGE'] <= 25]

merged_old = merged[merged['PLAYER_AGE'] > 25]

merged = merged.drop(['PLAYER_AGE'], axis=1)

by_year = merged.groupby('Season').mean().reset_index()

by_year = by_year.drop(['PF', 'PLUS_MINUS'], axis = 1)

year_melted = pd.melt(by_year, id_vars='Season', var_name='variable')

year_melted['variable'] = year_melted['variable'].replace({'AST': 'ASSISTS', 'PTS': 'POINTS', 'STL': 'STEALS', 'BLK': 'BLOCKS', 'PLUS_MINUS': 'PLUS MINUS', 'REB': 'REBOUNDS', 'TOV': 'TURNOVERS'})

by_year_young = merged_young.groupby('Season').mean().reset_index()

by_year_young = by_year_young.drop(['PF', 'PLUS_MINUS','PLAYER_AGE'], axis = 1)

year_melted_young = pd.melt(by_year_young, id_vars='Season', var_name='variable')

year_melted_young['variable'] = year_melted_young['variable'].replace({'AST': 'ASSISTS', 'PTS': 'POINTS', 'STL': 'STEALS', 'BLK': 'BLOCKS', 'PLUS_MINUS': 'PLUS MINUS', 'REB': 'REBOUNDS', 'TOV': 'TURNOVERS'})

by_year_old = merged_old.groupby('Season').mean().reset_index()

by_year_old = by_year_old.drop(['PF', 'PLUS_MINUS','PLAYER_AGE'], axis = 1)

year_melted_old = pd.melt(by_year_old, id_vars='Season', var_name='variable')

year_melted_old['variable'] = year_melted_old['variable'].replace({'AST': 'ASSISTS', 'PTS': 'POINTS', 'STL': 'STEALS', 'BLK': 'BLOCKS', 'PLUS_MINUS': 'PLUS MINUS', 'REB': 'REBOUNDS', 'TOV': 'TURNOVERS'})

year_melted_old['Agroup'] = 'Over 25'

year_melted_young['Agroup'] = '25 and Under'

combined_df = pd.concat([year_melted_old, year_melted_young])

from plotnine import ggplot, aes, geom_line, geom_hline, facet_wrap, labs, theme_minimal, theme, element_text

plot1 = (ggplot(combined_df, aes(x='Season', y='value', color='Agroup', group='variable+Agroup'))
 + geom_line()
 + geom_hline(yintercept=0, linetype='dashed', color='black')
 + facet_wrap('~ variable', scales='free_y', ncol=2)
 + labs(x='Season', y='Difference', title='Difference in Per Minute Statistics Over the Last Seven Seasons for Players with 24 MPG')
 + theme_minimal()
 + theme(figure_size=(12, 6),
         plot_title=element_text(hjust=0.5),
         axis_text_x=element_text(angle=45)))
         
save_plot_to_s3(plot1, 'bttj-final-s3', 'api-call/plot1_jake.png')

column_means_young = merged_young.drop(['Player_id', 'Season', 'PLAYER_ID', 'PLUS_MINUS', 'other'], axis=1).mean()

column_means_young = pd.DataFrame(column_means_young).transpose()

column_means_old =  merged_old.drop(['Player_id', 'Season', 'PLAYER_ID', 'PLUS_MINUS', 'other'], axis=1).mean()

column_means_old = pd.DataFrame(column_means_old).transpose()

column_means_old['Age'] = 'Over 25'

column_means_young['Age'] = '25 and Under'

column_me = pd.concat([column_means_young, column_means_old])

column_me = column_me.drop(['PLAYER_AGE', 'TOV'], axis=1)

column_me_long = pd.melt(column_me, id_vars=['Age'], var_name='Statistic', value_name='Mean')

plot = (ggplot(column_me_long, aes(x='Statistic', y='Mean', fill='Age'))
        + geom_bar(stat='identity', position='dodge')
        + labs(x='Statistic', y='Mean Per Min Difference (After Bday - Before Bday)', title='Mean Per Minute Stat Differences')
        + theme(axis_text_x=element_text(rotation=45, hjust=1))) + theme_bw() + theme(figure_size=(8,6))

for i in merged_young.drop(['Player_id',  'Season', 'PLAYER_ID', 'SEASON_ID', 'PLAYER_AGE', 'other', 'PLUS_MINUS'], axis=1).columns:
    if max(merged_young[i]) - min(merged_young[i]) < 1.5:
        plot = (ggplot(merged) +
        aes(x=i)+
        geom_histogram(binwidth=.01, color='black', fill='orange') +
        labs(title='Histogram of Difference of ' + i, x='Value', y='Frequency') +
        theme_bw())
        print(plot)
    else:
        plot = (ggplot(merged) +
        aes(x=i)+
        geom_histogram(binwidth=.05, color='black', fill='lightblue') +
        labs(title='Histogram of Difference of ' + i, x='Value', y='Frequency') +
        theme_bw())
        print(plot)
        
save_plot_to_s3(plot, 'bttj-final-s3', 'api-call/plot_jake.png')
