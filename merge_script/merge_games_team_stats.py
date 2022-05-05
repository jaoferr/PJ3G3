#!/usr/bin/env python
# coding: utf-8

# #### Merging games dataframe and season stats dataframe

# > Run all cells in sequence<br>
# > Assumes season stats & game stats files are in '..\data' directory

# In[1]:


import pandas as pd
import os
from tqdm import tqdm


# In[2]:


full_path = os.getcwd()
base_path = full_path
# base_path = str(Path(full_path).parents[0])  # one dir up
base_path = os.path.join(base_path, 'data')

try:
    team_stats_file = os.path.join(base_path, '1970-2020.csv')
    team_schedules_file = os.path.join(base_path, '1970-2020_team_schedule.csv')
    df_stats_raw = pd.read_csv(team_stats_file, sep=';')
    df_schedules_raw = pd.read_csv(team_schedules_file, sep=';')

except FileNotFoundError:
    team_stats_file = os.path.join(base_path, '1970-2021.csv')
    team_schedules_file = os.path.join(base_path, '1970-2021_team_schedule.csv')
    df_stats_raw = pd.read_csv(team_stats_file, sep=';')
    df_schedules_raw = pd.read_csv(team_schedules_file, sep=';')

except:
    raise FileNotFoundError(f"no data files found in {base_path}")
    

# ---

# In[9]:


df_stats = df_stats_raw.copy()
df_schedules = df_schedules_raw.copy()


# In[11]:


# unnamed_cols = {
#     'Unnamed: 0': 'year',
#     'Unnamed: 1': 'team',
#     'Unnamed: 2': 'week_number'
# }
# df_stats.drop('Unnamed: 1', axis=1, inplace=True)

unnamed_cols = {
    'Unnamed: 0': 'id',
}
df_stats.rename(columns=unnamed_cols, inplace=True)
df_schedules.rename(columns=unnamed_cols, inplace=True)


# #### add 'is_playoff' col

# In[25]:


df_schedules['is_playoff'] = False

for season in tqdm(df_schedules['year'].unique()):
    df_season = df_schedules.query('year == @season')  # only current season
    # print(season, end=', ', flush=False)
    for team in df_season['team'].unique():
        df_season_team = df_season.query('team == @team')  # only current team
        try:  # try to find a row with 'playoff' value
            playoff_index = df_season_team[df_season_team['game_date'] == 'Playoffs'].index[0]
        except IndexError:
            playoff_index = False

        max_index = df_season_team.index[-1]
        if playoff_index:  # if there is a playoff row
            for row in range(playoff_index + 1, max_index + 1):  # for games after playoff row and before last row of current team and current season
                # playoff_game = df_season_team.loc[row].to_dict()
                
                df_schedules.loc[row, 'is_playoff'] = True

# #### remove 'bye week' & 'playoff' rows

# In[35]:


df_bye_week = df_schedules[(df_schedules['opp'] == 'Bye Week') | (df_schedules['game_date'] == 'Playoffs')]
df_schedules = df_schedules.drop(df_bye_week.index, axis=0)


# ---

# #### column cleaning

# ##### home & away games

# In[36]:


home_games = df_schedules[df_schedules['game_location'] == '@'].index
away_games = df_schedules[pd.isna(df_schedules['game_location'])].index

df_schedules.loc[home_games, 'game_location'] = 'home'
df_schedules.loc[away_games, 'game_location'] = 'away'

# ##### games with overtime

# In[37]:


ot_games = df_schedules[df_schedules['overtime'] == 'OT'].index
non_ot_games = df_schedules[pd.isna(df_schedules['overtime'])].index

df_schedules.loc[ot_games, 'overtime'] = True
df_schedules.loc[non_ot_games, 'overtime'] = False

# #### further column cleaning

# In[38]:


columns = df_schedules.columns
df_schedules = df_schedules.drop('boxscore_word', axis=1)

# #### the actual merge

# ##### Column prefixes
#     - gs_  : game_stat_
#     - hts_ : home_team_stat_
#     - ats_ : away_team_stat_

# In[40]:


df_schedules.columns = 'gs_' + df_schedules.columns

df_home_team_stats = df_stats.copy()
df_home_team_stats.columns =  'hts_' + df_home_team_stats.columns

df_away_team_stats = df_stats.copy()
df_away_team_stats.columns =  'ats_' + df_away_team_stats.columns


# In[41]:


df_games = pd.merge(
    df_schedules, 
    df_home_team_stats, 
    how='inner', 
    left_on=['gs_team', 'gs_year'], 
    right_on=['hts_team', 'hts_year'],
)
print('With home team stats:', df_games.shape)

df_games = pd.merge(
    df_games, 
    df_away_team_stats, 
    how='inner', 
    left_on=['gs_opp', 'gs_year'], 
    right_on=['ats_team', 'ats_year'],
)
print('With home and away team stats:', df_games.shape)

# ---

# #### playoffs and regular season

# In[43]:


df_regular_season = df_games[df_games['gs_is_playoff'] == False]
df_playoffs = df_games[df_games['gs_is_playoff'] == True]


# #### export to csv

df_games = df_games.reset_index().drop('index', axis=1)
df_playoffs = df_playoffs.reset_index().drop('index', axis=1)
df_regular_season = df_regular_season.reset_index().drop('index', axis=1)

# In[54]:
print('')
print(f'{"Exporting raw data":<30}', end='')
df_games.to_csv(os.path.join(base_path, 'all_games.csv'), sep=';', encoding='utf-8', index=True)
df_playoffs.to_csv(os.path.join(base_path, 'playoffs.csv'), sep=';', encoding='utf-8', index=True)
df_regular_season.to_csv(os.path.join(base_path, 'regular_season.csv'), sep=';', encoding='utf-8', index=True)

print('Done')
# In[]:

# #### kaggle files
print(f'{"Exporting kaggle data":<30}', end='')
cols_to_drop = [
    'gs_overtime', 'gs_pts_off', 'gs_pts_def',
    'gs_first_down_off', 'gs_yards_off', 'gs_pass_yds_off',
    'gs_rush_yds_off', 'gs_to_off', 'gs_first_down_def',
    'gs_yards_def', 'gs_pass_yds_def', 'gs_rush_yds_def',
    'gs_to_def', 'gs_exp_pts_off', 'gs_exp_pts_def', 'gs_exp_pts_st',
    'gs_is_playoff', 'gs_team_record',
    'hts_year', 'hts_team', 'hts_team.1',
    'ats_year', 'ats_team', 'ats_team.1'
]
df_playoffs.drop(cols_to_drop, axis=1, inplace=True)
df_playoffs = df_playoffs[df_playoffs['gs_game_location'].isin(['N', 'home'])]

# renaming cols
cols_to_rename = {
    'gs_game_outcome': 'winorlose'
}
df_playoffs.rename(cols_to_rename, axis=1, inplace=True)

# reordering
new_col_order = ['gs_id', 'gs_year', 'gs_team', 'gs_opp']
new_col_order += df_playoffs.drop(new_col_order + ['winorlose'], axis=1).columns.tolist() + ['winorlose']
df_playoffs = df_playoffs[new_col_order]

# In[]:
# exporting kaggle datasets
df_kaggle_train = df_playoffs[~df_playoffs['gs_year'].isin([2018, 2019, 2020, 2021])]  # years not in
df_kaggle_test = df_playoffs[df_playoffs['gs_year'].isin([2018, 2019, 2020, 2021])]  # years in
df_kaggle_test_labels = df_kaggle_test[['gs_id', 'winorlose']]

df_kaggle_test.drop('winorlose', axis=1, inplace=True)  # target col

sample_submission = [(i, 'W') for i in range(10)] + [(i, 'L') for i in range(10, 20)]
df_sample_submission = pd.DataFrame(sample_submission, columns=['gs_id', 'winorlose'])

df_kaggle_train.to_csv(os.path.join(base_path, 'df_kaggle_train.csv'), sep=';', encoding='utf-8', index=False)
df_kaggle_test.to_csv(os.path.join(base_path, 'df_kaggle_test.csv'), sep=';', encoding='utf-8', index=False)
df_kaggle_test_labels.to_csv(os.path.join(base_path, 'df_kaggle_test_labels.csv'), sep=';', encoding='utf-8', index=False)
df_sample_submission.to_csv(os.path.join(base_path, 'df_kaggle_sample_submission.csv'), sep=';', encoding='utf-8', index=False)
print('Done')
