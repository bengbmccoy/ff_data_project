# TODO
# 1. Handle appending new data instead of just overwiriting the whole table

# import required libraries
import argparse
import os
import json
import pandas as pd
import sqlite3
import sys

# parse arguments
parser = argparse.ArgumentParser()
parser.add_argument('-write', action='store_true', default=False,
                    help='Write data to the database')
parser.add_argument('-verbose', action='store_true', default=False,
                    help='Print all steps')
args = parser.parse_args()

# get all raw json files
path_to_json = '../../data/raw_json/play_by_play/'
json_files = [pos_json for pos_json in os.listdir(path_to_json) if pos_json.endswith('.json')]

# print all files to be transformed
if args.verbose:
    print(json_files) 

json_data = []

for i in json_files:
    with open(os.path.join(path_to_json, i)) as f:
        json_data.append(json.load(f))

# init the rush play dataframe
rush_result_columns = ['game_id', 'game_type', 'game_weather', 'year', 'season_type', 'week', 'period_id', 'period_num', 
                    'pbp_id', 'pbp_type', 'event_id', 'event_type', 'clock', 
                  'home_points', 'away_points', 'play_type', 'play_description', 'men_in_box', 'fake_punt', 
                  'fake_field_goal', 'screen_pass', 'blitz', 'play_direction', 'left_tightends', 'right_tightends',
                  'hash_mark', 'qb_at_snap', 'huddle', 'running_lane', 'play_action', 'run_pass_option', 'official',
                  'start_clock', 'start_down', 'start_yfd', 'start_possession', 'start_location_team', 'start_location_yardline',
                  'end_clock', 'end_down', 'end_yfd', 'end_possession', 'end_location_team', 'end_location_yardline',
                  'attempt', 'yards', 'firstdown', 'inside_20', 'goal_to_go', 'broken_tackles', 'kneel_down', 'scramble', 
                  'yards_after_contact', 'player_id', 'player_name', 'player_position', 'result']
results_df = pd.DataFrame(columns=rush_result_columns)

# init the pass play dataframe
pass_result_columns = ['game_id', 'period_id', 'pbp_id', 'pbp_type', 'event_id', 'event_type', 'clock', 
                  'home_points', 'away_points', 'play_type', 'play_description', 'fake_punt', 'fake_field_goal', 
                  'screen_pass', 'play_action', 'run_pass_option']
results_df = pd.DataFrame(columns=pass_result_columns)


# iterate through JSONs
for data in json_data:

# flatten data and select columns for writing to DB
    row_game_id = data['id']
    row_game_type = data['game_type']

    # sometimes weather data is missing
    try:
        row_weather = data['weather']['condition']
    except:
        row_weather = 'Unknown'

    row_year = data['summary']['season']['year']
    row_season_type = data['summary']['season']['type']

    row_week = data['summary']['week']['sequence']

    for i in data['periods']:

        row_

    # # iterate through the games field in each week
    # for j in item['summary']:
        
    #     row_year = j['season']['year']
    #     row_season_type = j['season']['type']

    #     # create dict of useful info to append to results df
    #     append_dict = {
    #         'year': row_year,
    #         'season_type': row_season_type,
    #         'week': row_weeks,
    #         'game_id': row_game_id,
    #         'date': row_game_date,
    #         'home_team': row_home_team,
    #         'away_team': row_away_team,
    #         'conference_game': row_conference_game,
    #         'venue': row_venue,
    #         'roof_type': row_roof_type,
    #         'weather': row_weather,
    #         'wind_speed': row_wind_speed,
    #         'temp': row_temp,
    #         'home_points': row_home_points,
    #         'away_points': row_away_points

    #     }

    #     # convert to pandas df and append to exisitng results_df
    #     new_row_df = pd.DataFrame([append_dict])
    #     results_df = pd.concat([results_df, new_row_df], ignore_index=True)

    print(row_game_id)
    print(row_game_type)
    print(row_weather)
    print(row_year)
    print(row_week)