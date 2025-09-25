# TODO
# 1. Handle appending new data instead of just overwiriting the whole table

# import required libraries
import argparse
import os
import json
import pandas as pd
import sqlite3
import sys

def flatten_json(nested_json, parent_key='', sep='_'):
    """
    Flattens a nested JSON object into a single-level dictionary.

    Args:
        nested_json (dict): The nested JSON object to flatten.
        parent_key (str): The prefix for keys in the flattened dictionary.
        sep (str): The separator to use between parent and child keys.

    Returns:
        dict: A flattened dictionary.
    """
    flat_dict = {}
    for key, value in nested_json.items():
        new_key = f"{parent_key}{sep}{key}" if parent_key else key
        if isinstance(value, dict):
            flat_dict.update(flatten_json(value, new_key, sep=sep))
        elif isinstance(value, list):
            for i, item in enumerate(value):
                if isinstance(item, dict):
                    flat_dict.update(flatten_json(item, f"{new_key}{sep}{i}", sep=sep))
                else:
                    flat_dict[f"{new_key}{sep}{i}"] = item
        else:
            flat_dict[new_key] = value
    return flat_dict


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

paths = []
values = []

event_elements = []

for i in json_files:
    with open(os.path.join(path_to_json, i)) as f:

        json_data.append(json.load(f))

# init the pass play dataframe
play_data_columns = ['game_id', 'game_type', 'weather', 'year', 'season_type', 'week', 
                     'period_type', 'period_id', 'period_number', 
                     'event_id', 'play_type', 'clock', 'home_points', 'away_points', 'description', 'flat_events_dict']
results_df = pd.DataFrame(columns=play_data_columns)



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

    for period in data['periods']:

        row_period_type = period['period_type']
        row_period_id = period['id']
        row_period_number = period['number']

        for play in period['pbp']:

            if play['type'] == "drive":

                for i in play['events']:

                    try:

                        row_event_id = i['id']
                        row_play_type = i['play_type']
                        row_clock = i['clock']
                        row_home_points = i['home_points']
                        row_away_points = i['away_points']
                        row_play_description = i['description']

                        flattened_data = flatten_json(i)
                        row_flat_events_dict = flattened_data

                        # create dict of useful info to append to results df
                        append_dict = {
                            'game_id' : row_game_id,
                            'game_type' : row_game_type,
                            'weather' : row_weather,
                            'year' : row_year,
                            'season_type' : row_season_type,
                            'week' : row_week,
                            'period_type' : row_period_type,
                            'period_id' : row_period_id,
                            'period_number' : row_period_number,
                            'event_id' : row_event_id,
                            'play_type' : row_play_type,
                            'play_type' : row_play_type,
                            'play_type' : row_play_type,
                            'play_type' : row_play_type,
                            'clock': row_clock,
                            'home_points' : row_home_points, 
                            'away_points' : row_away_points,
                            'play_description' : row_play_description,
                            'flat_events_dict' : row_flat_events_dict,
                        }

                        print(append_dict)

                    except:

                        print(row_period_id)


                    # convert to pandas df and append to exisitng results_df
                    new_row_df = pd.DataFrame([append_dict])

                    print(results_df.columns)

                    print(new_row_df.columns)

                    results_df = pd.concat([results_df, new_row_df], ignore_index=True)
                        
print(results_df.groupby('play_type')['play_type'].count())
