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

# # init the pass play dataframe
# play_data_columns = ['game_id', 'game_type', 'weather', 'year', 'season_type', 'week', 
#                      'period_type', 'period_id', 'period_number', 
#                      'event_id', 'play_type', 'clock', 'home_points', 'away_points', 'description', 'flat_events_dict']
# results_df = pd.DataFrame(columns=play_data_columns)

transform_play_events = pd.DataFrame()
transform_play_details = pd.DataFrame()

event_fields = ['away_points','blitz','clock','created_at','description','fake_field_goal',
                'fake_punt','goaltogo','hash_mark','home_points','huddle','id','left_tightends','men_in_box',
                'official','pass_route','play_action','play_direction','players_rushed','pocket_location',
                'qb_at_snap','right_tightends','run_pass_option','running_lane','scoring_play','screen_pass',
                'sequence','type','updated_at','wall_clock', 'play_type']

play_detail_fields = ['category','description','direction','alias','yardline','first_touch','no_attempt',
                      'onside','description','no_play','result','yards','reason_missed','result','sack_split',
                      'safety','sequence','alias','yardline','yards']



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

                    if i['type'] != 'event':

                        base_dict = {}

                        base_dict['game_id'] = row_game_id
                        base_dict['game_type'] = row_game_type
                        base_dict['weather'] = row_weather
                        base_dict['year'] = row_year
                        base_dict['season_type'] = row_season_type
                        base_dict['week'] = row_week
                        base_dict['period_type'] = row_period_type
                        base_dict['period_id'] = row_period_id
                        base_dict['period_number'] = row_period_number

                        event_dict = {} | base_dict

                        for j in event_fields:
                            try:
                                event_dict[j] = i[j]
                            except:
                                event_dict[j] = None

                        new_event_df = pd.DataFrame([event_dict])
                        transform_play_events = pd.concat([transform_play_events, new_event_df], ignore_index=True)

                        details_dict = {} | event_dict

                        for k in i['details']:
                            # print(k)

                            for l in play_detail_fields:

                                try:
                                    details_dict[l] = k[l]
                                except:
                                    details_dict[l] = None

                            # convert to pandas df and append to exisitng results_df
                            new_detail_df = pd.DataFrame([details_dict])
                            transform_play_details = pd.concat([transform_play_details, new_detail_df], ignore_index=True)

                            # print(new_row_df)

# for debugging
if args.verbose:
    print('pass')


# write to database
if args.write:
    db_directory = "../../data/sqlite/"  # Name of the directory to store the database
    db_filename = "ff_proj.db"
    event_table_name = 'transform_play_by_play_events'
    detail_table_name = 'transform_play_by_play_details'

    db_path = os.path.join(db_directory, db_filename)

    try:
        print('connecting')
        conn = sqlite3.connect(db_path)

        print('writing transform_play_events')
        transform_play_events.to_sql(event_table_name, conn, if_exists='replace', index=False)

        print('writing transform_play_details')
        transform_play_details.to_sql(detail_table_name, conn, if_exists='replace', index=False)

    except:
        print("Could not connect to database")
        sys.exit(1)

    print("Data written to database")

    conn.close()