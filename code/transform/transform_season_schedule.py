
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
path_to_json = '../../data/raw_json/season_schedule/'
json_files = [pos_json for pos_json in os.listdir(path_to_json) if pos_json.endswith('.json')]

# print all files to be transformed
if args.verbose:
    print(json_files)  # for me this prints ['foo.json']

json_data = []

for i in json_files:
    with open(os.path.join(path_to_json, i)) as f:
        json_data.append(json.load(f))

# init the results dataframe
result_columns = ["year", "season_type", "week", "game_id", "date", "conference_game", 'venue', 'roof_type', 'home_team', 'away_team', 'weather', 'wind', 'temp', 'home_points', 'away_points']
results_df = pd.DataFrame(columns=result_columns)

# iterate through JSONs
for data in json_data:

    # flatten data and select columns for writing to DB
    row_year = data['year']
    row_season_type = data['type']

    # iterate through the weeks field
    for i, item in enumerate(data['weeks']):
        
        row_weeks = item['sequence']

        # iterate through the games field in each week
        for j in item['games']:
            
            row_game_id = j['id']
            row_game_date = j['scheduled']
            row_conference_game = j['conference_game']
            row_venue = j['venue']['name']
            row_roof_type = j['venue']['roof_type']
            row_home_team = j['home']['alias']
            row_away_team = j['away']['alias']
            row_wind_speed = j['weather']['wind']['speed']
            row_temp = j['weather']['temp']
            row_home_points = j['scoring']['home_points']
            row_away_points = j['scoring']['away_points']

            # sometimes weather data is missing
            try:
                row_weather = j['weather']['condition']
            except:
                row_weather = "Unknown"

            # create dict of useful info to append to results df
            append_dict = {
                'year': row_year,
                'season_type': row_season_type,
                'week': row_weeks,
                'game_id': row_game_id,
                'date': row_game_date,
                'home_team': row_home_team,
                'away_team': row_away_team,
                'conference_game': row_conference_game,
                'venue': row_venue,
                'roof_type': row_roof_type,
                'weather': row_weather,
                'wind_speed': row_wind_speed,
                'temp': row_temp,
                'home_points': row_home_points,
                'away_points': row_away_points

            }

            # convert to pandas df and append to exisitng results_df
            new_row_df = pd.DataFrame([append_dict])
            results_df = pd.concat([results_df, new_row_df], ignore_index=True)

# for debugging
if args.verbose:
    print(results_df)

# write to database
if args.write:
    db_directory = "../../data/sqlite/"  # Name of the directory to store the database
    db_filename = "ff_proj.db"
    table_name = 'transform_season_schedule'

    db_path = os.path.join(db_directory, db_filename)

    try:
        conn = sqlite3.connect(db_path)
        results_df.to_sql(table_name, conn, if_exists='replace', index=False)
    except:
        print("Could not connect to database")
        sys.exit(1)

    print("Data written to database")

    conn.close()