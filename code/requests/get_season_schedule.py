# This script takes a year and season type for an NFL season, hits the Tournament List API endpoint from sportradar, flattens the returned JSON and saves it in a SQLite db.

# TODO
# 1. Enforce which years and types can be passed in correctly

# import required libraries
import requests
import argparse
import os
from dotenv import load_dotenv
import json
import pandas as pd
import numpy as np
import sqlite3
import sys


# Load environment variables from .env file
load_dotenv()
api_key = os.getenv("API_KEY")

# parse arguments
parser = argparse.ArgumentParser()
parser.add_argument('-test', action='store_true', default=True,
                    help='take data from a pre-saved JSON file for testing')
parser.add_argument('--g', action='store_true', default=False,
                    help='include this flag to actually hit the URL')
parser.add_argument('-year', type=int, default=2024,
                    help='year of season to request')
parser.add_argument('-type', type=str, default="REG",
                    help='type of season to request')
args = parser.parse_args()

# define params for GET request to sportradar
url = "https://api.sportradar.com/nfl/official/trial/v7/en/games/" + str(args.year) + "/" + str(args.type) + "/schedule.json"
headers = {
    "accept": "application/json",
    "x-api-key": api_key
}

# URL check
print("Using URL: " + url)

# init the results dataframe
result_columns = ["year", "season_type", "week", "game_id", "date", "conference_game", 'venue', 'roof_type', 'home_team', 'away_team', 'weather', 'wind', 'temp', 'home_points', 'away_points']
results_df = pd.DataFrame(columns=result_columns)

# call API endpoint
if args.g:
    print("Requesting data from: " + url)
    response = requests.get(url, headers=headers)
    data = response.json()

    # print response
    print(response.json())

    # save data to file just in case
    with open('backup.json', 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

# load in example data for debugging and testing
if args.test:
    print("Test data")
    with open('../../data/example_data/season_schedule.json') as f:
        data = json.load(f)

# flatten data and select columns for writing to DB
row_year = data['year']
row_season_type = data['type']

for i, item in enumerate(data['weeks']):
    
    row_weeks = item['sequence']

    for j in item['games']:

        print(j['id'])
        
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

        try:
            row_weather = j['weather']['condition']
        except:
            row_weather = "Unknown"

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

        new_row_df = pd.DataFrame([append_dict])
        results_df = pd.concat([results_df, new_row_df], ignore_index=True)

# for debugging
print(results_df)

# write to database
db_directory = "../../data/sqlite/"  # Name of the directory to store the database
db_filename = "ff_proj.db"
table_name = 'raw_season_schedule'

db_path = os.path.join(db_directory, db_filename)

try:
    conn = sqlite3.connect(db_path)
    results_df.to_sql(table_name, conn, if_exists='replace', index=False)
except:
    print("Could not connect to database")
    sys.exit(1)

conn.close()