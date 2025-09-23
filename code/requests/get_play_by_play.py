# this script takes user CLI for a year and season type of games to request, then finds the required game IDs in 
# the transform_season_schedule table in the sqlite database, requests all the play-by-play data for a list of 
# game IDs, and saves this raw JSON data to the raw_json/play_by_play folder.

# TODO
# 1. Enforce which years and types can be passed in correctly
# 2. Add verbose argument and print statements

# import required libraries
import requests
import argparse
import os
from dotenv import load_dotenv
import json
import sqlite3
import time

# Load environment variables from .env file
load_dotenv()
api_key = os.getenv("API_KEY")

# parse arguments
parser = argparse.ArgumentParser()
parser.add_argument('--g', action='store_true', default=False,
                    help='include this flag to actually hit the URL')
parser.add_argument('-year', type=int, default=2024,
                    help='year of season to request')
parser.add_argument('-type', type=str, default="REG",
                    help='type of season to request')
parser.add_argument('-limit', type=int, default=1,
                    help='how many game_ids to retrieve')
args = parser.parse_args()

# SQL query to request the game_ids from the transform_season_schedule table based on user input
sql_query = "SELECT game_id, year, season_type FROM transform_season_schedule where year = " + str(args.year) + " and season_type = '" + str(args.type) + "' limit " + str(args.limit) + ";"

# make connection to the sqlite database
conn = sqlite3.connect('../../data/sqlite/ff_proj.db')
cursor = conn.cursor()

# attempt to request game IDs, add them to a list, close DB connection when complete
try:
    game_ids = []
    cursor.execute(sql_query)
    rows = cursor.fetchall()

    for row in rows:
        game_ids.append(row[0])

except sqlite3.Error as e:
    print(f"Error executing query: {e}")

finally:
    if conn:
        conn.close()

# print game IDs to request for visibility
print("Fetched the below game IDs:")
print(game_ids)

# call API endpoint
if args.g:

    # iterate through game IDs
    for i in game_ids:
        
        url = "https://api.sportradar.com/nfl/official/trial/v7/en/games/" + str(i) + "/pbp.json"
        print("Requesting data from: " + url)

        # request logic
        headers = {
            "accept": "application/json",
            "x-api-key": api_key
        }

        # call API endpoint
        response = requests.get(url, headers=headers)
        data = response.json()

        # save data to raw_json direcotry
        with open('../../data/raw_json/play_by_play/raw_play_by_play_' + str(args.year) + '_' + str(args.type) + '_' + str(i) + '.json', 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

        # pause to not blacklisted by sportradar
        print("One second pause....")
        time.sleep(1)