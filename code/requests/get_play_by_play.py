# this script takes user CLI for a year and season type of games to request, then finds the required game IDs in 
# the transform_season_schedule table in the sqlite database, requests all the play-by-play data for a list of 
# game IDs, and saves this raw JSON data to the raw_json/play_by_play folder.

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

sql_query = "SELECT game_id, year, season_type FROM transform_season_schedule where year = " + str(args.year) + " and season_type = '" + str(args.type) + "' limit " + str(args.limit) + ";"
print(sql_query)

# get list of game IDs logic goes here
conn = sqlite3.connect('../../data/sqlite/ff_proj.db')
cursor = conn.cursor()

try:
    cursor.execute(sql_query)

    # 4. Fetch the results
    # fetchall() retrieves all rows as a list of tuples.
    # fetchone() retrieves a single row.
    # fetchmany(size) retrieves a specified number of rows.
    rows = cursor.fetchall()
    game_ids = []

    # 5. Process the results
    for row in rows:
        game_ids.append(row[0])

except sqlite3.Error as e:
    print(f"Error executing query: {e}")

finally:
    # 6. Close the database connection
    # It's good practice to close the connection when done.
    if conn:
        conn.close()

print("Fetched the below game IDs:")
print(game_ids)

# call API endpoint
if args.g:

    for i in game_ids:
        url = "https://api.sportradar.com/nfl/official/trial/v7/en/games/" + str(i) + "/pbp.json"

        print("Requesting data from: " + url)

        # request logic
        headers = {
            "accept": "application/json",
            "x-api-key": api_key
        }

        response = requests.get(url, headers=headers)
        data = response.json()

        # save data to file just in case
        with open('../../data/raw_json/play_by_play/raw_play_by_play_' + str(args.year) + '_' + str(args.type) + '_' + str(i) + '.json', 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

        print("One second pause....")
        time.sleep(1)

# save JSON to raw_json/play_by_play directory here