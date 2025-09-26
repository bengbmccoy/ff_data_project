# this script takes user CLI for a year and season type of games to request, then finds the required game IDs in 
# the transform_season_schedule table in the sqlite database, requests all the play-by-play data for a list of 
# game IDs, and saves this raw JSON data to the raw_json/play_by_play folder.

# TODO
# 1. Enforce which years and types can be passed in correctly
# 2. Add verbose argument and print statements

# import required libraries
import requests
import os
from dotenv import load_dotenv
import json
import sqlite3
import time


def main(year=2024, type="REG", limit=2, verbose=False, prod=False):
    
    # get required environment variables
    api_key = get_env_var("API_KEY")

    # find existing game_IDs in data/raw_json/play_by_play
    # get all raw json files
    path_to_json = '../data/raw_json/play_by_play/'
    json_files = [pos_json for pos_json in os.listdir(path_to_json) if pos_json.endswith('.json')]

    # get all existing game_IDs in raw data
    raw_data_game_ids = []
    for i in json_files:
        j = i.split(".")[0]
        raw_data_game_ids.append(j.split("_")[-1])

    # SQL query to request the game_ids from the transform_season_schedule table based on user input
    sql_query = "SELECT game_id, year, season_type FROM transform_season_schedule where year = " + str(year) + " and season_type = '" + str(type) + "';"

    game_ids = get_game_ids_transform_schdule(sql_query)
    
    # add incremental & limit logic
    new_game_ids =[game_id for game_id in game_ids if game_id not in raw_data_game_ids]
    if limit:
        request_game_ids = new_game_ids[:limit]

    # print game IDs to request for visibility
    if verbose:
        print("Fetched the below game IDs:")
        print(request_game_ids)
        print("Requesting a total of " + str(len(request_game_ids)) + " game IDs")
        if len(request_game_ids) < limit:
            print("Only requesting " + str(len(request_game_ids)) + " games due to incremental logic")

    # call API endpoint
    if prod:
        perform_gets(request_game_ids, api_key, year, type)
    

def perform_gets(request_game_ids, api_key, year, type):

    # iterate through game IDs
    for i in request_game_ids:
        
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
        with open('../data/raw_json/play_by_play/raw_play_by_play_' + str(year) + '_' + str(type) + '_' + str(i) + '.json', 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

        # pause to not blacklisted by sportradar
        print("One second pause....")
        time.sleep(1)

def get_game_ids_transform_schdule(sql_query):

    # make connection to the sqlite database
    conn = sqlite3.connect('../data/sqlite/ff_proj.db')
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

    return game_ids


def get_env_var(env_var):
    # Load environment variables from .env file
    load_dotenv()
    api_key = os.getenv(env_var)

    return api_key


if __name__ == "__main__":
    main()