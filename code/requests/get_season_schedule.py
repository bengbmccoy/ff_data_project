# This script takes a year and season type for an NFL season, hits the Tournament List API endpoint from sportradar, flattens the returned JSON and saves it in a SQLite db.

# TODO
# 1. Enforce which years and types can be passed in correctly
# 2. Add verbose argument and print statements

# import required libraries
import requests
import argparse
import os
from dotenv import load_dotenv
import json

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
args = parser.parse_args()

# define params for GET request to sportradar
url = "https://api.sportradar.com/nfl/official/trial/v7/en/games/" + str(args.year) + "/" + str(args.type) + "/schedule.json"
headers = {
    "accept": "application/json",
    "x-api-key": api_key
}

# URL check
print("Using URL: " + url)

# call API endpoint
if args.g:
    print("Requesting data from: " + url)

    response = requests.get(url, headers=headers)
    data = response.json()

    # print response
    print(response.json())

    # save data to file just in case
    with open('../../data/raw_json/season_schedule/raw_season_schedule_' + str(args.year) + '_' + str(args.type) + '.json', 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

