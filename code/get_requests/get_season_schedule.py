# This script takes a year and season type for an NFL season, hits the Tournament List API endpoint from sportradar, flattens the returned JSON and saves it in a SQLite db.

# TODO


# import required libraries
import requests
import sys
import os
from dotenv import load_dotenv
import json


def main(year=2024, type="REG", verbose=False, prod=False):

    # get required environment variables
    api_key = get_env_var("API_KEY")

    # Enforce api variables:
    if enforce_api_variables(year, type):
        pass
    else:
        print("API variables check failed")
        print('year: ' + str(year) + '    type: ' + str(type))
        sys.exit(1)

    # define params for GET request to sportradar
    url = "https://api.sportradar.com/nfl/official/trial/v7/en/games/" + str(year) + "/" + str(type) + "/schedule.json"
    headers = {
        "accept": "application/json",
        "x-api-key": api_key
    }

    # URL check
    if verbose:
        print("Using URL: " + url)

    # call API endpoint
    if prod:
        
        if verbose:
            print("Requesting data from: " + url)

        # send GET request
        response = requests.get(url, headers=headers)
        data = response.json()

        # print response
        if verbose:
            print("Data received of length: " + str(len(data)))

        # save data to file
        with open('../data/raw_json/season_schedule/raw_season_schedule_' + str(year) + '_' + str(type) + '.json', 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

        if verbose:
            print("Raw data saved to: " + '../data/raw_json/season_schedule/raw_season_schedule_' + str(year) + '_' + str(type) + '.json')


def enforce_api_variables(year, type):
    # Check that we are not wasting API calls
    if year not in range(2000, 2026):
        return False
    if type not in ["REG", "PST", "PRE"]:
        return False
    
    return True

def get_env_var(env_var):
    # Load environment variables from .env file
    load_dotenv()
    api_key = os.getenv(env_var)

    return api_key


if __name__ == "__main__":
    main()