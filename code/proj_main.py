

import argparse

from get_requests import get_season_schedule
from get_requests import get_play_by_play
from transform import transform_season_schedule


def main():

    print('SUHHHH')

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
    parser.add_argument('-verbose', action='store_true', default=False,
                        help='Print all steps')
    args = parser.parse_args()

if __name__ == "__main__":
    main()