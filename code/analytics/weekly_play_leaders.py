# This script runs a number of SQL CTEs on the ff_proj/transform_play_by_play to determine which player is the leader in each play catgeory over time

import sqlite3
import pandas as pd
import matplotlib.pyplot as plt

# SQL query to request the game_ids from the transform_season_schedule table based on user input
sql_query = """
with non_penalty_plays as (
	select 
		distinct *
	from 
		transform_play_by_play_details 
	where 
		id not in 
			(select id from transform_play_by_play_details where category == 'penalty' and description like "%accepted%")
)
, weekly_aggs as (
	select 
		year,
		week,
		player_name,
		category,
		count(category) as weekly_count
	from non_penalty_plays
	group by 1, 2, 3, 4
	order by 1, 2, 3, 4
)
SELECT
	year,
    week,
    player_name,
    category,
    weekly_count,
    SUM(weekly_count) OVER (PARTITION BY category, player_name ORDER BY week) AS cumulative_weekly_count
FROM
    weekly_aggs
group by player_name, category, week
ORDER BY
    week;
"""

# make connection to the sqlite database
conn = sqlite3.connect('../../data/sqlite/ff_proj.db')
cursor = conn.cursor()

# attempt to request game IDs, add them to a list, close DB connection when complete
try:
    game_ids = []
    cursor.execute(sql_query)
    rows = cursor.fetchall()

except sqlite3.Error as e:
    print(f"Error executing query: {e}")

finally:
    if conn:
        conn.close()

weekly_agg_table = pd.DataFrame(rows, columns=['year', 'week', 'player_name', 'category', 'weekly_count', 'cumulative_weekly_count'])

print(weekly_agg_table)

aaron_jones_df = weekly_agg_table.loc[(weekly_agg_table['category'] == 'field_goal') & (weekly_agg_table['player_name'] == 'Cameron Dicker')]
print(aaron_jones_df)


# plot_table = weekly_agg_table.loc[(weekly_agg_table['category'] == 'rush')]

# fig, ax = plt.subplots()

# for key, grp in plot_table.groupby(['player_name']):
#     ax = grp.plot(ax=ax, kind='line', x='week', y='cumulative_weekly_count', label=key)

# plt.legend(loc='best')
# plt.show()