import statsapi
import pandas as pd
from datetime import date
import pyarrow as pa
import pyarrow.parquet as pq

# GET GAMEPKS FOR THE DAY ------------------------------------------------
today = date.today()
sched = statsapi.get("schedule", {"sportId": 1, "startDate": today, "endDate": today,"gameType": "R,F,D,L,W", "fields": "dates,date,games,gamePk, gameType"})

game_list = sched['dates'][0]['games']
game_df = pd.DataFrame(game_list)
gamepks = game_df['gamePk']


# GET DAILY LINEUPS ------------------------------------------------------
pitchers = statsapi.get("schedule", {"sportId": 1, "startDate": today, "endDate": today, "hydrate": "probablePitcher(note)"})
pitcher_games = pitchers['dates'][0]['games']

pitcher_list = []

for i in range(len(pitcher_games)):
    pitcher_game_teams = pitcher_games[i]['teams']

    pitcher_away_dict = {}
    pitcher_away_dict['pitcher_away_id'] = pitcher_game_teams['away']['team']['id']
    pitcher_away_dict['pitcher_away_teamname'] = pitcher_game_teams['away']['team']['name']
    pitcher_away_dict['pitcher_away_pitcherid'] = pitcher_game_teams['away']['probablePitcher']['id']
    pitcher_away_dict['pitcher_away_fullname'] = pitcher_game_teams['away']['probablePitcher']['fullName']

    pitcher_home_dict = {}
    pitcher_home_dict['pitcher_home_id'] = pitcher_game_teams['home']['team']['id']
    pitcher_home_dict['pitcher_home_teamname'] = pitcher_game_teams['home']['team']['name']
    pitcher_home_dict['pitcher_home_pitcherid'] = pitcher_game_teams['home']['probablePitcher']['id']
    pitcher_home_dict['pitcher_home_fullname'] = pitcher_game_teams['home']['probablePitcher']['fullName']

    pitcher_dict = {**pitcher_away_dict, **pitcher_home_dict}
    pitcher_df = pd.DataFrame(pitcher_dict,index=[0])

    pitcher_list.append(pitcher_df)


pitcher_df_complete = pd.concat(pitcher_list)
pitcher_df_table = pa.Table.from_pandas(pitcher_df_complete, preserve_index=False)
pq.write_table(pitcher_df_table, 'projects/mlb-fantasy/mlb-dk-get-data/data/raw/season_n_playoff_game_data_2001_to_2012_a.parquet')


batters_list = []

for gamepk in gamepks:

    game = statsapi.get("game", {"gamePk": gamepk})
    game_boxscore = game['liveData']['boxscore']['teams']

    away_team = game_boxscore['away']['team']['name']
    away_teamid = game_boxscore['away']['team']['id']
    home_team = game_boxscore['home']['team']['name']
    home_teamid = game_boxscore['home']['team']['id']

    away_batters = game_boxscore['away']['batters']
    home_batters = game_boxscore['home']['batters']

    away_batters_worder = pd.DataFrame(away_batters, columns=['batter_playerid'])
    away_batters_worder['batter_order'] = away_batters_worder.index + 1
    away_batters_worder['team'] = away_team
    away_batters_worder['teamid'] = away_teamid
    away_batters_worder['homeaway'] = 'away'
    away_batters_worder['gamepk'] = gamepk
    
    home_batters_worder = pd.DataFrame(home_batters, columns=['batter_playerid'])
    home_batters_worder['batter_order'] = home_batters_worder.index + 1
    home_batters_worder['team'] = home_team
    home_batters_worder['teamid'] = home_teamid
    away_batters_worder['homeaway'] = 'home'
    home_batters_worder['gamepk'] = gamepk
    
    batters_list.append(away_batters_worder)
    batters_list.append(home_batters_worder)

batter_df_complete = pd.concat(batters_list)

# IF ONE OF THEBATTERING ORDER HAS NULLS EXIT AND ONCE COMPLETE 

# DK CONTESTS AND PLAYER SALARIES -----------------------------------------------

from draft_kings import Sport, Client
from dataclasses import asdict, dataclass

contest = Client().contests(sport=Sport.MLB)
contests_list = contest.contests
contests_df = pd.json_normalize(asdict(obj) for obj in contests_list)
contests_filtered = contests_df[(contests_df['entries_details.fee'] < 5) & (contests_df['name'].str.contains('Single'))]

draft_group_id_list = []

for i in range(len(contests_filtered)):
    draft_group_id_list.append(contests_filtered.iloc[i]['draft_group_id'])


draftables_a = Client().draftables(draft_group_id=contests_filtered.iloc[0]['draft_group_id'])
players_list = draftables_a.players
players_df = pd.json_normalize(asdict(obj) for obj in players_list)
players_df_filtered = players_df[['draftable_id', 'player_id', 'position_name', 'roster_slot_id', 'salary', 'name_details.first', 'name_details.last', 'team_details.abbreviation']]



draftables_b = Client().draftables(draft_group_id=contests_filtered.iloc[1]['draft_group_id'])

