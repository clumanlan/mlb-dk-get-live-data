import pandas as pd
import statsapi
from datetime import datetime, timedelta
import awswrangler as wr
import boto3 
import json
import time
from datetime import date
from draft_kings import Sport, Client
from dataclasses import asdict, dataclass


def get_secret():

    secret_name = "dkuser_aws_keys"
    region_name = "us-east-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    get_secret_value_response = client.get_secret_value(SecretId=secret_name)

    secret_response = get_secret_value_response['SecretString']

    return json.loads(secret_response)
    
    # secret_dict = get_secret()


def todays_date():
    return date.today()

def get_todays_gamepks(today):
    sched = statsapi.get("schedule", {"sportId": 1, "startDate": today, "endDate": today,"gameType": "R,F,D,L,W", "fields": "dates,date,games,gamePk, gameType"})

    game_list = sched['dates'][0]['games']
    game_df = pd.DataFrame(game_list)
    gamepks = game_df['gamePk']

    return gamepks

def get_pitcher_lineups(today):

    pitchers = statsapi.get("schedule", {"sportId": 1, "startDate": today, "endDate": today, "hydrate": "probablePitcher(note)"})
    pitcher_games = pitchers['dates'][0]['games']

    pitcher_list = []

    for i in range(len(pitcher_games)):
        pitcher_game_teams = pitcher_games[i]['teams']

        pitcher_away_dict = {}
        pitcher_away_dict['pitcher_teamid'] = pitcher_game_teams['away']['team']['id']
        pitcher_away_dict['pitcher_teamname'] = pitcher_game_teams['away']['team']['name']
        pitcher_away_dict['pitcher_playerid'] = pitcher_game_teams['away']['probablePitcher']['id']
        pitcher_away_dict['pitcher_fullname'] = pitcher_game_teams['away']['probablePitcher']['fullName']
        pitcher_away_dict['home_away'] = "away"

        pitcher_home_dict = {}
        pitcher_home_dict['pitcher_teamid'] = pitcher_game_teams['home']['team']['id']
        pitcher_home_dict['pitcher_teamname'] = pitcher_game_teams['home']['team']['name']
        pitcher_home_dict['pitcher_playerid'] = pitcher_game_teams['home']['probablePitcher']['id']
        pitcher_home_dict['pitcher_fullname'] = pitcher_game_teams['home']['probablePitcher']['fullName']
        pitcher_home_dict['home_away'] = "home"


        pitcher_away_dict = {**pitcher_away_dict}
        pitcher_home_dict = {**pitcher_home_dict}

        pitcher_away_df = pd.DataFrame(pitcher_away_dict,index=[0])
        pitcher_home_df = pd.DataFrame(pitcher_home_dict,index=[0])

        pitcher_list.append(pitcher_away_df)
        pitcher_list.append(pitcher_home_df)


    pitcher_df_complete = pd.concat(pitcher_list)

    #EXIT IF MISSING A PITCHER
    return pitcher_df_complete


def get_batter_lineups(gamepks):

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
        away_batters_worder['home_away'] = 'away'
        away_batters_worder['gamepk'] = gamepk
        
        home_batters_worder = pd.DataFrame(home_batters, columns=['batter_playerid'])
        home_batters_worder['batter_order'] = home_batters_worder.index + 1
        home_batters_worder['team'] = home_team
        home_batters_worder['teamid'] = home_teamid
        home_batters_worder['home_away'] = 'home'
        home_batters_worder['gamepk'] = gamepk
        
        batters_list.append(away_batters_worder)
        batters_list.append(home_batters_worder)

    batter_df_complete = pd.concat(batters_list)

    return batter_df_complete

# IF ONE OF THE BATTERING ORDER HAS NULLS EXIT AND ONCE COMPLETE 
def dk_single_entry_contests():
    
    contest = Client().contests(sport=Sport.MLB)
    contests_list = contest.contests
    contests_df = pd.json_normalize(asdict(obj) for obj in contests_list)
    contests_filtered = contests_df[(contests_df['entries_details.fee'] < 5) & (contests_df['name'].str.contains('Single'))]

    draft_groupid_list = []
    for i in range(len(contests_filtered)):
        draft_groupid_list.append(contests_filtered.iloc[i]['draft_group_id'])

    draftable_list = []
    for i in draft_groupid_list:
        draftables = Client().draftables(draft_group_id=i)
        players_list = draftables.players
        players_df = pd.json_normalize(asdict(obj) for obj in players_list)
        players_df_filtered = players_df[['draftable_id', 'player_id', 'position_name', 'roster_slot_id', 'salary', 'name_details.first', 'name_details.last', 'team_details.abbreviation']]
        players_df_filtered['draft_groupid'] = i

        draftable_list.append(players_df_filtered)

    single_entry_draftlist = pd.concat(draftable_list)
    
    return single_entry_draftlist


def write_data_to_s3(pitcher_df_complete, batter_df_complete, single_entry_draftlist):

    current_date = datetime.utcnow().strftime('%Y-%m-%d')
    
    secret_dict = get_secret()
    aws_key_id = secret_dict['aws_access_key_id']
    aws_secret = secret_dict['aws_secret_access_key']

    session = boto3.Session(
        aws_access_key_id=aws_key_id,
        aws_secret_access_key=aws_secret)
    
    wr.s3.to_parquet(
            df=pitcher_df_complete,
            path="s3://mlbdk-model/live/pitcher/pitcher_df_complete_{}.parquet".format(current_date),
            boto3_session=session
        )
    print(pitcher_df_complete.shape)
    wr.s3.to_parquet(
            df=batter_df_complete,
            path="s3://mlbdk-model/live/batter/batter_df_complete_{}.parquet".format(current_date),
            boto3_session=session
        )
    print(batter_df_complete.shape)
    wr.s3.to_parquet(
            df=single_entry_draftlist,
            path="s3://mlbdk-model/live/dk-contests/single-entry/single_entry_draftlist_{}.parquet".format(current_date),
            boto3_session=session
        )
    print(single_entry_draftlist.shape)

def handler(event, context):
    
    today = todays_date()
    gamepks = get_todays_gamepks(today)

    pitcher_lineups = get_pitcher_lineups(today)
    batter_lineups = get_batter_lineups(gamepks)
    single_entry_draftlist = dk_single_entry_contests()

    time.sleep(15) # sleep so previous function has enough time to write to disk

    write_data_to_s3(pitcher_lineups, batter_lineups, single_entry_draftlist)

handler(None, None)


