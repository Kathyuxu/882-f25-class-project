import functions_framework
from google.cloud import secretmanager
from google.cloud import storage
import duckdb
import pandas as pd
import json

# settings
project_id = 'btibert-ba882-fall25'
secret_id = 'MotherDuck'   #<---------- this is the name of the secret you created
version_id = 'latest'

# db setup
db = 'nfl'
schema = "raw"
db_schema = f"{db}.{schema}"

@functions_framework.http
def task(request):

    # Parse the request data
    request_json = request.get_json(silent=True)
    print(f"request: {json.dumps(request_json)}")
    # we should be passing in a payload, but for rapid testing, ensure there is a dictionary
    if request_json is None:
        request_json = {'num_entries':1} # force eval on hardcoded test data

    # instantiate the services 
    sm = secretmanager.SecretManagerServiceClient()
    storage_client = storage.Client()

    # Build the resource name of the secret version
    secret_name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

    # Access the secret version in order to get the MotherDuck token
    response = sm.access_secret_version(request={"name": secret_name})
    md_token = response.payload.data.decode("UTF-8")

    md = duckdb.connect(f'md:?motherduck_token={md_token}') 

    ##################################################### core logic: read json from gcs, parse, and insert/store

    # if there aren't any records to process, exit
    num_entries = request_json.get('num_entries', 0)
    if num_entries < 1:
        print("no entries found for the date evaluated downsream")
        return {}, 200
    
    # read in the file from gcs that was created as part of this "run"
    bucket_name = request_json.get("bucket_name", "btibert-ba882-fall25-nfl")
    bucket = storage_client.bucket(bucket_name)
    blob_name = request_json.get("blob_name", "raw/scoreboard/487db0274c68/data.json")
    blob = bucket.blob(blob_name)
    data_str = blob.download_as_text()
    j = json.loads(data_str)
    print(f"number of entries parsed: {len(j.get('events'))} ==========")

    # parse out the events -> these are "games", which are a list
    events = j.get('events')

    # isolate data elements
    venues = []
    games = []
    teams = []
    gameteam = []

    # walk the data - this 100% could be improved?  how? what would you do differently?
    for e in events:
        game_id = e.get('id')
        start_date = e.get('date')
        season = e['season']['year']
        week = e['week']['number']
        venue = e['competitions'][0]['venue']
        teams = e['competitions'][0]['competitors']
        attendance = e['competitions'][0]['attendance']
        ingest_timestamp = pd.Timestamp.now("UTC").tz_localize(None)
        source_path = bucket_name + blob_name
        run_id = request_json.get('run_id')
        
        # append the info for games and venues
        games.append(
            {
                'game_id': game_id,
                'start_date': start_date,
                'season': season,
                'week': week,
                'venue_id': venue['id'],
                'attendance': attendance,
                'ingest_timestamp': ingest_timestamp,
                'source_path': source_path,
                'run_id': run_id
            }
        )

        venues.append(
            {
                'id': venue['id'],
                'fullname': venue['fullName'],
                'city': venue['address']['city'],
                'state': venue['address']['state'],
                'country': venue['address']['country'],
                'indoor': venue['indoor'],
                'ingest_timestamp': ingest_timestamp,
                'source_path': source_path,
                'run_id': run_id
            }
        )

        # parse out teams by using json to flatten out the data
        teams_parsed  = pd.json_normalize(teams)

        teams = teams_parsed.copy()
        team_cols = ['id', 
                     'team.name', 
                     'team.abbreviation',
                     'team.displayName',
                     'team.shortDisplayName',
                     'team.color',
                     'team.alternateColor',
                     'team.venue.id',
                     'team.logo']
        teams = teams[team_cols]
        rename_mapper = {
            'team.name':'name',
            'team.displayName': 'display_name',
            'team.shortDisplayName': 'short_name',
            'team.color': 'color',
            'team.alternateColor': 'alternate_color',
            'team.venue.id': 'venue_id',
            'team.logo':'logo'
        }
        teams = teams.rename(columns=rename_mapper)
        teams['ingest_timestamp'] = ingest_timestamp
        teams['source_path'] = source_path
        teams['run_id'] = run_id
        teams.append(teams)

        # the data are mostly there for game team
        game_team = teams_parsed.copy()
        game_team = game_team[['id', 'homeAway', 'score']]
        game_team.insert(0, 'game_id', game_id) # insert the game id as the first column
        game_team = game_team.rename(columns = {
            'homeAway': 'home_away'
        })
        game_team['ingest_timestamp'] = ingest_timestamp
        game_team['source_path'] = source_path
        game_team['run_id'] = run_id
        gameteam.append(game_team)
    
    # with the data parsed, put into dataframes across the full dataset
    games_df = pd.DataFrame(games)
    venues_df = pd.DataFrame(venues)
    teams_df = pd.concat(teams)
    gt_df = pd.concat(gameteam)

    # log
    print(f"length of games: {len(games_df)} =======")
    print(f"length of games: {len(venues_df)} =======")
    print(f"length of games: {len(teams_df)} =======")
    print(f"length of games: {len(gt_df)} =======")


    return {}, 200




















    
    
