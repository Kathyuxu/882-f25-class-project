from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.operators.python import get_current_context
import requests

# helper
def invoke_function(url, params={}) ->dict:
    """
    Invoke our cloud function url and optionally pass data for the function to use
    """
    resp = requests.get(url, params=params)
    resp.raise_for_status()
    return resp.json()


@dag(
    schedule=None,                 # run manually for the demo
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["raw", "ingest"]
)
def nfl_raw_pipeline():

    @task
    def schema():
        url = "https://us-central1-btibert-ba882-fall25.cloudfunctions.net/raw-schema-setup"
        resp = invoke_function(url)
        print(resp)
        return resp

    @task
    def extract(payload:dict) -> dict:
        url = "https://us-central1-btibert-ba882-fall25.cloudfunctions.net/raw-extract-scoreboard"
        ctx = get_current_context()
        # add the pieces of info to the payload passed through
        payload['run_id'] = ctx["dag_run"].run_id
        payload['date'] = ctx["ds_nodash"]
        resp = invoke_function(url, params=payload)
        print("response=============================")
        print(resp)
        return resp

    @task
    def load(payload:dict) -> dict:
        url = "https://us-central1-btibert-ba882-fall25.cloudfunctions.net/raw-load-scoreboard"
        ctx = get_current_context()
        print("incoming payload =======================")
        print(payload)
        payload['date'] = ctx["ds_nodash"]
        print("final payload =======================")
        print(payload)
        resp = invoke_function(url, params=payload)
        return resp
    
    @task
    def extract_game_ids(load_payload: dict) -> list[str]:
        # returns [] if key missing or empty → mapped task will be skipped
        ids = load_payload.get("game_ids") or []
        # dedupe, stringify to be safe for query params
        ids = [str(x) for x in dict.fromkeys(ids)]
        print(f"Found {len(ids)} game_ids: {ids}")
        return ids

    @task
    def parse_load_game_detail(game_id: str) -> dict:
        url = "https://us-central1-btibert-ba882-fall25.cloudfunctions.net/raw-parse-game"
        ctx = get_current_context()
        params = {
            "game_id": game_id,
            "run_id": ctx["dag_run"].run_id,
        }
        resp = invoke_function(url, params=params)
        print(f"Finished game_id={game_id}")
        return resp

    schema_result = schema()
    extract_result = extract(schema_result)
    load_result = load(extract_result)
    game_ids = extract_game_ids(load_result)
    _ = parse_load_game_detail.expand(game_id=game_ids)


nfl_raw_pipeline()