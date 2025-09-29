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
    schedule=None,                 
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["stage"]
)
def nfl_seed_stage():

    @task
    def schema():
        url = "https://us-central1-btibert-ba882-fall25.cloudfunctions.net/stage-schema"
        resp = invoke_function(url)
        print(resp)
        return resp
    
    @task
    def load():
        url = "https://us-central1-btibert-ba882-fall25.cloudfunctions.net/stage-load-tables"
        resp = invoke_function(url)
        print(resp)
        return resp   
    
    schema() >> load()

nfl_seed_stage()