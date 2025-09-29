import requests

def invoke_function(url, params={}) ->dict:
    """
    Invoke our cloud function url and optionally pass data for the function to use
    """
    resp = requests.get(url, params=params)
    resp.raise_for_status()
    return resp.json()

url = "https://us-central1-btibert-ba882-fall25.cloudfunctions.net/raw-parse-game"
invoke_function(url, params={'game_id':401772936})