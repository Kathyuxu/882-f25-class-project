# this dag will use the Variable flow supported in Airflow
# this _should_ read the environment variable created in the interface via Environment > Airflow Variables
# this will be None on local, but a silly value when "deployed"

from datetime import datetime
from airflow.decorators import dag, task
from airflow.models import Variable
import os

@dag(
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["demo", "env-vars"],
)
def get_env_val():

    @task
    def fetch_brockvar():
        # Pulls the Airflow Variable named "brockvar"
        brockvar_value = Variable.get("brockvar", default_var=None)
        # If you want to see it in the task logs:
        print(f"brockvar = {brockvar_value}")
        # returning it will push it to XCom
        return brockvar_value
    
    @task
    def fetch_envvar():
        # Read the environment variable named "BROCKVAR"
        # You must set BROCKVAR in your container / k8s / docker-compose, etc.
        md_tok = os.environ.get("MOTHERDUCK_TOKEN")
        if md_tok is None:
            # handle missing var as you like
            raise ValueError("Environment variable md_tok is not set")
        print(f"md_tok = {md_tok[:7]}")
        return md_tok
    

    fetch_brockvar()
    fetch_envvar()

get_env_val()
