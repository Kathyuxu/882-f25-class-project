# this dag will use the Variable flow supported in Airflow
# this _should_ read the environment variable created in the interface via Environment > Airflow Variables
# this will be None on local, but a silly value when "deployed"

from datetime import datetime
from airflow.decorators import dag, task
from airflow.models import Variable

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

    fetch_brockvar()

get_env_val()
