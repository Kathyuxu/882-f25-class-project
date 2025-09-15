import functions_framework
from google.cloud import storage
import requests
import json
import datetime
import uuid

# settings
project_id = 'btibert-ba882-fall25'
secret_id = 'MotherDuck'   #<---------- this is the name of the secret you created
version_id = 'latest'
bucket_name = 'btibert-ba882-fall25-nfl'  #<------ we created this bucket at the end of the Cloud Storage section

####################################################### helpers

def upload_to_gcs(bucket_name, path, run_id, data):
    """Uploads data to a Google Cloud Storage bucket."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob_name = f"{path}/{run_id}/data.json"
    blob = bucket.blob(blob_name)

    # Upload the data (here it's a serialized string)
    blob.upload_from_string(data)
    print(f"File {blob_name} uploaded to {bucket_name}.")

    return {'bucket_name':bucket_name, 'blob_name': blob_name}

####################################################### core task

@functions_framework.http
def task(request):
    # grab the date from query string (?date=YYYYMMDD)
    yyyymmdd = request.args.get("date")
    if not yyyymmdd:
        yyyymmdd = (datetime.datetime.utcnow() - datetime.timedelta(days=1)).strftime("%Y%m%d")
    print(f"date for the job: {yyyymmdd}")

    # grab the run id
    run_id = request.args.get("run_id")
    if not run_id:
        run_id = uuid.uuid4().hex[:12]
    print(f"run_id: {run_id}")
    
    # get the data
    url = f"https://btibert-bu--ba882-class-project-202509-api.modal.run/day?date={yyyymmdd}"
    print(f"url: {url}")
    resp = requests.get(url)
    resp.raise_for_status()
    print(f"requested url - status {resp.status_code}")
    j = resp.json()

    # write the data to GCS
    j_string = json.dumps(j)
    season = j['leagues'][0]['season']['year']
    week = j['events'][0]['week']['number']
    _path = f"raw/scoreboard/season={season}/week={week}"
    gcs_path = upload_to_gcs(bucket_name, path=_path, run_id=run_id, data=j_string)

    # return the metadata
    return {
        "num_entries": len(j.get('events', [])), 
        "run_id": run_id, 
        "bucket_name":gcs_path.get('bucket_name'),
        "blob_name": gcs_path.get('blob_name')
    }, 200