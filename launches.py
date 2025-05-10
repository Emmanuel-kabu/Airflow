import airflow
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta
import json
import requests 
import requests.exceptions as exceptions
import pathlib
from pathlib import Path




#instantiating the DAG
dag = DAG(
    dag_id = "download_rocket_launches",
    start_date  = datetime.now() - timedelta(days=14),
    schedule_interval = None,
)

# functon to download the the launches data
download_launches = BashOperator(
    task_id = "download_launches",
    bash_command="curl -o /tmp/launches.json -L 'https://ll.thespacedevs.com/2.0.0/launch/upcoming'",
    dag = dag
)

def _get_pictures():
    image_dir = Path("/tmp/images")
    image_dir.mkdir(parents=True, exist_ok=True)

    with open("/tmp/launches.json") as f:
        launches = json.load(f)
        image_urls = [launch["image"] for launch in launches["results"]]

        for image_url in image_urls:
            try:
                response = requests.get(image_url)
                image_filename = image_url.split("/")[-1]
                target_file = image_dir / image_filename

                with open(target_file, "wb") as f:
                    f.write(response.content)
                    print(f"Downloaded {image_url} to {target_file}")

            except exceptions.MissingSchema:
                print(f"{image_url} appears to be invalid")
            except exceptions.ConnectionError:
                print(f"Could not connect to {image_url}") 

get_pictures = PythonOperator(
    task_id = "get_picture",
    python_callable = _get_pictures,
    dag = dag
)            


notify = BashOperator(
    task_id = "notify",
    bash_command='echo "There are now $(ls /tmp/images/ | wc -l) images."',
    dag = dag
)

#Setting other of execution
download_launches >> get_pictures >> notify