from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
import json

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 2),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def fetch_air_quality_data():
    url = "http://api.waqi.info/feed/shanghai/?token=c729941b2543bf33457af3f9a56069bafd457218"
    response = requests.get(url)
    data = response.json()
    
    if data["status"] == "ok":
        # Process data
        aqi = data["data"]["aqi"]
        dominant_pollutant = data["data"]["dominentpol"]
        iaqi = data["data"]["iaqi"]
        forecast = data["data"]["forecast"]["daily"]
        
        # Print or log data for verification
        print(f"AQI: {aqi}")
        print(f"Dominant Pollutant: {dominant_pollutant}")
        print(f"IAQI: {json.dumps(iaqi, indent=4)}")
        print(f"Forecast: {json.dumps(forecast, indent=4)}")
    else:
        raise ValueError("Failed to fetch data")

with DAG(
    'air_quality_dag',
    default_args=default_args,
    description='A DAG to fetch and process air quality data for Shanghai',
    schedule_interval=timedelta(days=1),
    access_control={
		'All': {
			'can_read',
			'can_edit',
			'can_delete'
		}
	}
) as dag:

    fetch_data_task = PythonOperator(
        task_id='fetch_air_quality_data',
        python_callable=fetch_air_quality_data,
    )

    fetch_data_task
