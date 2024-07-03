from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
import json
import pyodbc
from sqlalchemy import create_engine, Table, Column, Integer, Float, String, MetaData, DateTime
from sqlalchemy.orm import sessionmaker
from dateutil import parser

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 2),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

DATABASE_URI = 'mssql+pyodbc://username:password@server/database?driver=ODBC+Driver+17+for+SQL+Server'

def check_and_save_data():
    url = "http://api.waqi.info/feed/shanghai/?token=c729941b2543bf33457af3f9a56069bafd457218"
    response = requests.get(url)
    data = response.json()
    
    if data["status"] == "ok":
        # Extract necessary data
        timestamp = parser.isoparse(data["data"]["time"]["iso"]).strftime('%Y-%m-%d %H:%M:%S')
        aqi = data["data"]["aqi"]
        dominant_pollutant = data["data"]["dominentpol"]
        iaqi = json.dumps(data["data"]["iaqi"])
        forecast = json.dumps(data["data"]["forecast"]["daily"])
        
        # Connect to SQL Server
        engine = create_engine(DATABASE_URI)
        connection = engine.connect()
        Session = sessionmaker(bind=engine)
        session = Session()

        # Create table if not exists
        metadata = MetaData()
        air_quality_table = Table('air_quality', metadata,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('timestamp', DateTime, unique=True, nullable=False),
            Column('aqi', Integer),
            Column('dominant_pollutant', String(50)),
            Column('iaqi', String),
            Column('forecast', String),
        )
        metadata.create_all(engine)

        # Check if data already exists
        result = session.query(air_quality_table).filter_by(timestamp=timestamp).first()

        if not result:
            # Insert data if not exists
            ins = air_quality_table.insert().values(
                timestamp=timestamp,
                aqi=aqi,
                dominant_pollutant=dominant_pollutant,
                iaqi=iaqi,
                forecast=forecast
            )
            connection.execute(ins)
            print("Data inserted successfully")
        else:
            print("Data already exists in the database")

        session.close()
        connection.close()
    else:
        raise ValueError("Failed to fetch data")

with DAG(
    'air_quality_dag',
    default_args=default_args,
    description='A DAG to fetch, check, and save air quality data for Shanghai',
    schedule_interval=timedelta(days=1),
    access_control={
		'role_<username>': {
			'can_read',
			'can_edit',
			'can_delete'
		}
	}
) as dag:

    check_and_save_data_task = PythonOperator(
        task_id='check_and_save_data',
        python_callable=check_and_save_data,
    )

    check_and_save_data_task
