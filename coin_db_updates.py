from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.dag_parsing import get_parsed_dags
from airflow.models import Variable
from datetime import datetime
import pandas as pd
import mysql.connector
import teradatasql


def extract_data_from_teradata():
    teradata_conn = teradatasql.connect(host='10.150.104.171', user='dbc', password='dbc')
    cursor = teradata_conn.cursor()

    cursor.execute("SELECT DatabaseName FROM DBC.Databases WHERE DatabaseName='CryptoDB'")
    db_exists = cursor.fetchone()

    if not db_exists:
        raise ValueError("Database 'CryptoDB' does not exist on Teradata server.")

    cursor.execute("SELECT TableName FROM DBC.TablesV WHERE DatabaseName='CryptoDB'")
    tables = [table[0] for table in cursor.fetchall()]

    data_dict = {}
    for table in tables:
        cursor.execute(f"SELECT * FROM CryptoDB.{table}")
        columns = [desc[0] for desc in cursor.description]
        data = cursor.fetchall()
        df = pd.DataFrame(data, columns=columns)
        data_dict[table] = df

    cursor.close()
    teradata_conn.close()

    return data_dict


def load_data_to_mysql(data_dict):
    mysql_conn = mysql.connector.connect(
        host='10.150.104.187', user='root', password='HPEpassword!'
    )
    cursor = mysql_conn.cursor()

    cursor.execute("SHOW DATABASES LIKE 'CryptoDB'")
    db_exists = cursor.fetchone()

    if not db_exists:
        cursor.execute("CREATE DATABASE CryptoDB")
        mysql_conn.commit()

    cursor.execute("USE CryptoDB")

    for table_name, df in data_dict.items():
        cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
        table_exists = cursor.fetchone()

        if not table_exists:
            columns = ", ".join([f"`{col}` TEXT" for col in df.columns])
            cursor.execute(f"CREATE TABLE {table_name} ({columns})")
            mysql_conn.commit()

        for _, row in df.iterrows():
            placeholders = ", ".join(["%s"] * len(row))
            cursor.execute(
                f"INSERT INTO {table_name} VALUES ({placeholders})",
                tuple(row)
            )
    mysql_conn.commit()
    cursor.close()
    mysql_conn.close()


def sync_teradata_to_mysql():
    data_dict = extract_data_from_teradata()
    load_data_to_mysql(data_dict)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
}

define_access_control = {
    'All': {'can_read', 'can_edit', 'can_delete'}
}

with DAG(
    'teradata_to_mysql_sync',
    default_args=default_args,
    description='Sync all tables from Teradata CryptoDB to MySQL',
    schedule_interval='@daily',
    catchup=False,
    access_control=define_access_control
) as dag:

    sync_task = PythonOperator(
        task_id='sync_teradata_mysql',
        python_callable=sync_teradata_to_mysql
    )

    sync_task
