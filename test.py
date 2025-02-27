from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
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


def ensure_database_exists():
    """
    Check if CryptoDB exists on MySQL server at 10.150.104.187 before attempting to create it.
    If it doesn't exist, attempt to create it.
    """
    try:
        mysql_conn = mysql.connector.connect(
            host='10.150.104.187', user='root', password='HPEpassword!'
        )
        cursor = mysql_conn.cursor()

        print("Checking if database 'CryptoDB' exists on 10.150.104.187...")
        cursor.execute("SHOW DATABASES LIKE 'CryptoDB'")
        db_exists = cursor.fetchone()
        if db_exists:
            print("Database 'CryptoDB' already exists on 10.150.104.187.")
        else:
            print("Database 'CryptoDB' does not exist. Attempting to create it...")
            try:
                cursor.execute("CREATE DATABASE CryptoDB")
                mysql_conn.commit()
                print("Database 'CryptoDB' created successfully on 10.150.104.187.")
            except mysql.connector.Error as err:
                print(f"Failed creating database: {err}")
                print("Ensure that the user 'root'@'10.150.104.183' has the required CREATE DATABASE privileges.")
                raise

    except mysql.connector.Error as conn_err:
        print(f"MySQL connection failed: {conn_err}")
        raise

    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'mysql_conn' in locals() and mysql_conn.is_connected():
            mysql_conn.close()


def load_data_to_mysql(data_dict):
    ensure_database_exists()

    mysql_conn = mysql.connector.connect(
        host='10.150.104.187', user='root', password='HPEpassword!', database='CryptoDB'
    )
    cursor = mysql_conn.cursor()

    try:
        # Create tables and insert data
        for table_name, df in data_dict.items():
            cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
            table_exists = cursor.fetchone()

            if not table_exists:
                columns = ", ".join([f"`{col}` TEXT" for col in df.columns])
                try:
                    cursor.execute(f"CREATE TABLE {table_name} ({columns})")
                    mysql_conn.commit()
                    print(f"Table '{table_name}' created successfully.")
                except mysql.connector.Error as err:
                    print(f"Failed creating table {table_name}: {err}")
                    raise
            else:
                print(f"Table '{table_name}' already exists.")

            for _, row in df.iterrows():
                placeholders = ", ".join(["%s"] * len(row))
                try:
                    cursor.execute(
                        f"INSERT INTO {table_name} VALUES ({placeholders})",
                        tuple(row)
                    )
                except mysql.connector.Error as err:
                    print(f"Failed inserting data into {table_name}: {err}")
                    raise
        mysql_conn.commit()

    finally:
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
    description='test',
    schedule_interval='@daily',
    catchup=False,
    access_control=define_access_control
) as dag:

    sync_task = PythonOperator(
        task_id='test',
        python_callable=sync_teradata_to_mysql
    )

    sync_task
