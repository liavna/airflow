from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import teradatasql
import pandas as pd
import json
import subprocess
import os

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define access control
define_access_control = {
    'All': {'can_read', 'can_edit', 'can_delete'}
}

# Define DAG
dag = DAG(
    'teradata_to_maprdb_crypto_prices',
    default_args=default_args,
    description='A DAG to transfer data from Teradata (CryptoPrices) to MapR-DB JSON every 5 minutes (without Spark)',
    schedule_interval='*/5 * * * *',
    catchup=False,
    access_control=define_access_control,
)


def transfer_crypto_prices():
    # Teradata connection details
    teradata_host = "10.150.104.171"
    teradata_user = "dbc"
    teradata_password = "dbc"
    teradata_db = "CryptoDB"
    teradata_table = "CryptoPrices"

    # MapR-DB details
    mapr_db_name = "CryptoDB"
    mapr_table = "CryptoPrices"
    mapr_table_path = f"/mapr/{mapr_db_name}/{mapr_table}"
    unique_date_column = "last_updated"  # Ensuring last_updated is used

    # 1. Connect to Teradata and fetch data
    print("Connecting to Teradata...")
    conn = teradatasql.connect(host=teradata_host, user=teradata_user, password=teradata_password)
    query = f"SELECT * FROM {teradata_db}.{teradata_table};"
    df = pd.read_sql(query, conn)
    conn.close()
    print(f"Fetched {len(df)} rows from Teradata.")

    # 2. Check if MapR-DB table exists
    def check_table_exists():
        result = subprocess.run(["maprcli", "table", "info", "-path", mapr_table_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        return "No such table" not in result.stderr

    # 3. Create table if it doesn't exist
    if not check_table_exists():
        print(f"Table {mapr_table} does not exist. Creating...")
        create_table_cmd = ["maprcli", "table", "create", "-path", mapr_table_path, "-tabletype", "json"]
        subprocess.run(create_table_cmd, check=True)
    else:
        print(f"Table {mapr_table} already exists.")

    # 4. Fetch existing data from MapR-DB
    print("Fetching existing data from MapR-DB...")
    fetch_existing_cmd = f"mapr dbshell -c 'find {mapr_table_path} --f json'"
    result = subprocess.run(fetch_existing_cmd, shell=True, capture_output=True, text=True)
    existing_data = [json.loads(line) for line in result.stdout.splitlines() if line.strip()]
    existing_df = pd.DataFrame(existing_data)

    # 5. Identify new rows using last_updated column
    if unique_date_column in df.columns and not existing_df.empty:
        df = df[~df[unique_date_column].isin(existing_df[unique_date_column])]

    # 6. Insert new rows into MapR-DB
    if not df.empty:
        print(f"Inserting {len(df)} new rows into MapR-DB...")
        for _, row in df.iterrows():
            json_doc = json.dumps(row.to_dict())
            insert_cmd = f"echo 'insert into {mapr_table_path} values {json_doc}' | mapr dbshell"
            os.system(insert_cmd)
    else:
        print("No new data to insert.")

    print("Data transfer complete.")


# Task to transfer data
transfer_task = PythonOperator(
    task_id="transfer_crypto_prices",
    python_callable=transfer_crypto_prices,
    dag=dag,
)

transfer_task
