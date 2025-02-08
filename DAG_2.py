import os
import logging
import pandas as pd
import shutil
from datetime import datetime, timedelta
from psycopg2.extras import execute_values
from airflow import AirflowException
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable

#########################################################
#
#   DAG Settings
#
#########################################################

dag_default_args = {
    'owner': 'BDE_LAB_6',
    'start_date': datetime.now() - timedelta(days=2+4),
    'email': [],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'wait_for_downstream': False,
}

dag = DAG(
    dag_id='load_dag',
    default_args=dag_default_args,
    schedule_interval=None,
    catchup=True,
    max_active_runs=1,
    concurrency=5
)

#########################################################
#
#   Load Environment Variables
#
#########################################################
AIRFLOW_DATA = "/home/airflow/gcs/data"
RAW = AIRFLOW_DATA + "/raw/"

#########################################################
#
#   Custom Logics for Operators
#
#########################################################
def clean_csv_data(df):
    """
    Function to clean CSV data by removing excess commas (empty columns),
    handle missing data, and convert date columns from DD/MM/YYYY to YYYY-MM-DD.
    """
    # Remove columns that contain only NaN (likely caused by extra commas)
    df_cleaned = df.dropna(axis=1, how='all')

    # Debugging: print out the scraped_date column before conversion
    # logging.info(f"Before conversion: {df_cleaned['scraped_date'].head(5)}")

    # Handle missing data and specific boolean columns
    for column in df_cleaned.columns:
        # If the column is numeric, fill missing values with 0
        if pd.api.types.is_numeric_dtype(df_cleaned[column]):
            df_cleaned[column] = df_cleaned[column].fillna(0)
        
        # If the column is a string (object type), fill missing values with 'Unknown'
        elif pd.api.types.is_string_dtype(df_cleaned[column]):
            df_cleaned[column] = df_cleaned[column].fillna('Unknown')

        # Handle boolean columns (like host_is_superhost and has_availability)
        if column in ['host_is_superhost', 'has_availability']:
            # Replace 't' and 'f' with boolean True and False, and handle NaN as None
            df_cleaned[column] = df_cleaned[column].replace({'t': True, 'f': False}).fillna(None)
            df_cleaned[column] = df_cleaned[column].astype('bool', errors='ignore').replace({pd.NA: None})

    # Check for date columns and convert them to YYYY-MM-DD format
    
    # for column in df_cleaned.columns:
        # if 'since' in column.lower():
            # Attempt to parse with a broader range of formats
    #         try:
    #             df_cleaned[column] = pd.to_datetime(df_cleaned[column], errors='coerce').dt.strftime('%Y-%m-%d')
    #         except Exception as e:
    #             logging.error(f"Error converting date column: {column}. Error: {e}")

    #         # Replace NaT (invalid dates) with None (which will be interpreted as NULL in SQL)
    #         df_cleaned[column] = df_cleaned[column].replace({pd.NaT: None})

    # # Debugging: print out the scraped_date column after conversion
    # logging.info(f"After conversion: {df_cleaned['scraped_date'].head(5)}")

    return df_cleaned




def load_csv_to_postgres_func(file_name, table_name, **kwargs):
    # Setup Postgres connection
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()

    # Check if the file exists
    file_path = RAW + file_name
    if not os.path.exists(file_path):
        logging.info(f"No {file_name} file found.")
        return None

    # Generate dataframe by reading the CSV file
    df = pd.read_csv(file_path)

    # Clean the CSV data by removing extra columns (caused by excess commas) and handling missing data
    df_cleaned = clean_csv_data(df)

    if len(df_cleaned) > 0:
        values = df_cleaned.to_dict('split')['data']
        logging.info(f"Prepared data for insertion into {table_name}: {values[:5]}...")  # Log the first 5 rows

        insert_sql = f"""
                    INSERT INTO {table_name}
                    VALUES %s
                    """
        result = execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(df_cleaned))
        conn_ps.commit()

        # Move the processed file to the archive folder
        archive_folder = os.path.join(RAW, 'archive')
        if not os.path.exists(archive_folder):
            os.makedirs(archive_folder)
        shutil.move(file_path, os.path.join(archive_folder, file_name))

    return None

#########################################################
#
#   Function to trigger dbt Cloud Job
#
#########################################################

def trigger_dbt_cloud_job(**kwargs):
    # Get the dbt Cloud URL, account ID, and job ID from Airflow Variables
    dbt_cloud_url = Variable.get("DBT_CLOUD_URL")
    dbt_cloud_account_id = Variable.get("DBT_CLOUD_ACCOUNT_ID")
    dbt_cloud_job_id = Variable.get("DBT_CLOUD_JOB_ID")

    # Define the URL for the dbt Cloud job API dynamically using URL, account ID, and job ID
    url = f"https://{dbt_cloud_url}/api/v2/accounts/{dbt_cloud_account_id}/jobs/{dbt_cloud_job_id}/run/"

    # Get the dbt Cloud API token from Airflow Variables
    dbt_cloud_token = Variable.get("DBT_CLOUD_API_TOKEN")

    # Define the headers and body for the request
    headers = {
        'Authorization': f'Token {dbt_cloud_token}',
        'Content-Type': 'application/json'
    }
    data = {
        "cause": "Triggered via API"
    }

    # Make the POST request to trigger the dbt Cloud job
    response = requests.post(url, headers=headers, json=data)

    # Check if the response is successful
    if response.status_code == 200:
        logging.info("Successfully triggered dbt Cloud job.")
        return response.json()
    else:
        logging.error(f"Failed to trigger dbt Cloud job: {response.status_code}, {response.text}")
        raise AirflowException("Failed to trigger dbt Cloud job.")

#########################################################
#
#   DAG Operator Setup
#
#########################################################


# Task 2: Load the RAW_LGACODE table
load_raw_lgacode_task = PythonOperator(
    task_id="load_raw_lgacode_task",
    python_callable=load_csv_to_postgres_func,
    provide_context=True,
    op_kwargs={
        'file_name': 'NSW_LGA_CODE.csv',
        'table_name': 'bronze.raw_lgacode'
    },
    dag=dag
)

# Task 3: Load the RAW_LGASUBURB table
load_raw_lgasuburb_task = PythonOperator(
    task_id="load_raw_lgasuburb_task",
    python_callable=load_csv_to_postgres_func,
    provide_context=True,
    op_kwargs={
        'file_name': 'NSW_LGA_SUBURB.csv',
        'table_name': 'bronze.raw_lgasuburb'
    },
    dag=dag
)

# Task 4: Load the RAW_CENSUSG1 table
load_raw_censusg1_task = PythonOperator(
    task_id="load_raw_censusg1_task",
    python_callable=load_csv_to_postgres_func,
    provide_context=True,
    op_kwargs={
        'file_name': '2016Census_G01_NSW_LGA.csv',
        'table_name': 'bronze.raw_censusg1'
    },
    dag=dag
)

# Task 5: Load the RAW_CENSUSG2 table
load_raw_censusg2_task = PythonOperator(
    task_id="load_raw_censusg2_task",
    python_callable=load_csv_to_postgres_func,
    provide_context=True,
    op_kwargs={
        'file_name': '2016Census_G02_NSW_LGA.csv',
        'table_name': 'bronze.raw_censusg2'
    },
    dag=dag
)

# Sequentially load monthly Airbnb datasets
months = ["05_2020", "06_2020", "07_2020", "08_2020"]
load_tasks = []

for month in months:
    task = PythonOperator(
        task_id=f"load_data_{month}",
        python_callable=load_csv_to_postgres_func,
        provide_context=True,
        op_kwargs={
            'file_name': f'{month}.csv',
            'table_name': 'bronze.raw_listing'
        },
        dag=dag
    )
    load_tasks.append(task)
# Task to trigger dbt Cloud job after data load
trigger_dbt_task = PythonOperator(
    task_id='trigger_dbt_cloud_job',
    python_callable=trigger_dbt_cloud_job,
    provide_context=True,
    dag=dag
)



# Set dependencies to ensure correct order of operations


load_raw_listing_task >> load_raw_lgacode_task >> load_raw_lgasuburb_task >> load_raw_censusg1_task >> load_raw_censusg2_task

# Link the census loading tasks to monthly load tasks
load_raw_censusg2_task >> load_tasks[0]

# Chain all monthly loading tasks sequentially
for i in range(1, len(load_tasks)):
    load_tasks[i - 1] >> load_tasks[i]

# Ensure dbt runs after all data loading tasks
load_tasks[-1] >> trigger_dbt_task