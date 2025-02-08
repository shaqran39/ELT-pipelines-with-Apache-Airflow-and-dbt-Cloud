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
#   DAG Operator Setup
#
#########################################################

# Task 1: Load the RAW_LISTING table
load_raw_listing_task = PythonOperator(
    task_id="load_raw_listing_task",
    python_callable=load_csv_to_postgres_func,
    provide_context=True,
    op_kwargs={
        'file_name': '05_2020.csv',
        'table_name': 'bronze.raw_listing'
    },
    dag=dag
)

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

# Define the task dependencies
[load_raw_listing_task, load_raw_lgacode_task, load_raw_lgasuburb_task, load_raw_censusg1_task] >> load_raw_censusg2_task
