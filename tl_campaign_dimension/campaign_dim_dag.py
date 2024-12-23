from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine
import pandas as pd
from dotenv import load_dotenv
import os
import sys
import traceback
from datetime import datetime

# Load environment variables from .env file
load_dotenv()

# Database connection parameters
db_host = os.getenv('DB_HOST')
db_port = os.getenv('DB_PORT')
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')
database_name = 'shopzada_datawarehouse'

# Define the DAG
dag = DAG(
    'campaign_data_pipeline',
    description='DAG to load campaign data into PostgreSQL',
    schedule_interval=None,
    start_date=datetime(2024, 12, 1),
    catchup=False,
)

# Debugging function to print environment and connection details
def print_debug_info():
    print("Debug Information:")
    print(f"DB_HOST: {db_host}")
    print(f"DB_PORT: {db_port}")
    print(f"DB_USER: {db_user}")
    print(f"DB_PASSWORD: {'*' * len(db_password) if db_password else 'Not Set'}")

    # Check current working directory and list files
    print("\nCurrent Working Directory:", os.getcwd())
    print("\nFiles in current directory:")
    try:
        print(os.listdir('.'))
    except Exception as e:
        print(f"Error listing directory: {e}")

def check_and_create_database(database_name, **kwargs):
    print(f"Received database_name: {database_name}")
    print(kwargs)

    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()

    # Enable autocommit mode to avoid running inside a transaction block
    conn.set_session(autocommit=True)

    try:
        # Check if the database exists
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s;
            """, (database_name,))
            exists = cursor.fetchone()

            if exists:
                print(f"Database {database_name} already exists. Skipping creation.")
            else:
                # Create the database if it doesn't exist
                cursor.execute(f"CREATE DATABASE {database_name};")
                print(f"Database {database_name} created successfully")

    except Exception as e:
        raise AirflowException(f"Error during database creation: {e}")
    finally:
        # Disable autocommit if needed (to restore default behavior)
        conn.set_session(autocommit=False)

# Task 2: Create the Table
def create_table():
    try:
        # Validate environment variables again
        if not all([db_host, db_port, db_user, db_password]):
            raise AirflowException("Missing database connection environment variables")

        # Create connection string specifically for the new database
        db_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{database_name}"
        engine = create_engine(db_url)

        # Create the table using SQLAlchemy
        with engine.connect() as connection:
            connection.execute("""
            CREATE TABLE IF NOT EXISTS campaign_dimension (
                campaign_id VARCHAR(7) PRIMARY KEY,
                campaign_name VARCHAR(100) NOT NULL,
                campaign_description TEXT NOT NULL,
                campaign_discount DECIMAL(5, 2) NOT NULL
            )
            """)
        print("Table creation completed successfully")

    except Exception as e:
        print("Full Error Traceback:")
        traceback.print_exc()
        raise AirflowException(f"Error during table creation: {e}")

# Task 3: Process and Load CSV Data
def process_and_load_csv():
    try:
        # Validate environment variables
        if not all([db_host, db_port, db_user, db_password]):
            raise AirflowException("Missing database connection environment variables")

        # Corrected file path with forward slashes
        cleaned_campaign = "/mnt/c/Users/giogen/Documents/ust 3csf/1st sem/WAREHOUSING/shopzada/et_marketing_department/Cleaned Data/cleaned_campaign_data.csv"

        print(f"Attempting to read CSV from: {cleaned_campaign}")

        # Check if the file exists before attempting to read
        if not os.path.exists(cleaned_campaign):
            print(f"Error: The file {cleaned_campaign} does not exist!")
            raise AirflowException(f"File {cleaned_campaign} not found.")
        else:
            print(f"File found: {cleaned_campaign}")

        # Read the cleaned campaign data from CSV
        campaign_data_df = pd.read_csv(cleaned_campaign)

        # Rename columns
        campaign_data_df.columns = ['campaign_' + col if (col == 'discount') else col for col in campaign_data_df.columns]

        # Define the output file path for backup (you can modify this path as needed)
        merged_file_path = "mnt/c/Users/giogen/Documents/ust 3csf/1st sem/WAREHOUSING/shopzada/tl_campaign_dimension/Merged Data/campaign_dim.csv"

        if os.name == 'nt':  # For Windows
            merged_file_path = "C:/Users/giogen/Documents/ust 3csf/1st sem/WAREHOUSING/shopzada/tl_campaign_dimension/Merged Data/merged_campaign_data.csv"
        else:  # For WSL or other Unix-like systems
            merged_file_path = "/mnt/c/Users/giogen/Documents/ust 3csf/1st sem/WAREHOUSING/shopzada/tl_campaign_dimension/Merged Data/merged_campaign_data.csv"

        # Ensure the directory exists
        directory = os.path.dirname(merged_file_path)
        if not os.path.exists(directory):
            os.makedirs(directory)
            print(f"Created directory: {directory}")

        try:
            # Assuming you have the campaign_data_df loaded, save the CSV
            campaign_data_df.to_csv(merged_file_path, index=False)
            print(f"CSV file saved successfully at {merged_file_path}")
        except Exception as e:
            raise AirflowException(f"Error during CSV processing and loading: {e}")

        # Store to CSV for backup
        campaign_data_df.to_csv(merged_file_path, index=False)
        print(f"Backup stored at: {merged_file_path}")

        # Create connection string specifically for the new database
        db_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{database_name}"
        engine = create_engine(db_url)

        # Load data into database
        campaign_data_df.to_sql('campaign_dimension', engine, if_exists='replace', index=False)

        print("CSV processing and database loading completed successfully")

    except Exception as e:
        print("Full Error Traceback:")
        traceback.print_exc()
        raise AirflowException(f"Error during CSV processing and loading: {e}")

# Define Airflow tasks
task_print_debug = PythonOperator(
    task_id='print_debug_info',
    python_callable=print_debug_info,
    dag=dag,
)

task_check_create_db = PythonOperator(
    task_id='check_and_create_database',
    python_callable=check_and_create_database,
    op_kwargs={'database_name': 'shopzada_datawarehouse'},  # Pass as a dictionary
    provide_context=True,
    dag=dag,
)

task_create_table = PythonOperator(
    task_id='create_table',
    python_callable=create_table,
    dag=dag,
)

task_process_load_csv = PythonOperator(
    task_id='process_and_load_csv',
    python_callable=process_and_load_csv,
    dag=dag,
)

# Define task dependencies
task_print_debug >> task_check_create_db >> task_create_table >> task_process_load_csv


