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

# Database connection parameters
db_url = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}"
database_name = 'shopzada_datawarehouse'

# Define the DAG
dag = DAG(
    'staff_data_pipeline',
    description='DAG to load staff data into PostgreSQL',
    schedule_interval=None,  # Set to your desired schedule
    start_date=datetime(2024, 12, 1),
    catchup=False,
)

# Task 1: Check if Database Exists
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
            CREATE TABLE staff_dimension (
	            staff_id VARCHAR(9) PRIMARY KEY,
	            staff_name VARCHAR(100) NOT NULL,
	            staff_job_level VARCHAR(20) NOT NULL,
	            staff_street VARCHAR(100) NOT NULL,
	            staff_state VARCHAR(50) NOT NULL,
	            staff_city VARCHAR(50) NOT NULL,
	            staff_country VARCHAR(100) NOT NULL,
	            staff_contact_number VARCHAR(15) NOT NULL,
	            staff_creation_date TIMESTAMP NOT NULL
            )
            """)
        print("Table creation completed successfully")

    except Exception as e:
        print("Full Error Traceback:")
        traceback.print_exc()
        raise AirflowException(f"Error during table creation: {e}")


# Task 3: Process and Load CSV Data
def process_and_load_html():
    try:
        # Validate environment variables
        if not all([db_host, db_port, db_user, db_password]):
            raise AirflowException("Missing database connection environment variables")

        # Corrected file path with forward slashes
        clean_staff_data = "/mnt/c/Users/giogen/Documents/ust 3csf/1st sem/WAREHOUSING/shopzada/et_enterprise_department/Cleaned Data/cleaned_staff_data.html"

        print(f"Attempting to read HTML from: {clean_staff_data}")

        # Check if the file exists before attempting to read
        if not os.path.exists(clean_staff_data):
            print(f"Error: The file {clean_staff_data} does not exist!")
            raise AirflowException(f"File {clean_staff_data} not found.")
        else:
            print(f"File found: {clean_staff_data}")

        # Read the cleaned campaign data from CSV
        staff_data_df = pd.read_html(clean_staff_data)[0]

        # Rename columns
        staff_data_df.columns = ['staff_' + col if (col != 'staff_id') else col for col in staff_data_df.columns]

        # Define the output file path for backup (you can modify this path as needed)
        merged_file_path = "mnt/c/Users/giogen/Documents/ust 3csf/1st sem/WAREHOUSING/shopzada/tl_staff_dimension/Merged Data/staff_dim.csv"

        if os.name == 'nt':  # For Windows
            merged_file_path = "C:/Users/giogen/Documents/ust 3csf/1st sem/WAREHOUSING/shopzada/tl_staff_dimension/Merged Data/merged_staff_data.csv"
        else:  # For WSL or other Unix-like systems
            merged_file_path = "/mnt/c/Users/giogen/Documents/ust 3csf/1st sem/WAREHOUSING/shopzada/tl_staff_dimension/Merged Data/merged_staff_data.csv"

        # Ensure the directory exists
        directory = os.path.dirname(merged_file_path)
        if not os.path.exists(directory):
            os.makedirs(directory)
            print(f"Created directory: {directory}")

        try:
            # Assuming you have the campaign_data_df loaded, save the CSV
            staff_data_df.to_csv(merged_file_path, index=False)
            print(f"CSV file saved successfully at {merged_file_path}")
        except Exception as e:
            raise AirflowException(f"Error during CSV processing and loading: {e}")


        # Create connection string specifically for the new database
        db_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{database_name}"
        engine = create_engine(db_url)

        # Load data into database
        staff_data_df.to_sql('staff_dimension', engine, if_exists='replace', index=False)

        print("CSV processing and database loading completed successfully")

    except Exception as e:
        print("Full Error Traceback:")
        traceback.print_exc()
        raise AirflowException(f"Error during CSV processing and loading: {e}")

# Define Airflow tasks
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

task_process_load_html = PythonOperator(
    task_id='process_and_load_html',
    python_callable=process_and_load_html,
    dag=dag,
)

# Define task dependencies
task_check_create_db >> task_create_table >> task_process_load_html