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
    'user_data_pipeline',
    description='DAG to load user data into PostgreSQL',
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
            CREATE TABLE IF NOT EXISTS users_dimension (
	            user_id VARCHAR(10) PRIMARY KEY,
	            user_name VARCHAR(100) NOT NULL,
	            user_job_title VARCHAR(50) NOT NULL,
	            user_job_level VARCHAR(50),
	            user_credit_card_number VARCHAR(64) NOT NULL, -- hashed
	            user_issuing_bank VARCHAR(50) NOT NULL,
	            user_street VARCHAR(100) NOT NULL,
	            user_state VARCHAR(50) NOT NULL,
	            user_city VARCHAR(50) NOT NULL,
	            user_country VARCHAR(100) NOT NULL,
	            user_birthdate TIMESTAMP NOT NULL,
	            user_gender VARCHAR(10) NOT NULL,
	            user_device_address VARCHAR(17) NOT NULL,
	            user_type VARCHAR(20) NOT NULL,
	            user_creation_date TIMESTAMP NOT NULL
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
        clean_user_data = '/mnt/c/Users/giogen/Documents/ust 3csf/1st sem/WAREHOUSING/shopzada/et_customer_management_department/Cleaned Data/cleaned_user_data.csv'
        clean_user_credit_path = '/mnt/c/Users/giogen/Documents/ust 3csf/1st sem/WAREHOUSING/shopzada/et_customer_management_department/Cleaned Data/cleaned_user_credit_card.csv'
        clean_user_job = '/mnt/c/Users/giogen/Documents/ust 3csf/1st sem/WAREHOUSING/shopzada/et_customer_management_department/Cleaned Data/cleaned_user_job.csv'
        # cleaned_product_list = "/mnt/c/Users/giogen/Documents/ust 3csf/1st sem/WAREHOUSING/shopzada/et_business_department/Cleaned Data/cleaned_product_list.csv"

        print(f"Attempting to read CSV from: {clean_user_data}")

        # Check if the file exists before attempting to read
        if not os.path.exists(clean_user_data):
            print(f"Error: The file {clean_user_data} does not exist!")
            raise AirflowException(f"File {clean_user_data} not found.")
        else:
            print(f"File found: {clean_user_data}")

        print(f"Attempting to read CSV from: {clean_user_credit_path}")

        # Check if the file exists before attempting to read
        if not os.path.exists(clean_user_credit_path):
            print(f"Error: The file {clean_user_credit_path} does not exist!")
            raise AirflowException(f"File {clean_user_credit_path} not found.")
        else:
            print(f"File found: {clean_user_credit_path}")
        
        # Check if the file exists before attempting to read
        if not os.path.exists(clean_user_job):
            print(f"Error: The file {clean_user_job} does not exist!")
            raise AirflowException(f"File {clean_user_job} not found.")
        else:
            print(f"File found: {clean_user_job}")

        print(f"Attempting to read CSV from: {clean_user_job}")

        # Read the cleaned campaign data from CSV
        user_data_df = pd.read_csv(clean_user_data)
        user_credit_card_df = pd.read_csv(clean_user_credit_path)
        user_job_df = pd.read_csv(clean_user_job)

        user_data_with_credit_df = pd.merge(user_data_df, user_credit_card_df, on=['user_id', 'name'], how='outer')
        user_data_with_credit_with_job_df = pd.merge(user_data_with_credit_df, user_job_df, on=['user_id', 'name'], how='outer')

        # Rename columns
        user_data_with_credit_with_job_df.columns = ['user_' + col if (col != 'user_id' and col != 'user_type')  else col for col in user_data_with_credit_with_job_df.columns]

        # Define the output file path for backup (you can modify this path as needed)
        merged_file_path = "mnt/c/Users/giogen/Documents/ust 3csf/1st sem/WAREHOUSING/shopzada/tl_user_dimension/Merged Data/user_dim.csv"

        if os.name == 'nt':  # For Windows
            merged_file_path = "C:/Users/giogen/Documents/ust 3csf/1st sem/WAREHOUSING/shopzada/tl_user_dimension/Merged Data/user_dim.csv"
        else:  # For WSL or other Unix-like systems
            merged_file_path = "/mnt/c/Users/giogen/Documents/ust 3csf/1st sem/WAREHOUSING/shopzada/tl_user_dimension/Merged Data/user_dim.csv"

        # Ensure the directory exists
        directory = os.path.dirname(merged_file_path)
        if not os.path.exists(directory):
            os.makedirs(directory)
            print(f"Created directory: {directory}")

        try:
            # Assuming you have the campaign_data_df loaded, save the CSV
            user_data_with_credit_with_job_df.to_csv(merged_file_path, index=False)
            print(f"CSV file saved successfully at {merged_file_path}")
        except Exception as e:
            raise AirflowException(f"Error during CSV processing and loading: {e}")

        # Store to CSV for backup
        user_data_with_credit_with_job_df.to_csv(merged_file_path, index=False)
        print(f"Backup stored at: {merged_file_path}")

        # Create connection string specifically for the new database
        db_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{database_name}"
        engine = create_engine(db_url)

        # Load data into database
        user_data_with_credit_with_job_df.to_sql('user_dimension', engine, if_exists='replace', index=False)

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

task_process_load_csv = PythonOperator(
    task_id='process_and_load_csv',
    python_callable=process_and_load_csv,
    dag=dag,
)

# Define task dependencies
task_check_create_db >> task_create_table >> task_process_load_csv