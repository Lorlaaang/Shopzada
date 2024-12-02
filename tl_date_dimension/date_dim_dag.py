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
 CREATE TABLE IF NOT EXISTS date_dimension (
        date_id SERIAL PRIMARY KEY,
        product_sale_id VARCHAR(10),
        date_full DATE NOT NULL,
        date_year INT NOT NULL,
        date_quarter INT NOT NULL,
        date_month INT NOT NULL,
        date_month_name VARCHAR(20) NOT NULL,
        date_day INT NOT NULL,
        date_day_name VARCHAR(20) NOT NULL,
        date_is_weekend BOOLEAN NOT NULL,
        date_is_holiday BOOLEAN NOT NULL,
        date_holiday_name VARCHAR(50),
        FOREIGN KEY (product_sale_id) REFERENCES product_sale_fact(product_sale_id)
            )
            """)
        print("Table creation completed successfully")

    except Exception as e:
        print("Full Error Traceback:")
        traceback.print_exc()
        raise AirflowException(f"Error during table creation: {e}")

# Task 3: Process and Load Date Data
def process_and_load_date_data():
    # Import the date algorithm from load_date_dim.py
    from load_date_dim import date_dimension_df

    # Store to CSV for backup
    dir = os.path.join(os.getcwd(), "tl_date_dimension", "Merged Data")
    os.makedirs(dir, exist_ok=True)
    merged_file_path = f"{dir}/date_dim.csv"
    date_dimension_df.to_csv(merged_file_path, index=False)

    # Load data into database
    engine = create_engine(db_url)
    date_dimension_df.to_sql('date_dimension', engine, if_exists='append', index=False)

# Define Airflow tasks
task_check_create_db = PythonOperator(
    task_id='check_and_create_database',
    python_callable=check_and_create_database,
    dag=dag,
)

task_create_table = PythonOperator(
    task_id='create_table',
    python_callable=create_table,
    dag=dag,
)

task_process_load_date_data = PythonOperator(
    task_id='process_and_load_date_data',
    python_callable=process_and_load_date_data,
    dag=dag,
)

# Define task dependencies
task_check_create_db >> task_create_table >> task_process_load_date_data