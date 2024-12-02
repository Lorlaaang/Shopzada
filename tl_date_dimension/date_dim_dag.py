from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine
import pandas as pd
from dotenv import load_dotenv
import os
from datetime import datetime

# Load environment variables from .env file
load_dotenv()

# Database connection parameters
db_url = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}"
database_name = 'shopzada_datawarehouse'

# Define the DAG
dag = DAG(
    'date_data_pipeline',
    description='DAG to load date data into PostgreSQL',
    schedule_interval=None,  # Set to your desired schedule
    start_date=datetime(2024, 12, 1),
    catchup=False,
)

# Task 1: Check if Database Exists
def check_and_create_database(database_name: str):
    pg_hook = PostgresHook(postgres_conn_id='postgres_conn')  # Replace with your connection ID
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    # Connect to the 'postgres' database, which is used for administrative tasks
    cursor.execute(f"SELECT current_database();")
    current_db = cursor.fetchone()[0]
    
    if current_db != 'postgres':
        raise AirflowException("You need to be connected to the 'postgres' database to create a new database.")
    
    try:
        # Check if the database exists
        cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{database_name}'")
        exists = cursor.fetchone()

        if not exists:
            print(f"Database {database_name} does not exist. Creating...")
            cursor.execute(f"CREATE DATABASE {database_name}")
            conn.commit()
            print(f"Database {database_name} created successfully.")
        else:
            print(f"Database {database_name} already exists.")
    
    except Exception as e:
        print(f"Error while checking or creating database: {e}")
        raise AirflowException(f"Error during database creation: {e}")
    finally:
        cursor.close()
        conn.close()

# Task 2: Create the Table
def create_table():
    pg_hook = PostgresHook(postgres_conn_id='postgres_conn')  # Replace with your connection ID
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    # Switch to the database
    cursor.execute(f"\\c {database_name}")

    # Create the table
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS date_dimension (
        date_id SERIAL PRIMARY KEY,
        product_sale_id INT,
        date_full DATE NOT NULL,
        date_year INT NOT NULL,
        date_quarter INT NOT NULL,
        date_month INT NOT NULL,
        date_month_name VARCHAR(20) NOT NULL,
        date_day INT NOT NULL,
        date_day_name VARCHAR(20) NOT NULL,
        date_is_weekend BOOLEAN NOT NULL,
        date_is_holiday BOOLEAN NOT NULL,
        date_holiday_name VARCHAR(50)
    );
    """
    cursor.execute(create_table_sql)
    conn.commit()

# Task 3: Process and Load CSV Data
def process_and_load_csv():
    # Read the cleaned date data from CSV
    cleaned_date_data = 'et_date_dimension/Cleaned Data/cleaned_date_data.csv'
    date_data_df = pd.read_csv(cleaned_date_data)

    # Store to CSV for backup
    dir = os.path.join(os.getcwd(), "tl_date_dimension", "Merged Data")
    merged_file_path = f"{dir}/date_dim.csv"
    date_data_df.to_csv(merged_file_path, index=False)

    # Load data into database
    engine = create_engine(db_url)
    date_data_df.to_sql('date_dimension', engine, if_exists='append', index=False)

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

task_process_load_csv = PythonOperator(
    task_id='process_and_load_csv',
    python_callable=process_and_load_csv,
    dag=dag,
)

# Define task dependencies
task_check_create_db >> task_create_table >> task_process_load_csv