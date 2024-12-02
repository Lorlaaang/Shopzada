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
    'merchant_data_pipeline',
    description='DAG to load merchant data into PostgreSQL',
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
    CREATE TABLE IF NOT EXISTS merchant_dimension (
        merchant_id SERIAL PRIMARY KEY,
        product_sale_id VARCHAR(10)
        merchant_name VARCHAR(255) NOT NULL,
        merchant_street VARCHAR(255) NOT NULL,
        merchant_state VARCHAR(255) NOT NULL,
        merchant_city VARCHAR(255) NOT NULL,
        merchant_country VARCHAR(255) NOT NULL,
        merchant_contact_number VARCHAR(50) NOT NULL,
        merchant_creation_date DATE NOT NULL
        FOREIGN KEY (product_sale_id) REFERENCES product_sale_fact(product_sale_id)
    );
    """
    cursor.execute(create_table_sql)
    conn.commit()

# Task 3: Process and Load HTML Data
def process_and_load_html():
    # Read the cleaned merchant data from HTML
    clean_merchant_data = 'et_enterprise_department/Cleaned Data/cleaned_merchant_data.html'
    merchant_data_df = pd.read_html(clean_merchant_data)[0]

    # Rename columns to match the database schema
    merchant_data_df.columns = ['merchant_' + col if (col != 'merchant_id') else col for col in merchant_data_df.columns]

    # Store to CSV for backup
    dir = os.path.join(os.getcwd(), "tl_enterprise_department", "Merged Data")
    merged_file_path = f"{dir}/merchant_dim.csv"
    merchant_data_df.to_csv(merged_file_path, index=False)

    # Load data into database
    engine = create_engine(db_url)
    merchant_data_df.to_sql('merchant_dimension', engine, if_exists='append', index=False)

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

task_process_load_html = PythonOperator(
    task_id='process_and_load_html',
    python_callable=process_and_load_html,
    dag=dag,
)

# Define task dependencies
task_check_create_db >> task_create_table >> task_process_load_html