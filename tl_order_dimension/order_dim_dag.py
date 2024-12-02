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
    'order_data_pipeline',
    description='DAG to load order data into PostgreSQL',
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
    CREATE TABLE IF NOT EXISTS order_dimension (
        order_id SERIAL PRIMARY KEY,
        product_sale_id INT,
        estimated_arrival DATE NOT NULL,
        delays_in_days INT NOT NULL,
        is_discount_availed BOOLEAN NOT NULL
    );
    """
    cursor.execute(create_table_sql)
    conn.commit()

# Task 3: Process and Load CSV Data
def process_and_load_csv():
    # File paths
    clean_order_data_paths = [
        'et_operations_department/Cleaned Data/cleaned_order_data_20200101-20200701.csv',
        'et_operations_department/Cleaned Data/cleaned_order_data_20200701-20211001.csv',
        'et_operations_department/Cleaned Data/cleaned_order_data_20211001-20220101.csv',
        'et_operations_department/Cleaned Data/cleaned_order_data_20220101-20221201.csv',
        'et_operations_department/Cleaned Data/cleaned_order_data_20221201-20230601.csv',
        'et_operations_department/Cleaned Data/cleaned_order_data_20230601-20240101.csv'
    ]
    clean_transaction_campaign_data_path = 'et_marketing_department/Cleaned Data/cleaned_transactional_campaign_data.csv'
    clean_order_delays_path = 'et_operations_department/Cleaned Data/cleaned_order_delays.csv'

    # Read files
    order_data_dfs = [pd.read_csv(path) for path in clean_order_data_paths]
    transactional_campaign_data_df = pd.read_csv(clean_transaction_campaign_data_path)
    order_delays_df = pd.read_csv(clean_order_delays_path)

    # Combine order data
    order_data_combined = pd.concat(order_data_dfs, ignore_index=True)

    # Merge files for loading
    merge_key1 = ['order_id', 'estimated_arrival_in_days', 'transaction_date']
    merge_key2 = ['order_id']
    order_data_with_discount_avail_df = pd.merge(order_data_combined, transactional_campaign_data_df, on=merge_key1, how='outer')
    order_data_with_discount_avail_with_delays_df = pd.merge(order_data_with_discount_avail_df, order_delays_df, on=merge_key2, how='outer')

    order_data_with_discount_avail_with_delays_df.drop(columns=['user_id', 'transaction_date', 'campaign_id'], inplace=True)

    # Fill null values to fit DB
    order_data_with_discount_avail_with_delays_df['availed'] = order_data_with_discount_avail_with_delays_df['availed'].fillna(False)
    order_data_with_discount_avail_with_delays_df['delay in days'] = order_data_with_discount_avail_with_delays_df['delay in days'].fillna(0)

    # Rename columns to fit DB
    order_data_with_discount_avail_with_delays_df.columns = [col.replace(' ', '_') for col in order_data_with_discount_avail_with_delays_df.columns]
    final_order_data_combined = order_data_with_discount_avail_with_delays_df.rename(columns={'availed': 'is_discount_availed'})
    final_order_data_combined.columns = ['order_' + col if (col != 'order_id' and col != 'user_type') else col for col in final_order_data_combined.columns]

    # Load data into database
    engine = create_engine(db_url)
    final_order_data_combined.to_sql('order_dimension', engine, if_exists='append', index=False)

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