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
    );
    """
    cursor.execute(create_table_sql)
    conn.commit()

# Task 3: Process and Load CSV Data
def process_and_load_csv():
    # File paths for individual cleaned files
    clean_order_data_paths = [
        'et_operations_department/Cleaned Data/cleaned_order_data_20200101-20200701.csv',
        'et_operations_department/Cleaned Data/cleaned_order_data_20200701-20211001.csv',
        'et_operations_department/Cleaned Data/cleaned_order_data_20211001-20220101.csv',
        'et_operations_department/Cleaned Data/cleaned_order_data_20220101-20221201.csv',
        'et_operations_department/Cleaned Data/cleaned_order_data_20221201-20230601.csv',
        'et_operations_department/Cleaned Data/cleaned_order_data_20230601-20240101.csv'
    ]
    clean_transaction_campaign_data_path = 'et_marketing_department/Cleaned Data/cleaned_transactional_campaign_data.csv'
    clean_user_data_path = 'et_customer_management_department/Cleaned Data/cleaned_user_data.csv'

    # Read files
    order_data_dfs = [pd.read_csv(path) for path in clean_order_data_paths]
    order_data_combined = pd.concat(order_data_dfs, ignore_index=True)
    transactional_campaign_data_df = pd.read_csv(clean_transaction_campaign_data_path)
    user_data_df = pd.read_csv(clean_user_data_path)

    # Extract date-related columns
    date_columns = ['transaction_date', 'estimated_arrival', 'creation_date', 'birthdate']
    order_dates = order_data_combined[['transaction_date', 'estimated_arrival']].copy()
    campaign_dates = transactional_campaign_data_df[['transaction_date']].copy()
    user_dates = user_data_df[['creation_date', 'birthdate']].copy()

    # Rename columns for consistency
    order_dates.rename(columns={'transaction_date': 'date_full'}, inplace=True)
    campaign_dates.rename(columns={'transaction_date': 'date_full'}, inplace=True)
    user_dates.rename(columns={'creation_date': 'date_full', 'birthdate': 'date_full'}, inplace=True)

    # Combine all date data
    all_dates = pd.concat([order_dates, campaign_dates, user_dates], ignore_index=True).drop_duplicates()

    # Process date data
    all_dates['date_full'] = pd.to_datetime(all_dates['date_full'], errors='coerce')
    all_dates = all_dates.dropna(subset=['date_full'])  # Drop rows where date parsing failed
    all_dates['date_year'] = all_dates['date_full'].dt.year
    all_dates['date_quarter'] = all_dates['date_full'].dt.quarter
    all_dates['date_month'] = all_dates['date_full'].dt.month
    all_dates['date_month_name'] = all_dates['date_full'].dt.strftime('%B')
    all_dates['date_day'] = all_dates['date_full'].dt.day
    all_dates['date_day_name'] = all_dates['date_full'].dt.strftime('%A')
    all_dates['date_is_weekend'] = all_dates['date_full'].dt.weekday >= 5
    all_dates['date_is_holiday'] = all_dates['date_full'].dt.strftime('%m-%d').isin([
        "01-01", "04-09", "05-01", "06-12", "08-21", "11-01", "11-02", "12-25", "12-30"
    ])
    all_dates['date_holiday_name'] = all_dates['date_full'].dt.strftime('%m-%d').map({
        "01-01": "New Year's Day",
        "04-09": "Araw ng Kagitingan",
        "05-01": "Labor Day",
        "06-12": "Independence Day",
        "08-21": "Ninoy Aquino Day",
        "11-01": "All Saints' Day",
        "11-02": "All Souls' Day",
        "12-25": "Christmas Day",
        "12-30": "Rizal Day"
    }).fillna('')

    # Store to CSV for backup
    dir = os.path.join(os.getcwd(), "tl_date_dimension", "Merged Data")
    os.makedirs(dir, exist_ok=True)
    merged_file_path = f"{dir}/date_dim.csv"
    all_dates.to_csv(merged_file_path, index=False)

    # Load data into database
    engine = create_engine(db_url)
    all_dates.to_sql('date_dimension', engine, if_exists='append', index=False)

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