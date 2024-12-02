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
    CREATE TABLE IF NOT EXISTS product_sale_fact (
        product_sale_id SERIAL PRIMARY KEY,
        date_id INT,
        order_id INT,
        user_id INT,
        campaign_id VARCHAR(7),
        staff_id VARCHAR(8),
        merchant_id VARCHAR(7),
        product_id INT,
        quantity INT NOT NULL,
        FOREIGN KEY (date_id) REFERENCES date_dimension(date_id),
        FOREIGN KEY (order_id) REFERENCES order_dimension(order_id),
        FOREIGN KEY (user_id) REFERENCES user_dimension(user_id),
        FOREIGN KEY (campaign_id) REFERENCES campaign_dimension(campaign_id),
        FOREIGN KEY (staff_id) REFERENCES staff_dimension(staff_id),
        FOREIGN KEY (merchant_id) REFERENCES merchant_dimension(merchant_id),
        FOREIGN KEY (product_id) REFERENCES product_dimension(product_id)
            """)
        print("Table creation completed successfully")

    except Exception as e:
        print("Full Error Traceback:")
        traceback.print_exc()
        raise AirflowException(f"Error during table creation: {e}")
# Task 3: Process and Load Data
def process_and_load_data():
    # Import the data processing logic from load_product_sale_fact.py
    from load_product_sale_fact import final_order_data_combined

    # Store to CSV for backup
    dir = os.path.join(os.getcwd(), "tl_product_sale_fact", "Merged Data")
    os.makedirs(dir, exist_ok=True)
    merged_file_path = f"{dir}/product_sale_fact.csv"
    final_order_data_combined.to_csv(merged_file_path, index=False)

    # Load data into database
    engine = create_engine(db_url)
    final_order_data_combined.to_sql('product_sale_fact', engine, if_exists='append', index=False)

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

task_process_load_data = PythonOperator(
    task_id='process_and_load_data',
    python_callable=process_and_load_data,
    dag=dag,
)

# Define task dependencies
task_check_create_db >> task_create_table >> task_process_load_data