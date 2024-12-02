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
    'product_sale_fact_data_pipeline',
    description='DAG to load product sale fact data into PostgreSQL',
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
    );
    """
    cursor.execute(create_table_sql)
    conn.commit()

# Task 3: Process and Load Data
def process_and_load_data():
    # File paths
    clean_product_list = 'et_business_department/Cleaned Data/cleaned_product_list.csv'
    clean_order_data_paths = [
        'et_operations_department/Cleaned Data/cleaned_order_data_20200101-20200701.csv',
        'et_operations_department/Cleaned Data/cleaned_order_data_20200701-20211001.csv',
        'et_operations_department/Cleaned Data/cleaned_order_data_20211001-20220101.csv',
        'et_operations_department/Cleaned Data/cleaned_order_data_20220101-20221201.csv',
        'et_operations_department/Cleaned Data/cleaned_order_data_20221201-20230601.csv',
        'et_operations_department/Cleaned Data/cleaned_order_data_20230601-20240101.csv'
    ]
    clean_transaction_campaign_data_path = 'et_marketing_department/Cleaned Data/cleaned_transactional_campaign_data.csv'
    clean_order_with_merchant_paths = [
        'et_enterprise_department/Cleaned Data/cleaned_order_with_merchant_data1.parquet',
        'et_enterprise_department/Cleaned Data/cleaned_order_with_merchant_data2.parquet',
        'et_enterprise_department/Cleaned Data/cleaned_order_with_merchant_data3.csv'
    ]
    clean_staff_data = 'et_enterprise_department/Cleaned Data/cleaned_staff_data.html'
    clean_merchant_data = 'et_enterprise_department/Cleaned Data/cleaned_merchant_data.html'
    clean_line_item_data_paths = [
        'et_operations_department/Cleaned Data/cleaned_line_item_data1.csv',
        'et_operations_department/Cleaned Data/cleaned_line_item_data2.csv',
        'et_operations_department/Cleaned Data/cleaned_line_item_data3.csv'
    ]

    # Read files
    order_data_dfs = [pd.read_csv(path) for path in clean_order_data_paths]
    order_data_combined = pd.concat(order_data_dfs, ignore_index=True)
    product_list_df = pd.read_csv(clean_product_list)
    transactional_campaign_data_df = pd.read_csv(clean_transaction_campaign_data_path)
    order_with_merchant_dfs = [pd.read_parquet(path) if path.endswith('.parquet') else pd.read_csv(path) for path in clean_order_with_merchant_paths]
    order_with_merchant_data_combined = pd.concat(order_with_merchant_dfs, ignore_index=True)
    merchant_data_df = pd.read_html(clean_merchant_data)[0]
    staff_data_df = pd.read_html(clean_staff_data)[0]
    line_item_data_dfs = [pd.read_csv(path) for path in clean_line_item_data_paths]
    line_item_data_combined = pd.concat(line_item_data_dfs, ignore_index=True)

    # Remove rows with merchants not in the merchant data
    order_with_merchant_data_combined = order_with_merchant_data_combined.merge(
        merchant_data_df,
        on='merchant_id',  
        how='inner'        
    )
    order_with_merchant_data_combined.drop(columns=['creation_date', 'street', 'name', 'city', 'state', 'country', 'contact_number'], inplace=True)

    # Remove rows with staff not in the staff data
    order_with_merchant_data_combined = order_with_merchant_data_combined.merge(
        staff_data_df,
        on='staff_id',  
        how='inner'        
    )
    order_with_merchant_data_combined.drop(columns=['creation_date', 'job_level', 'street', 'name', 'city', 'state', 'country', 'contact_number'], inplace=True)

    # Remove rows with products not in the product data
    line_item_data_combined = line_item_data_combined.merge(
        product_list_df,
        on='product_id',  
        how='inner'        
    )
    line_item_data_combined.drop(columns=['product_name_x', 'product_name_y', 'price_x', 'price_y', 'product_type'], inplace=True)
    transactional_campaign_data_df.drop(columns=['availed', 'estimated_arrival_in_days'], inplace=True)
    order_data_combined.drop(columns=['estimated_arrival_in_days'], inplace=True)

    # Merging
    line_item_data_combined_with_order = pd.merge(line_item_data_combined, order_data_combined, on=['order_id'], how='inner')
    line_item_data_combined_with_order_campaign = pd.merge(line_item_data_combined_with_order, transactional_campaign_data_df, on=['order_id', 'transaction_date'], how='left')
    line_item_data_combined_with_order_campaign_merchant_staff = pd.merge(line_item_data_combined_with_order_campaign, order_with_merchant_data_combined, on=['order_id'], how='left')

    # Fill null to fit db
    line_item_data_combined_with_order_campaign_merchant_staff['campaign_id'] = line_item_data_combined_with_order_campaign_merchant_staff['campaign_id'].fillna('C00000')
    line_item_data_combined_with_order_campaign_merchant_staff['merchant_id'] = line_item_data_combined_with_order_campaign_merchant_staff['merchant_id'].fillna('M00000')
    line_item_data_combined_with_order_campaign_merchant_staff['staff_id'] = line_item_data_combined_with_order_campaign_merchant_staff['staff_id'].fillna('S0000000')

    # Rename column to fit db
    order_data_with_campaign_merchant_staff_products_prices = line_item_data_combined_with_order_campaign_merchant_staff.rename(columns={'transaction_date': 'date_id'})

    # Load data into database
    engine = create_engine(db_url)
    order_data_with_campaign_merchant_staff_products_prices.to_sql('product_sale_fact', engine, if_exists='append', index=False)

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