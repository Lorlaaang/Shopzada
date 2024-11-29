import pandas as pd
import sqlite3
import os
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    filename="etl_pipeline_log.txt",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

DATABASE_NAME = "shopzada.db"

def summarize_data(data, table_name):
    """
    Log a summary of the data for validation.
    """
    logging.info(f"--- Summary for {table_name} ---")
    numeric_summary = data.select_dtypes(include=["number"]).describe()
    logging.info(f"Numeric Summary:\n{numeric_summary.to_string()}")
    categorical_summary = data.select_dtypes(exclude=["number"]).describe(include="all")
    logging.info(f"Categorical Summary:\n{categorical_summary.to_string()}")

def load_file(file_path):
    """
    Load file based on the extension.
    """
    try:
        logging.info(f"Loading file: {file_path}")
        if file_path.endswith(".csv"):
            return pd.read_csv(file_path)
        elif file_path.endswith(".parquet"):
            return pd.read_parquet(file_path)
        elif file_path.endswith(".xlsx"):
            return pd.read_excel(file_path)
        elif file_path.endswith(".json"):
            return pd.read_json(file_path)
        else:
            raise ValueError(f"Unsupported file format: {file_path}")
    except Exception as e:
        logging.error(f"Error loading file {file_path}: {e}")
        raise

def insert_to_db(data, table_name):
    """
    Insert cleaned data into the SQLite database.
    """
    try:
        with sqlite3.connect(DATABASE_NAME) as conn:
            data.to_sql(table_name, conn, if_exists="replace", index=False)
        logging.info(f"Data inserted into table {table_name}")
    except Exception as e:
        logging.error(f"Error inserting data into {table_name}: {e}")
        raise

def process_dates_dim(file_path):
    try:
        data = load_file(file_path)
        logging.info("Processing dates_dim...")

        # Transformations
        data["full_date"] = pd.to_datetime(data["full_date"], errors="coerce")
        data["year"] = data["full_date"].dt.year
        data["quarter"] = data["full_date"].dt.quarter
        data["month"] = data["full_date"].dt.month
        data["month_name"] = data["full_date"].dt.month_name()
        data["day"] = data["full_date"].dt.day
        data["day_name"] = data["full_date"].dt.day_name()
        data["is_weekend"] = data["day_name"].isin(["Saturday", "Sunday"])

        summarize_data(data, "dates_dim")
        insert_to_db(data, "dates_dim")
    except Exception as e:
        logging.error(f"Error processing dates_dim: {e}")

def process_users_dim(file_path):
    try:
        data = load_file(file_path)
        logging.info("Processing users_dim...")

        # Transformations
        data["name"] = data["name"].str.title()
        data["job_title"] = data["job_title"].str.title()
        data["credit_card_number"] = data["credit_card_number"].str.replace("-", "")
        data["issuing_bank"] = data["issuing_bank"].str.title()

        summarize_data(data, "users_dim")
        insert_to_db(data, "users_dim")
    except Exception as e:
        logging.error(f"Error processing users_dim: {e}")

def process_orders_fact(file_path):
    try:
        data = load_file(file_path)
        logging.info("Processing orders_fact...")

        # Transformations
        data["discount_availed"] = data["discount_availed"].fillna(0)
        data["order_id"] = data["order_id"].str.upper()
        data["staff_id"] = data["staff_id"].astype(int)
        data["user_id"] = data["user_id"].str.upper()

        summarize_data(data, "orders_fact")
        insert_to_db(data, "orders_fact")
    except Exception as e:
        logging.error(f"Error processing orders_fact: {e}")

def process_products_dim(file_path):
    try:
        data = load_file(file_path)
        logging.info("Processing products_dim...")

        # Transformations
        data["product_name"] = data["product_name"].str.title()
        data["product_type"] = data["product_type"].str.lower()
        data["price"] = data["price"].fillna(data["price"].median())

        summarize_data(data, "products_dim")
        insert_to_db(data, "products_dim")
    except Exception as e:
        logging.error(f"Error processing products_dim: {e}")

if __name__ == "__main__":
    logging.info("Starting Data Pipeline...")

    # File-to-table mapping
    file_table_mappings = {
        "dates_data.csv": process_dates_dim,
        "users_data.csv": process_users_dim,
        "orders_data.csv": process_orders_fact,
        "products_data.csv": process_products_dim,
    }

    input_folder = r"C:\Users\lorlang\Documents\3rdYear\Data Warehouse\Shopzada_ETL\Project Dataset\Operations Department"
    for file_name, process_function in file_table_mappings.items():
        file_path = os.path.join(input_folder, file_name)
        process_function(file_path)

    logging.info("Data Pipeline Execution Complete.")
