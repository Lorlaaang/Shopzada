import os
import pandas as pd
import sqlite3
import logging

# Configure logging
logging.basicConfig(
    filename="operations_etl_log.txt",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# SQLite database configuration
DB_FILE = "shopzada.db"

# Initialize SQLite connection
conn = sqlite3.connect(DB_FILE)

# Define the Operations Department directory
input_folder = os.path.join(
    os.getcwd(),
    "Project Dataset-20241024T131910Z-001 (1)",
    "Project Dataset",
    "Operations Department",
)

# Helper function to clean and standardize data
# Helper function to clean and standardize data
def transform_data(df, table_name):
    try:
        logging.info(f"Transforming data for table: {table_name}")

        if table_name == "orders":
            # Standardize 'User ID'
            if "user_id" in df.columns:
                df["user_id"] = df["user_id"].str.strip().str.upper().str.replace("USER", "U")
                df = df[df["user_id"].str.match(r"^U\d{5}$", na=False)]

            # Clean 'estimated_arrival_in_days' to be numeric
            if "estimated_arrival_in_days" in df.columns:
                import re  # Ensure 're' module is available
                df["estimated_arrival_in_days"] = df["estimated_arrival_in_days"].apply(
                    lambda x: re.sub(r"\D", "", str(x))
                )
                df["estimated_arrival_in_days"] = pd.to_numeric(
                    df["estimated_arrival_in_days"], errors="coerce"
                )

            # Standardize 'Transaction Date'
            if "transaction_date" in df.columns:
                df["transaction_date"] = pd.to_datetime(
                    df["transaction_date"], errors="coerce"
                ).dt.strftime("%Y/%m/%d")

        elif table_name == "line_items":
            # Standardize 'Quantity'
            if "quantity" in df.columns:
                df["quantity"] = df["quantity"].str.replace(r"[^\d]", "", regex=True) + " PCs"

            # Ensure 'Price' has two decimal places
            if "price" in df.columns:
                df["price"] = pd.to_numeric(df["price"], errors="coerce").map("{:.2f}".format)

        elif table_name == "products":
            # Capitalize each word in 'Product Name'
            if "product_name" in df.columns:
                df["product_name"] = df["product_name"].str.title()

            # Clean 'Product ID'
            if "product_id" in df.columns:
                df["product_id"] = df["product_id"].str.strip().str.upper()
                df["product_id"] = df["product_id"].str.replace("PRODUCT", "P", regex=False)
                df = df[df["product_id"].str.match(r"^P\d{5}$", na=False)]

        logging.info(f"Preview of transformed data:\n{df.head().to_string()}")
        return df

    except Exception as e:
        logging.error(f"Error transforming data for {table_name}: {e}")
        raise


# Process and load data into SQLite
def process_file(file_name, table_name):
    try:
        file_extension = file_name.split(".")[-1]
        file_path = os.path.join(input_folder, file_name)

        if file_extension == "csv":
            df = pd.read_csv(file_path)
        elif file_extension == "json":
            df = pd.read_json(file_path)
        elif file_extension == "parquet":
            df = pd.read_parquet(file_path)
        elif file_extension in ["xlsx", "xls"]:
            df = pd.read_excel(file_path)
        elif file_extension == "html":
            df = pd.read_html(file_path)[0]
        elif file_extension == "pickle":
            df = pd.read_pickle(file_path)
        else:
            logging.warning(f"Unsupported file type: {file_name}")
            return

        df = transform_data(df, table_name)
        df.to_sql(table_name, conn, if_exists="append", index=False)
        logging.info(f"Data loaded into {table_name} successfully.")

    except Exception as e:
        logging.error(f"Error processing file {file_name}: {e}")
        raise

# Main function to execute the ETL pipeline
def main():
    files_to_tables = {
        "order_data_20211001-20220101.csv": "orders",
        "order_data_20200101-20200701.parquet": "orders",
        "order_data_20221201-20230601.json": "orders",
        "order_data_20200701-20211001.pickle": "orders",
        "order_data_20230601-20240101.html": "orders",
        "order_data_20220101-20221201.xlsx": "orders",
        "line_item_data_prices1.csv": "line_items",
        "line_item_data_prices2.csv": "line_items",
        "line_item_data_prices3.parquet": "line_items",
        "line_item_data_products1.csv": "products",
        "line_item_data_products2.csv": "products",
        "line_item_data_products3.parquet": "products",
        "order_delays.html": "order_delays",
    }

    for file_name, table_name in files_to_tables.items():
        process_file(file_name, table_name)

    conn.close()

if __name__ == "__main__":
    main()
