import os
import pandas as pd
import logging
import re

# Define directories
base_folder = os.getcwd()
raw_data_folder = os.path.join(base_folder, "Operations Department", "Raw Data")
cleaned_data_folder = os.path.join(base_folder, "Operations Department", "Cleaned Data")
logs_file_path = os.path.join(base_folder, "Operations Department", "Logs_Operations.txt")

# Configure logging
logging.basicConfig(
    filename=logs_file_path,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# Ensure cleaned data folder exists
os.makedirs(cleaned_data_folder, exist_ok=True)

# Transform data function
def transform_data(df, table_name):
    try:
        logging.info(f"Transforming data for table: {table_name}")

        # Drop unnamed columns
        unnamed_columns = [col for col in df.columns if col.startswith("Unnamed")]
        if unnamed_columns:
            logging.info(f"Dropping unnamed columns: {unnamed_columns}")
            df.drop(columns=unnamed_columns, inplace=True)

        # Handle duplicates
        logging.info(f"Before dropping duplicates: {len(df)} rows")
        df.drop_duplicates(inplace=True)
        logging.info(f"After dropping duplicates: {len(df)} rows")

        # Drop nulls in critical columns based on table
        critical_columns = {
            "orders": ["order_id", "user_id", "transaction_date"],
            "line_items": ["quantity", "price"],
            "products": ["product_id", "product_name"],
        }

        if table_name in critical_columns:
            logging.info(f"Before dropping nulls: {len(df)} rows")
            df.dropna(subset=critical_columns[table_name], inplace=True)
            logging.info(f"After dropping nulls: {len(df)} rows")

        if table_name == "orders":
            # Standardize 'User ID'
            if "user_id" in df.columns:
                logging.info(f"Before cleaning user_id: {df['user_id'].head()}")
                df["user_id"] = df["user_id"].str.strip().str.upper().str.replace("USER", "U")
                df = df[df["user_id"].str.match(r"^U\d{5}$", na=False)]
                logging.info(f"After cleaning user_id: {df['user_id'].head()}")

            # Handle variations in column names for 'estimated_arrival_in_days'
            if "estimated arrival" in df.columns:
                df.rename(columns={"estimated arrival": "estimated_arrival_in_days"}, inplace=True)

            # Clean 'estimated_arrival_in_days' to be numeric
            if "estimated_arrival_in_days" in df.columns:
                logging.info(f"Before cleaning estimated_arrival_in_days: {df['estimated_arrival_in_days'].head()}")
                df["estimated_arrival_in_days"] = df["estimated_arrival_in_days"].apply(
                    lambda x: re.sub(r"\D", "", str(x)) if pd.notnull(x) else x
                )
                df["estimated_arrival_in_days"] = pd.to_numeric(
                    df["estimated_arrival_in_days"], errors="coerce"
                )
                logging.info(f"After cleaning estimated_arrival_in_days: {df['estimated_arrival_in_days'].head()}")

            # Standardize 'Transaction Date' to YYYY-MM-DD
            if "transaction_date" in df.columns:
                logging.info(f"Before cleaning transaction_date: {df['transaction_date'].head()}")
                df["transaction_date"] = pd.to_datetime(
                    df["transaction_date"], errors="coerce"
                ).dt.strftime("%Y-%m-%d")
                logging.info(f"After cleaning transaction_date: {df['transaction_date'].head()}")

        elif table_name == "line_items":
            # Standardize 'Quantity' to be an integer
            if "quantity" in df.columns:
                logging.info(f"Before cleaning quantity: {df['quantity'].head()}")
                df["quantity"] = df["quantity"].str.replace(r"[^\d]", "", regex=True).astype(int)
                logging.info(f"After cleaning quantity: {df['quantity'].head()}")

            # Ensure 'Price' has two decimal places
            if "price" in df.columns:
                logging.info(f"Before cleaning price: {df['price'].head()}")
                df["price"] = pd.to_numeric(df["price"], errors="coerce").map("{:.2f}".format)
                logging.info(f"After cleaning price: {df['price'].head()}")

        elif table_name == "products":
            # Capitalize each word in 'Product Name'
            if "product_name" in df.columns:
                logging.info(f"Before cleaning product_name: {df['product_name'].head()}")
                df["product_name"] = df["product_name"].str.title()
                logging.info(f"After cleaning product_name: {df['product_name'].head()}")

            # Clean 'Product ID'
            if "product_id" in df.columns:
                logging.info(f"Before cleaning product_id: {df['product_id'].head()}")
                df["product_id"] = df["product_id"].str.strip().str.upper()
                df["product_id"] = df["product_id"].str.replace("PRODUCT", "P", regex=False)
                df = df[df["product_id"].str.match(r"^P\d{5}$", na=False)]
                logging.info(f"After cleaning product_id: {df['product_id'].head()}")

        logging.info(f"Preview of transformed data:\n{df.head().to_string()}")
        return df

    except Exception as e:
        logging.error(f"Error transforming data for {table_name}: {e}")
        raise

# Process and save cleaned data as CSV
def process_file(file_name, table_name):
    try:
        file_extension = file_name.split(".")[-1]
        file_path = os.path.join(raw_data_folder, file_name)
        output_file_name = f"cleaned_{file_name.rsplit('.', 1)[0]}.csv" 
        output_file_path = os.path.join(cleaned_data_folder, output_file_name)

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

        df.to_csv(output_file_path, index=False)
        logging.info(f"Cleaned data saved to {output_file_path}")

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

if __name__ == "__main__":
    main()
