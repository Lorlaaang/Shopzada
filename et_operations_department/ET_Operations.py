import os
import pandas as pd
import logging
import re

# Define directories
base_folder = os.path.dirname(os.getcwd())
raw_data_folder = os.path.join(base_folder, "Data Source", "operations_department")
cleaned_data_folder = os.path.join(base_folder, "et_operations_department", "Cleaned Data")
logs_file_path = os.path.join(base_folder, "et_operations_department", "Logs_Operations.txt")

# Configure logging
logging.basicConfig(
    filename=logs_file_path,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# Ensure cleaned data folder exists
os.makedirs(cleaned_data_folder, exist_ok=True)

# Transform data function for regular datasets
def transform_data(df, table_name):
    try:
        logging.info(f"Transforming data for table: {table_name}")

        # **Added code to drop unnamed columns**
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

        # Additional transformations
        if table_name == "orders":
            if "user_id" in df.columns:
                df["user_id"] = df["user_id"].str.strip().str.upper().str.replace("USER", "U")
                df = df[df["user_id"].str.match(r"^U\d{5}$", na=False)]
            if "transaction_date" in df.columns:
                df["transaction_date"] = pd.to_datetime(
                    df["transaction_date"], errors="coerce"
                ).dt.strftime("%Y-%m-%d")
            # **Rename 'estimated arrival' to 'estimated_arrival_in_days' and clean the data**
            if "estimated arrival" in df.columns:
                df.rename(columns={"estimated arrival": "estimated_arrival_in_days"}, inplace=True)
            if "estimated_arrival_in_days" in df.columns:
                df["estimated_arrival_in_days"] = (
                    df["estimated_arrival_in_days"]
                    .astype(str)
                    .str.replace("days", "", regex=False)
                    .str.strip()
                )
                df["estimated_arrival_in_days"] = pd.to_numeric(
                    df["estimated_arrival_in_days"], errors="coerce"
                ).astype("Int64")
        elif table_name == "line_items":
            if "quantity" in df.columns:
                df["quantity"] = pd.to_numeric(
                    df["quantity"].str.replace(r"[^\d]", "", regex=True),
                    errors="coerce",
                ).fillna(0).astype(int)
            if "price" in df.columns:
                df["price"] = pd.to_numeric(df["price"], errors="coerce").map("{:.2f}".format)
        elif table_name == "products":
            if "product_name" in df.columns:
                df["product_name"] = df["product_name"].str.title()
            if "product_id" in df.columns:
                df["product_id"] = (
                    df["product_id"]
                    .str.strip()
                    .str.upper()
                    .str.replace("PRODUCT", "P", regex=False)
                )
                df = df[df["product_id"].str.match(r"^P\d{5}$", na=False)]

        logging.info(f"Preview of transformed data:\n{df.head().to_string()}")
        return df

    except Exception as e:
        logging.error(f"Error transforming data for {table_name}: {e}")
        raise

# Transform data function for merged files
def transform_merged_data(df):
    try:
        logging.info("Applying additional cleaning to merged data")

        # Drop duplicate and unnecessary columns from merge
        df = df.loc[:, ~df.columns.str.contains("^Unnamed")].copy()  # Explicitly create a copy

        # Capitalize product names
        if "product_name" in df.columns:
            df["product_name"] = df["product_name"].str.title()

        # Standardize product IDs to P##### format
        if "product_id" in df.columns:
            df["product_id"] = (
                df["product_id"]
                .str.strip()
                .str.upper()
                .str.replace("PRODUCT", "P", regex=False)
            )
            df = df[df["product_id"].str.match(r"^P\d{5}$", na=False)].copy()

        # Standardize 'quantity' and 'price' columns
        if "quantity" in df.columns:
            df["quantity"] = pd.to_numeric(
                df["quantity"].str.replace(r"[^\d]", "", regex=True),
                errors="coerce",
            ).fillna(0).astype(int)
        if "price" in df.columns:
            df["price"] = pd.to_numeric(df["price"], errors="coerce").map("{:.2f}".format)

        # Drop duplicates post-merge
        df.drop_duplicates(inplace=True)
        logging.info(f"After cleaning, merged data has {len(df)} rows")

        return df

    except Exception as e:
        logging.error(f"Error transforming merged data: {e}")
        raise

# Function to merge specific files
def merge_files(prices_file, products_file, output_index):
    try:
        prices_path = os.path.join(raw_data_folder, prices_file)
        products_path = os.path.join(raw_data_folder, products_file)

        # Read both files
        prices_df = (
            pd.read_csv(prices_path)
            if prices_file.endswith(".csv")
            else pd.read_parquet(prices_path)
        )
        products_df = (
            pd.read_csv(products_path)
            if products_file.endswith(".csv")
            else pd.read_parquet(products_path)
        )

        # Normalize column names
        prices_df.columns = prices_df.columns.str.strip()
        products_df.columns = products_df.columns.str.strip()

        # Merge on order_id
        merged_df = pd.merge(products_df, prices_df, left_index=True, right_index=True)
        
        merged_df.drop(columns=['order_id_x'], inplace=True)
        merged_df = merged_df.rename(columns={'order_id_y': 'order_id'})

        # Clean merged data
        merged_df = transform_merged_data(merged_df)

        # Save the merged and cleaned file
        final_output_file_name = f"cleaned_line_item_data{output_index}.csv"
        final_output_file_path = os.path.join(cleaned_data_folder, final_output_file_name)
        merged_df.to_csv(final_output_file_path, index=False)
        logging.info(f"Merged and cleaned file saved to {final_output_file_path}")

    except Exception as e:
        logging.error(f"Error merging {prices_file} and {products_file}: {e}")
        raise

# Process and save cleaned data
def process_file(file_name, table_name):
    try:
        file_path = os.path.join(raw_data_folder, file_name)
        # **Ensure output file is CSV and named appropriately**
        output_file_name = f"cleaned_{os.path.splitext(file_name)[0]}.csv"
        output_file_path = os.path.join(cleaned_data_folder, output_file_name)

        # Check file extension and process accordingly
        if file_name.endswith(".csv"):
            df = pd.read_csv(file_path)
        elif file_name.endswith(".parquet"):
            try:
                df = pd.read_parquet(file_path)
            except Exception as e:
                logging.error(f"Failed to read Parquet file {file_name}: {e}")
                return  # Skip this file and continue with the others
        elif file_name.endswith(".pickle"):
            try:
                df = pd.read_pickle(file_path)
            except Exception as e:
                logging.error(f"Failed to read Pickle file {file_name}: {e}")
                return
        elif file_name.endswith(".xlsx") or file_name.endswith(".xls"):
            df = pd.read_excel(file_path)
        elif file_name.endswith(".json"):
            df = pd.read_json(file_path)
        elif file_name.endswith(".html"):
            try:
                df = pd.read_html(file_path)[0]
            except Exception as e:
                logging.error(f"Failed to read HTML file {file_name}: {e}")
                return
        else:
            logging.warning(f"Unsupported file type for {file_name}. Skipping...")
            return

        # Transform and save the data
        df = transform_data(df, table_name)
        df.to_csv(output_file_path, index=False)
        logging.info(f"Cleaned data saved to {output_file_path}")

    except Exception as e:
        logging.error(f"Error processing file {file_name}: {e}")
        raise

# Main function
def main():
    # Define file pairs to merge
    files_to_merge = [
        ("line_item_data_prices1.csv", "line_item_data_products1.csv", "1"),
        ("line_item_data_prices2.csv", "line_item_data_products2.csv", "2"),
        ("line_item_data_prices3.parquet", "line_item_data_products3.parquet", "3"),
    ]

    for prices_file, products_file, output_index in files_to_merge:
        merge_files(prices_file, products_file, output_index)

    # Include all order files in the files_to_tables dictionary
    files_to_tables = {
        "order_data_20211001-20220101.csv": "orders",
        "order_data_20200101-20200701.parquet": "orders",
        "order_data_20200701-20211001.pickle": "orders",
        "order_data_20230601-20240101.html": "orders",
        "order_data_20220101-20221201.xlsx": "orders",
        "order_data_20221201-20230601.json": "orders",
        "order_delays.html": "order_delays",  # Assuming this is another table
    }

    for file_name, table_name in files_to_tables.items():
        process_file(file_name, table_name)

if __name__ == "__main__":
    main()
