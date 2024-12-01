# Shopzada ETL Pipeline

This repository contains ETL (Extract, Transform, Load) scripts for the Shopzada Data Warehouse project. The scripts are organized into different departments: Business, Customer Management, and Operations. Each script is responsible for extracting raw data, transforming it, and loading the cleaned data into the appropriate format.

## Table of Contents

- [Business Department](#business-department)
- [Customer Management Department](#customer-management-department)
- [Operations Department](#operations-department)
- [Usage](#usage)
- [Logging](#logging)

## Business Department

### Script: `ET_Business.py`

This script handles the extraction, transformation, and loading of data for the Business Department.

#### File Paths

- **Raw Data**: `Business Department\Raw Data\product_list.xlsx`
- **Cleaned Data**: `Business Department\Cleaned Data\cleaned_product_list.csv`

#### Functions

- **`clean_product_list(df)`**: Cleans and standardizes the `product_list` DataFrame.
  - Drops the first column.
  - Cleans `product_id` by removing spaces, enforcing uppercase, and removing hyphens. Formats as `P#####`.
  - Generates new unique IDs for duplicate `product_id` entries.
  - Cleans `product_name` by removing leading/trailing spaces and converting to title case.

- **`clean_product_type(df)`**: Cleans and standardizes the `product_type` column.
  - Fills null values in `product_type` with 'Unknown'.
  - Capitalizes words in `product_type` except for "and".

- **`standardize_price(df)`**: Standardizes the `price` column.
  - Rounds the `price` column to two decimal places.

#### Usage

1. **Load the dataset**:
   ```python
   df_product_list = pd.read_excel(product_list_path)
   ```

2. **Apply cleaning functions**:
   ```python
   df_product_list = clean_product_list(df_product_list)
   df_product_list = clean_product_type(df_product_list)
   df_product_list = standardize_price(df_product_list)
   ```

3. **Export cleaned data to CSV**:
   ```python
   df_product_list.to_csv(cleaned_product_list_path, index=False)
   ```

## Customer Management Department

### Script: `ET_CustomerManagement.py`

This script handles the extraction, transformation, and loading of data for the Customer Management Department.

#### File Paths

- **Raw Data**:
  - `Customer Management Department\Raw Data\user_credit_card.pickle`
  - `Customer Management Department\Raw Data\user_data.json`
  - `Customer Management Department\Raw Data\user_job.csv`
- **Cleaned Data**:
  - `Customer Management Department\Cleaned Data\cleaned_user_credit_card.csv`
  - `Customer Management Department\Cleaned Data\cleaned_user_data.csv`
  - `Customer Management Department\Cleaned Data\cleaned_user_job.csv`

#### Functions

- **`clean_user_credit_card(df)`**: Cleans and standardizes the `user_credit_card` DataFrame.
  - Cleans `user_id` by removing spaces, enforcing uppercase, and removing hyphens. Formats as `U#####`.
  - Cleans `name` by removing leading/trailing spaces, converting to title case, and handling middle initials or names with hyphens.

- **`clean_user_data(df)`**: Cleans and standardizes the `user_data` DataFrame.
  - Cleans `user_id` by removing spaces, enforcing uppercase, and removing hyphens. Formats as `U#####`.
  - Standardizes `creation_date` and `birthdate` to `YYYY-MM-DDTHH:MM:SS` format.
  - Cleans `name` by removing leading/trailing spaces, converting to title case, and handling middle initials or names with hyphens.
  - Capitalizes `country` names.

- **`clean_user_job(df)`**: Cleans and standardizes the `user_job` DataFrame.
  - Drops the first column.
  - Cleans `user_id` by removing spaces, enforcing uppercase, and removing hyphens. Formats as `U#####`.
  - Cleans `name` by removing leading/trailing spaces, converting to title case, and handling middle initials or names with hyphens.

- **`check_and_resolve_duplicates(df, subset)`**: Checks for and resolves duplicates in any DataFrame.
  - Identifies duplicates based on the specified subset of columns.
  - Resolves duplicates by keeping the first occurrence.

#### Usage

1. **Load the datasets**:
   ```python
   df_user_credit_card = pd.read_pickle(user_credit_card_path)
   df_user_data = pd.read_json(user_data_path)
   df_user_job = pd.read_csv(user_job_path)
   ```

2. **Apply cleaning functions**:
   ```python
   df_user_credit_card = clean_user_credit_card(df_user_credit_card)
   df_user_data = clean_user_data(df_user_data)
   df_user_job = clean_user_job(df_user_job)
   ```

3. **Check and resolve duplicates**:
   ```python
   df_user_credit_card = check_and_resolve_duplicates(df_user_credit_card, subset=['user_id', 'credit_card_number'])
   df_user_data = check_and_resolve_duplicates(df_user_data, subset=['user_id'])
   df_user_job = check_and_resolve_duplicates(df_user_job, subset=['user_id'])
   ```

4. **Export cleaned data to CSV**:
   ```python
   df_user_credit_card.to_csv(cleaned_user_credit_card_path, index=False)
   df_user_data.to_csv(cleaned_user_data_path, index=False)
   df_user_job.to_csv(cleaned_user_job_path, index=False)
   ```

## Operations Department

### Script: `ET_Operations.py`

This script handles the extraction, transformation, and loading of data for the Operations Department.

#### File Paths

- **Raw Data**: Located in the `Operations Department\Raw Data` directory.
- **Cleaned Data**: Located in the `Operations Department\Cleaned Data` directory.

#### Functions

- **`transform_data(df, table_name)`**: Transforms the data based on the table name.
  - Handles duplicates.
  - Drops nulls in critical columns.
  - Standardizes `user_id`, `transaction_date`, `quantity`, `price`, `product_name`, and `product_id`.

- **`process_file(file_name, table_name)`**: Processes and saves cleaned data.
  - Loads the data from various file formats.
  - Applies the `transform_data` function.
  - Saves the cleaned data to the appropriate file format.

#### Usage

1. **Define the files to tables mapping**:
   ```python
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
   ```

2. **Process each file**:
   ```python
   for file_name, table_name in files_to_tables.items():
       process_file(file_name, table_name)
   ```

## Logging

Each script is configured to log its operations to a log file located in the same directory as the script. The log files are named as follows:

- **Business Department**: `business_department.log`
- **Customer Management Department**: `customer_management_department.log`
- **Operations Department**: `Logs_Operations.txt`

The logs provide detailed information about the operations performed by each script, including data transformations, duplicate handling, and any errors encountered.

## Conclusion

This repository provides a comprehensive ETL pipeline for the Shopzada project, with separate scripts for the Business, Customer Management, and Operations departments. Each script is designed to handle specific data extraction, transformation, and loading tasks, ensuring that the data is cleaned and standardized before being saved to the appropriate format.
