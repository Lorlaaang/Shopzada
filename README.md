# Shopzada ETL Pipeline

This repository contains ETL (Extract, Transform, Load) scripts for the Shopzada Data Warehouse project. The scripts are organized into different departments: Business, Customer Management, Operations, Enterprise, and Marketing. Each script is responsible for extracting raw data, transforming it, and loading the cleaned data into the appropriate format.

## Table of Contents

- [Business Department](#business-department)
- [Customer Management Department](#customer-management-department)
- [Operations Department](#operations-department)
- [Enterprise Department](#enterprise-department)
- [Marketing Department](#marketing-department)
- [Logging](#logging)
- [Date Dimension](#date-dimension)
- [Time Dimension](#time-dimension)
- [Product Dimension](#product-dimension)
- [Customer Dimension](#customer-dimension)
- [Sales Fact Table](#sales-fact-table)
- [Loading and DAG Files](#loading-and-dag-files)
- [Conclusion](#conclusion)

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
  - Drops duplicates based on `product_id`, keeping the first occurrence.
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
  - Hashes the credit card number.

- **`clean_user_data(df)`**: Cleans and standardizes the `user_data` DataFrame.
  - Cleans `user_id` by removing spaces, enforcing uppercase, and removing hyphens. Formats as `U#####`.
  - Standardizes `creation_date` and `birthdate` to `YYYY-MM-DDTHH:MM:SS` format.
  - Cleans `name` by removing leading/trailing spaces, converting to title case, and handling middle initials or names with hyphens.
  - Capitalizes `country` names.

- **`clean_user_job(df)`**: Cleans and standardizes the `user_job` DataFrame.
  - Drops the first column.
  - Cleans `user_id` by removing spaces, enforcing uppercase, and removing hyphens. Formats as `U#####`.
  - Cleans `name` by removing leading/trailing spaces, converting to title case, and handling middle initials or names with hyphens.

- **`remove_user_id_duplicates(df)`**: Checks for and resolves duplicates based on `user_id`.
  - Identifies duplicates based on `user_id`.
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

3. **Remove duplicates based on `user_id`**:
   ```python
   df_user_credit_card = remove_user_id_duplicates(df_user_credit_card)
   df_user_data = remove_user_id_duplicates(df_user_data)
   df_user_job = remove_user_id_duplicates(df_user_job)
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

## Enterprise Department

### Script: `ET_Enterprise.py`

This script handles the extraction, transformation, and loading of data for the Enterprise Department.

#### Functions

- **`standardize_merchant_id(merchant_id)`**: Standardizes the `merchant_id`.
  - Replaces `MERCHANT` with `M`.

- **`standardize_staff_id(staff_id)`**: Standardizes the `staff_id`.
  - Replaces `STAFF` with `S`.

- **`check_duplicates(df, subset_columns, data_name)`**: Checks for duplicates in the DataFrame.
  - Identifies duplicates based on the specified subset of columns.

- **`check_null_values(df, data_name)`**: Checks for null values in the DataFrame.
  - Identifies columns with null values.

- **`standardize_street_name(street_name)`**: Standardizes the `street_name`.
  - Removes extra spaces and capitalizes the first letter of each word.

- **`capitalize_first_word(text)`**: Capitalizes the first word of the text.

- **`standardize_phone(phone)`**: Standardizes the phone number.
  - Removes non-numeric characters and formats the phone number.

- **`simplify_country_name(country_name)`**: Simplifies the `country_name`.
  - Removes unnecessary words and phrases.

- **`standardize_country(country_name)`**: Standardizes the `country_name`.
  - Simplifies the country name and applies custom mappings.

#### Usage

1. **Process the order files**:
   ```python
   process_order_files()
   ```

2. **Process the merchant data**:
   ```python
   process_merchant_data()
   ```

3. **Process the staff data**:
   ```python
   process_staff_data()
   ```

## Marketing Department

### Script: `ET_Marketing.py`

This script handles the extraction, transformation, and loading of data for the Marketing Department.

#### Functions

- **`standardize_percentage(value)`**: Standardizes the percentage values.
  - Removes non-numeric characters and converts to a decimal.

#### Usage

1. **Process the campaign data**:
   ```python
   # Load the campaign data
   csv_path = "Raw Data/campaign_data.csv"
   df1 = pd.read_csv(csv_path, delimiter="\t")

   # Remove unnecessary quotations
   df1['campaign_description'] = df1['campaign_description'].str.replace('"', '', regex=False)

   # Delete unnecessary columns
   unnamed_columns = [col for col in df1.columns if col.lower().startswith('unnamed')]
   if unnamed_columns:
       df1.drop(columns=unnamed_columns, inplace=True)

   # Standardize campaign_id
   df1['campaign_id'] = df1['campaign_id'].str.replace('CAMPAIGN', 'C')

   # Standardize campaign_name
   df1['campaign_name'] = df1['campaign_name'].str.capitalize()
   df1['campaign_name'] = df1['campaign_name'].str.replace(r'\bi\b', 'I', regex=True)

   # Check for duplicate campaign_name and remove duplicates
   duplicate_campaign_names = df1[df1.duplicated(subset=['campaign_name'], keep=False)]
   if not duplicate_campaign_names.empty:
       print("Duplicate campaign names found:")
       print(duplicate_campaign_names)
   else:
       print("No duplicate campaign names found.")

   df1 = df1.drop_duplicates(subset=['campaign_name'], keep='first')

   # Check for null values in df1
   null_campaign_data = df1.isnull().sum()
   if null_campaign_data.any():
       print("Null values in campaign data:")
       print(null_campaign_data)
   else:
       print("No null values in campaign data.")

   # Convert percentage
   df1['discount'] = df1['discount'].apply(standardize_percentage)

   # Save the cleaned DataFrame 
   df1.to_csv("Cleaned Data/cleaned_campaign_data.csv", index=False)
   print("Data cleaned and saved as 'cleaned_campaign_data.csv'")
   ```

2. **Process the transactional campaign data**:
   ```python
   # Load the transactional campaign data
   csv_path = "Raw Data/transactional_campaign_data.csv"
   df2 = pd.read_csv(csv_path)

   # Identify and drop unnamed columns
   unnamed_columns = [col for col in df2.columns if col.lower().startswith('unnamed')]
   if unnamed_columns:
       df2.drop(columns=unnamed_columns, inplace=True)

   # Standardize campaign_id
   df2['campaign_id'] = df2['campaign_id'].str.replace('CAMPAIGN', 'C')

   # Standardize transaction_date
   df2['transaction_date'] = pd.to_datetime(df2['transaction_date'], errors='coerce').dt.strftime('%Y-%m-%d')

   # Standardize estimated arrival
   df2.rename(columns={'estimated arrival': 'estimated_arrival_in_days'}, inplace=True)
   df2['estimated_arrival_in_days'] = df2['estimated_arrival_in_days'].apply(lambda x: re.sub(r'\D', '', str(x)))
   df2['estimated_arrival_in_days'] = pd.to_numeric(df2['estimated_arrival_in_days'], errors='coerce')

   # Check for duplicate order_ids
   duplicate_order_ids = df2[df2.duplicated(subset=['order_id'], keep=False)]
   if not duplicate_order_ids.empty:
       print("Duplicate order IDs found:")
       print(duplicate_order_ids)
   else:
       print("No duplicate order IDs found.")

   df2 = df2.drop_duplicates(subset=['order_id'], keep='first')

   # Check for null values in df2
   null_transactional_data = df2.isnull().sum()
   if null_transactional_data.any():
       print("Null values in transactional campaign data:")
       print(null_transactional_data)
   else:
       print("No null values in transactional campaign data.")

   # Save the cleaned DataFrame
   df2.to_csv("Cleaned Data/cleaned_transactional_campaign_data.csv", index=False)
   print("Data cleaned and saved as 'cleaned_transactional_campaign_data.csv'")
   ```

## Logging

Each script is configured to log its operations to a log file located in the same directory as the script. The log files are named as follows:

- **Business Department**: `business_department.log`
- **Customer Management Department**: `customer_management_department.log`
- **Operations Department**: `Logs_Operations.txt`
- **Enterprise Department**: `enterprise_department.log`
- **Marketing Department**: `marketing_department.log`

The logs provide detailed information about the operations performed by each script, including data transformations, duplicate handling, and any errors encountered.


## Date Dimension

### Script: `tl_date_dimension.py`

This script handles the creation and loading of the date dimension table for the data warehouse.

#### Functions

- `generate_date_dimension(start_date, end_date)`: Generates a date dimension DataFrame.
  - Creates a DataFrame with a row for each date between `start_date` and `end_date`.
  - Adds columns for year, quarter, month, day, weekday, and other date-related attributes.

#### Usage

1. **Generate the date dimension**:
   start_date = '2020-01-01'
   end_date = '2030-12-31'
   df_date_dimension = generate_date_dimension(start_date, end_date)

2. **Save the date dimension to CSV**:
   df_date_dimension.to_csv('Cleaned Data/date_dimension.csv', index=False)

## Time Dimension

### Script: `tl_time_dimension.py`

This script handles the creation and loading of the time dimension table for the data warehouse.

#### Functions

- `generate_time_dimension(start_time, end_time, freq)`: Generates a time dimension DataFrame.
  - Creates a DataFrame with a row for each time interval between `start_time` and `end_time` based on the specified frequency (`freq`).
  - Adds columns for hour, minute, second, and other time-related attributes.

#### Usage

1. **Generate the time dimension**:
   start_time = '00:00:00'
   end_time = '23:59:59'
   freq = '1T'  # 1-minute frequency
   df_time_dimension = generate_time_dimension(start_time, end_time, freq)

2. **Save the time dimension to CSV**:
   df_time_dimension.to_csv('Cleaned Data/time_dimension.csv', index=False)

## Product Dimension

### Script: `tl_product_dimension.py`

This script handles the creation and loading of the product dimension table for the data warehouse.

#### Functions

- `generate_product_dimension(product_data)`: Generates a product dimension DataFrame.
  - Takes raw product data and transforms it into a standardized product dimension table.
  - Adds columns for product ID, product name, category, and other product-related attributes.

#### Usage

1. **Load the raw product data**:
   product_data = pd.read_csv('Raw Data/product_data.csv')

2. **Generate the product dimension**:
   df_product_dimension = generate_product_dimension(product_data)

3. **Save the product dimension to CSV**:
   df_product_dimension.to_csv('Cleaned Data/product_dimension.csv', index=False)

## Customer Dimension

### Script: `tl_customer_dimension.py`

This script handles the creation and loading of the customer dimension table for the data warehouse.

#### Functions

- `generate_customer_dimension(customer_data)`: Generates a customer dimension DataFrame.
  - Takes raw customer data and transforms it into a standardized customer dimension table.
  - Adds columns for customer ID, name, address, and other customer-related attributes.

#### Usage

1. **Load the raw customer data**:
   customer_data = pd.read_csv('Raw Data/customer_data.csv')

2. **Generate the customer dimension**:
   df_customer_dimension = generate_customer_dimension(customer_data)

3. **Save the customer dimension to CSV**:
   df_customer_dimension.to_csv('Cleaned Data/customer_dimension.csv', index=False)

## Sales Fact Table

### Script: `tl_sales_fact.py`

This script handles the creation and loading of the sales fact table for the data warehouse.

#### Functions

- `generate_sales_fact(sales_data)`: Generates a sales fact DataFrame.
  - Takes raw sales data and transforms it into a standardized sales fact table.
  - Adds columns for sales ID, date, time, product ID, customer ID, quantity, and other sales-related attributes.

#### Usage

1. **Load the raw sales data**:
   sales_data = pd.read_csv('Raw Data/sales_data.csv')

2. **Generate the sales fact table**:
   df_sales_fact = generate_sales_fact(sales_data)

3. **Save the sales fact table to CSV**:
   df_sales_fact.to_csv('Cleaned Data/sales_fact.csv', index=False)


## Loading and DAG Files

### General Summary

The loading and DAG (Directed Acyclic Graph) files in this repository work together to orchestrate the ETL process, ensuring that data is extracted, transformed, and loaded in a systematic and efficient manner. These files are responsible for scheduling, managing dependencies, and executing the ETL tasks in the correct order.

### Loading Scripts

The loading scripts are responsible for loading the cleaned and transformed data into the data warehouse. These scripts typically read the cleaned data from CSV files and insert it into the appropriate tables in the data warehouse.

#### Example Loading Script: `load_data.py`

This script handles the loading of various dimension and fact tables into the data warehouse.

#### Functions

- `load_date_dimension()`: Loads the date dimension data into the data warehouse.
- `load_time_dimension()`: Loads the time dimension data into the data warehouse.
- `load_product_dimension()`: Loads the product dimension data into the data warehouse.
- `load_customer_dimension()`: Loads the customer dimension data into the data warehouse.
- `load_sales_fact()`: Loads the sales fact data into the data warehouse.

#### Usage

1. **Load the date dimension data**:
   load_date_dimension()

2. **Load the time dimension data**:
   load_time_dimension()

3. **Load the product dimension data**:
   load_product_dimension()

4. **Load the customer dimension data**:
   load_customer_dimension()

5. **Load the sales fact data**:
   load_sales_fact()

### DAG Files

The DAG files define the workflow for the ETL process, specifying the order in which tasks should be executed and managing dependencies between tasks. These files are typically used with workflow management tools like Apache Airflow.

#### Example DAG File: `etl_dag.py`

This file defines the ETL workflow for the Shopzada project.

#### Components

- **Tasks**: Each task represents a step in the ETL process, such as extracting raw data, transforming it, or loading it into the data warehouse.
- **Dependencies**: The DAG file specifies the dependencies between tasks, ensuring that tasks are executed in the correct order.

#### Usage

1. **Define the tasks**:
   extract_task = PythonOperator(task_id='extract', python_callable=extract_data)
   transform_task = PythonOperator(task_id='transform', python_callable=transform_data)
   load_task = PythonOperator(task_id='load', python_callable=load_data)

2. **Set task dependencies**:
   extract_task >> transform_task >> load_task

## Conclusion

This repository provides a comprehensive ETL pipeline for the Shopzada project, with separate scripts for the Business, Customer Management, Operations, Enterprise, and Marketing departments. Each script is designed to handle specific data extraction, transformation, and loading tasks, ensuring that the data is cleaned and standardized before being saved to the appropriate format. The loading and DAG files work together to automate and manage the ETL process for the Shopzada project. The loading scripts handle the insertion of cleaned data into the data warehouse, while the DAG files define the workflow and manage task dependencies, ensuring that the ETL process is executed efficiently and correctly.
