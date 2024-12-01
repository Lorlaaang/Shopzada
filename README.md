# Shopzada ETL Pipeline

This repository contains ETL (Extract, Transform, Load) scripts for the Shopzada Data Warehouse project. The scripts are organized into different departments: Business, Customer Management, Operations, Enterprise, and Marketing. Each script is responsible for extracting raw data, transforming it, and loading the cleaned data into the appropriate format.

## Table of Contents

- [Business Department](#business-department)
- [Customer Management Department](#customer-management-department)
- [Operations Department](#operations-department)
- [Enterprise Department](#enterprise-department)
- [Marketing Department](#marketing-department)
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

## Conclusion

This repository provides a comprehensive ETL pipeline for the Shopzada project, with separate scripts for the Business, Customer Management, Operations, Enterprise, and Marketing departments. Each script is designed to handle specific data extraction, transformation, and loading tasks, ensuring that the data is cleaned and standardized before being saved to the appropriate format.
