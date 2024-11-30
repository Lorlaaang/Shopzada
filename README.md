# ETL-Pipelines

Data Warehouse project code

### Description of Repository

This script performs data extraction, cleaning, and standardization for datasets from the Business Department and Customer Management Department. The script handles various tasks such as dropping unnecessary columns, standardizing IDs, cleaning names, resolving duplicates, and more.

## File Paths
The script processes the following files:

product_list.xlsx (Business Department)
user_credit_card.pickle (Customer Management Department)
user_data.json (Customer Management Department)
user_job.csv (Customer Management Department)

Functions

## Business Department Functions

#### clean_product_list(df)
Description: Cleans and standardizes the product_list DataFrame.
##### Tasks:
- Drops the first column.
- Cleans product_id by removing spaces, enforcing uppercase, and removing hyphens. Formats as P#####.
- Generates new unique IDs for duplicate product_id entries.
- Cleans product_name by removing leading/trailing spaces and converting to title case.

#### clean_product_type(df)
Description: Cleans and standardizes the product_type column in the product_list DataFrame.
##### Tasks:
- Fills null values in product_type with 'Unknown'.
- Capitalizes words in product_type except for "and".

#### standardize_price(df)
Description: Standardizes the price column in the product_list DataFrame.
##### Tasks:
- Rounds the price column to two decimal places.


# Customer Department Functions

#### clean_user_credit_card(df)
Description: Cleans and standardizes the user_credit_card DataFrame.
##### Tasks:
- Cleans user_id by removing spaces, enforcing uppercase, and removing hyphens. Formats as U#####.
- Cleans name by removing leading/trailing spaces, converting to title case, and handling middle initials or names with hyphens.

#### clean_user_data(df)
Description: Cleans and standardizes the user_data DataFrame.
##### Tasks:
- Cleans user_id by removing spaces, enforcing uppercase, and removing hyphens. Formats as U#####.
- Standardizes creation_date and birthdate to YYYY-MM-DDTHH:MM:SS format.
- Cleans name by removing leading/trailing spaces, converting to title case, and handling middle initials or names with hyphens.
- Capitalizes country names.

#### clean_user_job(df)
Description: Cleans and standardizes the user_job DataFrame.
##### Tasks:
- Drops the first column.
- Cleans user_id by removing spaces, enforcing uppercase, and removing hyphens. Formats as U#####.
- Cleans name by removing leading/trailing spaces, converting to title case, and handling middle initials or names with hyphens.
- Prints distinct values of job_title and job_level.
  
# Utility Functions

### check_and_resolve_duplicates(df, subset, keep='first')
Description: Checks for and resolves duplicates in any DataFrame.
Parameters:
df: The DataFrame to check for duplicates.
subset: The subset of columns to consider for identifying duplicates.
keep: Specifies which duplicates to keep. Default is 'first'.
#### Tasks:
- Identifies duplicates based on the specified subset of columns.
- Resolves duplicates by keeping the specified occurrence.


# Usage
1. **Load the datasets:**
```python
df_product_list = pd.read_excel(product_list_path)
df_user_credit_card = pd.read_pickle(user_credit_card_path)
df_user_data = pd.read_json(user_data_path)
df_user_job = pd.read_csv(user_job_path)
```

2. **Apply Business Department Functions:**

```python
df_product_list = clean_product_list(df_product_list)
df_product_list = clean_product_type(df_product_list)
df_product_list = standardize_price(df_product_list)
```

3. Apply Customer Department Functions:

```python
df_user_credit_card = clean_user_credit_card(df_user_credit_card)
df_user_data = clean_user_data(df_user_data)
df_user_job = clean_user_job(df_user_job)
```

4. Check and resolve duplicates:
   
```python
df_product_list = check_and_resolve_duplicates(df_product_list, subset=['product_name'])
df_user_credit_card = check_and_resolve_duplicates(df_user_credit_card, subset=['credit_card_number', 'issuing_bank'])
df_user_data = check_and_resolve_duplicates(df_user_data, subset=['user_id'])
df_user_job = check_and_resolve_duplicates(df_user_job, subset=['user_id'])
```
5. Verify the changes:

```python
print("\nProduct List after standardization:")
print(df_product_list.head(10))

print("\nUser Credit Card after standardization:")
print(df_user_credit_card.head(10))

print("\nUser Data after standardization:")
print(df_user_data.head(10))

print("\nUser Job after standardization:")
print(df_user_job.head(10))
```

###### This README provides an overview of the functions and their purposes, as well as instructions on how to use the script to clean and standardize the data.
