import pandas as pd
import logging
import os
from pathlib import Path

# Configure logging
log_path = Path(__file__).parent / 'business_department.log'
logging.basicConfig(filename=log_path, level=logging.INFO, 
                    format='%(asctime)s:%(levelname)s:%(message)s')

# Base directory
base_dir = Path(__file__).parent.parent

# File paths
product_list_path = base_dir / 'Data Source' / 'business_department' / 'product_list.xlsx'
cleaned_product_list_path = base_dir / 'et_business_department' / 'Cleaned Data' / 'cleaned_product_list.csv'

# Load the dataset
try:
    df_product_list = pd.read_excel(product_list_path)
    logging.info("Loaded product_list.xlsx successfully.")
except Exception as e:
    logging.error(f"Error loading product_list.xlsx: {e}")

# Business Department Functions
def clean_product_list(df):
    # Drop the first column
    df = df.drop(df.columns[0], axis=1)
    
    # Clean product_id
    def clean_product_id(product_id):
        if isinstance(product_id, str):
            product_id = product_id.replace(" ", "").replace("-", "").upper()
            product_id = 'P' + product_id[7:]
        return product_id

    df['product_id'] = df['product_id'].apply(clean_product_id)

    # Clean product_name
    def clean_product_name(product_name):
        if isinstance(product_name, str):
            product_name = product_name.strip().title()
        return product_name

    df['product_name'] = df['product_name'].apply(clean_product_name)

    # Correct typos and reassign product types
    typo_corrections = {
        'toolss': 'TOOLS',
        'cosmetic': 'COSMETICS'
    }
    product_type_reassignments = {
        'BALLOON': 'TOYS AND ENTERTAINMENT',
        'BOW TIE': 'ACCESSORY',
        'JEWELRY': 'ACCESSORY',
        'WASHING MACHINE': 'APPLIANCES'
    }
    merged_product_types = {
        'TECHNOLOGY': 'ELECTRONICS AND TECHNOLOGY',
        'STATIONARY': 'STATIONARY AND SCHOOL SUPPLIES'
    }

    def correct_typos(product_type):
        return typo_corrections.get(product_type, product_type)

    def reassign_product_types(product_type):
        return product_type_reassignments.get(product_type, product_type)

    def merge_product_types(product_type):
        return merged_product_types.get(product_type, product_type)

    df['product_type'] = df['product_type'].apply(correct_typos).apply(reassign_product_types).apply(merge_product_types)

    # Ensure all values in PRODUCT_NAME and PRODUCT_TYPE are uppercase
    df['product_name'] = df['product_name'].str.upper()
    df['product_type'] = df['product_type'].str.upper()

    # Drop duplicates based on product_id, keeping the most recent occurrence
    df = df.sort_values(by='product_id').drop_duplicates(subset=['product_id'], keep='last')

    # Ensure PRODUCT_ID is alphanumeric
    df['product_id'] = df['product_id'].astype(str)

    # Ensure PRODUCT_NAME and PRODUCT_TYPE are strings
    df['product_name'] = df['product_name'].astype(str)
    df['product_type'] = df['product_type'].astype(str)

    # Remove vague categories like OTHERS
    df = df[df['product_type'] != 'OTHERS']

    # Ensure the dataset is neat and consistently formatted
    df = df.reset_index(drop=True)

    return df

def clean_product_type(df):
    # Fill null values in product_type with 'UNKNOWN'
    df['product_type'] = df['product_type'].fillna('UNKNOWN')

    # Custom function to capitalize words except "and"
    def capitalize_except_and(product_type):
        words = product_type.split()
        capitalized_words = [word.capitalize() if word.lower() != 'and' else word.lower() for word in words]
        return ' '.join(capitalized_words)

    df['product_type'] = df['product_type'].apply(capitalize_except_and)
    
    return df

def standardize_price(df):
    df['price'] = df['price'].round(2)
    return df

# Apply Business Department Functions
try:
    df_product_list = clean_product_list(df_product_list)
    logging.info("Cleaned product_list successfully.")
    df_product_list = clean_product_type(df_product_list)
    logging.info("Cleaned product_type successfully.")
    df_product_list = standardize_price(df_product_list)
    logging.info("Standardized price successfully.")
except Exception as e:
    logging.error(f"Error processing product_list: {e}")

# Export cleaned data to CSV
try:
    df_product_list.to_csv(cleaned_product_list_path, index=False)
    logging.info("Exported cleaned product_list to CSV successfully.")
except Exception as e:
    logging.error(f"Error exporting cleaned product_list to CSV: {e}")

# Verify the changes
print("\nProduct List after standardization:")
print(df_product_list.head(10))