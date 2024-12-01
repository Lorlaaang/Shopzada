import pandas as pd
import logging
import os

# Configure logging
log_path = os.path.join(os.path.dirname(__file__), 'business_department.log')
logging.basicConfig(filename=log_path, level=logging.INFO, 
                    format='%(asctime)s:%(levelname)s:%(message)s')

# File paths
product_list_path = r'Business Department\Raw Data\product_list.xlsx'
cleaned_product_list_path = r'Business Department\Cleaned Data\cleaned_product_list.csv'

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

    # Get duplicate product IDs
    duplicate_product_ids = df[df.duplicated(['product_id'], keep=False)]

    # Generate new unique IDs for duplicate product_id entries
    def generate_unique_id(existing_ids):
        max_id = max([int(id[1:]) for id in existing_ids if id.startswith('P')])
        new_id = f'P{max_id + 1:05d}'
        return new_id

    existing_ids = set(df['product_id'])
    for index, row in duplicate_product_ids.iterrows():
        new_id = generate_unique_id(existing_ids)
        df.at[index, 'product_id'] = new_id
        existing_ids.add(new_id)

    # Clean product_name
    def clean_product_name(product_name):
        return product_name.strip().title()

    df['product_name'] = df['product_name'].apply(clean_product_name)
    
    return df

def clean_product_type(df):
    # Fill null values in product_type with 'Unknown'
    df['product_type'] = df['product_type'].fillna('Unknown')

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