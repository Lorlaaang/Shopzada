import pandas as pd
import hashlib
import logging
import os

# Configure logging
log_path = os.path.join(os.path.dirname(__file__), 'customer_management_department.log')
logging.basicConfig(filename=log_path, level=logging.INFO, 
                    format='%(asctime)s:%(levelname)s:%(message)s')

# File paths
user_credit_card_path = r'Customer Management Department\Raw Data\user_credit_card.pickle'
user_data_path = r'Customer Management Department\Raw Data\user_data.json'
user_job_path = r'Customer Management Department\Raw Data\user_job.csv'

cleaned_user_credit_card_path = r'Shopzada\Customer Management Department\Cleaned Data\cleaned_user_credit_card.csv'
cleaned_user_data_path = r'Shopzada\Customer Management Department\Cleaned Data\cleaned_user_data.csv'
cleaned_user_job_path = r'Shopzada\Customer Management Department\Cleaned Data\cleaned_user_job.csv'

# Load the datasets
try:
    df_user_credit_card = pd.read_pickle(user_credit_card_path)
    logging.info("Loaded user_credit_card.pickle successfully.")
except Exception as e:
    logging.error(f"Error loading user_credit_card.pickle: {e}")

try:
    df_user_data = pd.read_json(user_data_path)
    logging.info("Loaded user_data.json successfully.")
except Exception as e:
    logging.error(f"Error loading user_data.json: {e}")

try:
    df_user_job = pd.read_csv(user_job_path)
    logging.info("Loaded user_job.csv successfully.")
except Exception as e:
    logging.error(f"Error loading user_job.csv: {e}")

# Customer Department Functions
def clean_user_credit_card(df):
    # Clean user_id
    def clean_user_id(user_id):
        if isinstance(user_id, str):
            user_id = user_id.replace(" ", "").replace("-", "").upper()
            if user_id.startswith('USER'):
                user_id = 'U' + user_id[4:]
            elif not user_id.startswith('U'):
                user_id = 'U' + user_id
        return user_id

    df['user_id'] = df['user_id'].apply(clean_user_id)

    # Clean name
    def clean_name(name):
        if isinstance(name, str):
            name = name.strip().title()
            name = name.replace(".", ". ").replace("-", "- ")
        return name

    df['name'] = df['name'].apply(clean_name)

    # Hash credit card number
    def hash_credit_card_number(credit_card_number):
        return hashlib.sha256(str(credit_card_number).encode('utf-8')).hexdigest()

    df['credit_card_number'] = df['credit_card_number'].apply(hash_credit_card_number)

    return df

def clean_user_data(df):
    # Clean user_id
    def clean_user_id(user_id):
        if isinstance(user_id, str):
            user_id = user_id.replace(" ", "").replace("-", "").upper()
            if user_id.startswith('USER'):
                user_id = 'U' + user_id[4:]
            elif not user_id.startswith('U'):
                user_id = 'U' + user_id
        return user_id

    df['user_id'] = df['user_id'].apply(clean_user_id)

    # Standardize creation_date and birthdate to YYYY-MM-DDTHH:MM:SS format
    def standardize_date(date):
        try:
            return pd.to_datetime(date).strftime('%Y-%m-%dT%H:%M:%S')
        except ValueError:
            return 'Invalid Data'

    df['creation_date'] = df['creation_date'].apply(standardize_date)
    df['birthdate'] = df['birthdate'].apply(standardize_date)

    # Clean name
    def clean_name(name):
        if isinstance(name, str):
            name = name.strip().title()
            name = name.replace(".", ". ").replace("-", "- ")
        return name

    df['name'] = df['name'].apply(clean_name)

    # Capitalize country names
    df['country'] = df['country'].apply(lambda x: x.upper() if isinstance(x, str) else x)

    # Capitalize the first letter of all words in the street column
    def capitalize_street(street):
        if isinstance(street, str):
            return street.title()
        return street

    df['street'] = df['street'].apply(capitalize_street)
    
    return df

def clean_user_job(df):
    # Drop the first column
    df = df.drop(df.columns[0], axis=1)

    # Clean user_id
    def clean_user_id(user_id):
        if isinstance(user_id, str):
            user_id = user_id.replace(" ", "").replace("-", "").upper()
            if user_id.startswith('USER'):
                user_id = 'U' + user_id[4:]
            elif not user_id.startswith('U'):
                user_id = 'U' + user_id
        return user_id

    df['user_id'] = df['user_id'].apply(clean_user_id)

    # Clean name
    def clean_name(name):
        if isinstance(name, str):
            name = name.strip().title()
            name = name.replace(".", ". ").replace("-", "- ")
        return name

    df['name'] = df['name'].apply(clean_name)

      # Fill null values in job_level with 'N/A'
    df['job_level'] = df['job_level'].fillna('N/A')
    
    return df

# Function to check for duplicates based on user_id
def remove_user_id_duplicates(df):
    # Identify duplicates based on user_id
    duplicates_before = df[df.duplicated(subset=['user_id'], keep=False)]
    logging.info(f"Number of duplicates before removal: {len(duplicates_before)}")
    
    # Print the details of duplicates
    if not duplicates_before.empty:
        logging.info("Duplicate user_id entries:")
        for user_id in duplicates_before['user_id'].unique():
            dup_rows = duplicates_before[duplicates_before['user_id'] == user_id]
            logging.info(f"\nUser ID {user_id} has {len(dup_rows)} duplicate entries:")
            logging.info(dup_rows)
    
    # Remove duplicates, keeping the first occurrence
    df_cleaned = df.drop_duplicates(subset=['user_id'], keep='first')
    
    # Log the number of rows removed
    rows_removed = len(df) - len(df_cleaned)
    logging.info(f"Rows removed due to duplicate user_id: {rows_removed}")
    
    return df_cleaned

# Apply Customer Department Functions
try:
    df_user_credit_card = clean_user_credit_card(df_user_credit_card)
    logging.info("Cleaned user_credit_card successfully.")
    df_user_data = clean_user_data(df_user_data)
    logging.info("Cleaned user_data successfully.")
    df_user_job = clean_user_job(df_user_job)
    logging.info("Cleaned user_job successfully.")
except Exception as e:
    logging.error(f"Error processing customer data: {e}")

# Remove duplicates based on user_id
try:
    df_user_credit_card = remove_user_id_duplicates(df_user_credit_card)
    df_user_data = remove_user_id_duplicates(df_user_data)
    df_user_job = remove_user_id_duplicates(df_user_job)
    logging.info("Removed user_id duplicates successfully.")
except Exception as e:
    logging.error(f"Error removing user_id duplicates: {e}")

# Export cleaned data to CSV
try:
    df_user_credit_card.to_csv(cleaned_user_credit_card_path, index=False)
    logging.info("Exported cleaned user_credit_card to CSV successfully.")
    df_user_data.to_csv(cleaned_user_data_path, index=False)
    logging.info("Exported cleaned user_data to CSV successfully.")
    df_user_job.to_csv(cleaned_user_job_path, index=False)
    logging.info("Exported cleaned user_job to CSV successfully.")
except Exception as e:
    logging.error(f"Error exporting cleaned data to CSV: {e}")

# Verify the changes
print("\nUser Credit Card after standardization:")
print(df_user_credit_card.head(10))

print("\nUser Data after standardization:")
print(df_user_data.head(10))

print("\nUser Job after standardization:")
print(df_user_job.head(10))