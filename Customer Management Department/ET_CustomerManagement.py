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

cleaned_user_credit_card_path = r'Customer Management Department\Cleaned Data\cleaned_user_credit_card.csv'
cleaned_user_data_path = r'Customer Management Department\Cleaned Data\cleaned_user_data.csv'
cleaned_user_job_path = r'Customer Management Department\Cleaned Data\cleaned_user_job.csv'

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
            user_id = 'U' + user_id[1:]
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
            user_id = 'U' + user_id[1:]
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
    
    return df

def clean_user_job(df):
    # Drop the first column
    df = df.drop(df.columns[0], axis=1)

    # Clean user_id
    def clean_user_id(user_id):
        if isinstance(user_id, str):
            user_id = user_id.replace(" ", "").replace("-", "").upper()
            user_id = 'U' + user_id[1:]
        return user_id

    df['user_id'] = df['user_id'].apply(clean_user_id)

    # Clean name
    def clean_name(name):
        if isinstance(name, str):
            name = name.strip().title()
            name = name.replace(".", ". ").replace("-", "- ")
        return name

    df['name'] = df['name'].apply(clean_name)
    
    return df

# Function to check for duplicates in any DataFrame
def check_and_resolve_duplicates(df, subset):
    # Identify duplicates
    duplicates = df[df.duplicated(subset=subset, keep=False)]
    logging.info(f"Duplicates in {subset}: {duplicates}")

    # Resolve duplicates by keeping the first occurrence
    df = df.drop_duplicates(subset=subset, keep='first')
    return df

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

# Check and resolve duplicates
try:
    df_user_credit_card = check_and_resolve_duplicates(df_user_credit_card, subset=['user_id', 'credit_card_number'])
    df_user_data = check_and_resolve_duplicates(df_user_data, subset=['user_id'])
    df_user_job = check_and_resolve_duplicates(df_user_job, subset=['user_id'])
    logging.info("Checked and resolved duplicates successfully.")
except Exception as e:
    logging.error(f"Error checking and resolving duplicates: {e}")

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