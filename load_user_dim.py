from sqlalchemy import create_engine
import pandas as pd
from dotenv import load_dotenv
import os

# Load environment variables from the .env file
load_dotenv()

db_url = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"

table_name = 'user_dimension'
merge_key = ['user_id', 'name']

# FILE NAMES AND LOCATIONS
clean_user_data = 'Customer Management Department/Cleaned Data/cleaned_user_data.csv'
clean_user_credit_path = 'Customer Management Department/Cleaned Data/cleaned_user_credit_card.csv'
clean_user_job = 'Customer Management Department/Cleaned Data/cleaned_user_job.csv'

# read files
user_data_df = pd.read_csv(clean_user_data)
user_credit_df = pd.read_csv(clean_user_credit_path)
user_job_df = pd.read_csv(clean_user_job)

# merge files for loading
user_data_with_credit_df = pd.merge(user_data_df, user_credit_df, on=merge_key, how='outer')
user_data_with_credit_with_job_df = pd.merge(user_data_with_credit_df, user_job_df, on=merge_key, how='outer')

# log here combined all

# rename columns to fit db
user_data_with_credit_with_job_df.columns = ['user_' + col if (col != 'user_id' and col != 'user_type')  else col for col in user_data_with_credit_with_job_df.columns]

# load data into database
engine = create_engine(db_url)
user_data_with_credit_with_job_df.to_sql(table_name, engine, if_exists='append', index=False)



