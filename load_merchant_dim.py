from sqlalchemy import create_engine
import pandas as pd
from dotenv import load_dotenv
import os

# Load environment variables from the .env file
load_dotenv()

db_url = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"

table_name = 'merchant_dimension'

# FILE NAMES AND LOCATIONS
clean_merchant_data = 'Enterprise Department/Cleaned Data/cleaned_merchant_data.html'

# read files
merchant_data_df = pd.read_html(clean_merchant_data)[0]

merchant_data_df.columns = ['merchant_' + col if (col != 'merchant_id') else col for col in merchant_data_df.columns]

# load data into database
engine = create_engine(db_url)
merchant_data_df.to_sql(table_name, engine, if_exists='append', index=False)
