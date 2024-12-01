from sqlalchemy import create_engine
import pandas as pd
from dotenv import load_dotenv
import os

# Load environment variables from the .env file
load_dotenv()

db_url = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"

table_name = 'campaign_dimension'

# FILE NAMES AND LOCATIONS
clean_campaign_data = 'Marketing Department/Cleaned Data/cleaned_campaign_data.csv'

# read files
campaign_data_df = pd.read_csv(clean_campaign_data)

campaign_data_df.columns = ['campaign_' + col if (col == 'discount') else col for col in campaign_data_df.columns]

# load data into database
engine = create_engine(db_url)
campaign_data_df.to_sql(table_name, engine, if_exists='append', index=False)
