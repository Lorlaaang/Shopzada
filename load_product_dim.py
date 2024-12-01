from sqlalchemy import create_engine
import pandas as pd
from dotenv import load_dotenv
import os

# Load environment variables from the .env file
load_dotenv()

db_url = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"

table_name = 'product_dimension'

# FILE NAMES AND LOCATIONS
clean_product_list = 'Business Department/Cleaned Data/cleaned_product_list.csv'

# read files
product_list_df = pd.read_csv(clean_product_list)

product_list_df.columns = ['product_' + col if (col == 'price') else col for col in product_list_df.columns]

# load data into database
engine = create_engine(db_url)
product_list_df.to_sql(table_name, engine, if_exists='append', index=False)
