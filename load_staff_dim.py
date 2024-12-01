from sqlalchemy import create_engine
import pandas as pd
from dotenv import load_dotenv
import os

# Load environment variables from the .env file
load_dotenv()

db_url = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"

table_name = 'staff_dimension'

# FILE NAMES AND LOCATIONS
clean_staff_data = 'Enterprise Department/Cleaned Data/cleaned_staff_data.html'

# read files
staff_data_df = pd.read_html(clean_staff_data)[0]

staff_data_df.columns = ['staff_' + col if (col != 'staff_id') else col for col in staff_data_df.columns]

# load data into database
engine = create_engine(db_url)
staff_data_df.to_sql(table_name, engine, if_exists='append', index=False)
