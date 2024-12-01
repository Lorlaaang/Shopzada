from sqlalchemy import create_engine
import pandas as pd
from dotenv import load_dotenv
import os

# Load environment variables from the .env file
load_dotenv()

db_url = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"

table_name = 'order_dimension'
merge_key1 = ['order_id', 'estimated_arrival_in_days', 'transaction_date']
merge_key2 = ['order_id']

# FILE NAMES AND LOCATIONS
clean_order_data_path1 = 'et_operations_department/Cleaned Data/cleaned_order_data_20200101-20200701.csv'
clean_order_data_path2 = 'et_operations_department/Cleaned Data/cleaned_order_data_20200701-20211001.csv'
clean_order_data_path3 = 'et_operations_department/Cleaned Data/cleaned_order_data_20211001-20220101.csv'
clean_order_data_path4 = 'et_operations_department/Cleaned Data/cleaned_order_data_20220101-20221201.csv' 
clean_order_data_path5 = 'et_operations_department/Cleaned Data/cleaned_order_data_20221201-20230601.csv'
clean_order_data_path6 = 'et_operations_department/Cleaned Data/cleaned_order_data_20230601-20240101.csv'
clean_transaction_campaign_data_path = 'et_marketing_department/Cleaned Data/cleaned_transactional_campaign_data.csv'
clean_order_delays_path = 'et_operations_department/Cleaned Data/cleaned_order_delays.csv'

# read files
order_data_1_df = pd.read_csv(clean_order_data_path1)
order_data_2_df = pd.read_csv(clean_order_data_path2)
order_data_3_df = pd.read_csv(clean_order_data_path3)
order_data_4_df = pd.read_csv(clean_order_data_path4)
order_data_5_df = pd.read_csv(clean_order_data_path5)
order_data_6_df = pd.read_csv(clean_order_data_path6)
transactional_campaign_data_df = pd.read_csv(clean_transaction_campaign_data_path)
order_delays_df = pd.read_csv(clean_order_delays_path)

order_data_combined = pd.concat([order_data_1_df, order_data_2_df, order_data_3_df, order_data_4_df, order_data_5_df, order_data_6_df], ignore_index=True)

# merge files for loading
order_data_with_discount_avail_df = pd.merge(order_data_combined, transactional_campaign_data_df, on=merge_key1, how='outer')
order_data_with_discount_avail_with_delays_df = pd.merge(order_data_with_discount_avail_df, order_delays_df, on=merge_key2, how='outer')

order_data_with_discount_avail_with_delays_df.drop(columns=['user_id', 'transaction_date', 'campaign_id'], inplace=True)

# log here combined all
# fill null to fit db
order_data_with_discount_avail_with_delays_df['availed'] = order_data_with_discount_avail_with_delays_df['availed'].fillna(False)
order_data_with_discount_avail_with_delays_df['delay in days'] = order_data_with_discount_avail_with_delays_df['delay in days'].fillna(0)

# rename columns to fit db
order_data_with_discount_avail_with_delays_df.columns = [col.replace(' ', '_') for col in order_data_with_discount_avail_with_delays_df.columns]
final_order_data_combined = order_data_with_discount_avail_with_delays_df.rename(columns={'availed': 'is_discount_availed'})
final_order_data_combined.columns = ['order_' + col if (col != 'order_id' and col != 'user_type')  else col for col in final_order_data_combined.columns]

# load data into database
engine = create_engine(db_url)
final_order_data_combined.to_sql(table_name, engine, if_exists='append', index=False)



