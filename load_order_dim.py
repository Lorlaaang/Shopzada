from sqlalchemy import create_engine
import pandas as pd
from dotenv import load_dotenv
import os

# Load environment variables from the .env file
load_dotenv()

db_url = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"

table_name = 'user_dimension'
merge_key = ['order_id']

# FILE NAMES AND LOCATIONS
clean_order_data_path1 = 'Operations Department/Cleaned Data/cleaned_order_data_20200101-20200701.parquet'
clean_order_data_path2 = 'Operations Department/Cleaned Data/cleaned_order_data_20200701-20211001.pickle'
clean_order_data_path3 = 'Operations Department/Cleaned Data/cleaned_order_data_20211001-20220101.csv'
clean_order_data_path4 = 'Operations Department/Cleaned Data/cleaned_order_data_20220101-20221201.xlsx' 
clean_order_data_path5 = 'Operations Department/Cleaned Data/cleaned_order_data_20221201-20230601.json'
clean_transaction_campaign_data_path = 'Marketing Department/Cleaned Data/cleaned_transactional_campaign_data.csv'
clean_order_delays_path = 'Operations Department/Cleaned Data/cleaned_order_delays.html'

# read files
order_data_1_df = pd.read_parquet(clean_order_data_path1)
order_data_2_df = pd.read_pickle(clean_order_data_path2)
order_data_3_df = pd.read_csv(clean_order_data_path3)
order_data_4_df = pd.read_excel(clean_order_data_path4, engine='openpyxl')
order_data_5_df = pd.read_csv(clean_order_data_path5)
transactional_campaign_data_df = pd.read_csv(clean_transaction_campaign_data_path)
order_delays_df = pd.read_json(clean_order_delays_path)

# merge files for loading
order_data_1_df_with_discount_avail = pd.merge(order_data_1_df, transactional_campaign_data_df, on=merge_key, how='outer')
order_data_1_df_with_discount_avail_with_delays = pd.merge(order_data_1_df_with_discount_avail, order_delays_df, on=merge_key, how='outer')

order_data_2_df_with_discount_avail = pd.merge(order_data_2_df, transactional_campaign_data_df, on=merge_key, how='outer')
order_data_2_df_with_discount_avail_with_delays = pd.merge(order_data_2_df_with_discount_avail, order_delays_df, on=merge_key, how='outer')

order_data_3_df_with_discount_avail = pd.merge(order_data_3_df, transactional_campaign_data_df, on=merge_key, how='outer')
order_data_3_df_with_discount_avail_with_delays = pd.merge(order_data_3_df_with_discount_avail, order_delays_df, on=merge_key, how='outer')

order_data_4_df_with_discount_avail = pd.merge(order_data_4_df, transactional_campaign_data_df, on=merge_key, how='outer')
order_data_4_df_with_discount_avail_with_delays = pd.merge(order_data_4_df_with_discount_avail, order_delays_df, on=merge_key, how='outer')

order_data_5_df_with_discount_avail = pd.merge(order_data_5_df, transactional_campaign_data_df, on=merge_key, how='outer')
order_data_5_df_with_discount_avail_with_delays = pd.merge(order_data_5_df_with_discount_avail, order_delays_df, on=merge_key, how='outer')

order_data_combined = pd.concat([order_data_1_df_with_discount_avail_with_delays, order_data_2_df_with_discount_avail_with_delays, order_data_3_df_with_discount_avail_with_delays, order_data_4_df_with_discount_avail_with_delays, order_data_5_df_with_discount_avail_with_delays], ignore_index=True)

print(order_data_combined.head())

# log here combined all
# rename columns to fit db
# user_data_with_credit_with_job_df.columns = ['user_' + col if (col != 'user_id' and col != 'user_type')  else col for col in user_data_with_credit_with_job_df.columns]

# load data into database
# engine = create_engine(db_url)
# user_data_with_credit_with_job_df.to_sql(table_name, engine, if_exists='append', index=False)



