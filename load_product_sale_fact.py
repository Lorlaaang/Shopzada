from sqlalchemy import create_engine
import pandas as pd
from dotenv import load_dotenv
import os

# Load environment variables from the .env file
load_dotenv()

db_url = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"

table_name = 'product_sale_fact'

# FILE NAMES AND LOCATIONS
clean_product_list = 'Business Department/Cleaned Data/cleaned_product_list.csv'
clean_order_data_path1 = 'Operations Department/Cleaned Data/cleaned_order_data_20200101-20200701.csv'
clean_order_data_path2 = 'Operations Department/Cleaned Data/cleaned_order_data_20200701-20211001.csv'
clean_order_data_path3 = 'Operations Department/Cleaned Data/cleaned_order_data_20211001-20220101.csv'
clean_order_data_path4 = 'Operations Department/Cleaned Data/cleaned_order_data_20220101-20221201.csv' 
clean_order_data_path5 = 'Operations Department/Cleaned Data/cleaned_order_data_20221201-20230601.csv'
clean_order_data_path6 = 'Operations Department/Cleaned Data/cleaned_order_data_20230601-20240101.csv'

clean_transaction_campaign_data_path = 'Marketing Department/Cleaned Data/cleaned_transactional_campaign_data.csv'

clean_order_with_merchant_path1 = 'Enterprise Department/Cleaned Data/cleaned_order_with_merchant_data1.parquet'
clean_order_with_merchant_path2 = 'Enterprise Department/Cleaned Data/cleaned_order_with_merchant_data2.parquet'
clean_order_with_merchant_path3 = 'Enterprise Department/Cleaned Data/cleaned_order_with_merchant_data3.csv'
clean_staff_data = 'Enterprise Department/Cleaned Data/cleaned_staff_data.html'
clean_merchant_data = 'Enterprise Department/Cleaned Data/cleaned_merchant_data.html'

clean_line_item_data1 = 'Operations Department/Cleaned Data/cleaned_line_item_data1.csv'
clean_line_item_data2 = 'Operations Department/Cleaned Data/cleaned_line_item_data2.csv'
clean_line_item_data3 = 'Operations Department/Cleaned Data/cleaned_line_item_data3.csv'

# read files
order_data_1_df = pd.read_csv(clean_order_data_path1)
order_data_2_df = pd.read_csv(clean_order_data_path2)
order_data_3_df = pd.read_csv(clean_order_data_path3)
order_data_4_df = pd.read_csv(clean_order_data_path4)
order_data_5_df = pd.read_csv(clean_order_data_path5)
order_data_6_df = pd.read_csv(clean_order_data_path6)
order_data_combined = pd.concat([order_data_1_df, order_data_2_df, order_data_3_df, order_data_4_df, order_data_5_df, order_data_6_df], ignore_index=True)
product_list_df = pd.read_csv(clean_product_list)

transactional_campaign_data_df = pd.read_csv(clean_transaction_campaign_data_path)

order_with_merchant_path1 = pd.read_parquet(clean_order_with_merchant_path1)
order_with_merchant_path2 = pd.read_parquet(clean_order_with_merchant_path2)
order_with_merchant_path3 = pd.read_csv(clean_order_with_merchant_path3)
merchant_data_df = pd.read_html(clean_merchant_data)[0]
staff_data_df = pd.read_html(clean_staff_data)[0]
order_with_merchant_data_combined = pd.concat([order_with_merchant_path1, order_with_merchant_path2, order_with_merchant_path3], ignore_index=True)

line_item_data1 = pd.read_csv(clean_line_item_data1)
line_item_data2 = pd.read_csv(clean_line_item_data2)
line_item_data3 = pd.read_csv(clean_line_item_data3)
line_item_data_combined = pd.concat([line_item_data1, line_item_data2, line_item_data3], ignore_index=True)

# remove rows with merchants not in the merchant data
order_with_merchant_data_combined = order_with_merchant_data_combined.merge(
    merchant_data_df,
    on='merchant_id',  
    how='inner'        
)
order_with_merchant_data_combined.drop(columns=['creation_date', 'street', 'name', 'city', 'state', 'country', 'contact_number'], inplace=True)

# remove rows with merchants not in the merchant data
order_with_merchant_data_combined = order_with_merchant_data_combined.merge(
    staff_data_df,
    on='staff_id',  
    how='inner'        
)
order_with_merchant_data_combined.drop(columns=['creation_date', 'job_level', 'street', 'name', 'city', 'state', 'country', 'contact_number'], inplace=True)

# remove rows with products not in the product data
line_item_data_combined = line_item_data_combined.merge(
    product_list_df,
    on='product_id',  
    how='inner'        
)

line_item_data_combined.drop(columns=['product_name_x', 'product_name_y', 'price_x', 'price_y', 'product_type'], inplace=True)
transactional_campaign_data_df.drop(columns=['availed', 'estimated_arrival_in_days'], inplace=True)
order_data_combined.drop(columns=['estimated_arrival_in_days'], inplace=True)

# merging
line_item_data_combined_with_order = pd.merge(line_item_data_combined, order_data_combined, on=['order_id'], how='inner')
line_item_data_combined_with_order_campaign = pd.merge(line_item_data_combined_with_order, transactional_campaign_data_df, on=['order_id', 'transaction_date'], how='left')

line_item_data_combined_with_order_campaign_merchant_staff = pd.merge(line_item_data_combined_with_order_campaign, order_with_merchant_data_combined, on=['order_id'], how='left')

# fill null to fit db
line_item_data_combined_with_order_campaign_merchant_staff['campaign_id'] = line_item_data_combined_with_order_campaign_merchant_staff['campaign_id'].fillna('C00000')
line_item_data_combined_with_order_campaign_merchant_staff['merchant_id'] = line_item_data_combined_with_order_campaign_merchant_staff['merchant_id'].fillna('M00000')
line_item_data_combined_with_order_campaign_merchant_staff['staff_id'] = line_item_data_combined_with_order_campaign_merchant_staff['staff_id'].fillna('S0000000')

# rename column to fit db
order_data_with_campaign_merchant_staff_products_prices = line_item_data_combined_with_order_campaign_merchant_staff.rename(columns={'transaction_date': 'date_id'})

print(len(order_data_with_campaign_merchant_staff_products_prices))
print(order_data_with_campaign_merchant_staff_products_prices.columns)
print(order_data_with_campaign_merchant_staff_products_prices.isnull().any())

# load data into database
engine = create_engine(db_url)
order_data_with_campaign_merchant_staff_products_prices.to_sql(table_name, engine, if_exists='append', index=False)