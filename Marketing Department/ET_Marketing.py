import pandas as pd
import re

# Process the campaign data
csv_path = "Raw Data/campaign_data.csv"
df1 = pd.read_csv(csv_path, delimiter="\t")

# Delete unnecessary columns
unnamed_columns = [col for col in df1.columns if col.lower().startswith('unnamed')]
if unnamed_columns:
    df1.drop(columns=unnamed_columns, inplace=True)

# Check for duplicate campaign_name and remove duplicates
duplicate_campaign_names = df1[df1.duplicated(subset=['campaign_name'], keep=False)]
if not duplicate_campaign_names.empty:
    print("Duplicate campaign names found:")
    print(duplicate_campaign_names)
else:
    print("No duplicate campaign names found.")

df1 = df1.drop_duplicates(subset=['campaign_name'], keep='first')

# Check for null values in df1
null_campaign_data = df1.isnull().sum()
if null_campaign_data.any():
    print("Null values in campaign data:")
    print(null_campaign_data)
else:
    print("No null values in campaign data.")

# Remove unnecessary quotations
df1['campaign_description'] = df1['campaign_description'].str.replace('"', '', regex=False)

# Standardize campaign_id
df1['campaign_id'] = df1['campaign_id'].str.replace('CAMPAIGN', 'C')

# Standardize campaign_name
df1['campaign_name'] = df1['campaign_name'].str.capitalize()
df1['campaign_name'] = df1['campaign_name'].str.replace(r'\bi\b', 'I', regex=True)

# Convert percentage
def standardize_percentage(value):
    value = re.sub(r'[^\d.]', '', str(value))
    if value:
        return float(value) / 100
    return 0 

df1['discount'] = df1['discount'].apply(standardize_percentage)

# Save the cleaned DataFrame 
df1.to_csv("Cleaned Data/cleaned_campaign_data.csv", index=False)
print("Data cleaned and saved as 'cleaned_campaign_data.csv'")

# Process the transactional campaign data
csv_path = "Raw Data/transactional_campaign_data.csv"
df2 = pd.read_csv(csv_path)

# Identify and drop unnamed columns
unnamed_columns = [col for col in df2.columns if col.lower().startswith('unnamed')]
if unnamed_columns:
    df2.drop(columns=unnamed_columns, inplace=True)

# Standardize campaign_id
df2['campaign_id'] = df2['campaign_id'].str.replace('CAMPAIGN', 'C')

# Standardize transaction_date
df2['transaction_date'] = pd.to_datetime(df2['transaction_date'], errors='coerce').dt.strftime('%Y-%m-%d')

# Standardize estimated arrival
df2.rename(columns={'estimated arrival': 'estimated_arrival_in_days'}, inplace=True)
df2['estimated_arrival_in_days'] = df2['estimated_arrival_in_days'].apply(lambda x: re.sub(r'\D', '', str(x)))
df2['estimated_arrival_in_days'] = pd.to_numeric(df2['estimated_arrival_in_days'], errors='coerce')

# Cast to boolean availed
df2['availed'] = df2['availed'].astype(bool)

# Check for duplicate order_ids
duplicate_order_ids = df2[df2.duplicated(subset=['order_id'], keep=False)]
if not duplicate_order_ids.empty:
    print("Duplicate order IDs found:")
    print(duplicate_order_ids)
else:
    print("No duplicate order IDs found.")

df2 = df2.drop_duplicates(subset=['order_id'], keep='first')

# Check for null values in df2
null_transactional_data = df2.isnull().sum()
if null_transactional_data.any():
    print("Null values in transactional campaign data:")
    print(null_transactional_data)
else:
    print("No null values in transactional campaign data.")

# Standardize campaign_id
df2['campaign_id'] = df2['campaign_id'].str.replace('CAMPAIGN', 'C')

# Standardize transaction_date
df2['transaction_date'] = pd.to_datetime(df2['transaction_date'], errors='coerce').dt.strftime('%Y-%m-%d')

# Standardize estimated arrival
df2.rename(columns={'estimated arrival': 'estimated_arrival_in_days'}, inplace=True)
df2['estimated_arrival_in_days'] = df2['estimated_arrival_in_days'].apply(lambda x: re.sub(r'\D', '', str(x)))
df2['estimated_arrival_in_days'] = pd.to_numeric(df2['estimated_arrival_in_days'], errors='coerce')

# Save the cleaned DataFrame
df2.to_csv("Cleaned Data/cleaned_transactional_campaign_data.csv", index=False)
print("Data cleaned and saved as 'cleaned_transactional_campaign_data.csv'")
