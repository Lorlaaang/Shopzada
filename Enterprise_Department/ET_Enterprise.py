import pandas as pd
import re

# Standardize merchant_id
def standardize_merchant_id(merchant_id):
    return re.sub(r'MERCHANT(\d+)', r'M\1', merchant_id)

# Standardize staff_id
def standardize_staff_id(staff_id):
    return re.sub(r'STAFF(\d+)', r'S\1', staff_id)

# Function to check for duplicates
def check_duplicates(df, subset_columns, data_name="Data"):
    duplicates = df[df.duplicated(subset=subset_columns, keep=False)]
    if not duplicates.empty:
        print(f"Duplicate found in {data_name}!")
        print(duplicates)
    else:
        print(f"No duplicates found in {data_name}.")

# Null value checker function
def check_null_values(df, data_name="Data"):
    null_data = df.isnull().sum()
    if null_data.any():
        print(f"Null values in {data_name}:")
        print(null_data[null_data > 0])
    else:
        print(f"No null values found in {data_name}.")

# Standardize street
def standardize_street_name(street_name):
    return re.sub(r'\s+', ' ', street_name.title().strip())

def capitalize_first_word(text):
    words = text.split()
    if words:
        words[0] = words[0].capitalize()  
    return ' '.join(words)

# Standardize phone numbers
def standardize_phone(phone):
    phone = re.sub(r'[^\d]', '', phone)
    if len(phone) == 11 and phone.startswith('1'):
        phone = phone[1:]
    return f"{phone[:3]}-{phone[3:6]}-{phone[6:]}" if len(phone) == 10 else "Invalid Number"

# Standardize country_name
def simplify_country_name(country_name):
    if not country_name:
        return country_name  
    country_name = re.sub(r'\(.*?\)', '', country_name)  
    country_name = re.sub(r'\b(province|republic|democratic|federal|state|united|kingdom|people|of|the|, Republic of|, Democratic Republic of the|, State of|, United Republic of)\b', '', country_name, flags=re.IGNORECASE)
    country_name = re.sub(r'\s+', ' ', country_name).strip()
    return country_name

custom_country_map = { 
    "Ã land Islands": "Åland Islands",
    "Saint BarthÃ©lemy": "Saint Barthélemy",
    "Viet Nam": "Vietnam",
    "RÃ©union": "Réunion",
    "Lao 's" : "Laos",
    "CuraÃ§ao": "Curaçao",
    "CÃ´te d'Ivoire": "Ivory Coast",
    "Taiwan, China": "China"
}

def standardize_country(country_name):
    if not country_name:
        return country_name 
    simplified_name = simplify_country_name(country_name)
    if simplified_name in custom_country_map:
        return custom_country_map[simplified_name]
    return simplified_name

# Process the order files
def process_order_files():
    duplicate_columns = ['order_id', 'merchant_id', 'staff_id']

    df1 = pd.read_parquet('Raw Data/order_with_merchant_data1.parquet')
    df1['merchant_id'] = df1['merchant_id'].apply(standardize_merchant_id)
    df1['staff_id'] = df1['staff_id'].apply(standardize_staff_id)
    check_duplicates(df1, duplicate_columns, "order_with_merchant_data1")
    check_null_values(df1, "order_with_merchant_data1")  # Check for null values
    df1.to_parquet('Cleaned Data/cleaned_order_with_merchant_data1.parquet', index=False)

    df2 = pd.read_parquet('Raw Data/order_with_merchant_data2.parquet')
    df2['merchant_id'] = df2['merchant_id'].apply(standardize_merchant_id)
    df2['staff_id'] = df2['staff_id'].apply(standardize_staff_id)
    check_duplicates(df2, duplicate_columns, "order_with_merchant_data2")
    check_null_values(df2, "order_with_merchant_data2")  
    df2.to_parquet('Cleaned Data/cleaned_order_with_merchant_data2.parquet', index=False)

    df3 = pd.read_csv('Raw Data/order_with_merchant_data3.csv')
    df3['merchant_id'] = df3['merchant_id'].apply(standardize_merchant_id)
    df3['staff_id'] = df3['staff_id'].apply(standardize_staff_id)
    unnamed_columns = [col for col in df3.columns if col.lower().startswith('unnamed')]
    df3.drop(columns=unnamed_columns, inplace=True)

    check_duplicates(df3, duplicate_columns, "order_with_merchant_data3")
    check_null_values(df3, "order_with_merchant_data3")  
    df3.to_csv('Cleaned Data/cleaned_order_with_merchant_data3.csv', index=False)

    print("Order with Merchant Data cleaned")

# Process the merchant data
def process_merchant_data():
    df_list = pd.read_html('Raw Data/merchant_data.html') 
    df = df_list[0] 

    # Standardize street
    df['street'] = df['street'].apply(standardize_street_name)

    # Delete unnecessary columns
    unnamed_columns = [col for col in df.columns if col.lower().startswith('unnamed')]
    df.drop(columns=unnamed_columns, inplace=True)

    # Standardize merchant_id 
    df['merchant_id'] = df['merchant_id'].apply(standardize_merchant_id)
    
    # Format creation_date 
    df['creation_date'] = pd.to_datetime(df['creation_date']).dt.strftime('%Y-%m-%dT%H:%M:%S')
    
    # Standardize contact_number 
    df['contact_number'] = df['contact_number'].apply(standardize_phone)
    
    # Standardize country names
    df['country'] = df['country'].apply(standardize_country)
    
    # Ensure consistent column names
    df.columns = df.columns.str.lower().str.replace(' ', '_')

    # Sort by 'creation_date' 
    df.sort_values(by='creation_date', ascending=True, inplace=True)

    # Drop duplicate merchant_id and names
    check_duplicates(df, ['merchant_id'], "merchant_data")
    check_null_values(df, "merchant_data") 
    df = df.drop_duplicates(subset='merchant_id', keep='first')

    name_duplicates = df[df.duplicated(subset='name', keep=False)]
    if not name_duplicates.empty:
        print("Duplicate names found! Dropping the duplicates....")
    df = df.drop_duplicates(subset='name', keep='first')

    # Save the DataFrame
    df.to_html('Cleaned Data/cleaned_merchant_data.html', index=False)
    print(f"Cleaned data saved as cleaned_merchant_data.html")

# Process staff data
def process_staff_data():
    df_list = pd.read_html('Raw Data/staff_data.html')  
    df = df_list[0] 

    # Standardize street
    df['street'] = df['street'].apply(standardize_street_name)
    
    # Standardize job_level
    df['job_level'] = df['job_level'].apply(capitalize_first_word)

    # Delete unnecessary columns
    unnamed_columns = [col for col in df.columns if col.lower().startswith('unnamed')]
    df.drop(columns=unnamed_columns, inplace=True)

    # Standardize staff_id
    df['staff_id'] = df['staff_id'].apply(standardize_staff_id)

    # Format creation_date
    df['creation_date'] = pd.to_datetime(df['creation_date']).dt.strftime('%Y-%m-%dT%H:%M:%S')

    # Standardize contact_number
    df['contact_number'] = df['contact_number'].apply(standardize_phone)

    # Standardize country names
    df['country'] = df['country'].apply(standardize_country)

    # Ensure consistent column names
    df.columns = df.columns.str.lower().str.replace(' ', '_')

    # Sort by 'creation_date'
    df.sort_values(by='creation_date', ascending=True, inplace=True)

    # Delete duplicate staff_id and names
    check_duplicates(df, ['staff_id'], "staff_data")
    check_null_values(df, "staff_data")
    df = df.drop_duplicates(subset='staff_id', keep='first')

    name_duplicates = df[df.duplicated(subset='name', keep=False)]
    if not name_duplicates.empty:
        print("Duplicate staff name found! Dropping the duplicates....")
    df = df.drop_duplicates(subset='name', keep='first')

    # Save the cleaned DataFrame as an HTML file
    df.to_html('Cleaned Data/cleaned_staff_data.html', index=False)
    print(f"Cleaned data saved as cleaned_staff_data.html")

if __name__ == "__main__":
    process_order_files()
    process_merchant_data()
    process_staff_data()
