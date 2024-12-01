from sqlalchemy import create_engine
import pandas as pd
from dotenv import load_dotenv
import os
import numpy as np
from datetime import datetime, timedelta
import calendar

# Load environment variables from the .env file
load_dotenv()

db_url = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"

philippine_holidays = [
    "01-01",  # New Year's Day
    "04-09",  # Araw ng Kagitingan
    "05-01",  # Labor Day
    "06-12",  # Independence Day
    "08-21",  # Ninoy Aquino Day
    "11-01",  # All Saints' Day
    "11-02",  # All Souls' Day
    "12-25",  # Christmas Day
    "12-30",  # Rizal Day
]
table_name = 'date_dimension'

# FILE NAMES AND LOCATIONS
clean_order_data = 'Operations Department/Cleaned Data/cleaned_order_data_20200101-20200701.parquet'

# read files
order_data_df = pd.read_parquet(clean_order_data)

order_data_df['transaction_date'] = pd.to_datetime(order_data_df['transaction_date'])
oldest_date = order_data_df['transaction_date'].min()

# Step 2: Generate date range from the oldest date to the current date + 5 years
end_date = datetime.now() + timedelta(days=5*365)  # Approx 5 years ahead
date_range = pd.date_range(start=oldest_date, end=end_date, freq='D')

# Step 3: Create DataFrame for date_dimension table
date_dimension_data = []

for date in date_range:
    # Determine the year, quarter, month, day, and other properties
    date_year = date.year
    date_quarter = (date.month - 1) // 3 + 1
    date_month = date.month
    date_month_name = calendar.month_name[date_month]
    date_day_in_month = date.day
    date_day_name = calendar.day_name[date.weekday()]
    date_is_weekend = date.weekday() >= 5  # Saturday (5) or Sunday (6)
    
    # Check if the current date is a holiday
    holiday_str = date.strftime("%m-%d")  # Convert to MM-DD format
    date_is_holiday = holiday_str in philippine_holidays
    date_holiday_name = None

    if date_is_holiday:
        # You can also specify holiday names if desired
        if holiday_str == "01-01":
            date_holiday_name = "New Year's Day"
        elif holiday_str == "04-09":
            date_holiday_name = "Araw ng Kagitingan"
        elif holiday_str == "05-01":
            date_holiday_name = "Labor Day"
        elif holiday_str == "06-12":
            date_holiday_name = "Independence Day"
        elif holiday_str == "08-21":
            date_holiday_name = "Ninoy Aquino Day"
        elif holiday_str == "11-01":
            date_holiday_name = "All Saints' Day"
        elif holiday_str == "11-02":
            date_holiday_name = "All Souls' Day"
        elif holiday_str == "12-25":
            date_holiday_name = "Christmas Day"
        elif holiday_str == "12-30":
            date_holiday_name = "Rizal Day"

    # Append to the date_dimension_data list
    date_dimension_data.append([
        date,  # date_id (DATE)
        date_year,  # date_year (VARCHAR)
        date_quarter,  # date_quarter (INT)
        date_month,  # date_month (INT)
        date_month_name,  # date_month_name (VARCHAR)
        date_day_in_month,  # date_day_in_month (INT)
        date_day_name,  # date_day_name (VARCHAR)
        date_is_weekend,  # date_is_weekend (BOOLEAN)
        date_is_holiday,  # date_is_holiday (BOOLEAN)
        date_holiday_name  # date_holiday_name (VARCHAR)
    ])

# Step 4: Convert the list to a DataFrame
date_dimension_df = pd.DataFrame(date_dimension_data, columns=[
    'date_id', 'date_year', 'date_quarter', 'date_month', 'date_month_name',
    'date_day_in_month', 'date_day_name', 'date_is_weekend', 'date_is_holiday', 'date_holiday_name'
])

# load data into database
engine = create_engine(db_url)
date_dimension_df.to_sql(table_name, engine, if_exists='append', index=False)