import os
import snowflake.connector
import pandas as pd
import schedule
import time
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(filename='etl_workflow.log', level=logging.INFO, format='%(asctime)s - %(message)s')

# Snowflake connection parameters (retrieve from environment variables for security)
SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE')
SNOWFLAKE_DATABASE = os.getenv('SNOWFLAKE_DATABASE')
SNOWFLAKE_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA')
SNOWFLAKE_TABLE = os.getenv('SNOWFLAKE_TABLE', 'PUBLIC.CAR_DATA')  # Default table name
TRANSFORMED_TABLE = os.getenv('TRANSFORMED_TABLE', 'transformed_cars_data')

# Step 1: Extract Data from Snowflake
def extract_data():
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA
    )
    query = f"SELECT * FROM {SNOWFLAKE_TABLE}"
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# Step 2: Transform the Data
def transform_data(df):
    # General transformation: Filtering cars by year and mileage (adjustable)
    transformed_df = df[(df['YEAR'] > 2017) & (df['MILEAGE'] < 50000)]
    return transformed_df

# Step 3: Load the Transformed Data Back into Snowflake
def load_data(transformed_df):
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA
    )
    cursor = conn.cursor()

    # Create a new table for the transformed data (adjustable as per needs)
    create_table_query = f"""
    CREATE OR REPLACE TABLE {TRANSFORMED_TABLE} AS 
    SELECT * FROM {SNOWFLAKE_TABLE} WHERE 1=0
    """
    cursor.execute(create_table_query)

    # Insert the transformed data into the new table
    for index, row in transformed_df.iterrows():
        insert_query = f"""
        INSERT INTO {TRANSFORMED_TABLE} (IDS, BRAND, MODEL, YEAR, COLOR, MILEAGE, PRICE, LOCATION)
        VALUES ({row['IDS']}, '{row['BRAND']}', '{row['MODEL']}', {row['YEAR']}, '{row['COLOR']}', {row['MILEAGE']}, {row['PRICE']}, '{row['LOCATION']}')
        """
        cursor.execute(insert_query)

    conn.commit()
    cursor.close()
    conn.close()

# Run the ETL Process
def run_etl():
    df = extract_data()
    transformed_df = transform_data(df)
    load_data(transformed_df)
    logging.info("ETL process completed successfully.")

# Schedule the ETL process to run daily at 1 PM (modifiable)
schedule.every().day.at("13:00").do(run_etl)

logging.info("ETL process scheduled to run at 1 PM daily")

# Keep the script running until the job is executed
while True:
    schedule.run_pending()
    time.sleep(1)

    # Exit script after the ETL process has run
    if datetime.now().hour >= 13:
        logging.info("ETL process completed for the day. Exiting...")
        break
