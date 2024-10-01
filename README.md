# Snowflake Data Processing Scripts

This repository contains two Python scripts that interact with a Snowflake database: 

1. **Snowflake Query Executor**: A script to execute multiple SQL queries on a Snowflake database and display results.
2. **ETL Process with Scheduling and Logging**: A script to perform a daily ETL (Extract, Transform, Load) process that retrieves data from a Snowflake table, applies transformations, and loads the transformed data back into Snowflake.

## Prerequisites

Make sure you have the following Python packages installed:

```bash
pip install snowflake-connector-python pandas schedule tabulate
```

# 1. Snowflake Query Executor

## Overview
The Snowflake Query Executor allows users to execute a list of SQL queries and display the results in a structured table format.

1.Setup
Environment Variables: Set the following environment variables to store your Snowflake credentials securely:
```
export SNOWFLAKE_USER='your_snowflake_username'
export SNOWFLAKE_PASSWORD='your_snowflake_password'
export SNOWFLAKE_ACCOUNT='your_snowflake_account'
export SNOWFLAKE_WAREHOUSE='your_snowflake_warehouse'
export SNOWFLAKE_DATABASE='your_snowflake_database'
export SNOWFLAKE_SCHEMA='your_snowflake_schema'
```
2. Modify Queries: You can modify the list of SQL queries in the script to suit your needs.

## Running the Script
To execute the Snowflake Query Executor script, use the following command:
```
python snowflake_query_executor.py
```
# 2. ETL Process with Scheduling and Logging
## Overview
This script performs a daily ETL process that extracts data from a Snowflake table, applies transformations, and loads the transformed data back into Snowflake. It logs progress and schedules the ETL process to run daily at a specified time.

## Setup
1. Environment Variables: S
Set the following environment variables for the ETL process:
```
export SNOWFLAKE_USER='your_snowflake_username'
export SNOWFLAKE_PASSWORD='your_snowflake_password'
export SNOWFLAKE_ACCOUNT='your_snowflake_account'
export SNOWFLAKE_WAREHOUSE='your_snowflake_warehouse'
export SNOWFLAKE_DATABASE='your_snowflake_database'
export SNOWFLAKE_SCHEMA='your_snowflake_schema'
export SNOWFLAKE_TABLE='your_snowflake_table'  # Optional, default is PUBLIC.CAR_DATA
export TRANSFORMED_TABLE='your_transformed_table'  # Optional, default is transformed_cars_data
```
2. Modify Schedule: By default, the ETL process is scheduled to run at 1 PM daily. You can adjust this in the script:
```
schedule.every().day.at("13:00").do(run_etl)
```
## Running the Script
To run the ETL process script, use the following command:
```
python etl_snowflake.py
```
## Notes
1. The ETL process filters cars based on their YEAR and MILEAGE. You can modify the transform_data function to apply different transformations as needed.
2. Ensure that you have the correct permissions to create new tables and insert data into Snowflake.
3. The ETL script will continuously run until the scheduled job is completed.
   
## Logging
Both scripts log their operations to respective log files (etl_workflow.log for the ETL script). Check these files for information about executed queries and the ETL process status.

## Conclusion
These scripts provide a solid foundation for interacting with Snowflake, executing queries, and managing data workflows. Feel free to modify and extend them according to your requirements.
