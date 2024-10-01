import snowflake.connector
from tabulate import tabulate

def create_snowflake_connection(user, password, account, role, warehouse, database, schema):
    """
    Establishes a connection to the Snowflake database.

    Parameters:
        user (str): Username for Snowflake.
        password (str): Password for Snowflake.
        account (str): Account identifier for Snowflake.
        role (str): Role for Snowflake access.
        warehouse (str): Warehouse to use.
        database (str): Target database.
        schema (str): Target schema.

    Returns:
        conn: Snowflake connection object.
    """
    conn = snowflake.connector.connect(
        user=user,
        password=password,
        account=account,
        role=role,
        warehouse=warehouse,
        database=database,
        schema=schema
    )
    return conn

def execute_queries(connection, queries):
    """
    Executes a list of queries and prints results in a tabulated format.

    Parameters:
        connection: Snowflake connection object.
        queries (list): List of SQL queries to execute.
    """
    cursor = connection.cursor()
    try:
        for query in queries:
            cursor.execute(query)
            data = cursor.fetchall()
            column_names = [col[0] for col in cursor.description]

            # Display results using tabulate for a structured format
            print(f"Results for query: {query}")
            print(tabulate(data, headers=column_names, tablefmt='grid'))
            print("\n")
    finally:
        cursor.close()

def main():
    # Example of user credentials (replace with environment variables or config files for security)
    user = 'your_username'
    password = 'your_password'
    account = 'your_account'
    role = 'your_role'
    warehouse = 'your_warehouse'
    database = 'your_database'
    schema = 'your_schema'

    # Establish connection
    conn = create_snowflake_connection(user, password, account, role, warehouse, database, schema)

    # List of SQL queries to execute
    queries = [
        "SELECT * FROM PUBLIC.CAR_DATA LIMIT 10",
        "SELECT * FROM PUBLIC.CAR_DATA WHERE BRAND = 'Toyota' LIMIT 10",
        "SELECT * FROM PUBLIC.CAR_DATA WHERE MILEAGE < 50000 LIMIT 10",
        "SELECT LOCATION, BRAND, AVG(PRICE) AS AveragePrice FROM PUBLIC.CAR_DATA GROUP BY LOCATION, BRAND ORDER BY LOCATION, AveragePrice DESC LIMIT 20"
    ]

    # Execute queries and display results
    execute_queries(conn, queries)

    # Close the connection
    conn.close()

if __name__ == "__main__":
    main()
