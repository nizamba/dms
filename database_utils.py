import pyodbc
import psycopg2
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,  # Change to DEBUG for more detailed logs
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("db_script.log"),  # Logs to a file
        logging.StreamHandler()                # Logs to console
    ]
)

# Function to fetch databases from the SQL Server
def get_databases(host, user, password):
    try:
        logging.info(f"Attempting to connect to SQL Server at {host} with provided credentials.")

        # Define the connection string with master database
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={host};"
            f"DATABASE=master;"  # Explicitly connect to master
            f"UID={user};"
            f"PWD={password};"
        )
        # Connect to the SQL Server
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        logging.info("Connected to SQL Server successfully.")

        # Execute the query to fetch non-system databases
        logging.info("Executing query to fetch databases, excluding system databases.")
        sql_query = (
            "SELECT name FROM sys.databases "
            "WHERE state_desc = 'ONLINE' "
            "AND name NOT IN ('master', 'model', 'msdb', 'tempdb');"
        )
        cursor.execute(sql_query)

        # Fetch all the database names
        databases = [row[0] for row in cursor.fetchall()]
        logging.info(f"Databases fetched successfully: {databases}")

        # Close the connection
        conn.close()
        logging.info("SQL Server connection closed.")

        return databases

    except pyodbc.Error as err:
        logging.error(f"Error fetching databases: {err}")
        for err_detail in err.args:
            logging.error(f"Error detail: {err_detail}")
        raise Exception(f"Database connection error: {err}")

if __name__ == "__main__":
    host = "your_server_name"  # Replace with your server name or IP
    user = "your_username"     # Replace with your SQL Server username
    password = "your_password" # Replace with your SQL Server password

    try:
        databases = get_databases(host, user, password)
        print("Databases:", databases)
    except Exception as e:
        logging.error(f"Failed to retrieve databases: {e}")

# Function to fetch databases from PostgreSQL
def get_pg_databases(host, user, password, port=5432):
    try:
        logging.info(f"Attempting to connect to PostgreSQL at {host} with provided credentials.")

        # Connect to the PostgreSQL server
        conn = psycopg2.connect(
            host=host,
            user=user,
            password=password,
            port=port,
            database="postgres"  # Default database in PostgreSQL
        )
        cursor = conn.cursor()

        logging.info("Connected to PostgreSQL successfully.")

        # Query to fetch all non-system databases
        logging.info("Executing query to fetch databases, excluding system databases.")
        sql_query = (
            "SELECT datname FROM pg_database "
            "WHERE datistemplate = false AND datallowconn = true;"
        )
        cursor.execute(sql_query)

        # Fetch all the database names
        databases = [row[0] for row in cursor.fetchall()]
        logging.info(f"Databases fetched successfully: {databases}")

        # Close the connection
        cursor.close()
        conn.close()
        logging.info("PostgreSQL connection closed.")

        return databases

    except psycopg2.Error as err:
        logging.error(f"Error fetching databases: {err}")
        raise Exception(f"Database connection error: {err}")

if __name__ == "__main__":
    host = "your_postgres_server"  # Replace with your PostgreSQL server name or IP
    user = "your_username"         # Replace with your PostgreSQL username
    password = "your_password"     # Replace with your PostgreSQL password

    try:
        databases = get_databases(host, user, password)
        print("Databases:", databases)
    except Exception as e:
        logging.error(f"Failed to retrieve databases: {e}")


# Function to fetch all schemas in a specific PostgreSQL database
def get_pg_schemas(host, user, password, database="actdb", port=5432):
    try:
        logging.info(f"Connecting to PostgreSQL at {host} to retrieve schemas from database '{database}'.")

        # Connect to the PostgreSQL database
        conn = psycopg2.connect(
            host=host,
            user=user,
            password=password,
            database=database,
            port=port
        )
        cursor = conn.cursor()

        logging.info("Connected to PostgreSQL successfully.")

        # Query to fetch all schemas (excluding system schemas)
        logging.info("Executing query to fetch schemas, excluding system schemas.")
        sql_query = (
            "SELECT schema_name FROM information_schema.schemata "
            "WHERE schema_name NOT IN ('information_schema', 'pg_catalog') "
            "AND schema_name NOT LIKE 'pg_toast%' "
            "AND schema_name NOT LIKE 'pg_temp%';"
        )
        cursor.execute(sql_query)

        # Fetch all schema names
        schemas = [row[0] for row in cursor.fetchall()]
        logging.info(f"Schemas fetched successfully: {schemas}")

        # Close the connection
        cursor.close()
        conn.close()
        logging.info("PostgreSQL connection closed.")

        return schemas

    except psycopg2.Error as err:
        logging.error(f"Error fetching schemas: {err}")
        raise Exception(f"Database connection error: {err}")

if __name__ == "__main__":
    host = "your_postgres_server"    # Replace with your PostgreSQL server name or IP
    user = "your_username"           # Replace with your PostgreSQL username
    password = "your_password"       # Replace with your PostgreSQL password
    database = "actdb"               # The database to query schemas from

    try:
        schemas = get_schemas(host, user, password, database)
        print("Schemas:", schemas)
    except Exception as e:
        logging.error(f"Failed to retrieve schemas: {e}")