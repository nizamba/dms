import tkinter as tk
import os
import logging
import pyodbc
import psycopg2
import ast
import sqlite3
import re
import boto3
import time
from datetime import datetime
from logging.handlers import RotatingFileHandler
from tkinter import ttk, messagebox, simpledialog
from contextlib import closing
from ansible_collections.community.general.plugins.modules.postgresql_db import db_matches
from database_utils import get_databases, get_pg_databases, get_pg_schemas
from botocore.exceptions import ClientError

# global variables
env = os.getenv('ENV', 'development')

# MSSQL Connection Details
mssql_connection = {
    "host": "10.220.3.97",
    "port": 1433,  # Default MSSQL port
    "user": "sa",
    "password": "Password1!",
    "database": "master"  # Default database; change as needed
}
# RDS PostgreSQL Connection Details
rds_postgres_connection = {
    "host": "nizlog14.cwinufw9r6rg.us-west-2.rds.amazonaws.com",
    "port": 5432,  # Default PostgreSQL port
    "user": "sa",
    "password": "mLIgHWFcyu",
    "database": "actdb"
}

# Dictionary to hold migration details for each database
databases_to_migrate = {
    "DemoDB": {
        "source_db_name": "DemoDB",
        "target_schema": "nizlog14_rcm",
        "dms_instance_arn": "None"
    },
    "DB2": {
        "source_db_name": "source_db_name_2",
        "target_schema": "target_schema_2",
        "dms_instance_arn": "your-dms-instance-arn-2"
    }
    # Add more databases as needed
}

# DMS Configuration
dms_create = True  # Set to True to create DMS resources; False to use existing
aws_region = "us-east-1"  # AWS region
aws_account = "your-aws-account-id"  # AWS account ID
disable_triggers_vars = []

# Create a named logger
activity_logger = logging.getLogger("ActivityLogger")
activity_logger.propagate = False

# Configure the logger
log_file = 'activity.log'
log_dir = os.path.dirname(log_file)

#logging configuration
if log_dir and not os.path.exists(log_dir):
    os.makedirs(log_dir)
if env == 'development':
    activity_logger.setLevel(logging.DEBUG)
elif env == 'production':
    activity_logger.setLevel(logging.INFO)

console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

activity_logger.addHandler(console_handler)  # Attach console handler to activity_logger
activity_handler = RotatingFileHandler(log_file, maxBytes=5000000, backupCount=5)
activity_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
activity_logger.addHandler(activity_handler)

# Test the logger
activity_logger.info("Logging initialized.")


def run_analyze_script(db_name, is_postgres=False, schema_name=None):
    analyze_template_path = os.path.join(os.path.dirname(__file__), 'mssql_analyze.sql')
    script = ""  # Initialize script variable
    conn = None
    cursor = None  # Initialize cursor to None
    try:
        # Connect to database
        if is_postgres:
            conn = psycopg2.connect(
                host=rds_postgres_connection["host"],
                database=rds_postgres_connection["database"],  # Using the default postgres database
                user=rds_postgres_connection["user"],
                password=rds_postgres_connection["password"]
            )
        else:
            conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};" \
                       f"SERVER={mssql_connection['host']};" \
                       f"DATABASE={db_name};" \
                       f"UID={mssql_connection['user']};" \
                       f"PWD={mssql_connection['password']}"
            conn = pyodbc.connect(conn_str)

        cursor = conn.cursor()

        # Read the script file
        if os.path.isfile(analyze_template_path):
            with open(analyze_template_path, 'r') as script_file:
                script = script_file.read()
        else:
                raise FileNotFoundError(f"Script file not found: {analyze_template_path}")

        # Execute the script
        activity_logger.info(f"Executing script on {'PostgreSQL' if is_postgres else 'MSSQL'} for {schema_name or db_name}.")
        cursor.execute(script)

        # Fetch results if available
        try:
            results = cursor.fetchall()
            activity_logger.info(f"Results: {results}")
        except (pyodbc.ProgrammingError, psycopg2.ProgrammingError):
            conn.commit()
            activity_logger.info("No results returned; transaction committed.")
            results = None

    except Exception as e:
        activity_logger.error(f"Error executing script for {'schema ' + schema_name if is_postgres else 'database ' + db_name}: {str(e)}")
        raise
    finally:
        # Close resources
        if cursor:
            cursor.close()
        if conn:
            conn.close()
    return results

def create_partition_alignment(db_name, is_postgres=True, schema_name=None):
    script_path = os.path.join(os.path.dirname(__file__), 'Partition_Alignment_General.sql')
    log_message = f"Creating partition alignment for {db_name}..."
    activity_logger.info(log_message)
    temp_script_path = None # Initialize script variable
    conn = None
    cursor = None
    results = None  # Initialize results to avoid referencing before assignment

    # Validate schema_name
    if not schema_name:
        log_message = "Partition alignment cancelled: No target schema name provided."
        activity_logger.info(log_message)
        return

    try:
        # Read and modify the SQL script
        if not os.path.isfile(script_path):
            raise FileNotFoundError(f"Script file not found: {script_path}")

        # Read and modify the SQL script
        with open(script_path, 'r') as file:
            sql_script = file.read()

        sql_script = sql_script.replace("Target_schema_name", schema_name)

        # Create a temporary file with the modified script
        temp_script_path = os.path.join(os.path.dirname(__file__), f'temp_partition_alignment_{db_name}.sql')
        with open(temp_script_path, 'w') as temp_file:
            temp_file.write(sql_script)

        # Establish database connection
        if is_postgres:
            conn = psycopg2.connect(
                host=rds_postgres_connection["host"],
                database="actdb",  # Using the default postgres database
                user=rds_postgres_connection["user"],
                password=rds_postgres_connection["password"]
            )
        else:
            conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};" \
                       f"SERVER={mssql_connection['host']};" \
                       f"DATABASE={db_name};" \
                       f"UID={mssql_connection['user']};" \
                       f"PWD={mssql_connection['password']}"
            conn = pyodbc.connect(conn_str)

        cursor = conn.cursor()


        # Execute the script
        activity_logger.info(f"Executing script on {'PostgreSQL' if is_postgres else 'MSSQL'} for {schema_name or db_name}.")
        with open(temp_script_path, 'r') as script_file:
            script = script_file.read()
        cursor.execute(script)

        # Fetch results if available
        try:
            results = cursor.fetchall()
            activity_logger.info(f"Results: {results}")
        except (pyodbc.ProgrammingError, psycopg2.ProgrammingError):
            conn.commit()
            activity_logger.info("No results returned; transaction committed.")

        # Save results to a file if any
        if results:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            result_file = f"{schema_name if is_postgres else db_name}_script_results_{timestamp}.txt"
            with open(result_file, 'w') as f:
                for row in results:
                    f.write(str(row) + '\n')
            activity_logger.info(f"Results saved to: {result_file}")
            return result_file

    except Exception as e:
        activity_logger.error(f"Error executing script for {'schema ' + schema_name if is_postgres else 'database ' + db_name}: {str(e)}")
        raise
    finally:
        # Cleanup resources
        if cursor:
            try:
                cursor.close()
            except Exception as e:
                activity_logger.warning(f"Failed to close cursor: {str(e)}", exc_info=True)
        if conn:
            try:
                conn.close()
            except Exception as e:
                activity_logger.warning(f"Failed to close connection: {str(e)}", exc_info=True)
        if temp_script_path and os.path.exists(temp_script_path):
            try:
                os.remove(temp_script_path)
                activity_logger.info(f"Temporary script file deleted: {temp_script_path}")
            except Exception as e:
                activity_logger.warning(f"Failed to delete temporary script file: {temp_script_path}. Error: {str(e)}", exc_info=True)

    return None

def disable_triggers_in_pg(db_name, disable_triggers_vars,):
    schema_name = db_name.lower()
    log_message = f"Processing disable triggers commands for schema {schema_name}..."
    activity_logger.info(log_message)

    # Verify if the schema exists before proceeding
    check_schema_script = f"SELECT schema_name FROM information_schema.schemata WHERE schema_name = '{schema_name}';"

    # Execute the check script
    schema_exists = execute_script_on_database(check_schema_script, False, "postgres", is_postgres=True,schema_name=schema_name)


if __name__ == "__main__":
    run_analyze_script(databases_to_migrate["DemoDB"]["source_db_name"])
    create_partition_alignment(databases_to_migrate["DemoDB"]["source_db_name"],False,databases_to_migrate["DemoDB"]["target_schema"])
    disable_triggers_in_pg(databases_to_migrate["DemoDB"]["target_schema"],disable_triggers_vars)