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
        "target_schema": "hint_plan",
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

#logging configuration
# Create a named logger
activity_logger = logging.getLogger("ActivityLogger")
activity_logger.propagate = False

# Configure the logger
log_file = 'activity.log'
log_dir = os.path.dirname(log_file)
if log_dir and not os.path.exists(log_dir):
    os.makedirs(log_dir)
if env == 'development':
    activity_logger.setLevel(logging.DEBUG)
elif env == 'production':
    activity_logger.setLevel(logging.INFO)


activity_handler = RotatingFileHandler(log_file, maxBytes=5000000, backupCount=5)
activity_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
activity_logger.addHandler(activity_handler)


console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

activity_logger.addHandler(console_handler)  # Attach console handler to activity_logger

# Test the logger
activity_logger.info("Logging initialized.")

def run_analyze_script(db_name, is_postgres=False, schema_name=None):
    analyze_template_path = os.path.join(os.path.dirname(__file__), 'mssql_analyze.sql')
    script = ""  # Initialize script variable
    conn = None
    cursor = None  # Initialize cursor to None
    try:
        # Determine the connection type
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
            results = None

    except Exception as e:
        activity_logger.error(f"Error executing script for {'schema ' + schema_name if is_postgres else 'database ' + db_name}: {str(e)}")
        raise
    finally:
        # Close resources
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()

if __name__ == "__main__":
    run_analyze_script(mssql_connection["database"])