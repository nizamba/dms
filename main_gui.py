import tkinter as tk
import os
import sys
import shutil
from pathlib import Path
import json
import logging
import random
import pyodbc
import psycopg2
from psycopg2 import sql
import ast
import sqlite3
import re
import boto3
import time
from datetime import datetime
from logging.handlers import RotatingFileHandler
from tkinter import ttk, messagebox, simpledialog
from contextlib import closing
# import ansible_collections.community.general.plugins.modules.postgresql_db
from iniparse.ini import lower
from database_utils import get_databases, get_pg_databases, get_pg_schemas
from botocore.exceptions import BotoCoreError, ClientError

# global variables
env = os.getenv('ENV', 'development')

# Initialize an empty report list - used for version match check
report = []
version_status = True

# MSSQL Connection Details
mssql_connection = {
    "host": "10.224.0.231",
    "port": 1433,  # Default MSSQL port
    "user": "sa",
    "password": "Actimize1",
    "database": "master"  # Default database; change as needed
}

# RDS PostgreSQL Connection Details
rds_postgres_connection = {
    "host": "fuga01.cwinufw9r6rg.us-west-2.rds.amazonaws.com",
    "port": 5432,  # Default PostgreSQL port
    "user": "sa",
    "password": "UHjDejdKGi",
    "database": "actdb"
}

# Dictionary to hold migration details for each database
databases_to_migrate = {
    "LIN_CDD_CDD_APP": {
        # "source_db": "dbo.case_managment_versions",
        "source_db": "LIN_CDD_CDD_APP",
        "target_schema": "fuga01_cdd_app",
        "dms_instance_arn": "None",
        "sourceendpointarn": None,
        "TargetEndpointArn": None,
        "product": "cdd_app"
    },
    "LIN_CDD_CDD_PRF": {
        "source_db": "LIN_CDD_CDD_PRF",
        "target_schema": "fuga01_cdd_prf",
        "dms_instance_arn": "None",
        "sourceendpointarn": None,
        "TargetEndpointArn": None,
        "product": "cdd_prf"
    },
    # "UDM": {
    #     "source_db": "UDM",
    #     "target_schema": "fuga01_udm_cds",
    #     "dms_instance_arn": "None",
    #     "sourceendpointarn": None,
    #     "TargetEndpointArn": None,
    #     "product": "udm" # udm,rcm,cdd_app, cdd_prf,sam_app,sam_prf, md
    # },
    "LIN_CDD_RCM": {
        "source_db": "LIN_CDD_RCM",
        "target_schema": "fuga01_rcm",
        "dms_instance_arn": "None",
        "sourceendpointarn": None,
        "TargetEndpointArn": None,
        "product": "rcm"
    }
    # Add more databases as needed
}

# DMS Configuration
dms_create = True # Set to True to create DMS resources; False to use existing
tablemappings = {}
tasksettings = {}
tags = [
    {'Key': 'Environment', 'Value': 'Production'},
    {'Key': 'Project', 'Value': 'DatabaseMigration'}
]

dms_details = {
    "instance_identifier": "fuga01-replication-instance2",
    "instance_class": "dms.t3.medium",
    "allocated_storage": 20,
    "subnet_group_name": "fuga-subnet-group",
    "public_access": False,
    "region": "us-west-2",
    "aws_profile": "dev",
    "VpcSecurityGroupIds": ["sg-022f2bab62aa433a0", "sg-0dc668870d406bba4"],
    "SourceEndpointArn": None,
    "TargetEndpointArn": None,
    "instancearn": None
}


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

def execute_script_on_database(task=None, script_input=None, db_name=None,is_postgres=False, schema_name=None):
    try:
        # Handle file or direct SQL input
        if os.path.isfile(script_input):
            with open(script_input, 'r') as script_file:
                script = script_file.read()
        else:
            script = script_input

        activity_logger.info(
            f"Executing {'PostgreSQL' if is_postgres else 'MSSQL'} script {script_input} for {schema_name or db_name}.")

        # Establish database connection
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

        # Execute the script
        cursor.execute(script)

        # Fetch results if available
        try:
            results = cursor.fetchall()
            activity_logger.info(f"Results: {results}")
        except (pyodbc.ProgrammingError, psycopg2.ProgrammingError):
            conn.commit()
            results = None
            activity_logger.info("No results returned; transaction committed.")

        # Close the connection
        cursor.close()
        conn.close()

        # Save results to a file if any
        if results:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            if task in (
                    "drop_foreign_keys_in_pg",
                    "disable_triggers_in_pg",
                    "enable_triggers_in_pg",
                    "recreate_foreign_keys_in_pg",
                    "Partition_Alignment_General",
                    "CDD_APP_Align_database_sequences",
                    "CDD_APP_Align_database_sequences",
                    "PostgreSQL_compare_data"
            ):
                result_file = f"{task}_{db_name}_result.sql"
            else:
                result_file = f"{schema_name if is_postgres else db_name}_script_results_{timestamp}.txt"

            with open(result_file, 'w') as f:
                for row in results:
                    f.write(str(row[0]) + '\n')
            activity_logger.info(f"Results saved to: {result_file}")
            return result_file
        else:
            return None

    except Exception as e:
        activity_logger.error(
            f"Error executing script for {'schema ' + schema_name if is_postgres else 'database ' + db_name}: {str(e)}")
        raise
def data_compare(db_name, schema_name):
    try:
        # schema_name = db_name.lower()
        activity_logger.info(f"Data Compare...")

        # Verify if the schema exists before proceeding
        check_schema_script = f"SELECT schema_name FROM information_schema.schemata WHERE schema_name = '{schema_name}';"

        # Execute the check script
        schema_exists = execute_script_on_database(script_input=check_schema_script,db_name=databases_to_migrate[db_name]["target_schema"], is_postgres=True, schema_name=schema_name)

        if not schema_exists:
            activity_logger.error(f"Schema '{schema_name}' does not exist. Skipping disable triggers commands.")
            return

        # Read the SQL template for disabling triggers
        sql_file_path = os.path.join(os.path.dirname(__file__), 'PostgreSQL_compare_data.sql')
        try:
            with open(sql_file_path, 'r') as file:
                sql_template = file.read()
        except FileNotFoundError:
            activity_logger.error(f"SQL file not found: {sql_file_path}")
            return

        # Replace placeholder with actual schema name
        sql_script = sql_template.replace('target_schema_name', schema_name)

        # Execute the script on the target PostgreSQL database
        task_name = os.path.basename(sql_file_path)  # Get 'drop_foreign_keys_in_pg.sql'
        name_without_extension = os.path.splitext(task_name)[0]  # Remove '.sql'
        execute_script_on_database(task=name_without_extension,script_input=sql_script,db_name=db_name, is_postgres=True, schema_name=schema_name)
        source_pg_file =  f"{name_without_extension}_{db_name}_result.sql" # PostgreSQL script output file
        target_sql_file = f"MSSQL_data_compare_based_of_PostgreSQL.sql" # Target SQL file for MSSQL
        # sql_name_without_extension = os.path.splitext(target_sql_file)[0]
        output_sql_file = f"MSSQL_compare_{db_name}_result.sql"
        sql_name_without_extension = os.path.splitext(output_sql_file)[0]

        #Read the PostgreSQL output file content
        with open(source_pg_file, 'r') as pg_data:
            pg_content = pg_data.read()

        #Read the target SQL file content
        with open(target_sql_file, 'r') as sql_data:
            target_content = sql_data.read()

        #Replace the placeholder 'PostgreSQL_compare_data' with the PostgreSQL content
        updated_content = target_content.replace('PostgreSQL_compare_data', pg_content)

        #Write the updated content to file
        with open(output_sql_file, 'w') as output_file:
            output_file.write(updated_content)

        print("Replacement completed!")



        script_2 = os.path.join(os.path.dirname(__file__),f"MSSQL_compare_{db_name}_result.sql")
        script_3 = script_2.replace('db_name', db_name)
        print(script_3)
        try:
            with open(script_3, 'r') as file:
                sql_template_1 = file.read()
        except FileNotFoundError:
            activity_logger.error(f"SQL file not found: {script_2}")
            return


        if os.path.exists(script_2):
            print("Script 2 Task is " + sql_name_without_extension)
            execute_script_on_database(task=sql_name_without_extension,script_input=sql_template_1,db_name= db_name,is_postgres=False, schema_name=schema_name)
            print(f"Finish run {script_2}")
        # script_2 = output_sql_file
        # if os.path.exists(script_2):
        # # #     execute_script_on_database(sql_name_without_extension,script_2, db_name,is_postgres=True, schema_name=schema_name)
        #     print(f"will run {script_2}")

        activity_logger.info(f"Data Compare")
    except Exception as e:
        activity_logger.error(f"Data Compare: {str(e)}")
def align_db_sequence(db_name, schema_name):
    try:
        # schema_name = db_name.lower()
        activity_logger.info(f"Align database sequence for schema {schema_name}...")

        # Verify if the schema exists before proceeding
        check_schema_script = f"SELECT schema_name FROM information_schema.schemata WHERE schema_name = '{schema_name}';"

        # Execute the check script
        schema_exists = execute_script_on_database(script_input=check_schema_script,db_name=databases_to_migrate[db_name]["target_schema"], is_postgres=True, schema_name=schema_name)

        if not schema_exists:
            activity_logger.error(f"Schema '{schema_name}' does not exist. Skipping disable triggers commands.")
            return
        sql_file_path = ""
        # Read the SQL template for disabling triggers
        if databases_to_migrate[db_name]["product"] == "cdd_prf":
            sql_file_path = os.path.join(os.path.dirname(__file__), 'CDD_PRF_Align_database_sequences.sql')
        elif  databases_to_migrate[db_name]["product"] == "cdd_app":
            sql_file_path = os.path.join(os.path.dirname(__file__), 'CDD_APP_Align_database_add')
        else:
            return

        try:
            with open(sql_file_path, 'r') as file:
                sql_template = file.read()
        except FileNotFoundError:
            activity_logger.error(f"SQL file not found: {sql_file_path}")
            return

        # Replace placeholder with actual schema name
        sql_script = sql_template.replace('target_schema_name', schema_name)

        # Execute the script on the target PostgreSQL database
        task_name = os.path.basename(sql_file_path)  # Get 'drop_foreign_keys_in_pg.sql'
        name_without_extension = os.path.splitext(task_name)[0]  # Remove '.sql'
        execute_script_on_database(name_without_extension,sql_script,db_name, is_postgres=False, schema_name=schema_name)
        # execute_script_on_database(name_without_extension,f'SELECT disable_command FROM {schema_name}.trigger_commands;', db_name, is_postgres=True, schema_name=schema_name)

        script_2 = f"{name_without_extension}_{db_name}_result.sql"
        if os.path.exists(script_2):
            execute_script_on_database(name_without_extension,script_2, db_name,is_postgres=True, schema_name=schema_name)
            # print(f"will run {script_2}")

        activity_logger.info(f"align sequence for schema {schema_name}")
    except Exception as e:
        activity_logger.error(f"Error in align sequence for schema {schema_name}: {str(e)}")
def run_analyze_script(db_name, is_postgres=False, schema_name=None):
    analyze_template_path = os.path.join(os.path.dirname(__file__), 'mssql_analyze.sql')
    activity_logger.info(f"Running 'Analyze' action for database: {db_name}")
    try:
        result_file = execute_script_on_database(script_input=analyze_template_path, db_name=db_name, is_postgres=is_postgres, schema_name=schema_name)  # True for source database
        if result_file:
            activity_logger.info(f"Analyze script executed successfully on {db_name}. Results saved to: {result_file}")
        else:
            activity_logger.info(f"Analyze script executed successfully on {db_name}. No results were returned.")
    except Exception as e:
        activity_logger.error(f"Failed to run analyze on {db_name}: {str(e)}")
        raise
def create_partition_alignment(db_name, schema_name=None, is_postgres=False):
    if databases_to_migrate[db_name]["product"]  == "cdd_prf":
        script_path = os.path.join(os.path.dirname(__file__), 'Partition_Alignment_General.sql')
    else:
        script_path = os.path.join(os.path.dirname(__file__), 'Partition_Alignment_General.sql')
    activity_logger.info(f"Creating partition alignment for {db_name}...")
    temp_script_path = None # Initialize script variable

    # Validate schema_name
    if not schema_name:
        activity_logger.info(f"Partition alignment cancelled: No target schema name provided.")
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

        print(temp_script_path)

        # Execute the modified script
        task_name = os.path.basename(script_path)  # Get 'drop_foreign_keys_in_pg.sql'
        name_without_extension = os.path.splitext(task_name)[0]  # Remove '.sql'
        result_file = execute_script_on_database(name_without_extension, temp_script_path,db_name,is_postgres, schema_name)  # True for source database
        print(result_file)


        script_2 = f"{name_without_extension}_{db_name}_result.sql"
        if os.path.exists(script_2):
            execute_script_on_database(name_without_extension,script_2, db_name,is_postgres=True, schema_name=schema_name)
            # print(f"will run {script_2}")

        #Remove the temporary file
        os.remove(temp_script_path)

        # Save results to a file if any
        if result_file:
            with open(result_file, 'r') as f:
                generated_sql = f.readlines()

            # Process and clean up the SQL statements
            cleaned_sql = []
            for line in generated_sql:
                try:
                    # Safely evaluate the string as a Python literal
                    tuple_content = ast.literal_eval(line.strip())
                    # Extract the SQL statement from the tuple and remove extra quotes
                    sql_statement = tuple_content[0].strip('"')
                    cleaned_sql.append(sql_statement)
                except:
                    # If there's any error in processing, keep the original line
                    cleaned_sql.append(line.strip())

            # Save the cleaned SQL to a new file
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"{db_name}_partition_alignment_{timestamp}.sql"
            with open(output_file, 'w') as f:
                for sql in cleaned_sql:
                    f.write(sql + '\n')


    except Exception as e:
        activity_logger.error(f"Error executing script for {'schema ' + schema_name if is_postgres else 'database ' + db_name}: {str(e)}")
        raise
    finally:
        if temp_script_path and os.path.exists(temp_script_path):
            try:
                os.remove(temp_script_path)
                activity_logger.info(f"Temporary script file deleted: {temp_script_path}")
            except Exception as e:
                activity_logger.warning(f"Failed to delete temporary script file: {temp_script_path}. Error: {str(e)}", exc_info=True)

    return None
def disable_triggers_in_pg(db_name, schema_name):
    try:
        # schema_name = db_name.lower()

        activity_logger.info(f"Processing disable triggers commands for schema {schema_name}...")

        # Verify if the schema exists before proceeding
        check_schema_script = f"SELECT schema_name FROM information_schema.schemata WHERE schema_name = '{schema_name}';"

        # Execute the check script
        schema_exists = execute_script_on_database(script_input=check_schema_script,db_name=databases_to_migrate[db_name]["target_schema"], is_postgres=True, schema_name=schema_name)

        if not schema_exists:
            activity_logger.error(f"Schema '{schema_name}' does not exist. Skipping disable triggers commands.")
            return

        # Read the SQL template for disabling triggers
        sql_file_path = os.path.join(os.path.dirname(__file__), 'disable_triggers_in_pg.sql')
        try:
            with open(sql_file_path, 'r') as file:
                sql_template = file.read()
        except FileNotFoundError:
            activity_logger.error(f"SQL file not found: {sql_file_path}")
            return

        # Replace placeholder with actual schema name
        sql_script = sql_template.replace('target_schema_name', schema_name)

        # Save the SQL script for reference
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        debug_sql_file = f"{schema_name}_disable_triggers_debug_{timestamp}.sql"
        with open(debug_sql_file, 'w') as f:
            f.write(sql_script)

        log_message = f"SQL script saved to: {debug_sql_file}"

        # Execute the script on the target PostgreSQL database
        task_name = os.path.basename(sql_file_path)  # Get 'drop_foreign_keys_in_pg.sql'
        name_without_extension = os.path.splitext(task_name)[0]  # Remove '.sql'
        execute_script_on_database(name_without_extension,sql_script,db_name, is_postgres=True, schema_name=schema_name)
        execute_script_on_database(name_without_extension,f'SELECT disable_command FROM {schema_name}.trigger_commands;', db_name, is_postgres=True, schema_name=schema_name)

        script_2 = f"{name_without_extension}_{db_name}_result.sql"
        if os.path.exists(script_2):
            execute_script_on_database(name_without_extension,script_2, db_name,is_postgres=True, schema_name=schema_name)
            # print(f"will run {script_2}")

        activity_logger.info(f"Triggers disabled for schema {schema_name}")
    except Exception as e:
        activity_logger.error(f"Error in disabling triggers for schema {schema_name}: {str(e)}")
def drop_fks_in_pg(db_name, schema_name):
    try:
        # schema_name = db_name.lower()  # Convert the schema name to lowercase to avoid case mismatches
        activity_logger.info(f"Processing drop foreign key commands for schema {schema_name}...")

        # Verify if the schema exists before proceeding
        check_schema_script = f"SELECT schema_name FROM information_schema.schemata WHERE schema_name = '{schema_name}';"

        # Execute the check script
        schema_exists = execute_script_on_database(script_input=check_schema_script, db_name=databases_to_migrate[db_name]["target_schema"],is_postgres=True, schema_name=schema_name)

        if not schema_exists:
            activity_logger.info(f"Schema '{schema_name}' does not exist. Skipping drop foreign key commands.")
            return

        # Read the SQL template
        sql_file_path = os.path.join(os.path.dirname(__file__), 'drop_foreign_keys_in_pg.sql')
        try:
            with open(sql_file_path, 'r') as file:
                sql_template = file.read()
        except FileNotFoundError:
            activity_logger.error(f"SQL file not found: {sql_file_path}")
            return

        # Replace placeholder with actual schema name
        sql_script = sql_template.replace('target_schema_name', schema_name)

        # Save the SQL script for reference
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        debug_sql_file = f"{schema_name}_drop_fks_debug_{timestamp}.sql"
        with open(debug_sql_file, 'w') as f:
            f.write(sql_script)

        activity_logger.info(f"SQL script saved to: {debug_sql_file}")

        # Execute the script on the target PostgreSQL database
        task_name = os.path.basename(sql_file_path)  # Get 'drop_foreign_keys_in_pg.sql'
        name_without_extension = os.path.splitext(task_name)[0]  # Remove '.sql'
        execute_script_on_database(name_without_extension,sql_script, db_name, is_postgres=True, schema_name=schema_name)
        execute_script_on_database(name_without_extension,f'SELECT drop_command FROM {schema_name}.foreign_key_commands;', db_name, is_postgres=True, schema_name=schema_name)
        # script_2 = name_without_extension + ".sql"
        script_2= f"{name_without_extension}_{db_name}_result.sql"
        if os.path.exists(script_2):
            execute_script_on_database(name_without_extension,script_2, db_name,is_postgres=True, schema_name=schema_name)
            # print(f"will run {script_2}")
            # execute_script_on_database(name_without_extension, script_2,db_name, is_postgres=True, schema_name=schema_name)

        activity_logger.info(f"Triggers disabled for schema {schema_name}")

    except Exception as e:
        activity_logger.info(f"Error in processing drop foreign keys for schema {schema_name}: {str(e)}")
def change_partition_owner(db_name, schema_name):
    try:
        # schema_name = db_name.lower()  # Convert the schema name to lowercase to avoid case mismatches
        activity_logger.info(f"Processing change ownership commands for schema {schema_name}...")

        # Verify if the schema exists before proceeding
        check_schema_script = f"SELECT schema_name FROM information_schema.schemata WHERE schema_name = '{schema_name}';"

        # Execute the check script
        schema_exists = execute_script_on_database(script_input=check_schema_script, db_name=databases_to_migrate[db_name]["source_db"],is_postgres=True, schema_name=schema_name)

        if not schema_exists:
            activity_logger.info(f"Schema '{schema_name}' does not exist. Skipping drop foreign key commands.")
            return

        # Read the SQL template
        sql_file_path = os.path.join(os.path.dirname(__file__), 'change_partition_ownership.sql')
        try:
            with open(sql_file_path, 'r') as file:
                sql_template = file.read()
        except FileNotFoundError:
            activity_logger.error(f"SQL file not found: {sql_file_path}")
            return

        # Replace placeholder with actual schema name
        sql_script = sql_template.replace('target_schema_name', schema_name)

        # Save the SQL script for reference
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        debug_sql_file = f"{schema_name}_drop_fks_debug_{timestamp}.sql"
        with open(debug_sql_file, 'w') as f:
            f.write(sql_script)

        activity_logger.info(f"SQL script saved to: {debug_sql_file}")

        # Execute the script on the target PostgreSQL database
        execute_script_on_database(script_input=sql_script, db_name=db_name, is_postgres=True, schema_name=schema_name)

        activity_logger.info(f"Triggers disabled for schema {schema_name}")

    except Exception as e:
        activity_logger.info(f"Error in processing drop foreign keys for schema {schema_name}: {str(e)}")
def enable_triggers(db_name, schema_name):
    try:
        # schema_name = db_name.lower()  # Convert the schema name to lowercase to avoid case mismatches
        activity_logger.info(f"Processing change ownership commands for schema {schema_name}...")

        # Verify if the schema exists before proceeding
        check_schema_script = f"SELECT schema_name FROM information_schema.schemata WHERE schema_name = '{schema_name}';"

        # Execute the check script
        schema_exists = execute_script_on_database(script_input=check_schema_script, db_name=databases_to_migrate[db_name]["source_db"],is_postgres=True,schema_name=schema_name)

        if not schema_exists:
            activity_logger.info(f"Schema '{schema_name}' does not exist. Skipping drop foreign key commands.")
            return

        # Read the SQL template
        sql_file_path = os.path.join(os.path.dirname(__file__), 'enable_triggers_in_pg.sql')
        try:
            with open(sql_file_path, 'r') as file:
                sql_template = file.read()
        except FileNotFoundError:
            activity_logger.error(f"SQL file not found: {sql_file_path}")
            return

        # Replace placeholder with actual schema name
        sql_script = sql_template.replace('target_schema_name', schema_name)

        # Save the SQL script for reference
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        debug_sql_file = f"{schema_name}_drop_fks_debug_{timestamp}.sql"
        with open(debug_sql_file, 'w') as f:
            f.write(sql_script)

        activity_logger.info(f"SQL script saved to: {debug_sql_file}")

        # Execute the script on the target PostgreSQL database
        task_name = os.path.basename(sql_file_path)  # Get 'drop_foreign_keys_in_pg.sql'
        name_without_extension = os.path.splitext(task_name)[0]  # Remove '.sql'
        execute_script_on_database(name_without_extension,sql_script, db_name, is_postgres=True, schema_name=schema_name)


        script_2 = f"{name_without_extension}_{db_name}_result.sql"
        if os.path.exists(script_2):
            execute_script_on_database(name_without_extension,script_2, db_name,is_postgres=True, schema_name=schema_name)

        activity_logger.info(f"Triggers disabled for schema {schema_name}")

    except Exception as e:
        activity_logger.info(f"Error in processing drop foreign keys for schema {schema_name}: {str(e)}")
def recreate_fks(db_name, schema_name):
    try:
        # schema_name = db_name.lower()  # Convert the schema name to lowercase to avoid case mismatches
        activity_logger.info(f"Processing change ownership commands for schema {schema_name}...")

        # Verify if the schema exists before proceeding
        check_schema_script = f"SELECT schema_name FROM information_schema.schemata WHERE schema_name = '{schema_name}';"

        # Execute the check script
        schema_exists = execute_script_on_database(script_input=check_schema_script, db_name=databases_to_migrate[db_name]["source_db"],is_postgres=True,schema_name=schema_name)

        if not schema_exists:
            activity_logger.info(f"Schema '{schema_name}' does not exist. Skipping drop foreign key commands.")
            return

        # Read the SQL template
        sql_file_path = os.path.join(os.path.dirname(__file__), 'recreate_foreign_keys_in_pg.sql')
        try:
            with open(sql_file_path, 'r') as file:
                sql_template = file.read()
        except FileNotFoundError:
            activity_logger.error(f"SQL file not found: {sql_file_path}")
            return

        # Replace placeholder with actual schema name
        sql_script = sql_template.replace('target_schema_name', schema_name)

        # Save the SQL script for reference
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        debug_sql_file = f"{schema_name}_drop_fks_debug_{timestamp}.sql"
        with open(debug_sql_file, 'w') as f:
            f.write(sql_script)

        activity_logger.info(f"SQL script saved to: {debug_sql_file}")

        # Execute the script on the target PostgreSQL database
        task_name = os.path.basename(sql_file_path)  # Get 'drop_foreign_keys_in_pg.sql'
        name_without_extension = os.path.splitext(task_name)[0]  # Remove '.sql'
        execute_script_on_database(name_without_extension, sql_script, db_name, is_postgres=True, schema_name=schema_name)

        script_2 = f"{name_without_extension}_{db_name}_result.sql"
        if os.path.exists(script_2):
            execute_script_on_database(name_without_extension, script_2, db_name, is_postgres=True, schema_name=schema_name)



        activity_logger.info(f"Triggers disabled for schema {schema_name}")

    except Exception as e:
        activity_logger.info(f"Error in processing drop foreign keys for schema {schema_name}: {str(e)}")
def update_statistics(db_name, schema_name):
    try:
        # schema_name = db_name.lower()  # Convert the schema name to lowercase to avoid case mismatches
        activity_logger.info(f"Processing change ownership commands for schema {schema_name}...")

        # Verify if the schema exists before proceeding
        check_schema_script = f"SELECT schema_name FROM information_schema.schemata WHERE schema_name = '{schema_name}';"

        # Execute the check script
        schema_exists = execute_script_on_database(script_input=check_schema_script, db_name=databases_to_migrate[db_name]["source_db"],is_postgres=True, schema_name=schema_name)

        if not schema_exists:
            activity_logger.info(f"Schema '{schema_name}' does not exist. Skipping drop foreign key commands.")
            return

        # Read the SQL template
        sql_file_path = os.path.join(os.path.dirname(__file__), 'update_statistics.sql')
        try:
            with open(sql_file_path, 'r') as file:
                sql_template = file.read()
        except FileNotFoundError:
            activity_logger.error(f"SQL file not found: {sql_file_path}")
            return

        # Replace placeholder with actual schema name
        sql_script = sql_template.replace('target_schema_name', schema_name)

        # Save the SQL script for reference
        # with open(debug_sql_file, 'w') as f:
        #     f.write(sql_script)

        # activity_logger.info(f"SQL script saved to: {debug_sql_file}")

        # Execute the script on the target PostgreSQL database
        execute_script_on_database(script_input=sql_script, db_name=db_name, is_postgres=True, schema_name=schema_name)

        # activity_logger.info(f"Triggers disabled for schema {schema_name}")

    except Exception as e:
        activity_logger.info(f"Error in processing drop foreign keys for schema {schema_name}: {str(e)}")
def create_dms_replication_instance(instance_identifier, instance_class, allocated_storage, subnet_group_name, security_group_ids, region, publicly_accessible=True, db_name = None):
    # Create a session with the specified profile
    session = boto3.Session(profile_name=dms_details["aws_profile"])
    # Create a DMS client for the specified region
    dms_client = session.client('dms', region_name=region)
    try:
        # Check if the replication instance already exists
        try:
            response = dms_client.describe_replication_instances(
                Filters=[{
                    'Name': 'replication-instance-id',
                    'Values': [instance_identifier]
                }]
            )
            # If the instance exists, fetch its ARN
            if response['ReplicationInstances']:
                instance_arn = response['ReplicationInstances'][0]['ReplicationInstanceArn']
                dms_details["instancearn"] = response['ReplicationInstances'][0]['ReplicationInstanceArn']
                print(f"Replication Instance '{instance_identifier}' already exists. ARN: {instance_arn}")
                return instance_arn
        except ClientError as describe_error:
            # If the error indicates the instance does not exist, proceed to create it
            if 'ResourceNotFoundFault' not in str(describe_error):
                raise
        # Create the replication instance
        response = dms_client.create_replication_instance(
            ReplicationInstanceIdentifier=instance_identifier,
            ReplicationInstanceClass=instance_class,
            AllocatedStorage=allocated_storage,
            ReplicationSubnetGroupIdentifier=subnet_group_name,
            VpcSecurityGroupIds=security_group_ids,
            PubliclyAccessible=publicly_accessible
        )

        # Wait for the instance to be available
        activity_logger.info("Waiting for replication instance to be available...\n")
        if not wait_for_replication_instance(dms_client, instance_identifier):
            messagebox.showerror("Error", "Failed to create replication instance")
            return
        print(f"Replication Instance '{instance_identifier}' created successfully!")
        # Store the replication instance ARN
        dms_details["instancearn"] = response['ReplicationInstance']['ReplicationInstanceArn']
        logging.info(f"Stored replication instance ARN: response['ReplicationInstance']['ReplicationInstanceArn']")

    except (BotoCoreError, ClientError) as error:
        print(f"Error creating replication instance: {error}")
        return None
    except Exception as e:
        activity_logger.error(f"Unexpected error: {str(e)}")
def configure_dms_endpoints(endpointtype,enginename,servername,port,databasename,username,password,region):
    """Configure source (SQL Server) and target (PostgreSQL) DMS endpoints"""
    try:
        endpoint_identifier = (
            f"postgres-target-{databasename.lower().replace('_', '-').strip('-')}" if endpointtype == 'target'
            else f"sqlserver-source-{databasename.lower().replace('_', '-').strip('-')}"
        )
        activity_logger.info(f"Processing endpoints for databases: {databasename}")

        session = boto3.Session(profile_name=dms_details["aws_profile"])

        # Create a DMS client for the specified region
        dms_client = session.client('dms', region_name=region)

        # Check if the endpoint already exists
        try:
            response = dms_client.describe_endpoints(
                Filters=[{
                    'Name': 'endpoint-id',
                    'Values': [endpoint_identifier]
                }]
            )
            # If the endpoint exists, fetch its ARN
            if response['Endpoints']:
                endpoint_arn = response['Endpoints'][0]['EndpointArn']
                arn_key = "TargetEndpointArn" if endpointtype == 'target' else "SourceEndpointArn"
                dms_details[arn_key] = endpoint_arn
                print(f"'{endpointtype}' Endpoint '{endpoint_identifier}' already exists. ARN: {endpoint_arn}")
                return endpoint_arn
        except ClientError as describe_error:
            # If the error indicates the endpoint does not exist, proceed to create it
            if 'ResourceNotFoundFault' not in str(describe_error):
                raise

        # Create source endpoints
        source_response = dms_client.create_endpoint(
            EndpointIdentifier=endpoint_identifier,
            EndpointType=endpointtype,
            EngineName=enginename,
            ServerName=servername,
            Port=port,
            DatabaseName=databasename,  # Use the database name from selected_databases
            Username=username,
            Password=password
        )
        activity_logger.info(f" '{endpointtype}' Endpoint '{endpoint_identifier}' created successfully! for {databasename}: {source_response['Endpoint']['EndpointArn']}\n")
        arn_key = "TargetEndpointArn" if endpointtype == 'target' else "SourceEndpointArn"
        # databases_to_migrate[db_name][arn_key] = source_response['Endpoint']['EndpointArn']
        dms_details[arn_key] = source_response['Endpoint']['EndpointArn']
        # databases_to_migrate[db_name][arn_key] = source_response['Endpoint']['EndpointArn']
        return source_response['Endpoint']['EndpointArn']
    except Exception as e:
        activity_logger.error(f"Unexpected error: {str(e)}")
        sys.exit(1)
def create_dms_task(replicationtaskidentifier,sourceendpointarn,targetendpointarn,migrationtype,tablemappings,replicationinstancearn,tasksettings,tags,databasename,region):
    try:
        activity_logger.info(f"Processing migration task for databases: {databasename}")

        session = boto3.Session(profile_name=dms_details["aws_profile"])
        # Create a DMS client for the specified region
        dms_client = session.client('dms', region_name=region)

        # Create Task
        response = dms_client.create_replication_task(
            ReplicationTaskIdentifier=replicationtaskidentifier,
            SourceEndpointArn=sourceendpointarn,
            TargetEndpointArn=targetendpointarn,
            MigrationType=migrationtype,
            TableMappings=tablemappings,
            ReplicationInstanceArn=replicationinstancearn,
            ReplicationTaskSettings=tasksettings,
            Tags = tags
        )
        activity_logger.info(f" 'Migration {migrationtype}' Task '{replicationtaskidentifier}' created successfully! for {databasename} \n")




    except Exception as e:
        activity_logger.error(f"Unexpected error: {str(e)}")
def wait_for_replication_instance(dms_client, instance_id):
    """Wait for replication instance to be available"""
    while True:
        response = dms_client.describe_replication_instances(
            Filters=[
                {
                    'Name': 'replication-instance-id',
                    'Values': [instance_id]
                }
            ]
        )

        if response['ReplicationInstances']:
            status = response['ReplicationInstances'][0]['ReplicationInstanceStatus']
            arn = response['ReplicationInstances'][0]['ReplicationInstanceArn']

            if status == 'available':
                activity_logger.info("Replication instance is now available.\n")
                return True
            elif status == 'creating':
                activity_logger.info("Waiting for replication instance to be available...\n")
                time.sleep(30)  # Wait 30 seconds before checking again
            else:
                activity_logger.info(f"Unexpected status: {status}\n")
                return False
        else:
            activity_logger.error("Replication instance not found.\n")
            return False
def get_product_version(script_input, db_name, is_postgres=False, schema_name=None):
    try:
        # Handle file or direct SQL input
        if os.path.isfile(script_input):
            with open(script_input, 'r') as script_file:
                # script = script_file.read()
                script = script_file.read().replace("{{schema_name}}", schema_name)
                print(script)
        else:
            script = script_input
        activity_logger.info(
            f"Executing {'PostgreSQL' if is_postgres else 'MSSQL'} script {script_input} for {schema_name or db_name}.")

        # Establish database connection
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
        print(script)
        # Execute the script
        cursor.execute(script)

        # Fetch results if available
        try:
            results = cursor.fetchall()
            activity_logger.info(f"Results: {results}")
            print(results[0])
        except (pyodbc.ProgrammingError, psycopg2.ProgrammingError):
            conn.commit()
            results = None
            activity_logger.info("No results returned; transaction committed.")

        # Close the connection
        cursor.close()
        conn.close()

        # Save results to a file if any
        if results:
        #     timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        #     result_file = f"{schema_name if is_postgres else db_name}_script_results_{timestamp}.txt"
        #     with open(result_file, 'w') as f:
        #         for row in results:
        #             f.write(str(row) + '\n')
        #     activity_logger.info(f"Results saved to: {result_file}")
        #     return result_file
            return results
        else:
            return None

    except Exception as e:
        activity_logger.error(
            f"Error executing script for {'schema ' + schema_name if is_postgres else 'database ' + db_name}: {str(e)}")
        raise
def generate_rule_id():
    return str(random.randint(100000000, 999999999))
def settings_json(table_category=None, maxlob_calculated_value = "1"):
    if table_category == "lob_table_json":
        settings = {
            "Logging": {
                "EnableLogging": False,
                "EnableLogContext": False,
                "LogComponents": [
                    {"Severity": "LOGGER_SEVERITY_DEFAULT", "Id": "TRANSFORMATION"},
                    {"Severity": "LOGGER_SEVERITY_DEFAULT", "Id": "SOURCE_UNLOAD"},
                    {"Severity": "LOGGER_SEVERITY_DEFAULT", "Id": "IO"},
                    {"Severity": "LOGGER_SEVERITY_DEFAULT", "Id": "TARGET_LOAD"},
                    {"Severity": "LOGGER_SEVERITY_DEFAULT", "Id": "PERFORMANCE"},
                    {"Severity": "LOGGER_SEVERITY_DEFAULT", "Id": "SOURCE_CAPTURE"},
                    {"Severity": "LOGGER_SEVERITY_DEFAULT", "Id": "SORTER"},
                    {"Severity": "LOGGER_SEVERITY_DEFAULT", "Id": "REST_SERVER"},
                    {"Severity": "LOGGER_SEVERITY_DEFAULT", "Id": "VALIDATOR_EXT"},
                    {"Severity": "LOGGER_SEVERITY_DEFAULT", "Id": "TARGET_APPLY"},
                    {"Severity": "LOGGER_SEVERITY_DEFAULT", "Id": "TASK_MANAGER"},
                    {"Severity": "LOGGER_SEVERITY_DEFAULT", "Id": "TABLES_MANAGER"},
                    {"Severity": "LOGGER_SEVERITY_DEFAULT", "Id": "METADATA_MANAGER"},
                    {"Severity": "LOGGER_SEVERITY_DEFAULT", "Id": "FILE_FACTORY"},
                    {"Severity": "LOGGER_SEVERITY_DEFAULT", "Id": "COMMON"},
                    {"Severity": "LOGGER_SEVERITY_DEFAULT", "Id": "ADDONS"},
                    {"Severity": "LOGGER_SEVERITY_DEFAULT", "Id": "DATA_STRUCTURE"},
                    {"Severity": "LOGGER_SEVERITY_DEFAULT", "Id": "COMMUNICATION"},
                    {"Severity": "LOGGER_SEVERITY_DEFAULT", "Id": "FILE_TRANSFER"}
                ],
                "CloudWatchLogGroup": None,
                "CloudWatchLogStream": None
            },
            "StreamBufferSettings": {
                "StreamBufferCount": 3,
                "CtrlStreamBufferSizeInMB": 5,
                "StreamBufferSizeInMB": 8
            },
            "ErrorBehavior": {
                "FailOnNoTablesCaptured": True,
                "ApplyErrorUpdatePolicy": "LOG_ERROR",
                "FailOnTransactionConsistencyBreached": False,
                "RecoverableErrorThrottlingMax": 1800,
                "DataErrorEscalationPolicy": "SUSPEND_TABLE",
                "ApplyErrorEscalationCount": 0,
                "RecoverableErrorStopRetryAfterThrottlingMax": True,
                "RecoverableErrorThrottling": True,
                "ApplyErrorFailOnTruncationDdl": False,
                "DataMaskingErrorPolicy": "STOP_TASK",
                "DataTruncationErrorPolicy": "LOG_ERROR",
                "ApplyErrorInsertPolicy": "LOG_ERROR",
                "EventErrorPolicy": "IGNORE",
                "ApplyErrorEscalationPolicy": "LOG_ERROR",
                "RecoverableErrorCount": -1,
                "DataErrorEscalationCount": 0,
                "TableErrorEscalationPolicy": "STOP_TASK",
                "RecoverableErrorInterval": 5,
                "ApplyErrorDeletePolicy": "IGNORE_RECORD",
                "TableErrorEscalationCount": 0,
                "FullLoadIgnoreConflicts": True,
                "DataErrorPolicy": "LOG_ERROR",
                "TableErrorPolicy": "SUSPEND_TABLE"
            },
            "TTSettings": None,
            "FullLoadSettings": {
                "CommitRate": 50000,
                "StopTaskCachedChangesApplied": False,
                "StopTaskCachedChangesNotApplied": False,
                "MaxFullLoadSubTasks": 49,
                "TransactionConsistencyTimeout": 2147483647,
                "CreatePkAfterFullLoad": False,
                "TargetTablePrepMode": "TRUNCATE_BEFORE_LOAD"
            },
            "TargetMetadata": {
                "ParallelApplyBufferSize": 0,
                "ParallelApplyQueuesPerThread": 0,
                "ParallelApplyThreads": 0,
                "TargetSchema": "",
                "InlineLobMaxSize": 0,
                "ParallelLoadQueuesPerThread": 0,
                "SupportLobs": True,
                "LobChunkSize": 0,
                "TaskRecoveryTableEnabled": False,
                "ParallelLoadThreads": 0,
                "LobMaxSize": maxlob_calculated_value,
                "BatchApplyEnabled": False,
                "FullLobMode": False,
                "LimitedSizeLobMode": True,
                "LoadMaxFileSize": 0,
                "ParallelLoadBufferSize": 0
            },
            "BeforeImageSettings": None,
            "ControlTablesSettings": {
                "HistoryTimeslotInMinutes": 5,
                "StatusTableEnabled": False,
                "SuspendedTablesTableEnabled": False,
                "HistoryTableEnabled": False,
                "ControlSchema": "",
                "FullLoadExceptionTableEnabled": False
            },
            "LoopbackPreventionSettings": None,
            "CharacterSetSettings": None,
            "FailTaskWhenCleanTaskResourceFailed": False,
            "ChangeProcessingTuning": {
                "StatementCacheSize": 50,
                "CommitTimeout": 1,
                "RecoveryTimeout": -1,
                "BatchApplyPreserveTransaction": True,
                "BatchApplyTimeoutMin": 1,
                "BatchSplitSize": 0,
                "BatchApplyTimeoutMax": 30,
                "MinTransactionSize": 1000,
                "MemoryKeepTime": 60,
                "BatchApplyMemoryLimit": 500,
                "MemoryLimitTotal": 1024
            },
            "ChangeProcessingDdlHandlingPolicy": {
                "HandleSourceTableDropped": True,
                "HandleSourceTableTruncated": True,
                "HandleSourceTableAltered": True
            },
            "PostProcessingRules": None
        }
    else:
        settings = {
            "Logging": {
                "EnableLogging": False,
                "EnableLogContext": False,
                "LogComponents": [
                    {"Severity": "LOGGER_SEVERITY_DEFAULT", "Id": "TRANSFORMATION"},
                    {"Severity": "LOGGER_SEVERITY_DEFAULT", "Id": "SOURCE_UNLOAD"},
                    {"Severity": "LOGGER_SEVERITY_DEFAULT", "Id": "IO"},
                    {"Severity": "LOGGER_SEVERITY_DEFAULT", "Id": "TARGET_LOAD"},
                    {"Severity": "LOGGER_SEVERITY_DEFAULT", "Id": "PERFORMANCE"},
                    {"Severity": "LOGGER_SEVERITY_DEFAULT", "Id": "SOURCE_CAPTURE"},
                    {"Severity": "LOGGER_SEVERITY_DEFAULT", "Id": "SORTER"},
                    {"Severity": "LOGGER_SEVERITY_DEFAULT", "Id": "REST_SERVER"},
                    {"Severity": "LOGGER_SEVERITY_DEFAULT", "Id": "VALIDATOR_EXT"},
                    {"Severity": "LOGGER_SEVERITY_DEFAULT", "Id": "TARGET_APPLY"},
                    {"Severity": "LOGGER_SEVERITY_DEFAULT", "Id": "TASK_MANAGER"},
                    {"Severity": "LOGGER_SEVERITY_DEFAULT", "Id": "TABLES_MANAGER"},
                    {"Severity": "LOGGER_SEVERITY_DEFAULT", "Id": "METADATA_MANAGER"},
                    {"Severity": "LOGGER_SEVERITY_DEFAULT", "Id": "FILE_FACTORY"},
                    {"Severity": "LOGGER_SEVERITY_DEFAULT", "Id": "COMMON"},
                    {"Severity": "LOGGER_SEVERITY_DEFAULT", "Id": "ADDONS"},
                    {"Severity": "LOGGER_SEVERITY_DEFAULT", "Id": "DATA_STRUCTURE"},
                    {"Severity": "LOGGER_SEVERITY_DEFAULT", "Id": "COMMUNICATION"},
                    {"Severity": "LOGGER_SEVERITY_DEFAULT", "Id": "FILE_TRANSFER"}
                ],
                "CloudWatchLogGroup": None,
                "CloudWatchLogStream": None
            },
            "StreamBufferSettings": {
                "StreamBufferCount": 3,
                "CtrlStreamBufferSizeInMB": 5,
                "StreamBufferSizeInMB": 8
            },
            "ErrorBehavior": {
                "FailOnNoTablesCaptured": True,
                "ApplyErrorUpdatePolicy": "LOG_ERROR",
                "FailOnTransactionConsistencyBreached": False,
                "RecoverableErrorThrottlingMax": 1800,
                "DataErrorEscalationPolicy": "SUSPEND_TABLE",
                "ApplyErrorEscalationCount": 0,
                "RecoverableErrorStopRetryAfterThrottlingMax": True,
                "RecoverableErrorThrottling": True,
                "ApplyErrorFailOnTruncationDdl": False,
                "DataMaskingErrorPolicy": "STOP_TASK",
                "DataTruncationErrorPolicy": "LOG_ERROR",
                "ApplyErrorInsertPolicy": "LOG_ERROR",
                "EventErrorPolicy": "IGNORE",
                "ApplyErrorEscalationPolicy": "LOG_ERROR",
                "RecoverableErrorCount": -1,
                "DataErrorEscalationCount": 0,
                "TableErrorEscalationPolicy": "STOP_TASK",
                "RecoverableErrorInterval": 5,
                "ApplyErrorDeletePolicy": "IGNORE_RECORD",
                "TableErrorEscalationCount": 0,
                "FullLoadIgnoreConflicts": True,
                "DataErrorPolicy": "LOG_ERROR",
                "TableErrorPolicy": "SUSPEND_TABLE"
            },
            "TTSettings": None,
            "FullLoadSettings": {
                "CommitRate": 50000,
                "StopTaskCachedChangesApplied": False,
                "StopTaskCachedChangesNotApplied": False,
                "MaxFullLoadSubTasks": 49,
                "TransactionConsistencyTimeout": 2147483647,
                "CreatePkAfterFullLoad": False,
                "TargetTablePrepMode": "TRUNCATE_BEFORE_LOAD"
            },
            "TargetMetadata": {
                "ParallelApplyBufferSize": 0,
                "ParallelApplyQueuesPerThread": 0,
                "ParallelApplyThreads": 0,
                "TargetSchema": "",
                "InlineLobMaxSize": 0,
                "ParallelLoadQueuesPerThread": 0,
                "SupportLobs": True,
                "LobChunkSize": 0,
                "TaskRecoveryTableEnabled": False,
                "ParallelLoadThreads": 0,
                "LobMaxSize": 1,
                "BatchApplyEnabled": False,
                "FullLobMode": False,
                "LimitedSizeLobMode": True,
                "LoadMaxFileSize": 0,
                "ParallelLoadBufferSize": 0
            },
            "BeforeImageSettings": None,
            "ControlTablesSettings": {
                "HistoryTimeslotInMinutes": 5,
                "StatusTableEnabled": False,
                "SuspendedTablesTableEnabled": False,
                "HistoryTableEnabled": False,
                "ControlSchema": "",
                "FullLoadExceptionTableEnabled": False
            },
            "LoopbackPreventionSettings": None,
            "CharacterSetSettings": None,
            "FailTaskWhenCleanTaskResourceFailed": False,
            "ChangeProcessingTuning": {
                "StatementCacheSize": 50,
                "CommitTimeout": 1,
                "RecoveryTimeout": -1,
                "BatchApplyPreserveTransaction": True,
                "BatchApplyTimeoutMin": 1,
                "BatchSplitSize": 0,
                "BatchApplyTimeoutMax": 30,
                "MinTransactionSize": 1000,
                "MemoryKeepTime": 60,
                "BatchApplyMemoryLimit": 500,
                "MemoryLimitTotal": 1024
            },
            "ChangeProcessingDdlHandlingPolicy": {
                "HandleSourceTableDropped": True,
                "HandleSourceTableTruncated": True,
                "HandleSourceTableAltered": True
            },
            "PostProcessingRules": None
        }


    return settings
def table_json(schema_name = None, table_name = None, input_schema_name = None,table_category = None, schema_table_pairs = []):
    if table_category == "partition_table_json":

        return {
            "rules": [
                {
                    "rule-type": "transformation",
                    "rule-id": generate_rule_id(),
                    "rule-name": generate_rule_id(),
                    "rule-target": "table",
                    "object-locator": {
                        "schema-name": schema_name,
                        "table-name": table_name
                    },
                    "parallel-load": None,
                    "rule-action": "convert-lowercase",
                    "value": None,
                    "old-value": None
                },
                {
                    "rule-type": "transformation",
                    "rule-id": generate_rule_id(),
                    "rule-name": generate_rule_id(),
                    "rule-target": "schema",
                    "object-locator": {
                        "schema-name": schema_name
                    },
                    "parallel-load": None,
                    "rule-action": "rename",
                    "value": input_schema_name.lower(),
                    "old-value": None
                },
                {
                    "rule-type": "selection",
                    "rule-id": generate_rule_id(),
                    "rule-name": generate_rule_id(),
                    "object-locator": {
                        "schema-name": schema_name,
                        "table-name": table_name
                    },
                    "rule-action": "include",
                    "filters": []
                },
                {
                    "rule-type": "table-settings",
                    "rule-id": generate_rule_id(),
                    "rule-name": generate_rule_id(),
                    "object-locator": {
                        "schema-name": schema_name,
                        "table-name": table_name
                    },
                    "parallel-load": {
                        "type": "partitions-auto"
                    }
                }
            ]
        }
    elif table_category == "non_partition_table_json":
        return {
            "rules": [
                {
                    "rule-type": "transformation",
                    "rule-id": generate_rule_id(),
                    "rule-name": generate_rule_id(),
                    "rule-target": "table",
                    "object-locator": {
                        "schema-name": schema_name,
                        "table-name": table_name
                    },
                    "parallel-load": None,
                    "rule-action": "convert-lowercase",
                    "value": None,
                    "old-value": None
                },
                {
                    "rule-type": "transformation",
                    "rule-id": generate_rule_id(),
                    "rule-name": generate_rule_id(),
                    "rule-target": "schema",
                    "object-locator": {
                        "schema-name": schema_name
                    },
                    "parallel-load": None,
                    "rule-action": "rename",
                    "value": input_schema_name.lower(),
                    "old-value": None
                },
                {
                    "rule-type": "selection",
                    "rule-id": generate_rule_id(),
                    "rule-name": generate_rule_id(),
                    "object-locator": {
                        "schema-name": schema_name,
                        "table-name": table_name
                    },
                    "rule-action": "include"
                }
            ]
        }
    elif table_category == "lob_table_json":
        lob_remaining_tables_rules = []

        for schema_name, table_name in schema_table_pairs:
            lob_include_rule = {
                    "rule-type": "selection",
                    "rule-id": generate_rule_id(),
                    "rule-name": generate_rule_id(),
                    "object-locator": {
                        "schema-name": schema_name,
                        "table-name": table_name
                    },
                    "rule-action": "include"
                }

            lob_remaining_tables_rules.append(lob_include_rule)

        # Add a single include rule for all remaining tables in the schema
        lob_remaining_json = {
            "rules": [
                {
                    "rule-type": "transformation",
                    "rule-id": generate_rule_id(),
                    "rule-name": generate_rule_id(),
                    "rule-target": "table",
                    "object-locator": {
                        "schema-name": schema_name,
                        "table-name": table_name
                    },
                    "parallel-load": None,
                    "rule-action": "convert-lowercase",
                    "value": None,
                    "old-value": None
                },
                {
                    "rule-type": "transformation",
                    "rule-id": generate_rule_id(),
                    "rule-name": generate_rule_id(),
                    "rule-target": "schema",
                    "object-locator": {
                        "schema-name": schema_name
                    },
                    "parallel-load": None,
                    "rule-action": "rename",
                    "value": input_schema_name.lower(),
                    "old-value": None
                },
                {
                    "rule-type": "selection",
                    "rule-id": generate_rule_id(),
                    "rule-name": generate_rule_id(),
                    "object-locator": {
                        "schema-name": schema_name,
                        "table-name": table_name
                    },
                    "rule-action": "include"
                }
            ] + lob_remaining_tables_rules
        }

        return lob_remaining_json
    else:
        remaining_tables_rules = []
        for schema_name, table_name in schema_table_pairs:
            # Add exclude rules for already allocated tables
            # for schema, table_name in allocated_tables:
            exclude_rule = {
                "rule-type": "selection",
                "rule-id": generate_rule_id(),
                "rule-name": generate_rule_id(),
                "object-locator": {
                    "schema-name": schema_name,
                    "table-name": table_name
                },
                "rule-action": "exclude",
                "filters": []
            }
            remaining_tables_rules.append(exclude_rule)

        print(remaining_tables_rules)
        # Add a single include rule for all remaining tables in the schema
        remaining_json = {
            "rules": [
                         {
                             "rule-type": "transformation",
                             "rule-id": generate_rule_id(),
                             "rule-name": generate_rule_id(),
                             "rule-target": "table",
                             "object-locator": {
                                 "schema-name": schema_name,
                                 "table-name": "%"
                             },
                             "rule-action": "convert-lowercase",
                             "value": None,
                             "old-value": None
                         },
                         {
                             "rule-type": "transformation",
                             "rule-id": generate_rule_id(),
                             "rule-name": generate_rule_id(),
                             "rule-target": "schema",
                             "object-locator": {
                                 "schema-name": schema_name
                             },
                             "rule-action": "rename",
                             "value": input_schema_name.lower(),
                             "old-value": None
                         },
                         {
                             "rule-type": "selection",
                             "rule-id": generate_rule_id(),
                             "rule-name": generate_rule_id(),
                             "object-locator": {
                                 "schema-name": schema_name,
                                 "table-name": "%"
                             },
                             "rule-action": "include",
                             "filters": []
                         }
                     ] + remaining_tables_rules
        }

        return remaining_json
def generate_json_files(db_name, script_input, target_schema):
    folders = ["non_partition_table_json", "partition_table_json", "lob_table_json", "remaining_table_json" ]
    script_input_name = script_input + '.sql'

    for folder in folders:
        if not os.path.exists(folder + "/" + db_name):
            os.makedirs(folder + "/" + db_name)

    # Handle file or direct SQL input
    if os.path.isfile(script_input_name):
        with open(script_input_name, 'r') as script_file:
            # script = script_file.read()
            script = script_file.read().replace("{{db_name}}", db_name)
            print(script)
    else:
        script = script_input

    # Establish the database connection using the credentials
    conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};" \
               f"SERVER={mssql_connection['host']};" \
               f"DATABASE={db_name};" \
               f"UID={mssql_connection['user']};" \
               f"PWD={mssql_connection['password']}"
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    # Execute the script
    cursor.execute(script)

    tables = cursor.fetchall()

    # Debugging print
    print("Number of tables fetched:", len(tables))

    allocated_tables = []
    lob_allocated_tables = []
    # remaining_schemas = set()
    func_category = os.path.basename(script_input)
    for table in tables:
        if table.TableName == "ActMigListTables":
            # Skip this table entirely
            continue

        # print("Category: " + func_category)
        if func_category == "remaining_table_json":
            # dms_json = table_json(table.SchemaName, table.TableName, target_schema, func_category)
            allocated_tables.append((table.SchemaName, table.TableName))
        elif func_category == "lob_table_json":
            lob_allocated_tables.append((table.SchemaName, table.TableName))
        else:
            dms_json = table_json(table.SchemaName, table.TableName, target_schema, func_category)
        folder = script_input
        filename = f"{folder}/{db_name}/dms_task_{table.SchemaName}_{table.TableName}.json"

        if func_category != "remaining_table_json" and func_category != "lob_table_json":

            # Write the JSON file for PartitionTables
            print(f"Creating JSON file: {filename}")
            with open(filename, 'w') as json_file:
                json.dump(dms_json, json_file, indent=4)

    if func_category == "remaining_table_json" and allocated_tables is not None :
        dms_json = table_json(input_schema_name = target_schema, schema_table_pairs = allocated_tables)
        folder = script_input
        filename = f"{folder}/{db_name}/remaining_dms_task_{table.TableName}.json"
        print(f"Creating JSON file: {filename}")
        with open(filename, 'w') as json_file:
            json.dump(dms_json, json_file, indent=4)
    elif func_category == "lob_table_json" and lob_allocated_tables is not None :
        lob_dms_json = table_json(input_schema_name = target_schema, table_category=func_category,schema_table_pairs=lob_allocated_tables)
        folder = script_input
        lob_filename = f"{folder}/{db_name}/dms_task_{table.SchemaName}_{table.TableName}.json"
        print(f"Creating JSON file: {filename}")
        with open(lob_filename, 'w') as lob_json_file:
            json.dump(lob_dms_json, lob_json_file, indent=4)


    conn.close()
def generate_dms_settings_files(db_name, script_input, func_category):
    folders = ["general_dms_task_settings", "lob_dms_task_settings" ]
    for folder in folders:
        if not os.path.exists(folder + "/" + db_name):
            os.makedirs(folder + "/" + db_name)

    # Handle file or direct SQL input
    if os.path.isfile(script_input):
        with open(script_input, 'r') as script_file:
            # script = script_file.read()
            script = script_file.read().replace("{{db_name}}", db_name)
            print(script)
    else:
        script = script_input

    # Establish the database connection using the credentials
    conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};" \
               f"SERVER={mssql_connection['host']};" \
               f"DATABASE={db_name};" \
               f"UID={mssql_connection['user']};" \
               f"PWD={mssql_connection['password']}"
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    # Execute the script
    cursor.execute(script)

    result = cursor.fetchone()

    # Process the result
    if result:
        max_lob_size_kb = int(result[0])  # Get the first column value
        calculated_value = max_lob_size_kb * 2
        print(f"Original MaxLOBSizeKB: {max_lob_size_kb}")
        print(f"Calculated Value (MaxLOBSizeKB * 2): {calculated_value}")
    else:
        print("No results found.")
        calculated_value = 1

    if func_category == "lob_table_json":
        folder = "lob_dms_task_settings"
        dms_settings_json = settings_json(func_category, calculated_value)
        filename_2 = f"{folder}/{db_name}/dms_task_lob_settings.json"
        with open(filename_2, 'w') as json_file_2:
            json.dump(dms_settings_json, json_file_2, indent=4)
    else:
        folder = "general_dms_task_settings"
        dms_settings_json = settings_json(func_category, str(calculated_value))
        filename_2 = f"{folder}/{db_name}/dms_task_general_settings.json"
        with open(filename_2, 'w') as json_file_2:
            json.dump(dms_settings_json, json_file_2, indent=4)

    conn.close()

if __name__ == "__main__":
    current_dir = os.getcwd()

    # # Loop over all Tables and verify source and destination app version match
    # for db_name, details in databases_to_migrate.items():
    #     if details["product"] == "udm":
    #         sql_schema = details["source_db"] + '_CDS'
    #         source_schema = details["source_db"] + "." + sql_schema
    #     sql_script = details["product"] + '_version.sql'
    #     pg_script = details["product"] + '_pg_version.sql'
    #     sql_version_script_path = os.path.join(os.path.dirname(__file__), sql_script)
    #     pg_version_script_path = os.path.join(os.path.dirname(__file__), pg_script)
    #     if details["product"] == "udm":
    #         source_version = get_product_version(sql_version_script_path, "master", is_postgres=False, schema_name=source_schema)
    #     else:
    #         source_version = get_product_version(sql_version_script_path, "master", is_postgres=False, schema_name=details["source_db"])
    #     target_version = get_product_version(pg_version_script_path, db_name, is_postgres=True, schema_name=details["target_schema"])
    #     print(details["product"] + " source version is => " + source_version[0][0])
    #     print(details["product"] + " target version is => " + target_version[0][0])
    #     # Compare versions and append to the report
    #     if source_version[0][0] == target_version[0][0]:
    #         report.append(f"{db_name}: Versions match (Version: {source_version[0][0]})")
    #         # print(f"{db_name}: Versions match (Version: {source_version})")
    #     else:
    #         report.append(f"{db_name}: Versions do not match (Source: {source_version[0][0]}), Target: {target_version[0][0]})")
    #         # print(f"{db_name}: Versions do not match (Source: {source_version}, Target: {target_version[0][0]})")
    #         version_status = False
    # print("\n".join(report))
    # if not version_status:
    #     sys.exit("Stopping the application due to mismatched versions.")
    # else:
    #     print("versions matched will continue migration process")

    # if dms_create:
    #     create_dms_replication_instance(dms_details["instance_identifier"], dms_details["instance_class"],dms_details["allocated_storage"], dms_details["subnet_group_name"], dms_details["VpcSecurityGroupIds"], dms_details["region"],dms_details["public_access"], db_name=None)
    #     # configure_dms_endpoints('source', 'sqlserver', mssql_connection["host"], 1433, mssql_connection["database"], mssql_connection["user"], mssql_connection["password"], dms_details["region"])
    #     configure_dms_endpoints('target', 'postgres', rds_postgres_connection["host"], 5432, rds_postgres_connection["database"], rds_postgres_connection["user"], rds_postgres_connection["password"], dms_details["region"])
    #     print(dms_details["TargetEndpointArn"])

    for db_name, details in databases_to_migrate.items():
        # Generate a timestamp
        # timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # details["sourceendpointarn"] = configure_dms_endpoints('source', 'sqlserver', mssql_connection["host"], 1433, details["source_db"], mssql_connection["user"], mssql_connection["password"], dms_details["region"])
        # print("source endpoint for DB " + db_name + " Is " + details["sourceendpointarn"])


    #     ##### run_analyze_script
    #     run_analyze_script(details["source_db"])
    #
    #     ##### create partition alignment
    #     create_partition_alignment(details["source_db"],schema_name=details["target_schema"],is_postgres=False)
    #
    #     ##### disable triggers
    #     disable_triggers_in_pg(db_name, details["target_schema"])
    #
    #     ##### drop fks
    #     drop_fks_in_pg(db_name, details["target_schema"])
    #     disable_triggers_in_pg(db_name, details["target_schema"])
    #     # enable_triggers(db_name, details["target_schema"])
    #     # recreate_fks(db_name, details["target_schema"])
    #     create_partition_alignment(db_name, details["target_schema"], is_postgres=False)
    #     # change_partition_owner(db_name, details["target_schema"])
    #     # update_statistics(db_name, details["target_schema"])
    #     # align_db_sequence(db_name, details["target_schema"])
        data_compare(db_name, details["target_schema"])
    #
    #     #####
    #     change_partition_owner(db_name, details["target_schema"])
    #     enable_triggers(db_name, details["target_schema"])
    #     recreate_fks(db_name, details["target_schema"])
    #     update_statistics(db_name, details["target_schema"])
    #
    #     #####generate_partition_table_json
    #     settings_script_path = os.path.join(os.path.dirname(__file__), 'lob_maxsizekb.sql')
    #     generate_dms_settings_files(db_name, settings_script_path, 'partition_table_json')
    #     json_script_path = os.path.join(os.path.dirname(__file__), 'partition_table_json')
    #     print(json_script_path)
    #     generate_json_files(db_name,json_script_path,details["target_schema"])
    #
    #     #####generate_non_partition_table_json
    #     settings_script_path = os.path.join(os.path.dirname(__file__), 'lob_maxsizekb.sql')
    #     generate_dms_settings_files(db_name, settings_script_path, 'non_partition_table_json')
    #     json_script_path = os.path.join(os.path.dirname(__file__), 'non_partition_table_json')
    #     print(json_script_path)
    #     generate_json_files(db_name, json_script_path, details["target_schema"])
    #
    #     #####generate_lob_table_json
    #     settings_script_path = os.path.join(os.path.dirname(__file__), 'lob_maxsizekb.sql')
    #     generate_dms_settings_files(db_name, settings_script_path, 'lob_table_json')
    #     json_script_path = os.path.join(os.path.dirname(__file__), 'lob_table_json')
    #     print(json_script_path)
    #     generate_json_files(db_name, json_script_path, details["target_schema"])
    #
    #     #####generate_reaming_table_json
    #     settings_script_path = os.path.join(os.path.dirname(__file__), 'lob_maxsizekb.sql')
    #     generate_dms_settings_files(db_name, settings_script_path, 'remaining_table_json')
    #     json_script_path = os.path.join(os.path.dirname(__file__), 'remaining_table_json')
    #     print(json_script_path)
    #     generate_json_files(db_name, json_script_path, details["target_schema"])
    #
    # target_dirs = {"lob_table_json", "remaining_table_json", "partition_table_json", "non_partition_table_json"}
    # base_directory = os.path.dirname(__file__)
    # for root, dirs, files in os.walk(base_directory):
    #     # Check if any part of the current directory path matches a target directory name
    #     if any(target in root.split(os.sep) for target in target_dirs):
    #         for file in files:
    #             if file.endswith(".json"):
    #                 file_path = os.path.join(root, file)
    #                 db_name = os.path.basename(os.path.dirname(file_path))  # Get parent directory name
    #                 if "lob_table_json" in file_path:
    #                     settings_file_path = os.path.join(current_dir, "lob_dms_task_settings", db_name, "dms_task_lob_settings.json")
    #                 else:
    #                     settings_file_path = os.path.join(current_dir, "general_dms_task_settings", db_name, "dms_task_general_settings.json")
    #                 print(settings_file_path)
    #                 print(f"Processing JSON file: {file_path} | DB Name: {db_name}")
    #                 with open(file_path, 'r') as f:
    #                     data = json.load(f)
    #                     # # Convert the dictionary to a JSON string
    #                     data_json = json.dumps(data)
    #                     # print(data)
    #                 with open(settings_file_path, 'r') as f:
    #                     data_settings = json.load(f)
    #                     # # Convert the dictionary to a JSON string
    #                     data_settings_json = json.dumps(data_settings)
    #                 base_replicationtaskidentifier =  os.path.splitext(os.path.basename(file_path))[0]
    #
    #                 base_replicationtaskidentifier = base_replicationtaskidentifier.replace("_", "-")
    #                 # Prepend db_name to the replication task identifier
    #                 replicationtaskidentifier = f"{db_name}_{base_replicationtaskidentifier}"
    #                 replicationtaskidentifier =  replicationtaskidentifier.replace("_", "-")
    #                 print(replicationtaskidentifier)
    #                 print(data_settings_json)
    #                 create_dms_task(lower(replicationtaskidentifier), databases_to_migrate[db_name]["sourceendpointarn"],dms_details["TargetEndpointArn"], 'full-load', data_json, dms_details["instancearn"], data_settings_json, tags,databases_to_migrate[db_name]["source_db"], dms_details["region"])
