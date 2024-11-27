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
from botocore.exceptions import BotoCoreError, ClientError

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
        "dms_instance_arn": "None",
        "SourceEndpointArn": None,
        "TargetEndpointArn": None
    },
    "DB2": {
        "source_db_name": "source_db_name_2",
        "target_schema": "target_schema_2",
        "dms_instance_arn": "your-dms-instance-arn-2",
        "SourceEndpointArn": None,
        "TargetEndpointArn": None
    }
    # Add more databases as needed
}

# DMS Configuration
dms_create = True # Set to True to create DMS resources; False to use existing
tablemappings = '''{
    "rules": [
        {
            "rule-type": "selection",
            "rule-id": "1",
            "rule-name": "include_all",
            "object-locator": {
                "schema-name": "%",
                "table-name": "%"
            },
            "rule-action": "include"
        }
    ]
}'''

tasksettings = '''{
    "TargetMetadata": {
        "TargetSchema": "",
        "SupportLobs": True,
        "FullLobMode": False,
        "LobChunkSize": 64,
        "LimitedSizeLobMode": True,
        "LobMaxSize": 32,
        "InlineLobMaxSize": 0
    },
    "Logging": {
        "EnableLogging": True
    }
}'''

tags = [
    {'Key': 'Environment', 'Value': 'Production'},
    {'Key': 'Project', 'Value': 'DatabaseMigration'}
]

dms_details = {
    "instance_identifier": "nizlog141-replication-instance01",
    "instance_class": "dms.t3.medium",
    "allocated_storage": 20,
    "subnet_group_name": "fuga-subnet-group",
    "public_access": False,
    "region": "us-west-2",
    "aws_profile": "dev",
    "VpcSecurityGroupIds": ["sg-022f2bab62aa433a0", "sg-0dc668870d406bba4"],
    "SourceEndpointArn": None,
    "TargetEndpointArn": None
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

def execute_script_on_database(script_input, db_name, is_postgres=False, schema_name=None):
    try:
        # Handle file or direct SQL input
        if os.path.isfile(script_input):
            with open(script_input, 'r') as script_file:
                script = script_file.read()
        else:
            script = script_input
        activity_logger.info(f"Executing {'PostgreSQL' if is_postgres else 'MSSQL'} script {script_input} for {schema_name or db_name}.")

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
            result_file = f"{schema_name if is_postgres else db_name}_script_results_{timestamp}.txt"
            with open(result_file, 'w') as f:
                for row in results:
                    f.write(str(row) + '\n')
            activity_logger.info(f"Results saved to: {result_file}")
            return result_file
        else:
            return None

    except Exception as e:
        activity_logger.error(f"Error executing script for {'schema ' + schema_name if is_postgres else 'database ' + db_name}: {str(e)}")
        raise

def run_analyze_script(db_name, is_postgres=False, schema_name=None):
    analyze_template_path = os.path.join(os.path.dirname(__file__), 'mssql_analyze.sql')
    activity_logger.info(f"Running 'Analyze' action for database: {db_name}")
    try:
        result_file = execute_script_on_database(analyze_template_path, db_name, is_postgres, schema_name)  # True for source database
        if result_file:
            activity_logger.info(f"Analyze script executed successfully on {db_name}. Results saved to: {result_file}")
        else:
            activity_logger.info(f"Analyze script executed successfully on {db_name}. No results were returned.")
    except Exception as e:
        activity_logger.error(f"Failed to run analyze on {db}: {str(e)}")
        raise


def create_partition_alignment(db_name, is_postgres=True, schema_name=None):
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

        # Execute the modified script
        result_file = execute_script_on_database(temp_script_path,db_name,is_postgres, schema_name)  # True for source database
        print(result_file)

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

def disable_triggers_in_pg(db_name):
    try:
        schema_name = db_name.lower()

        activity_logger.info(f"Processing disable triggers commands for schema {schema_name}...")

        # Verify if the schema exists before proceeding
        check_schema_script = f"SELECT schema_name FROM information_schema.schemata WHERE schema_name = '{schema_name}';"

        # Execute the check script
        schema_exists = execute_script_on_database(check_schema_script,databases_to_migrate["DemoDB"]["target_schema"], True, schema_name)

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
        execute_script_on_database(sql_script,"postgres", is_postgres=True, schema_name=schema_name)

        activity_logger.info(f"Triggers disabled for schema {schema_name}")
    except Exception as e:
        activity_logger.error(f"Error in disabling triggers for schema {schema_name}: {str(e)}")

def drop_fks_in_pg(db_name):
    try:
        schema_name = db_name.lower()  # Convert the schema name to lowercase to avoid case mismatches
        activity_logger.info(f"Processing drop foreign key commands for schema {schema_name}...")

        # Verify if the schema exists before proceeding
        check_schema_script = f"SELECT schema_name FROM information_schema.schemata WHERE schema_name = '{schema_name}';"

        # Execute the check script
        schema_exists = execute_script_on_database(check_schema_script, databases_to_migrate["DemoDB"]["target_schema"],True, schema_name)

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
        execute_script_on_database(sql_script, "postgres", is_postgres=True, schema_name=schema_name)

        activity_logger.info(f"Triggers disabled for schema {schema_name}")

    except Exception as e:
        activity_logger.info(f"Error in processing drop foreign keys for schema {schema_name}: {str(e)}")

def create_dms_replication_instance(instance_identifier, instance_class, allocated_storage, subnet_group_name, security_group_ids, region, publicly_accessible=True):
    # Create a session with the specified profile
    session = boto3.Session(profile_name=dms_details["aws_profile"])

    # Create a DMS client for the specified region
    dms_client = session.client('dms', region_name=region)

    # Create the DMS client
    # dms_client = boto3.client('dms', dms_details["region"])
    try:
        # Create the replication instance
        response = dms_client.create_replication_instance(
            ReplicationInstanceIdentifier=instance_identifier,
            ReplicationInstanceClass=instance_class,
            AllocatedStorage=allocated_storage,
            ReplicationSubnetGroupIdentifier=subnet_group_name,
            VpcSecurityGroupIds=security_group_ids,
            PubliclyAccessible=publicly_accessible
        )
        print(f"Replication Instance '{instance_identifier}' created successfully!")
        # Store the replication instance ARN
        databases_to_migrate["DemoDB"]["dms_instance_arn"] = response['ReplicationInstance']['ReplicationInstanceArn']
        logging.info(f"Stored replication instance ARN: databases_to_migrate['DemoDB']['dms_instance_arn']")

        # Wait for the instance to be available
        activity_logger.info("Waiting for replication instance to be available...\n")
        if not wait_for_replication_instance(dms_client, instance_identifier):
            messagebox.showerror("Error", "Failed to create replication instance")
            return

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
        databases_to_migrate["DemoDB"][arn_key] = source_response['Endpoint']['EndpointArn']

    except Exception as e:
        activity_logger.error(f"Unexpected error: {str(e)}")

def create_dms_task(sourceendpointarn,targetendpointarn,migrationtype,tablemappings,replicationinstancearn,tasksettings,tags,databasename,region):
    try:
        activity_logger.info(f"Processing migration task for databases: {databasename}")
        replicationtaskidentifier = "my-migration-task"

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


if __name__ == "__main__":
    # run_analyze_script(databases_to_migrate["DemoDB"]["source_db_name"])
    # create_partition_alignment(databases_to_migrate["DemoDB"]["source_db_name"],False,databases_to_migrate["DemoDB"]["target_schema"])
    # disable_triggers_in_pg(databases_to_migrate["DemoDB"]["target_schema"])
    # drop_fks_in_pg(databases_to_migrate["DemoDB"]["target_schema"])
    # create_dms_replication_instance(dms_details["instance_identifier"],dms_details["instance_class"],dms_details["allocated_storage"],dms_details["subnet_group_name"],dms_details["VpcSecurityGroupIds"],dms_details["region"],dms_details["public_access"])
    # print(databases_to_migrate["DemoDB"]["dms_instance_arn"])
    # configure_dms_endpoints('source','sqlserver',mssql_connection["host"],1433,databases_to_migrate["DemoDB"]["source_db_name"],mssql_connection["user"],mssql_connection["password"],dms_details["region"])
    # configure_dms_endpoints('target','postgres',rds_postgres_connection["host"], 5432,databases_to_migrate["DemoDB"]["target_schema"], rds_postgres_connection["user"],rds_postgres_connection["password"], dms_details["region"])
    # print(databases_to_migrate["DemoDB"]["TargetEndpointArn"])
    # print(databases_to_migrate["DemoDB"]["SourceEndpointArn"])
    # create_dms_task(databases_to_migrate["DemoDB"]["SourceEndpointArn"],databases_to_migrate["DemoDB"]["TargetEndpointArn"],'full-load',tablemappings,databases_to_migrate["DemoDB"]["dms_instance_arn"],tasksettings,tags,databases_to_migrate["DemoDB"]["source_db_name"],dms_details["region"])
    for db_name, details in databases_to_migrate.items():
        print(f"Processing {db_name}...")
        source_db_name = details["source_db_name"]
        target_schema = details["target_schema"]
        dms_instance_arn = details["dms_instance_arn"]
        source_endpoint_arn = details["SourceEndpointArn"]
        target_endpoint_arn = details["TargetEndpointArn"]

        print(f"  Source DB Name: {source_db_name}")
        print(f"  Target Schema: {target_schema}")
        print(f"  DMS Instance ARN: {dms_instance_arn}")
        print(f"  Source Endpoint ARN: {source_endpoint_arn}")
        print(f"  Target Endpoint ARN: {target_endpoint_arn}")
        print("-" * 40)
