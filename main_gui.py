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

env = os.getenv('ENV', 'development')
target_schema_name = None
aws_access_key = None
aws_secret_key = None
region = None
replication_instance_arn = None
selected_databases = []

if env == 'development':
    logging.basicConfig(
        filename='activity.log',
        format='%(asctime)s - %(levelname)s - %(message)s',
        level=logging.DEBUG
    )
    logging.info("Logging configured for development environment with DEBUG level.")

elif env == 'production':
    logging.basicConfig(
        handlers=[RotatingFileHandler('activity.log', maxBytes=5000000, backupCount=5)],
        format='%(asctime)s - %(levelname)s - %(message)s',
        level=logging.INFO
    )
    logging.info("Logging configured for production environment with INFO level and log rotation.")

root = tk.Tk()
root.title("DMS-Migration-Utility")
root.geometry("600x400")

# button = tk.Button(root, text="Start the Database Migration Process", command=lambda: fetch_databases())
# button.place(relx=0.5, rely=0.5, anchor="center", width=200, height=50)

# checkboxes = []
# all_databases = []
# source_host_input = source_user_input = source_password_input = None
# target_host_input = target_user_input = target_password_input = None
# select_all_btn = deselect_all_btn = migrate_button = search_entry = None

# selected_db_details = {}


# def show_error_with_copy_option(db, error_message):
#     error_window = tk.Toplevel()
#     error_window.title(f"Error in {db}")
#     error_window.geometry("400x300")

#     error_label = tk.Label(error_window, text=f"An error occurred in database {db}:", wraplength=380)
#     error_label.pack(pady=10)

#     error_text = tk.Text(error_window, wrap=tk.WORD, width=45, height=10)
#     error_text.insert(tk.END, error_message)
#     error_text.config(state=tk.DISABLED)
#     error_text.pack(pady=10)

#     def copy_to_clipboard():
#         error_window.clipboard_clear()
#         error_window.clipboard_append(error_message)
#         error_window.update()  # Required on some systems
#         messagebox.showinfo("Copied", "Error message copied to clipboard!")

#     copy_button = tk.Button(error_window, text="Copy Error Message", command=copy_to_clipboard)
#     copy_button.pack(pady=10)

# def fetch_databases():
#     try:
#         logging.info("Fetching databases initiated by user.")

#         # Source database details
#         source_host = simpledialog.askstring("Input", "Enter source host", initialvalue="10.220.3.97")
#         if not source_host:
#             logging.info("Fetching databases cancelled by the user.")
#             return
#         source_user = simpledialog.askstring("Input", "Enter source user", initialvalue="sa")
#         if not source_user:
#             logging.info("Fetching databases cancelled by the user.")
#             return
#         source_password = simpledialog.askstring("Input", "Enter source password", show='*', initialvalue="Password1!")
#         if source_password is None:
#             logging.info("Fetching databases cancelled by the user.")
#             return

#         # Target database details
#         target_host = simpledialog.askstring("Input", "Enter target host", initialvalue="nizlog14.cwinufw9r6rg.us-west-2.rds.amazonaws.com")
#         if not target_host:
#             logging.info("Fetching databases cancelled by the user.")
#             return
#         target_user = simpledialog.askstring("Input", "Enter target user", initialvalue="sa")
#         if not target_user:
#             logging.info("Fetching databases cancelled by the user.")
#             return
#         target_password = simpledialog.askstring("Input", "Enter target password", show='*', initialvalue="mLIgHWFcyu")
#         if target_password is None:
#             logging.info("Fetching databases cancelled by the user.")
#             return

#         global source_host_input, source_user_input, source_password_input
#         global target_host_input, target_user_input, target_password_input
#         source_host_input, source_user_input, source_password_input = source_host, source_user, source_password
#         target_host_input, target_user_input, target_password_input = target_host, target_user, target_password

#         logging.info(f"Fetching databases from source host: {source_host}")

#         global all_databases
#         all_databases = get_databases(source_host, source_user, source_password)
#         all_pg_databases = get_pg_databases(target_host, target_user, target_password)
#         all_pg_schemas = get_pg_schemas(target_host, target_user, target_password)
#         selected_schemas = {}


#         logging.info(f"Databases fetched successfully: {all_databases}")

#         button.place_forget()

#         # Create a frame to display source and target details
#         details_frame = tk.Frame(root)
#         details_frame.pack(pady=10)

#         source_label = tk.Label(details_frame, text=f"Source: {source_host}", font=("Arial", 10, "bold"))
#         source_label.pack(side="left", padx=10)

#         target_label = tk.Label(details_frame, text=f"Target: {target_host}", font=("Arial", 10, "bold"))
#         target_label.pack(side="left", padx=10)

#         control_frame = tk.Frame(root)
#         control_frame.pack(pady=10)

#         search_label = tk.Label(control_frame, text="Search:")
#         search_label.pack(side='left', padx=(0, 5))
#         search_var = tk.StringVar()
        
#         global select_all_btn, deselect_all_btn, migrate_button, search_entry
        
#         search_entry = tk.Entry(control_frame, textvariable=search_var)
#         search_entry.pack(side='left', padx=(0, 10))
#         search_entry.bind("<KeyRelease>", lambda event: filter_databases(search_var.get()))

#         select_all_btn = tk.Button(control_frame, text="Select All", command=select_all)
#         select_all_btn.pack(side='left', padx=5)
#         deselect_all_btn = tk.Button(control_frame, text="Deselect All", command=deselect_all)
#         deselect_all_btn.pack(side='left', padx=5)

#         migrate_button = tk.Button(control_frame, text="Migrate Selected Databases", command=migrate_selected_databases)
#         migrate_button.pack(side='left', padx=5)

#         checkbox_frame = tk.Frame(root)
#         checkbox_frame.pack(fill='both', expand=True, padx=10, pady=(0, 10))

#         scrollbar = tk.Scrollbar(checkbox_frame)
#         scrollbar.pack(side='right', fill='y')

#         canvas = tk.Canvas(checkbox_frame, yscrollcommand=scrollbar.set)
#         canvas.pack(side='left', fill='both', expand=True)

#         scrollbar.config(command=canvas.yview)

#         checkboxes_frame = tk.Frame(canvas)
#         canvas.create_window((0, 0), window=checkboxes_frame, anchor='nw')

#         global checkboxes
#         # Dictionary to store the selected details for each database
#         checkboxes = []
#         for db in all_databases:
#             var = tk.BooleanVar()
            
#             # Create a frame to hold both the Checkbutton and Combobox
#             db_frame = tk.Frame(checkboxes_frame)
#             db_frame.pack(anchor='w', fill='x', padx=5, pady=2)

#             # Create the Checkbutton for the database
#             chk = tk.Checkbutton(db_frame, text=db, variable=var)
#             chk.pack(side='left')

#             # Create the Combobox (dropdown) for the schemas
#             schema_dropdown = ttk.Combobox(db_frame, values=all_pg_schemas)
#             schema_dropdown.set("Select Schema")  # Placeholder text
#             schema_dropdown.pack(side='left', padx=5)
            
#             # Store the Checkbutton and Combobox for reference
#             checkboxes.append((chk, var, schema_dropdown))

#     except Exception as e:
#         logging.error(f"Error fetching databases: {e}")
#         messagebox.showerror("Error", str(e))

# def save_db_details(db_index, db_name):
#     # Get current selections and entries
#     if selected_db_details.get(db_name):
#         selected_db_details[db_name]["target_db"] = target_db_entry.get()
#         selected_db_details[db_name]["source_version"] = source_version_entry.get()
#         selected_db_details[db_name]["target_version"] = target_version_entry.get()
#     else:
#         selected_db_details[db_name] = {
#             "target_db": target_db_entry.get(),
#             "source_version": source_version_entry.get(),
#             "target_version": target_version_entry.get()
#         }

#     logging.info(f"Details saved for {db_name}: {selected_db_details[db_name]}")

# def filter_databases(query):
#     logging.info(f"Filtering databases with query: {query}")

#     for chk, var in checkboxes:
#         chk.pack_forget()

#     for chk, var in checkboxes:
#         db_name = chk.cget("text")
#         if query.lower() in db_name.lower():
#             chk.pack(anchor='w')

# def select_all():
#     logging.info("Selecting all databases.")
#     for chk, var in checkboxes:
#         var.set(True)

# def deselect_all():
#     logging.info("Deselecting all databases.")
#     for chk, var in checkboxes:
#         var.set(False)

# def disable_main_window_controls():
#     for chk, var in checkboxes:
#         chk.config(state=tk.DISABLED)
#     select_all_btn.config(state=tk.DISABLED)
#     deselect_all_btn.config(state=tk.DISABLED)
#     migrate_button.config(state=tk.DISABLED)
#     search_entry.config(state=tk.DISABLED)

# def enable_main_window_controls():
#     for chk, var in checkboxes:
#         chk.config(state=tk.NORMAL)
#     select_all_btn.config(state=tk.NORMAL)
#     deselect_all_btn.config(state=tk.NORMAL)
#     migrate_button.config(state=tk.NORMAL)
#     search_entry.config(state=tk.NORMAL)

# def execute_script_on_database(script_input, is_source, db_name, is_postgres=False, schema_name=None):
#     script = ""  # Initialize script variable
#     try:
#         if is_postgres:
#             # PostgreSQL connection using the target connection details
#             import psycopg2
#             conn = psycopg2.connect(
#                 host=target_host_input,
#                 database="actdb",  # Using the default postgres database
#                 user=target_user_input,
#                 password=target_password_input
#             )
#             cursor = conn.cursor()
            
#             # Handle file or direct SQL input
#             if os.path.isfile(script_input):
#                 with open(script_input, 'r') as script_file:
#                     script = script_file.read()
#             else:
#                 script = script_input

#             print(f"Executing PostgreSQL script for schema {schema_name}:\n{script}")

#             # Execute the script
#             cursor.execute(script)
#             try:
#                 results = cursor.fetchall()
#             except psycopg2.ProgrammingError:
#                 conn.commit()  # Commit any changes even if there are no results
#                 results = None
                
#         else:
#             # Regular SQL Server connection
#             server = source_host_input if is_source else target_host_input
#             username = source_user_input if is_source else target_user_input
#             password = source_password_input if is_source else target_password_input
            
#             conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={db_name};UID={username};PWD={password}'
#             conn = pyodbc.connect(conn_str)
#             cursor = conn.cursor()

#             if os.path.isfile(script_input):
#                 with open(script_input, 'r') as script_file:
#                     script = script_file.read()
#             else:
#                 script = script_input

#             cursor.execute(script)
#             try:
#                 results = cursor.fetchall()
#             except pyodbc.ProgrammingError:
#                 results = None

#         # Commit any changes
#         conn.commit()

#         # Close the connection
#         cursor.close()
#         conn.close()

#         # If there are results, save them to a file
#         if results:
#             timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
#             result_file = f"{schema_name if is_postgres else db_name}_script_results_{timestamp}.txt"
#             with open(result_file, 'w') as f:
#                 for row in results:
#                     f.write(str(row) + '\n')
#             return result_file
#         else:
#             return None

#     except Exception as e:
#         logging.error(f"Error executing script for {'schema ' + schema_name if is_postgres else 'database ' + db_name}: {str(e)}")
#         logging.error(f"Problematic SQL:\n{script}")
#         raise

# def simplify_create_table(command):
#     # Remove PostgreSQL-specific parts of the CREATE TABLE command
#     command = re.sub(r'\(.*?\)', '', command)  # Remove column definitions
#     command = re.sub(r'WITH \(.*?\)', '', command)  # Remove WITH clause
#     command = command.replace('IF NOT EXISTS', '')  # Remove IF NOT EXISTS
#     return command.strip() + '(id INTEGER PRIMARY KEY)'  # Add a simple column

# def create_partition_alignment(selected_databases, partition_vars, partition_progress_bars, log_text):
#     global target_schema_name
#     script_path = os.path.join(os.path.dirname(__file__), 'Partition_Alignment_General.sql')
    
#     # Prompt for target schema name
#     target_schema_name = simpledialog.askstring("Input", "Enter the target schema name:", initialvalue="public")
#     if not target_schema_name:
#         log_message = "Partition alignment cancelled: No target schema name provided."
#         log_text.insert(tk.END, log_message + "\n")
#         log_text.see(tk.END)
#         logging.info(log_message)
#         return

#     for idx, db in enumerate(selected_databases):
#         if partition_vars[idx].get():
#             try:
#                 log_message = f"Creating partition alignment for {db}..."
#                 log_text.insert(tk.END, log_message + "\n")
#                 log_text.see(tk.END)
#                 logging.info(log_message)
                
#                 # Read and modify the SQL script
#                 with open(script_path, 'r') as file:
#                     sql_script = file.read()
#                 modified_script = sql_script.replace("Target_schema_name", target_schema_name)
                
#                 # Create a temporary file with the modified script
#                 temp_script_path = os.path.join(os.path.dirname(__file__), f'temp_partition_alignment_{db}.sql')
#                 with open(temp_script_path, 'w') as file:
#                     file.write(modified_script)
                
#                 # Execute the modified script
#                 result_file = execute_script_on_database(temp_script_path, True, db)  # True for source database
                
#                 # Remove the temporary file
#                 os.remove(temp_script_path)
                
#                 if result_file:
#                     # Process the results
#                     with open(result_file, 'r') as f:
#                         generated_sql = f.readlines()
                    
#                     # Process and clean up the SQL statements
#                     cleaned_sql = []
#                     for line in generated_sql:
#                         try:
#                             # Safely evaluate the string as a Python literal
#                             tuple_content = ast.literal_eval(line.strip())
#                             # Extract the SQL statement from the tuple and remove extra quotes
#                             sql_statement = tuple_content[0].strip('"')
#                             cleaned_sql.append(sql_statement)
#                         except:
#                             # If there's any error in processing, keep the original line
#                             cleaned_sql.append(line.strip())
                    
#                     # Save the cleaned SQL to a new file
#                     timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
#                     output_file = f"{db}_partition_alignment_{timestamp}.sql"
#                     with open(output_file, 'w') as f:
#                         for sql in cleaned_sql:
#                             f.write(sql + '\n')
                    
#                     log_message = f"Partition alignment SQL generated for {db}. Saved to: {output_file}"
#                     log_text.insert(tk.END, log_message + "\n")
#                     log_text.see(tk.END)
#                     logging.info(log_message)
#                     messagebox.showinfo("Success", log_message)
#                 else:
#                     log_message = f"Partition alignment script executed for {db}, but no SQL was generated."
#                     log_text.insert(tk.END, log_message + "\n")
#                     log_text.see(tk.END)
#                     logging.info(log_message)
#                     messagebox.showinfo("Warning", log_message)
                
#                 # Update progress bar
#                 partition_progress_bars[0]['value'] = 100
                
#             except Exception as e:
#                 error_message = f"Error in creating partition alignment for {db}: {str(e)}"
#                 log_text.insert(tk.END, error_message + "\n")
#                 log_text.see(tk.END)
#                 logging.error(error_message)
#                 show_error_with_copy_option(db, error_message)
#                 partition_progress_bars[0]['value'] = 0
            
#             root.update_idletasks()

# def drop_fks_in_pg(selected_databases, drop_fks_vars, drop_fks_progress_bars, log_text):
#     for idx, db in enumerate(selected_databases):
#         if drop_fks_vars[idx].get():
#             try:
#                 schema_name = db.lower()  # Convert the schema name to lowercase to avoid case mismatches
#                 log_message = f"Processing drop foreign key commands for schema {schema_name}..."
#                 log_text.insert(tk.END, log_message + "\n")
#                 log_text.see(tk.END)
#                 logging.info(log_message)

#                 # Verify if the schema exists before proceeding
#                 check_schema_script = f"SELECT schema_name FROM information_schema.schemata WHERE schema_name = '{schema_name}';"
                
#                 # Execute the check script
#                 schema_exists = execute_script_on_database(check_schema_script, False, "postgres", is_postgres=True, schema_name=schema_name)
                
#                 if not schema_exists:
#                     error_message = f"Schema '{schema_name}' does not exist. Skipping drop foreign key commands."
#                     log_text.insert(tk.END, error_message + "\n")
#                     log_text.see(tk.END)
#                     logging.warning(error_message)
#                     continue
                
#                 # Read the SQL template
#                 sql_file_path = os.path.join(os.path.dirname(__file__), 'drop_foreign_keys_in_pg.sql')
#                 try:
#                     with open(sql_file_path, 'r') as file:
#                         sql_template = file.read()
#                 except FileNotFoundError:
#                     error_message = f"SQL file not found: {sql_file_path}"
#                     log_text.insert(tk.END, error_message + "\n")
#                     log_text.see(tk.END)
#                     logging.error(error_message)
#                     messagebox.showerror("Error", error_message)
#                     continue

#                 # Replace placeholder with actual schema name
#                 sql_script = sql_template.replace('target_schema_name', schema_name)

#                 # Save the SQL script for reference
#                 timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
#                 debug_sql_file = f"{schema_name}_drop_fks_debug_{timestamp}.sql"
#                 with open(debug_sql_file, 'w') as f:
#                     f.write(sql_script)
                
#                 log_message = f"SQL script saved to: {debug_sql_file}"
#                 log_text.insert(tk.END, log_message + "\n")
#                 log_text.see(tk.END)
#                 logging.info(log_message)

#                 # Execute the script on the target PostgreSQL database
#                 execute_script_on_database(sql_script, False, "postgres", is_postgres=True, schema_name=schema_name)

#                 log_message = f"Foreign key commands processed for schema {schema_name}"
#                 log_text.insert(tk.END, log_message + "\n")
#                 log_text.see(tk.END)
#                 logging.info(log_message)
                
#                 # Update progress bar
#                 drop_fks_progress_bars[idx]['value'] = 100
                
#             except Exception as e:
#                 error_message = f"Error in processing drop foreign keys for schema {schema_name}: {str(e)}"
#                 log_text.insert(tk.END, error_message + "\n")
#                 log_text.see(tk.END)
#                 logging.error(error_message)
#                 show_error_with_copy_option(db, error_message)
#                 drop_fks_progress_bars[idx]['value'] = 0
            
#             root.update_idletasks()

# def disable_triggers_in_pg(selected_databases, disable_triggers_vars, disable_triggers_progress_bars, log_text):
#     for idx, db in enumerate(selected_databases):
#         if disable_triggers_vars[idx].get():
#             try:
#                 schema_name = db.lower()  # Use the original database name without changing case
#                 log_message = f"Processing disable triggers commands for schema {schema_name}..."
#                 log_text.insert(tk.END, log_message + "\n")
#                 log_text.see(tk.END)
#                 logging.info(log_message)

#                 # Verify if the schema exists before proceeding
#                 check_schema_script = f"SELECT schema_name FROM information_schema.schemata WHERE schema_name = '{schema_name}';"
                
#                 # Execute the check script
#                 schema_exists = execute_script_on_database(check_schema_script, False, "postgres", is_postgres=True, schema_name=schema_name)
                
#                 if not schema_exists:
#                     error_message = f"Schema '{schema_name}' does not exist. Skipping disable triggers commands."
#                     log_text.insert(tk.END, error_message + "\n")
#                     log_text.see(tk.END)
#                     logging.warning(error_message)
#                     continue
                
#                 # Read the SQL template for disabling triggers
#                 sql_file_path = os.path.join(os.path.dirname(__file__), 'disable_triggers_in_pg.sql')
#                 try:
#                     with open(sql_file_path, 'r') as file:
#                         sql_template = file.read()
#                 except FileNotFoundError:
#                     error_message = f"SQL file not found: {sql_file_path}"
#                     log_text.insert(tk.END, error_message + "\n")
#                     log_text.see(tk.END)
#                     logging.error(error_message)
#                     messagebox.showerror("Error", error_message)
#                     continue

#                 # Replace placeholder with actual schema name
#                 sql_script = sql_template.replace('target_schema_name', schema_name)

#                 # Save the SQL script for reference
#                 timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
#                 debug_sql_file = f"{schema_name}_disable_triggers_debug_{timestamp}.sql"
#                 with open(debug_sql_file, 'w') as f:
#                     f.write(sql_script)
                
#                 log_message = f"SQL script saved to: {debug_sql_file}"
#                 log_text.insert(tk.END, log_message + "\n")
#                 log_text.see(tk.END)
#                 logging.info(log_message)

#                 # Execute the script on the target PostgreSQL database
#                 execute_script_on_database(sql_script, False, "postgres", is_postgres=True, schema_name=schema_name)

#                 log_message = f"Triggers disabled for schema {schema_name}"
#                 log_text.insert(tk.END, log_message + "\n")
#                 log_text.see(tk.END)
#                 logging.info(log_message)
                
#                 # Update progress bar
#                 disable_triggers_progress_bars[idx]['value'] = 100
                
#             except Exception as e:
#                 error_message = f"Error in disabling triggers for schema {schema_name}: {str(e)}"
#                 log_text.insert(tk.END, error_message + "\n")
#                 log_text.see(tk.END)
#                 logging.error(error_message)
#                 show_error_with_copy_option(db, error_message)
#                 disable_triggers_progress_bars[idx]['value'] = 0
            
#             root.update_idletasks()  

# def get_aws_resources(aws_access_key, aws_secret_key, region=None):
#     """
#     Get actual AWS resources available for the account
#     Parameters:
#         aws_access_key: AWS access key
#         aws_secret_key: AWS secret key
#         region: Optional specific region to get resources from
#     Returns:
#         Dictionary containing lists of AWS resources
#     """

#     logging.info(f"get_aws_resources called with region: {region}")

#     resources = {
#         'regions': [],
#         'vpcs': [],
#         'subnet_groups': [],
#         'security_groups': [],
#         'instance_classes': []
#     }
    
#     try:
#         # First verify AWS credentials using STS
#         sts_client = boto3.client(
#             'sts',
#             aws_access_key_id=aws_access_key,
#             aws_secret_access_key=aws_secret_key,
#             region_name='us-east-1'  # Use default region to list regions
#         )
        
#         try:
#             identity = sts_client.get_caller_identity()
#             logging.info(f"Successfully authenticated with AWS. Account ID: {identity['Account']}")
#         except Exception as e:
#             logging.error(f"AWS authentication failed: {str(e)}")
#             raise Exception("Failed to authenticate with AWS. Please check your credentials.")
        
#         # Get list of regions
#         ec2_client = boto3.client(
#             'ec2',
#             aws_access_key_id=aws_access_key,
#             aws_secret_access_key=aws_secret_key,
#             region_name='us-east-1'
#         )
        
#         logging.info("Fetching available AWS regions...")
#         regions_response = ec2_client.describe_regions()
#         resources['regions'] = [region['RegionName'] for region in regions_response['Regions']]
#         logging.info(f"Found {len(resources['regions'])} regions")
        
#         # If a specific region is provided, get resources for that region
#         if region:
#             logging.info(f"Fetching resources for region {region}")
            
#             # Create regional clients
#             regional_ec2_client = boto3.client(
#                 'ec2',
#                 aws_access_key_id=aws_access_key,
#                 aws_secret_access_key=aws_secret_key,
#                 region_name=region
#             )
            
#             dms_client = boto3.client(
#                 'dms',
#                 aws_access_key_id=aws_access_key,
#                 aws_secret_access_key=aws_secret_key,
#                 region_name=region
#             )
            
#             # Get available DMS instance classes
#             logging.info("Fetching available DMS instance classes...")
#             try:
#                 ordering_response = dms_client.describe_orderable_replication_instances()
#                 resources['instance_classes'] = sorted(list(set([
#                     instance['ReplicationInstanceClass']
#                     for instance in ordering_response['OrderableReplicationInstances']
#                 ])))
#                 logging.info(f"Found {len(resources['instance_classes'])} instance classes")
#             except Exception as e:
#                 logging.error(f"Error getting instance classes: {str(e)}")
            
#             # Get VPCs with error handling
#             logging.info("Fetching VPCs...")
#             try:
#                 vpc_response = regional_ec2_client.describe_vpcs()
#                 resources['vpcs'] = [
#                     {
#                         'id': vpc['VpcId'],
#                         'cidr': vpc['CidrBlock'],
#                         'tags': vpc.get('Tags', []),
#                         'is_default': vpc.get('IsDefault', False),
#                         'state': vpc.get('State', 'unknown')
#                     }
#                     for vpc in vpc_response['Vpcs']
#                 ]
#                 logging.info(f"Found {len(resources['vpcs'])} VPCs")
#             except Exception as e:
#                 logging.error(f"Error fetching VPCs: {str(e)}")
            
#             # Get DMS subnet groups with error handling
#             logging.info("Fetching DMS subnet groups...")
#             try:
#                 subnet_groups_response = dms_client.describe_replication_subnet_groups()
#                 resources['subnet_groups'] = [
#                     {
#                         'name': group['ReplicationSubnetGroupIdentifier'],
#                         'description': group['ReplicationSubnetGroupDescription'],
#                         'vpc_id': group['VpcId'],
#                         'subnet_ids': [subnet['SubnetIdentifier'] for subnet in group['Subnets']],
#                         'status': group['SubnetGroupStatus']
#                     }
#                     for group in subnet_groups_response['ReplicationSubnetGroups']
#                 ]
#                 logging.info(f"Found {len(resources['subnet_groups'])} subnet groups")
#             except dms_client.exceptions.ResourceNotFoundFault:
#                 logging.info("No subnet groups found")
#             except Exception as e:
#                 logging.error(f"Error fetching subnet groups: {str(e)}")
            
#             # Get security groups with error handling
#             logging.info("Fetching security groups...")
#             try:
#                 security_groups_response = regional_ec2_client.describe_security_groups()
#                 resources['security_groups'] = [
#                     {
#                         'GroupId': sg['GroupId'],
#                         'GroupName': sg['GroupName'],
#                         'Description': sg.get('Description', ''),
#                         'VpcId': sg.get('VpcId', ''),
#                         'Tags': sg.get('Tags', [])
#                     }
#                     for sg in security_groups_response['SecurityGroups']
#                 ]
#                 logging.info(f"Found {len(resources['security_groups'])} security groups")
#                 for sg in resources['security_groups']:
#                     logging.info(f"Security Group: {sg['GroupName']} ({sg['GroupId']}) in VPC: {sg['VpcId']}")
#             except Exception as e:
#                 logging.error(f"Error fetching security groups: {str(e)}")
                
#     except boto3.exceptions.ClientError as e:
#         error_message = f"AWS Client Error: {str(e)}"
#         logging.error(error_message)
#         raise
#     except Exception as e:
#         error_message = f"Error fetching AWS resources: {str(e)}"
#         logging.error(error_message)
#         raise
        
#     return resources

# def show_resource_selection_dialog(title, items, description="Select an item", multiple=False):
#     """Generic dialog for selecting AWS resources with search capability"""
#     # Add logging for region selection
#     logging.info(f"Showing selection dialog for: {title}")
#     logging.info(f"Available items: {items}")

#     dialog = tk.Toplevel()
#     dialog.title(title)
#     dialog.geometry("600x500")

#     # Main container
#     main_frame = tk.Frame(dialog)
#     main_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)

#     label = tk.Label(main_frame, text=description)
#     label.pack(pady=5)

#     # Search frame
#     search_frame = tk.Frame(main_frame)
#     search_frame.pack(fill=tk.X, pady=5)

#     search_label = tk.Label(search_frame, text="Search:")
#     search_label.pack(side=tk.LEFT)

#     search_var = tk.StringVar()
#     search_entry = tk.Entry(search_frame, textvariable=search_var, width=40)
#     search_entry.pack(side=tk.LEFT, padx=5)

#     # Create a frame for the listbox and scrollbar
#     list_frame = tk.Frame(main_frame)
#     list_frame.pack(fill=tk.BOTH, expand=True, pady=5)

#     # Add scrollbar
#     scrollbar = tk.Scrollbar(list_frame)
#     scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

#     # Create listbox
#     listbox = tk.Listbox(
#         list_frame, 
#         yscrollcommand=scrollbar.set, 
#         width=70, 
#         height=15,
#         selectmode=tk.MULTIPLE if multiple else tk.SINGLE
#     )
#     listbox.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

#     scrollbar.config(command=listbox.yview)

#     # Store all items for searching
#     all_items = []
#     display_texts = []

#     # Populate listbox
#     for item in items:
#         if isinstance(item, dict):
#             # Special handling for security groups
#             if 'GroupName' in item and 'GroupId' in item:
#                 display_text = f"{item['GroupName']} ({item['GroupId']})"
#                 if 'VpcId' in item and item['VpcId']:
#                     display_text += f" - VPC: {item['VpcId']}"
#             elif 'name' in item and 'id' in item:
#                 display_text = f"{item['name']} ({item['id']})"
#             else:
#                 display_text = str(item)
#         else:
#             display_text = str(item)
        
#         logging.info(f"Adding item to listbox: {display_text}")
#         all_items.append(item)
#         display_texts.append(display_text)
#         listbox.insert(tk.END, display_text)

#     def filter_items(event=None):
#         search_text = search_var.get().lower()
#         listbox.delete(0, tk.END)
#         for i, display_text in enumerate(display_texts):
#             if search_text in display_text.lower():
#                 listbox.insert(tk.END, display_text)

#     search_entry.bind('<KeyRelease>', filter_items)

#     selected_items = []

#     def on_select():
#         selections = listbox.curselection()
#         if not selections:
#             messagebox.showwarning("Warning", "Please select at least one item.")
#             return
        
#         if multiple:
#             selected_items.extend([all_items[i] for i in selections])
#         else:
#             selected_items.append(all_items[selections[0]])
        
#        # Add logging for selection
#         logging.info(f"Selected item(s): {selected_items}")
#         dialog.destroy() 

#     button_frame = tk.Frame(main_frame)
#     button_frame.pack(pady=10)

#     select_button = tk.Button(button_frame, text="Select", command=on_select)
#     select_button.pack(side=tk.LEFT, padx=5)

#     cancel_button = tk.Button(button_frame, text="Cancel", command=dialog.destroy)
#     cancel_button.pack(side=tk.LEFT, padx=5)

#     dialog.transient(dialog.master)
#     dialog.grab_set()
#     dialog.master.wait_window(dialog)

#     return selected_items if multiple else (selected_items[0] if selected_items else None)

# def verify_aws_setup(aws_access_key, aws_secret_key, region, log_text):
#     """
#     Verify AWS setup and permissions
#     """
#     try:
#         # Create EC2 client
#         ec2_client = boto3.client(
#             'ec2',
#             aws_access_key_id=aws_access_key,
#             aws_secret_access_key=aws_secret_key,
#             region_name=region
#         )
        
#         # Test VPC permissions
#         try:
#             vpcs = ec2_client.describe_vpcs()
#             log_text.insert(tk.END, f"VPC Access: Success\n")
#             if len(vpcs['Vpcs']) == 0:
#                 log_text.insert(tk.END, f"Warning: No VPCs found in region {region}. You need at least one VPC.\n")
#         except Exception as e:
#             log_text.insert(tk.END, f"Error accessing VPCs: {str(e)}\n")
#             return False

#         # Test Security Group permissions
#         try:
#             sgs = ec2_client.describe_security_groups()
#             log_text.insert(tk.END, f"Security Group Access: Success\n")
#         except Exception as e:
#             log_text.insert(tk.END, f"Error accessing Security Groups: {str(e)}\n")
#             return False

#         # Test DMS permissions
#         dms_client = boto3.client(
#             'dms',
#             aws_access_key_id=aws_access_key,
#             aws_secret_access_key=aws_secret_key,
#             region_name=region
#         )
        
#         try:
#             dms_client.describe_replication_subnet_groups()
#             log_text.insert(tk.END, f"DMS Access: Success\n")
#         except Exception as e:
#             log_text.insert(tk.END, f"Error accessing DMS: {str(e)}\n")
#             return False

#         return True

#     except Exception as e:
#         log_text.insert(tk.END, f"Error verifying AWS setup: {str(e)}\n")
#         return False

# def setup_replication_instance(log_text, progress_bar):
#     try:
#         progress_bar['value'] = 0

#         global aws_access_key, aws_secret_key, region, replication_instance_arn

#         # Get AWS credentials
#         aws_access_key = simpledialog.askstring("AWS Access Key", "Enter AWS Access Key:")
#         if not aws_access_key:
#             return
        
#         aws_secret_key = simpledialog.askstring("AWS Secret Key", "Enter AWS Secret Key:")
#         if not aws_secret_key:
#             return

#         # First, get all AWS resources without region
#         log_text.insert(tk.END, "Fetching available AWS regions...\n")
#         log_text.see(tk.END)
#         resources = get_aws_resources(aws_access_key, aws_secret_key)
        
#         # Show region selection
#         region = show_resource_selection_dialog(
#             "Select Region",
#             resources['regions'],
#             "Select an AWS region for the replication instance:"
#         )
#         if not region:
#             return

#         # Get instance identifier
#         instance_id = simpledialog.askstring("Instance ID", "Enter Replication Instance ID:")
#         if not instance_id:
#             return

#         # Get resources for selected region
#         log_text.insert(tk.END, f"Fetching resources for region {region}...\n")
#         log_text.see(tk.END)
#         resources = get_aws_resources(aws_access_key, aws_secret_key, region)
        
#         # Show instance class selection
#         instance_class = show_resource_selection_dialog(
#             "Select Instance Class",
#             resources['instance_classes'],
#             "Select a replication instance class:"
#         )
#         if not instance_class:
#             return
        
#         # Get storage size
#         allocated_storage = simpledialog.askinteger(
#             "Allocated Storage",
#             "Enter Allocated Storage (in GB):",
#             initialvalue=20
#         )
#         if not allocated_storage:
#             return
        
#         # Show subnet group selection
#         if resources['subnet_groups']:
#             subnet_group = show_resource_selection_dialog(
#                 "Select Subnet Group",
#                 resources['subnet_groups'],
#                 "Select a subnet group:"
#             )
#             if not subnet_group:
#                 return
#             subnet_group_id = subnet_group['name']
#         else:
#             subnet_group_id = simpledialog.askstring(
#                 "Subnet Group",
#                 "No subnet groups found. Enter Subnet Group Identifier:"
#             )
#             if not subnet_group_id:
#                 return

#         # Show security group selection with multiple selection enabled
#         security_groups = show_resource_selection_dialog(
#             "Select Security Groups",
#             resources['security_groups'],
#             "Select security groups (hold Ctrl/Cmd to select multiple):",
#             multiple=True
#         )
#         if not security_groups:
#             log_text.insert(tk.END, "Security group selection cancelled. Please select at least one security group.\n")
#             log_text.see(tk.END)
#             return

#         # Extract security group IDs and validate
#         vpc_security_group_ids = []
#         for sg in security_groups:
#             if isinstance(sg, dict) and 'GroupId' in sg:
#                 vpc_security_group_ids.append(sg['GroupId'])
#             elif isinstance(sg, dict) and 'id' in sg:
#                 vpc_security_group_ids.append(sg['id'])

#         # Validate we have at least one valid security group ID
#         if not vpc_security_group_ids:
#             error_message = "No valid security group IDs found. Please select valid security groups."
#             log_text.insert(tk.END, error_message + "\n")
#             log_text.see(tk.END)
#             logging.error(error_message)
#             messagebox.showerror("Error", error_message)
#             return

#         log_text.insert(tk.END, f"Selected security groups: {', '.join(vpc_security_group_ids)}\n")
#         log_text.see(tk.END)

#         progress_bar['value'] = 40
#         log_text.insert(tk.END, "Creating replication instance...\n")
#         log_text.see(tk.END)

#         # Create DMS client
#         dms_client = boto3.client(
#             'dms',
#             aws_access_key_id=aws_access_key,
#             aws_secret_access_key=aws_secret_key,
#             region_name=region
#         )

#         # Add a checkbox for public accessibility
#         public_access = messagebox.askyesno(
#             "Public Access",
#             "Do you want the replication instance to be publicly accessible?\n\n" +
#             "Note: This requires an Internet Gateway attached to the VPC."
#         )

#         # Create the replication instance
#         response = dms_client.create_replication_instance(
#             ReplicationInstanceIdentifier=instance_id,
#             ReplicationInstanceClass=instance_class,
#             AllocatedStorage=allocated_storage,
#             VpcSecurityGroupIds=vpc_security_group_ids,
#             ReplicationSubnetGroupIdentifier=subnet_group_id,
#             MultiAZ=False,
#             EngineVersion='3.5.1',
#             PubliclyAccessible=public_access
#         )

#         # Store the replication instance ARN
#         replication_instance_arn = response['ReplicationInstance']['ReplicationInstanceArn']
#         logging.info(f"Stored replication instance ARN: {replication_instance_arn}")

#         # Wait for the instance to be available
#         log_text.insert(tk.END, "Waiting for replication instance to be available...\n")
#         log_text.see(tk.END)
        
#         if not wait_for_replication_instance(dms_client, instance_id, log_text):
#             messagebox.showerror("Error", "Failed to create replication instance")
#             return

#         progress_bar['value'] = 100
#         log_message = "Replication instance creation completed successfully"
#         log_text.insert(tk.END, log_message + "\n")
#         log_text.see(tk.END)
#         logging.info(log_message)
#         messagebox.showinfo("Success", log_message)

#     except ClientError as e:
#         error_message = f"AWS Error: {str(e)}"
#         log_text.insert(tk.END, error_message + "\n")
#         log_text.see(tk.END)
#         logging.error(error_message)
#         messagebox.showerror("Error", error_message)
#         progress_bar['value'] = 0
#     except Exception as e:
#         error_message = f"Unexpected error: {str(e)}"
#         log_text.insert(tk.END, error_message + "\n")
#         log_text.see(tk.END)
#         logging.error(error_message)
#         messagebox.showerror("Error", error_message)
#         progress_bar['value'] = 0

# def wait_for_replication_instance(dms_client, instance_id, log_text):
#     """Wait for replication instance to be available"""
#     while True:
#         response = dms_client.describe_replication_instances(
#             Filters=[
#                 {
#                     'Name': 'replication-instance-id',
#                     'Values': [instance_id]
#                 }
#             ]
#         )
        
#         if response['ReplicationInstances']:
#             status = response['ReplicationInstances'][0]['ReplicationInstanceStatus']
#             arn = response['ReplicationInstances'][0]['ReplicationInstanceArn']
            
#             if status == 'available':
#                 global replication_instance_arn
#                 replication_instance_arn = arn
#                 log_text.insert(tk.END, "Replication instance is now available.\n")
#                 log_text.see(tk.END)
#                 return True
#             elif status == 'creating':
#                 log_text.insert(tk.END, "Waiting for replication instance to be available...\n")
#                 log_text.see(tk.END)
#                 time.sleep(30)  # Wait 30 seconds before checking again
#             else:
#                 log_text.insert(tk.END, f"Unexpected status: {status}\n")
#                 log_text.see(tk.END)
#                 return False
#         else:
#             log_text.insert(tk.END, "Replication instance not found.\n")
#             log_text.see(tk.END)
#             return False        

# def configure_dms_endpoints(log_text, progress_bar):
#     """Configure source (SQL Server) and target (PostgreSQL) DMS endpoints"""
#     try:
#         progress_bar['value'] = 0

#         global aws_access_key, aws_secret_key, region, replication_instance_arn

#         # Validate we have the required AWS credentials and connection details
#         if not all([aws_access_key, aws_secret_key, region]):
#             messagebox.showerror("Error", "AWS credentials and region not found. Please create the replication instance first.")
#             return
        
#         if not replication_instance_arn:
#             messagebox.showerror("Error", "Replication instance ARN not found. Please create the replication instance first.")
#             return

#         if not all([source_host_input, source_user_input, source_password_input,
#                    target_host_input, target_user_input, target_password_input]):
#             messagebox.showerror("Error", "Source and target connection details not found. Please start from the main screen.")
#             return
        
#         # Get selected databases from checkboxes
#         selected_databases = [chk.cget("text") for chk, var in checkboxes if var.get()]
        
#         if not selected_databases:
#             messagebox.showerror("Error", "No databases selected. Please select databases from the main screen.")
#             return

#         logging.info(f"Processing endpoints for databases: {selected_databases}")
        
#         # Create DMS client using the existing credentials and region
#         dms_client = boto3.client(
#             'dms',
#             aws_access_key_id=aws_access_key,
#             aws_secret_access_key=aws_secret_key,
#             region_name=region
#         )

#         progress_bar['value'] = 20
        
#         # Create source endpoints for each selected database
#         source_endpoints = {}
#         total_dbs = len(selected_databases)
#         for idx, db in enumerate(selected_databases):
#             log_text.insert(tk.END, f"Configuring source SQL Server endpoint for database: {db}...\n")
#             log_text.see(tk.END)
            
#             source_endpoint_identifier = f"sqlserver-source-{db.lower()}"
            
#             try:
#                 source_response = dms_client.create_endpoint(
#                     EndpointIdentifier=source_endpoint_identifier,
#                     EndpointType='source',
#                     EngineName='sqlserver',
#                     ServerName=source_host_input,
#                     Port=1433,
#                     DatabaseName=db,  # Use the database name from selected_databases
#                     Username=source_user_input,
#                     Password=source_password_input
#                 )
                
#                 source_endpoints[db] = source_response['Endpoint']['EndpointArn']
#                 log_text.insert(tk.END, f"Source endpoint created for database {db}: {source_endpoint_identifier}\n")
#                 log_text.see(tk.END)
                
#                 # Update progress bar for source endpoints
#                 progress_bar['value'] = 20 + (40 * (idx + 1) / total_dbs)
                
#             except ClientError as e:
#                 error_message = f"Error creating source endpoint for {db}: {str(e)}"
#                 log_text.insert(tk.END, error_message + "\n")
#                 log_text.see(tk.END)
#                 logging.error(error_message)
#                 messagebox.showerror("Error", error_message)
#                 continue

#         # Configure target endpoint (PostgreSQL)
#         log_text.insert(tk.END, "Configuring target PostgreSQL endpoint...\n")
#         log_text.see(tk.END)
        
#         target_endpoint_identifier = "postgresql-target"

#         try:
#             target_response = dms_client.create_endpoint(
#                 EndpointIdentifier=target_endpoint_identifier,
#                 EndpointType='target',
#                 EngineName='postgres',
#                 ServerName=target_host_input,
#                 Port=5432,
#                 DatabaseName='actdb',
#                 Username=target_user_input,
#                 Password=target_password_input
#             )
            
#             log_text.insert(tk.END, f"Target endpoint created: {target_endpoint_identifier}\n")
#             log_text.see(tk.END)
#             progress_bar['value'] = 80
            
#         except ClientError as e:
#             error_message = f"Error creating target endpoint: {str(e)}"
#             log_text.insert(tk.END, error_message + "\n")
#             log_text.see(tk.END)
#             logging.error(error_message)
#             messagebox.showerror("Error", error_message)
#             return

#         # Test endpoint connections
#         log_text.insert(tk.END, "Testing endpoint connections...\n")
#         log_text.see(tk.END)

#         try:
#             # Test each source endpoint
#             for db, endpoint_arn in source_endpoints.items():
#                 source_test = dms_client.test_connection(
#                     EndpointArn=endpoint_arn,
#                     ReplicationInstanceArn=replication_instance_arn
#                 )
#                 log_text.insert(tk.END, f"Source endpoint connection tested for {db}\n")
#                 log_text.see(tk.END)
            
#             # Test target endpoint
#             target_test = dms_client.test_connection(
#                 EndpointArn=target_response['Endpoint']['EndpointArn'],
#                 ReplicationInstanceArn=replication_instance_arn
#             )
            
#             log_text.insert(tk.END, "All endpoint connections tested successfully.\n")
#             log_text.see(tk.END)
            
#             progress_bar['value'] = 100
#             messagebox.showinfo("Success", "All endpoints have been created and tested successfully!")
            
#         except ClientError as e:
#             error_message = f"Error testing endpoints: {str(e)}"
#             log_text.insert(tk.END, error_message + "\n")
#             log_text.see(tk.END)
#             logging.error(error_message)
#             messagebox.showwarning("Warning", "Endpoints created but connection test failed. Please verify your connection details.")

#     except Exception as e:
#         error_message = f"Unexpected error: {str(e)}"
#         log_text.insert(tk.END, error_message + "\n")
#         log_text.see(tk.END)
#         logging.error(error_message)
#         messagebox.showerror("Error", error_message)
#         progress_bar['value'] = 0

# def create_dms_tasks(log_text, progress_bar):
#     # Implementation for creating DMS tasks
#     pass            

# def migrate_selected_databases():
#     selected_databases = [chk.cget("text") for chk, var in checkboxes if var.get()]

#     if not selected_databases:
#         logging.warning("Migration attempted with no databases selected.")
#         messagebox.showwarning("Warning", "No databases selected!")
#         return

#     logging.info(f"Migrating selected databases: {selected_databases}")

#     disable_main_window_controls()

#     migration_window = tk.Toplevel(root)
#     migration_window.title("Migrate Selected Databases")
#     migration_window.geometry("1200x800")  # Increased height to accommodate new section

#     def on_migration_window_close():
#         enable_main_window_controls()
#         migration_window.destroy()

#     migration_window.protocol("WM_DELETE_WINDOW", on_migration_window_close)

#     # Main frame to hold everything
#     main_frame = tk.Frame(migration_window)
#     main_frame.pack(fill="both", expand=True, padx=10, pady=10)

#     # Left frame for the table and buttons
#     left_frame = tk.Frame(main_frame)
#     left_frame.pack(side="left", fill="both", expand=True)

#     # Right frame for the log area
#     right_frame = tk.Frame(main_frame, width=300)
#     right_frame.pack(side="right", fill="both", expand=True, padx=(10, 0))

#     # Pre-Migration Preparation Frame
#     prep_frame = ttk.LabelFrame(left_frame, text="Pre-Migration Preparation")
#     prep_frame.pack(fill="both", expand=True, padx=5, pady=5)

#     table_frame = tk.Frame(prep_frame)
#     table_frame.pack(fill="both", expand=True, padx=3, pady=3)

#     # Increase the width of labels
#     label_width = 30
#     button_width = 30
    
#     action_label = tk.Label(table_frame, text="Action", borderwidth=1, relief="solid", width=label_width)
#     action_label.grid(row=0, column=0, padx=2, pady=2, sticky="nsew")

#     for col_id, db in enumerate(selected_databases, 1):
#         db_label = tk.Label(table_frame, text=db, borderwidth=1, relief="solid", width=15)
#         db_label.grid(row=0, column=col_id, padx=2, pady=2, sticky="nsew")

#     # Calculate the width of the longest button text
#     button_texts = ["Run Analyze", "Create Partition Alignment", "DROP FKs in PG", "Disable triggers in PG", "Create replication instance"]
#     max_button_width = max(len(text) for text in button_texts)
#     button_width = max(max_button_width, 30)  # Ensure button width is at least 30

#     # Run Analyze section
#     row_id = 1
#     action_label = tk.Label(table_frame, text="Run Analyze", borderwidth=1, relief="solid", width=label_width)
#     action_label.grid(row=row_id, column=0, padx=2, pady=2, sticky="nsew")

#     run_analyze_vars = []
#     run_analyze_progress_bars = []

#     for col_id, db in enumerate(selected_databases, 1):
#         var = tk.BooleanVar()
#         checkbox = tk.Checkbutton(table_frame, variable=var)
#         checkbox.grid(row=row_id, column=col_id, padx=2, pady=2, sticky="nsew")
#         run_analyze_vars.append(var)

#     run_btn = tk.Button(table_frame, text="Run Analyze", command=lambda: run_selected_actions(selected_databases, run_analyze_vars, run_analyze_progress_bars, log_text), width=button_width)
#     run_btn.grid(row=row_id, column=len(selected_databases) + 1, padx=2, pady=2, sticky="nsew")

#     progress_bar = ttk.Progressbar(table_frame, orient="horizontal", length=100, mode="determinate")
#     progress_bar.grid(row=row_id, column=len(selected_databases) + 2, padx=2, pady=2, sticky="nsew")
#     run_analyze_progress_bars.append(progress_bar)

# # Partition Alignment section
#     row_id = 2
#     partition_label = tk.Label(table_frame, text="Partition Alignment", borderwidth=1, relief="solid", width=label_width)
#     partition_label.grid(row=row_id, column=0, padx=2, pady=2, sticky="nsew")

#     partition_vars = []
#     partition_progress_bars = []

#     for col_id, db in enumerate(selected_databases, 1):
#         var = tk.BooleanVar()
#         checkbox = tk.Checkbutton(table_frame, variable=var)
#         checkbox.grid(row=row_id, column=col_id, padx=2, pady=2, sticky="nsew")
#         partition_vars.append(var)

#     partition_btn = tk.Button(table_frame, text="Create Partition Alignment", command=lambda: create_partition_alignment(selected_databases, partition_vars, partition_progress_bars, log_text), width=button_width)
#     partition_btn.grid(row=row_id, column=len(selected_databases) + 1, padx=2, pady=2, sticky="nsew")

#     progress_bar = ttk.Progressbar(table_frame, orient="horizontal", length=100, mode="determinate")
#     progress_bar.grid(row=row_id, column=len(selected_databases) + 2, padx=2, pady=2, sticky="nsew")
#     partition_progress_bars.append(progress_bar)

#     # DROP FKs in PG section
#     row_id = 3
#     drop_fks_label = tk.Label(table_frame, text="DROP FKs in PG", borderwidth=1, relief="solid", width=label_width)
#     drop_fks_label.grid(row=row_id, column=0, padx=2, pady=2, sticky="nsew")

#     drop_fks_vars = []
#     drop_fks_progress_bars = []

#     for col_id, db in enumerate(selected_databases, 1):
#         var = tk.BooleanVar()
#         checkbox = tk.Checkbutton(table_frame, variable=var)
#         checkbox.grid(row=row_id, column=col_id, padx=2, pady=2, sticky="nsew")
#         drop_fks_vars.append(var)

#     drop_fks_btn = tk.Button(table_frame, text="DROP FKs in PG", command=lambda: drop_fks_in_pg(selected_databases, drop_fks_vars, drop_fks_progress_bars, log_text), width=button_width)
#     drop_fks_btn.grid(row=row_id, column=len(selected_databases) + 1, padx=2, pady=2, sticky="nsew")

#     progress_bar = ttk.Progressbar(table_frame, orient="horizontal", length=100, mode="determinate")
#     progress_bar.grid(row=row_id, column=len(selected_databases) + 2, padx=2, pady=2, sticky="nsew")
#     drop_fks_progress_bars.append(progress_bar)

#     # Disable triggers in PG section
#     row_id = 4
#     disable_triggers_label = tk.Label(table_frame, text="Disable triggers in PG", borderwidth=1, relief="solid", width=label_width)
#     disable_triggers_label.grid(row=row_id, column=0, padx=2, pady=(2, 0), sticky="nsew")

#     disable_triggers_vars = []
#     disable_triggers_progress_bars = []

#     for col_id, db in enumerate(selected_databases, 1):
#         var = tk.BooleanVar()
#         checkbox = tk.Checkbutton(table_frame, variable=var)
#         checkbox.grid(row=row_id, column=col_id, padx=2, pady=(2, 0), sticky="nsew")
#         disable_triggers_vars.append(var)

#     disable_triggers_btn = tk.Button(table_frame, text="Disable triggers in PG", command=lambda: disable_triggers_in_pg(selected_databases, disable_triggers_vars, disable_triggers_progress_bars, log_text), width=button_width)
#     disable_triggers_btn.grid(row=row_id, column=len(selected_databases) + 1, padx=2, pady=(2, 0), sticky="nsew")

#     progress_bar = ttk.Progressbar(table_frame, orient="horizontal", length=100, mode="determinate")
#     progress_bar.grid(row=row_id, column=len(selected_databases) + 2, padx=2, pady=(2, 0), sticky="nsew")
#     disable_triggers_progress_bars.append(progress_bar)

#     # DMS Frame
#     dms_frame = ttk.LabelFrame(left_frame, text="DMS")
#     dms_frame.pack(fill="both", expand=True, padx=5, pady=5)

#     # DMS Frame content
#     dms_table_frame = tk.Frame(dms_frame)
#     dms_table_frame.pack(fill="both", expand=True, padx=3, pady=3)

#     # Action label
#     action_label = tk.Label(dms_table_frame, text="Action", borderwidth=1, relief="solid", width=label_width)
#     action_label.grid(row=0, column=0, padx=2, pady=2, sticky="nsew")

#     # Create Replication Instance section
#     create_replication_label = tk.Label(dms_table_frame, text="Create Replication Instance", borderwidth=1, relief="solid", width=label_width)
#     create_replication_label.grid(row=1, column=0, padx=2, pady=2, sticky="nsew")

#     create_replication_btn = tk.Button(dms_table_frame, text="Create replication instance", command=lambda: setup_replication_instance(log_text, replication_progress_bar), width=button_width)
#     create_replication_btn.grid(row=1, column=1, padx=2, pady=2, sticky="nsew")

#     replication_progress_bar = ttk.Progressbar(dms_table_frame, orient="horizontal", length=100, mode="determinate")
#     replication_progress_bar.grid(row=1, column=2, padx=2, pady=2, sticky="nsew")

#     # Configure DMS Endpoints section
#     configure_endpoints_label = tk.Label(dms_table_frame, text="Configure DMS Endpoints", borderwidth=1, relief="solid", width=label_width)
#     configure_endpoints_label.grid(row=2, column=0, padx=2, pady=2, sticky="nsew")

#     configure_endpoints_btn = tk.Button(dms_table_frame, text="Configure DMS Endpoints", command=lambda: configure_dms_endpoints(log_text, endpoints_progress_bar), width=button_width)
#     configure_endpoints_btn.grid(row=2, column=1, padx=2, pady=2, sticky="nsew")

#     endpoints_progress_bar = ttk.Progressbar(dms_table_frame, orient="horizontal", length=100, mode="determinate")
#     endpoints_progress_bar.grid(row=2, column=2, padx=2, pady=2, sticky="nsew")

#     # Create DMS Tasks section
#     create_tasks_label = tk.Label(dms_table_frame, text="Create DMS Tasks", borderwidth=1, relief="solid", width=label_width)
#     create_tasks_label.grid(row=3, column=0, padx=2, pady=2, sticky="nsew")

#     create_tasks_btn = tk.Button(dms_table_frame, text="Create DMS Tasks", command=lambda: create_dms_tasks(log_text, tasks_progress_bar), width=button_width)
#     create_tasks_btn.grid(row=3, column=1, padx=2, pady=2, sticky="nsew")

#     tasks_progress_bar = ttk.Progressbar(dms_table_frame, orient="horizontal", length=100, mode="determinate")
#     tasks_progress_bar.grid(row=3, column=2, padx=2, pady=2, sticky="nsew")

#     # Configure grid
#     dms_table_frame.grid_columnconfigure(1, weight=1)
#     dms_table_frame.grid_columnconfigure(2, weight=0)  # For the progress bars
#     for i in range(4):  # 4 rows: header, Create Replication Instance, Configure DMS Endpoints, and Create DMS Tasks
#         dms_table_frame.grid_rowconfigure(i, weight=1)

# # Post-Migration Frame
#     post_migration_frame = ttk.LabelFrame(left_frame, text="Post-Migration")
#     post_migration_frame.pack(fill="both", expand=True, padx=5, pady=5)

#     post_migration_table_frame = tk.Frame(post_migration_frame)
#     post_migration_table_frame.pack(fill="both", expand=True, padx=3, pady=3)

#     # Action label
#     action_label = tk.Label(post_migration_table_frame, text="Action", borderwidth=1, relief="solid", width=label_width)
#     action_label.grid(row=0, column=0, padx=2, pady=2, sticky="nsew")

#     # Database labels
#     for col_id, db in enumerate(selected_databases, 1):
#         db_label = tk.Label(post_migration_table_frame, text=db, borderwidth=1, relief="solid", width=15)
#         db_label.grid(row=0, column=col_id, padx=2, pady=2, sticky="nsew")

#     # Change partition ownership section
#     row_id = 1
#     change_ownership_label = tk.Label(post_migration_table_frame, text="Change partition ownership", borderwidth=1, relief="solid", width=label_width)
#     change_ownership_label.grid(row=row_id, column=0, padx=2, pady=2, sticky="nsew")

#     change_ownership_vars = []
#     for col_id, db in enumerate(selected_databases, 1):
#         var = tk.BooleanVar()
#         checkbox = tk.Checkbutton(post_migration_table_frame, variable=var)
#         checkbox.grid(row=row_id, column=col_id, padx=2, pady=2, sticky="nsew")
#         change_ownership_vars.append(var)

#     change_ownership_btn = tk.Button(post_migration_table_frame, text="Change partition ownership", command=lambda: change_partition_ownership(selected_databases, change_ownership_vars, change_ownership_progress_bar, log_text), width=button_width)
#     change_ownership_btn.grid(row=row_id, column=len(selected_databases) + 1, padx=2, pady=2, sticky="nsew")

#     change_ownership_progress_bar = ttk.Progressbar(post_migration_table_frame, orient="horizontal", length=100, mode="determinate")
#     change_ownership_progress_bar.grid(row=row_id, column=len(selected_databases) + 2, padx=2, pady=2, sticky="nsew")

#     # Enable triggers section
#     row_id = 2
#     enable_triggers_label = tk.Label(post_migration_table_frame, text="Enable triggers", borderwidth=1, relief="solid", width=label_width)
#     enable_triggers_label.grid(row=row_id, column=0, padx=2, pady=2, sticky="nsew")

#     enable_triggers_vars = []
#     for col_id, db in enumerate(selected_databases, 1):
#         var = tk.BooleanVar()
#         checkbox = tk.Checkbutton(post_migration_table_frame, variable=var)
#         checkbox.grid(row=row_id, column=col_id, padx=2, pady=2, sticky="nsew")
#         enable_triggers_vars.append(var)

#     enable_triggers_btn = tk.Button(post_migration_table_frame, text="Enable triggers", command=lambda: enable_triggers(selected_databases, enable_triggers_vars, enable_triggers_progress_bar, log_text), width=button_width)
#     enable_triggers_btn.grid(row=row_id, column=len(selected_databases) + 1, padx=2, pady=2, sticky="nsew")

#     enable_triggers_progress_bar = ttk.Progressbar(post_migration_table_frame, orient="horizontal", length=100, mode="determinate")
#     enable_triggers_progress_bar.grid(row=row_id, column=len(selected_databases) + 2, padx=2, pady=2, sticky="nsew")

#     # Re-create FKs section
#     row_id = 3
#     recreate_fks_label = tk.Label(post_migration_table_frame, text="Re-create FKs", borderwidth=1, relief="solid", width=label_width)
#     recreate_fks_label.grid(row=row_id, column=0, padx=2, pady=2, sticky="nsew")

#     recreate_fks_vars = []
#     for col_id, db in enumerate(selected_databases, 1):
#         var = tk.BooleanVar()
#         checkbox = tk.Checkbutton(post_migration_table_frame, variable=var)
#         checkbox.grid(row=row_id, column=col_id, padx=2, pady=2, sticky="nsew")
#         recreate_fks_vars.append(var)

#     recreate_fks_btn = tk.Button(post_migration_table_frame, text="Re-create FKs", command=lambda: recreate_fks(selected_databases, recreate_fks_vars, recreate_fks_progress_bar, log_text), width=button_width)
#     recreate_fks_btn.grid(row=row_id, column=len(selected_databases) + 1, padx=2, pady=2, sticky="nsew")

#     recreate_fks_progress_bar = ttk.Progressbar(post_migration_table_frame, orient="horizontal", length=100, mode="determinate")
#     recreate_fks_progress_bar.grid(row=row_id, column=len(selected_databases) + 2, padx=2, pady=2, sticky="nsew")

#     # Execute Analyze section
#     row_id = 4
#     execute_analyze_label = tk.Label(post_migration_table_frame, text="Execute Analyze", borderwidth=1, relief="solid", width=label_width)
#     execute_analyze_label.grid(row=row_id, column=0, padx=2, pady=2, sticky="nsew")

#     execute_analyze_vars = []
#     for col_id, db in enumerate(selected_databases, 1):
#         var = tk.BooleanVar()
#         checkbox = tk.Checkbutton(post_migration_table_frame, variable=var)
#         checkbox.grid(row=row_id, column=col_id, padx=2, pady=2, sticky="nsew")
#         execute_analyze_vars.append(var)

#     execute_analyze_btn = tk.Button(post_migration_table_frame, text="Execute Analyze", command=lambda: execute_analyze(selected_databases, execute_analyze_vars, execute_analyze_progress_bar, log_text), width=button_width)
#     execute_analyze_btn.grid(row=row_id, column=len(selected_databases) + 1, padx=2, pady=2, sticky="nsew")

#     execute_analyze_progress_bar = ttk.Progressbar(post_migration_table_frame, orient="horizontal", length=100, mode="determinate")
#     execute_analyze_progress_bar.grid(row=row_id, column=len(selected_databases) + 2, padx=2, pady=2, sticky="nsew")

#     # Align Sequences section
#     row_id = 5
#     align_sequences_label = tk.Label(post_migration_table_frame, text="Align Sequences", borderwidth=1, relief="solid", width=label_width)
#     align_sequences_label.grid(row=row_id, column=0, padx=2, pady=2, sticky="nsew")

#     align_sequences_vars = []
#     for col_id, db in enumerate(selected_databases, 1):
#         var = tk.BooleanVar()
#         checkbox = tk.Checkbutton(post_migration_table_frame, variable=var)
#         checkbox.grid(row=row_id, column=col_id, padx=2, pady=2, sticky="nsew")
#         align_sequences_vars.append(var)

#     align_sequences_btn = tk.Button(post_migration_table_frame, text="Align sequences", command=lambda: align_sequences(selected_databases, align_sequences_vars, align_sequences_progress_bar, log_text), width=button_width)
#     align_sequences_btn.grid(row=row_id, column=len(selected_databases) + 1, padx=2, pady=2, sticky="nsew")

#     align_sequences_progress_bar = ttk.Progressbar(post_migration_table_frame, orient="horizontal", length=100, mode="determinate")
#     align_sequences_progress_bar.grid(row=row_id, column=len(selected_databases) + 2, padx=2, pady=2, sticky="nsew")

#     # Run Data Comparison section
#     row_id = 6
#     run_comparison_label = tk.Label(post_migration_table_frame, text="Run Data Comparison", borderwidth=1, relief="solid", width=label_width)
#     run_comparison_label.grid(row=row_id, column=0, padx=2, pady=2, sticky="nsew")

#     run_comparison_vars = []
#     for col_id, db in enumerate(selected_databases, 1):
#         var = tk.BooleanVar()
#         checkbox = tk.Checkbutton(post_migration_table_frame, variable=var)
#         checkbox.grid(row=row_id, column=col_id, padx=2, pady=2, sticky="nsew")
#         run_comparison_vars.append(var)

#     run_comparison_btn = tk.Button(post_migration_table_frame, text="Run Data Comparison", command=lambda: run_data_comparison(selected_databases, run_comparison_vars, run_comparison_progress_bar, log_text), width=button_width)
#     run_comparison_btn.grid(row=row_id, column=len(selected_databases) + 1, padx=2, pady=2, sticky="nsew")

#     run_comparison_progress_bar = ttk.Progressbar(post_migration_table_frame, orient="horizontal", length=100, mode="determinate")
#     run_comparison_progress_bar.grid(row=row_id, column=len(selected_databases) + 2, padx=2, pady=2, sticky="nsew")

#     # Configure grid
#     post_migration_table_frame.grid_columnconfigure(len(selected_databases) + 1, weight=1)
#     post_migration_table_frame.grid_columnconfigure(len(selected_databases) + 2, weight=0)  # For the progress bars
#     for i in range(7):  # 7 rows: header and 6 action rows
#         post_migration_table_frame.grid_rowconfigure(i, weight=1)

#     # Log area
#     log_label = tk.Label(right_frame, text="Activity Log", font=("Arial", 12, "bold"))
#     log_label.pack(pady=(0, 5))

#     log_text = tk.Text(right_frame, wrap=tk.WORD, width=40, height=30)
#     log_text.pack(side="left", fill="both", expand=True)

#     log_scrollbar = tk.Scrollbar(right_frame, orient="vertical", command=log_text.yview)
#     log_scrollbar.pack(side="right", fill="y")

#     log_text.configure(yscrollcommand=log_scrollbar.set)

#     def add_log_entry(message):
#         log_text.insert(tk.END, f"{message}\n")
#         log_text.see(tk.END)  # Scroll to the bottom

#     add_log_entry("Migration process started.")

#     table_frame.grid_columnconfigure(len(selected_databases) + 1, weight=1)
#     table_frame.grid_columnconfigure(len(selected_databases) + 2, weight=0)  # For the progress bars
#     for i in range(table_frame.grid_size()[1]):  # Use the actual number of rows
#         table_frame.grid_rowconfigure(i, weight=1)

#     migration_window.grab_set()  # Make the migration window modal

# def run_selected_actions(selected_databases, run_analyze_vars, progress_bars, log_text):
#     analyze_template_path = os.path.join(os.path.dirname(__file__), 'mssql_analyze.sql')
#     success_databases = []

#     for idx, db in enumerate(selected_databases):
#         if run_analyze_vars[idx].get():
#             log_text.insert(tk.END, f"Running 'Analyze' action for database: {db}\n")
#             log_text.see(tk.END)
#             logging.info(f"Running 'Analyze' action for database: {db}")
#             progress_bars[0]['value'] = 0
#             try:
#                 result_file = execute_script_on_database(analyze_template_path, True, db)  # True for source database

#                 if result_file:
#                     log_message = f"Analyze script executed successfully on {db}. Results saved to: {result_file}"
#                 else:
#                     log_message = f"Analyze script executed successfully on {db}. No results were returned."
                
#                 log_text.insert(tk.END, log_message + "\n")
#                 log_text.see(tk.END)
#                 logging.info(log_message)
#                 messagebox.showinfo("Success", log_message)

#                 success_databases.append(db)
#                 progress_bars[0]['value'] = 100

#             except Exception as e:
#                 error_message = f"Failed to run analyze on {db}: {str(e)}"
#                 log_text.insert(tk.END, error_message + "\n")
#                 log_text.see(tk.END)
#                 logging.error(error_message)
#                 show_error_with_copy_option(db, error_message)
#                 progress_bars[0]['value'] = 0

#     if success_databases:
#         log_message = f"Analysis completed for databases: {', '.join(success_databases)}"
#         log_text.insert(tk.END, log_message + "\n")
#         log_text.see(tk.END)
#         logging.info(log_message)
#     else:
#         log_message = "No databases successfully completed the analysis."
#         log_text.insert(tk.END, log_message + "\n")
#         log_text.see(tk.END)
#         logging.warning(log_message)

# root.mainloop()

# # Example button action for setting up replication instance
# def on_setup_replication_instance_button_click():
#     setup_replication_instance()


# # Placeholder for pre-migration steps completion
# pre_migration_completed = False  # This should be updated based on actual pre-migration completion status

# def enable_setup_button():
#     if pre_migration_completed:
#         create_replication_btn.config(state=tk.NORMAL)
#     else:
#         create_replication_btn.config(state=tk.DISABLED)

# # Button for creating replication instance (hidden until pre-migration is completed)
# create_replication_btn = tk.Button(root, text="Create Replication Instance", command=on_setup_replication_instance_button_click, state=tk.DISABLED)
# create_replication_btn.pack(pady=20)

# # Function to show the replication instance setup button after pre-migration is completed
# def show_replication_instance_button():
#     global pre_migration_completed
#     pre_migration_completed = True
#     enable_setup_button()

# # Button to simulate pre-migration completion and enable replication instance setup
# complete_pre_migration_btn = tk.Button(root, text="Complete Pre-Migration Steps", command=show_replication_instance_button)
# complete_pre_migration_btn.pack(pady=20)

root.mainloop()