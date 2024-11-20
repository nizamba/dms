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
target_schema_name = None
aws_access_key = None
aws_secret_key = None
region = None
replication_instance_arn = None

all_databases = {}
selected_db_details = {}
all_pg_schemas = {}
selected_schemas = {}

checkboxes = []
source_host_input = source_user_input = source_password_input = None
target_host_input = target_user_input = target_password_input = None
select_all_btn = deselect_all_btn = migrate_button = search_entry = None


#logging configuration
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