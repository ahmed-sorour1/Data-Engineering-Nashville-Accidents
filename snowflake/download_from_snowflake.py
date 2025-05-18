#!/usr/bin/env python
# coding: utf-8
import os
from dotenv import load_dotenv
import snowflake.connector
import pandas as pd

# Load credentials from .env file
load_dotenv()

# Connect to Snowflake
conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA")
)

cursor = conn.cursor()

# ✅ Use absolute path based on script location
script_dir = os.path.dirname(os.path.abspath(__file__))
output_dir = os.path.join(script_dir, "final tables")
os.makedirs(output_dir, exist_ok=True)  # Ensure the directory exists

# Download and save tables
table_names = [
    'int_SK_full_table',
    'int_dim_weather',
    'int_dim_collision',
    'int_dim_harm',
    'int_dim_date',
    'int_dim_location',
    'fact_nashville_accident'
]

for table in table_names:
    df = pd.read_sql(f"SELECT * FROM {table}", conn)
    df.to_csv(os.path.join(output_dir, f"{table}.csv"), index=False)
    print(f"✅ Downloaded {len(df)} rows from {table} into {table}.csv")

# Cleanup
conn.close()
cursor.close()
