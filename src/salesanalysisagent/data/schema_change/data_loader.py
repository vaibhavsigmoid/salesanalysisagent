import pandas as pd
import mysql.connector
from sqlalchemy import create_engine

# File path
file_path = '/Users/gaurang/Documents/git/salesanalysisagent/src/salesanalysisagent/data/schema_change/sales_data.xlsx'

# Mapping
mapping = {
    'file_type': 'xlsx',
    'sheets': {
        'store_dim': {'database_name': 'pos_data', 'table_name': 'store_dim'},
        'product_dim': {'database_name': 'pos_data', 'table_name': 'product_dim'},
        'sales_fct': {'database_name': 'pos_data', 'table_name': 'sales_fct'}
    }
}

# MySQL connection details (replace with your actual details)
mysql_config = {
    'host': 'localhost',
    'user': 'your_username',
    'password': 'your_password',
    'database': 'pos_data'
}

# Create SQLAlchemy engine
engine = create_engine(f"mysql+mysqlconnector://{mysql_config['user']}:{mysql_config['password']}@{mysql_config['host']}/{mysql_config['database']}")

# Read Excel file and ingest data into MySQL tables
xl = pd.ExcelFile(file_path)

for sheet_name, sheet_mapping in mapping['sheets'].items():
    df = xl.parse(sheet_name)
    table_name = sheet_mapping['table_name']
    
    # Ingest data into MySQL
    df.to_sql(table_name, engine, if_exists='replace', index=False)
    
    print(f"Data from sheet '{sheet_name}' has been ingested into table '{table_name}'")

print("Data ingestion completed successfully.")
