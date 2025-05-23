import pandas as pd
import mysql.connector
from mysql.connector import Error

def fix_schema_and_load_data(csv_file_path, db_config):
    # Read the CSV file
    df = pd.read_csv(csv_file_path)

    try:
        # Connect to the MySQL database
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        # Add new columns
        new_columns = ['email', 'transaction_id', 'payment_method']
        for column in new_columns:
            cursor.execute(f"ALTER TABLE raw_pos ADD COLUMN {column} VARCHAR(255)")

        # Alter column types
        cursor.execute("ALTER TABLE raw_pos MODIFY COLUMN product_id VARCHAR(255)")
        cursor.execute("ALTER TABLE raw_pos MODIFY COLUMN alcohol_percentage FLOAT")
        cursor.execute("ALTER TABLE raw_pos MODIFY COLUMN unit_price FLOAT")
        cursor.execute("ALTER TABLE raw_pos MODIFY COLUMN total_sales FLOAT")

        # Prepare the INSERT statement
        columns = ', '.join(df.columns)
        placeholders = ', '.join(['%s'] * len(df.columns))
        insert_query = f"INSERT INTO raw_pos ({columns}) VALUES ({placeholders})"

        # Insert data
        for _, row in df.iterrows():
            cursor.execute(insert_query, tuple(row))

        # Commit the changes
        connection.commit()
        print("Schema fixed and data loaded successfully.")

    except Error as e:
        print(f"Error: {e}")

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

# Usage
db_config = {
    'host': 'localhost',
    'database': 'diageo_warehouse',
    'user': 'root',
    'password': 'password'
}

csv_file_path = '/Users/gaurang/Documents/git/salesanalysisagent/src/salesanalysisagent/data/combined_testcase/raw_pos.csv'
fix_schema_and_load_data(csv_file_path, db_config)
