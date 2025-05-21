import os
import traceback

import mysql.connector
import pandas as pd
from mysql.connector import Error

# MySQL configuration
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "password",
    "database": "diageo_warehouse",
}

# File paths
script_dir = os.path.dirname(__file__)
CSV_FILE_NAME = "new_data.csv"
CSV_FILE_PATH = os.path.join(script_dir, "..", "data", "self_healing", CSV_FILE_NAME)
ERROR_LOG_FILE = os.path.join(script_dir, "..", "logs", "error_log.txt")

# Expected columns
EXPECTED_COLUMNS = [
    "sales_id",
    "product_id",
    "store_id",
    "product_name",
    "brand",
    "category",
    "volume_ml",
    "alcohol_percentage",
    "store_name",
    "city",
    "state",
    "region",
    "store_type",
    "sales_date",
    "quantity_sold",
    "unit_price",
    "total_sales",
]

# SQL insert query
INSERT_QUERY = f"""
    INSERT INTO self_healing_demo ({', '.join(EXPECTED_COLUMNS)})
    VALUES ({', '.join(['%s'] * len(EXPECTED_COLUMNS))})
"""


def log_error(message: str):
    """Write error message and traceback to a log file."""
    with open(ERROR_LOG_FILE, "w") as log_file:
        log_file.write(message + "\n")
        log_file.write(traceback.format_exc() + "\n")
        log_file.write("-" * 80 + "\n")


def insert_dataframe_to_mysql(df: pd.DataFrame):
    connection = None
    cursor = None
    try:
        # Ensure only expected columns are used
        df = df[EXPECTED_COLUMNS]

        # Convert DataFrame to list of native Python tuples
        data = [tuple(row) for row in df.itertuples(index=False, name=None)]

        # Connect to MySQL
        connection = mysql.connector.connect(**DB_CONFIG)
        cursor = connection.cursor()

        # Insert records
        cursor.executemany(INSERT_QUERY, data)
        connection.commit()

        print(f"✅ {cursor.rowcount} records inserted successfully.")

    except Error as e:
        error_msg = f"MySQL Error: {e}"
        print("❌", error_msg)
        log_error(error_msg)

    except Exception as ex:
        error_msg = f"General Error: {ex}"
        print("❌", error_msg)
        log_error(error_msg)

    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()


def main():
    try:
        # Load CSV into a Pandas DataFrame
        df = pd.read_csv(CSV_FILE_PATH)

        # Check column count
        if len(df.columns) != len(EXPECTED_COLUMNS):
            raise ValueError(
                f"CSV column count mismatch. Expected {len(EXPECTED_COLUMNS)}, got {len(df.columns)}."
            )

        insert_dataframe_to_mysql(df)

    except Exception as e:
        error_msg = f"Error loading or processing CSV: {e}"
        print("❌", error_msg)
        log_error(error_msg)


if __name__ == "__main__":
    main()
