import json
import os

import mysql.connector
import pandas as pd
from crewai.tools import BaseTool


def get_schema(csv_file_path, sample_size=100):
    try:
        df = pd.read_csv(csv_file_path, nrows=sample_size)
        schema = dict(df.dtypes.apply(lambda dt: dt.name))
        return df, schema

    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return {}


def get_target_data_and_sample(
    username: str,
    password: str,
    host: str,
    table_name: str,
    database_name: str,
):
    """
    Connects to a MySQL database and returns 10 sample rows from the specified table and its schema.

    Args:
        username (str): MySQL username
        password (str): MySQL password
        host (str): MySQL host (e.g., '127.0.0.1')
        table (str): Table to query
        database (str): Database name (default: 'pos_data')

    Returns:
        df (pd.DataFrame): Sample data (10 rows)
        schema (dict): Column name -> dtype
    """
    try:
        connection = mysql.connector.connect(
            host=host, database=database_name, user=username, password=password
        )

        if connection.is_connected():
            cursor = connection.cursor()
            query = f"SELECT * FROM {table_name} LIMIT 10"
            cursor.execute(query)
            rows = cursor.fetchall()
            column_names = [i[0] for i in cursor.description]

            # Load into DataFrame
            df = pd.DataFrame(rows, columns=column_names)
            schema = dict(df.dtypes)

            return df, schema

    except Exception as e:
        print("Failed to connect or fetch data:", e)
        return None, None

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()


def get_target_info(file_name, json_file_path="mapping.json"):
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        with open(os.path.join(script_dir, "../", "data", json_file_path), "r") as file:
            data = json.load(file)

        if file_name in data:
            db_name = data[file_name].get("database_name")
            table_name = data[file_name].get("table_name")
            return db_name, table_name

        print(f"File name '{file_name}' not found in the JSON.")
        return None, None

    except (json.JSONDecodeError, FileNotFoundError) as e:
        print(f"Error reading JSON file: {e}")
        return None, None


def map_dtype_to_sql(dtype: str) -> str:
    """Map pandas dtypes to SQL types (generic)"""
    dtype = dtype.lower()
    if "int" in dtype:
        return "INTEGER"
    if "float" in dtype:
        return "REAL"
    if "bool" in dtype:
        return "BOOLEAN"
    if "datetime" in dtype:
        return "TIMESTAMP"
    return "TEXT"


def compare_schemas(source, target):

    only_in_source = [col for col in source if col not in target]
    only_in_target = [col for col in target if col not in source]

    type_mismatches = {
        col: (source[col], target[col])
        for col in source
        if col in target and source[col] != target[col]
    }

    return {
        "only_in_source": only_in_source,
        "only_in_target": only_in_target,
        "type_mismatches": type_mismatches,
    }


class SchemaValidatorTool(BaseTool):
    name: str = "schema_validator"
    description: str = (
        "Validates the schema of a CSV file against a target database table using a mapping file, and generates SQL to fix mismatches."
    )

    # def _run(self, df: pd.DataFrame, file_path: str, **kwargs) -> str:
    def _run(self, file_path: str, **kwargs) -> str:
        try:
            print(f"file path = {file_path}")
            # source_schema = dict(df.dtypes.apply(lambda dt: dt.name))
            source_df, source_schema = get_schema(file_path)
            db_name, table_name = get_target_info(os.path.basename(file_path))
            script_dir = os.path.dirname(os.path.abspath(__file__))
            print(f"script_dir = {script_dir}")
            dir_name = os.path.dirname(file_path)
            target_df, target_schema = get_target_data_and_sample(
                host="127.0.0.1",
                username="root",
                password="password",
                database_name=db_name,
                table_name=table_name,
            )
            # target_df, target_schema = get_schema(
            #     os.path.join(dir_name, table_name)
            #     # os.path.join(script_dir, "../data/extra_column", table_name)
            # )
            schema_diff = compare_schemas(source_schema, target_schema)

            # return (
            #     db_name,
            #     table_name,
            #     schema_diff,
            # )
            return {
                "database_name": db_name,
                "table_name": table_name,
                "schema_difference": schema_diff,
                "source_data": source_df,
                "target_data": target_df,
                "target_database_type": "mysql",
            }

        except Exception as e:
            return f"Error while validating schema: {str(e)}"
