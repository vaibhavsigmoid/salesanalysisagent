import json
import os
from typing import Type

import pandas as pd
from crewai.tools import BaseTool
from pydantic import BaseModel, ConfigDict, Field


def get_schema(csv_file_path, sample_size=100):
    try:
        df = pd.read_csv(csv_file_path, nrows=sample_size)
        schema = dict(df.dtypes.apply(lambda dt: dt.name))
        return schema

    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return {}


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


def compare_schemas(schema1, schema2):

    only_in_schema1 = [col for col in schema1 if col not in schema2]
    only_in_schema2 = [col for col in schema2 if col not in schema1]

    type_mismatches = {
        col: (schema1[col], schema2[col])
        for col in schema1
        if col in schema2 and schema1[col] != schema2[col]
    }

    return {
        "only_in_schema1": only_in_schema1,
        "only_in_schema2": only_in_schema2,
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
            source_schema = get_schema(file_path)
            db_name, table_name = get_target_info(os.path.basename(file_path))
            script_dir = os.path.dirname(os.path.abspath(__file__))
            print(f"script_dir = {script_dir}")
            target_schema = get_schema(
                os.path.join(script_dir, "../data/extra_column", table_name)
            )
            schema_diff = compare_schemas(source_schema, target_schema)

            return (
                db_name,
                table_name,
                schema_diff,
            )

        except Exception as e:
            return f"Error while validating schema: {str(e)}"
