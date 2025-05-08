import json
import os
import random
from pprint import pprint
from typing import Type

import pandas as pd
from crewai.tools import BaseTool
from pydantic import BaseModel, ConfigDict, Field


class FormatChangeDetectorInput(BaseModel):
    file_path: str = Field(..., description="Path to the new CSV file")
    model_config = ConfigDict(arbitrary_types_allowed=True)


class FormatChangeDetectorTool(BaseTool):
    name: str = "data_format_validator"
    description: str = (
        "Detects semantic data format changes between source CSV and target table and suggests a Pandas function to align data"
    )
    args_schema: Type[BaseModel] = FormatChangeDetectorInput

    def _run(self, file_path: str, **kwargs) -> str:
        try:
            # Load source file
            source_df = pd.read_csv(file_path)
            source_sample = source_df.sample(n=min(10, len(source_df)))

            # Load target table data from matching file based on mapping
            db_name, table_name = self.get_target_info(os.path.basename(file_path))
            script_dir = os.path.dirname(os.path.abspath(__file__))
            print(script_dir)
            print(table_name)
            target_path = os.path.join(script_dir, "../data/data_change_1", table_name)
            target_df = pd.read_csv(target_path)
            target_sample = target_df.sample(n=min(10, len(target_df)))

            pprint(source_sample)
            pprint(target_sample)
            return (source_sample, target_sample)

        except Exception as e:
            return f"Error comparing data formats: {str(e)}"

    def get_target_info(self, file_name, json_file_path="mapping.json"):
        try:
            script_dir = os.path.dirname(os.path.abspath(__file__))
            with open(os.path.join(script_dir, "../data", json_file_path), "r") as file:
                data = json.load(file)

            if file_name in data:
                return data[file_name].get("database_name"), data[file_name].get(
                    "table_name"
                )
            return None, None
        except Exception as e:
            print(f"Error reading mapping file: {e}")
            return None, None
