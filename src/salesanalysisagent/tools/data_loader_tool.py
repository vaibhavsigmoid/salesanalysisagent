import json
import os
from pprint import pprint
from typing import Type

from crewai.tools import BaseTool
from pydantic import BaseModel, Field


class DataLoaderToolInput(BaseModel):
    file_path: str = Field(..., description="Path to the sales data file.")
    # mapping_path: str = Field(..., description="Path to the mapping.json file.")


class DataLoaderTool(BaseTool):
    name: str = "DataLoaderTool"
    description: str = (
        "Given a file path and mapping.json, loads the data using appropriate method "
        "based on instructions from the LLM."
    )
    args_schema: Type[BaseModel] = DataLoaderToolInput

    def _run(self, file_path: str) -> dict:
        file_name = os.path.basename(file_path)
        script_dir = os.path.dirname(os.path.abspath(__file__))
        with open(os.path.join(script_dir, "../", "data", "mapping.json"), "r") as file:
            mapping = json.load(file)
            file_mapping = mapping.get(file_name)
            pprint(file_mapping)

        return {
            "file_name": file_name,
            "mapping": file_mapping,
            "target_database_type": "mysql",
        }
