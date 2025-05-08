from crewai.tools import BaseTool
from typing import Type
from pydantic import BaseModel, Field
import os
import pandas as pd


class DataLoaderToolInput(BaseModel):
    file_path: str = Field(..., description="Path to the sales data file.")


class DataLoaderTool(BaseTool):
    name: str = "DataLoaderTool"
    description: str = (
        "Loads sales data from a .csv or .txt file and returns a DataFrame."
    )
    args_schema: Type[BaseModel] = DataLoaderToolInput

    def _run(self, file_path: str) -> pd.DataFrame:
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"The file at {file_path} does not exist.")

        if not file_path.lower().endswith((".csv", ".txt")):
            raise ValueError(f"Only .csv or .txt files are supported.")

        try:
            df = pd.read_csv(file_path)
        except Exception as e:
            raise RuntimeError(f"Failed to load the file: {str(e)}")

        if df.empty:
            raise ValueError("The loaded file is empty.")

        return df
