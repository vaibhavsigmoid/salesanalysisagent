from crewai.tools import BaseTool
from typing import Type
from pydantic import BaseModel, Field
import os
import pandas as pd


class DataLoaderToolInput(BaseModel):
    file_path: str = Field(..., description="Path to the sales data file.")


class DataLoaderTool(BaseTool):
    name: str = "DataLoaderTool"
    # description: str = (
    #     "Loads sales data from a .csv or .txt file and returns a DataFrame."
    # )
    description: str = (
        "Detects the format of input data files and summarizes structure like Excel sheet names."
        "Also returns the loaded dataframe."
    )
    args_schema: Type[BaseModel] = DataLoaderToolInput

    def _run(self, file_path: str) -> pd.DataFrame:
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"The file at {file_path} does not exist.")

        # if not file_path.lower().endswith((".csv", ".txt")):
        #     raise ValueError(f"Only .csv or .txt files are supported.")

        # try:
        #     df = pd.read_csv(file_path)
        # except Exception as e:
        #     raise RuntimeError(f"Failed to load the file: {str(e)}")

        # if df.empty:
        #     raise ValueError("The loaded file is empty.")

        # return df
        ext = os.path.splitext(file_path)[1].lower()
        try:
            if ext == ".csv":
                df = pd.read_csv(file_path)
                return {"format": "csv", "dataframe": df}
            elif ext == ".parquet":
                df = pd.read_parquet(file_path)
                return {"format": "parquet", "dataframe": df}
            elif ext in [".xls", ".xlsx"]:
                xls = pd.ExcelFile(file_path)
                sheet_data = {sheet: xls.parse(sheet) for sheet in xls.sheet_names}
                return {
                    "format": "excel",
                    "sheets": xls.sheet_names,
                    "dataframes": sheet_data,
                }
            else:
                return {"error": f"Unsupported file type: {ext}"}
        except Exception as e:
            return {"error": f"Failed to read file: {str(e)}"}
