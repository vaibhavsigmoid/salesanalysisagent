import pandas as pd
from pydantic import BaseModel, Field
from typing import Type
from crewai.tools import BaseTool
from io import StringIO


class CleanValidateToolInput(BaseModel):
    dataframe_str: str = Field(..., description="CSV string of the mapped sales data.")


class CleanValidateTool(BaseTool):
    name: str = "CleanValidateTool"
    description: str = "Cleans and validates the sales data."
    args_schema: Type[BaseModel] = CleanValidateToolInput

    def _run(self, dataframe_str: str) -> str:
        # Convert the input string back to a DataFrame
        try:
            dataframe = pd.read_csv(StringIO(dataframe_str))
        except Exception as e:
            return f"Error reading CSV data: {str(e)}"

        # Clean the data
        if "sales_date" in dataframe.columns:
            dataframe["sales_date"] = pd.to_datetime(
                dataframe["sales_date"], errors="coerce"
            )

        cleaned_df = dataframe.dropna()

        return cleaned_df
