from crewai.tools import BaseTool
from typing import Type
from pydantic import BaseModel, Field, ConfigDict
import pandas as pd


class CleanValidateToolInput(BaseModel):
    dataframe: pd.DataFrame = Field(..., description="Mapped sales data DataFrame.")
    model_config = ConfigDict(arbitrary_types_allowed=True)


class CleanValidateTool(BaseTool):
    name: str = "CleanValidateTool"
    description: str = "Cleans and validates the sales data."
    args_schema: Type[BaseModel] = CleanValidateToolInput

    def _run(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        if "sales_date" in dataframe.columns:
            dataframe["sales_date"] = pd.to_datetime(
                dataframe["sales_date"], errors="coerce"
            )

        cleaned_df = dataframe.dropna()
        return cleaned_df
