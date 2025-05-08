from crewai.tools import BaseTool
from typing import Type
from pydantic import BaseModel, Field, ConfigDict
import pandas as pd


class CodeGenToolInput(BaseModel):
    dataframe: pd.DataFrame = Field(..., description="Cleaned and mapped sales data.")
    model_config = ConfigDict(arbitrary_types_allowed=True)


class CodeGenTool(BaseTool):
    name: str = "CodeGenTool"
    description: str = "Generates an ETL script for the cleaned and mapped sales data."
    args_schema: Type[BaseModel] = CodeGenToolInput

    def _run(self, dataframe: pd.DataFrame) -> str:
        script = f"""
import pandas as pd

# Load the data
df = pd.read_csv("path_to_sales_data.csv")

# Clean the data
df['sales_date'] = pd.to_datetime(df['sales_date'], errors='coerce')
df = df.dropna()

# Example transformation
# Add your processing logic here...

# Save the cleaned data
df.to_csv("processed_sales_data.csv", index=False)
"""
        return script
