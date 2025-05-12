# from crewai.tools import BaseTool
# from typing import Type
# from pydantic import BaseModel, Field, ConfigDict
# import pandas as pd


# class CodeGenToolInput(BaseModel):
#     dataframe: pd.DataFrame = Field(..., description="Cleaned and mapped sales data.")
#     model_config = ConfigDict(arbitrary_types_allowed=True)


# class CodeGenTool(BaseTool):
#     name: str = "CodeGenTool"
#     description: str = (
#         "Generates an ETL exicutable script for the cleaned and mapped sales data."
#     )
#     args_schema: Type[BaseModel] = CodeGenToolInput

#     def _run(self, dataframe: pd.DataFrame) -> str:
#         script = f"""
# import pandas as pd

# # Load the data
# df = pd.read_csv("path_to_sales_data.csv")

# # Clean the data
# df['sales_date'] = pd.to_datetime(df['sales_date'], errors='coerce')
# df = df.dropna()

# # Example transformation
# # Add your processing logic here...

# # Save the cleaned data
# df.to_csv("processed_sales_data.csv", index=False)
# """
#         return script

from crewai.tools import BaseTool
from typing import Type
from pydantic import BaseModel, Field
import pandas as pd
from io import StringIO


class CodeGenToolInput(BaseModel):
    dataframe_str: str = Field(
        ..., description="CSV string of the cleaned and mapped sales data."
    )


class CodeGenTool(BaseTool):
    name: str = "CodeGenTool"
    description: str = (
        "Generates an ETL executable script for the cleaned and mapped sales data."
    )
    args_schema: Type[BaseModel] = CodeGenToolInput

    def _run(self, dataframe_str: str) -> str:
        try:
            df = pd.read_csv(StringIO(dataframe_str))
        except Exception as e:
            return f"Error parsing CSV: {str(e)}"

        # Example ETL script generation
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
