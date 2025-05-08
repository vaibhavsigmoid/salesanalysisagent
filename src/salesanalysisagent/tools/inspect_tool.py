from crewai.tools import BaseTool
from typing import Type
from pydantic import BaseModel, Field
import pandas as pd


class InspectToolInput(BaseModel):
    file_path: str = Field(
        ..., description="Path to the sales data file (.csv or .txt)."
    )


class InspectTool(BaseTool):
    name: str = "InspectTool"
    description: str = (
        "Inspects the sales data file and outputs its schema and a preview."
    )
    args_schema: Type[BaseModel] = InspectToolInput

    def _run(self, file_path: str) -> str:
        try:
            df = pd.read_csv(file_path)
        except Exception as e:
            raise RuntimeError(f"Could not read file: {str(e)}")

        preview = df.head().to_string()
        schema = df.dtypes.apply(lambda dt: dt.name).to_dict()

        return f"Preview of data:\n{preview}\n\nSchema:\n{schema}"
