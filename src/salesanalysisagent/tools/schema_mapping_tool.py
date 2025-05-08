from pydantic import BaseModel, Field, ConfigDict
import pandas as pd
from crewai.tools import BaseTool
from typing import Type, Union


class SchemaMappingToolInput(BaseModel):
    df: Union[pd.DataFrame, dict] = Field(..., description="The input data to map")

    model_config = ConfigDict(arbitrary_types_allowed=True)


class SchemaMappingTool(BaseTool):
    name: str = "Schema Mapping Tool"
    description: str = "Maps an input dataframe (or dict) with unknown schema to a standard sales schema."
    args_schema: Type[BaseModel] = SchemaMappingToolInput

    def _run(self, df: Union[pd.DataFrame, dict]) -> pd.DataFrame:
        if isinstance(df, dict):
            df = pd.DataFrame(df)
        elif not isinstance(df, pd.DataFrame):
            raise ValueError("Input must be a pandas DataFrame or a dictionary.")

        # Define mapping from potential aliases to standard columns
        standard_columns = {
            "sales_date": ["sales_date", "date", "order_date"],
            "product_id": ["product_id", "item_id", "product_code"],
            "quantity": ["quantity", "qty", "amount"],
            "price": ["price", "unit_price", "cost"],
        }

        mapped_df = pd.DataFrame()
        missing_columns = []

        for std_col, aliases in standard_columns.items():
            matched = False
            for alias in aliases:
                if alias in df.columns:
                    mapped_df[std_col] = df[alias]
                    matched = True
                    break
            if not matched:
                missing_columns.append(std_col)

        if mapped_df.empty:
            raise ValueError(
                "Schema mapping failed: No matching columns found in the input data."
            )

        if missing_columns:
            print(
                f"Warning: Missing standard columns not found in input: {', '.join(missing_columns)}"
            )

        return mapped_df
