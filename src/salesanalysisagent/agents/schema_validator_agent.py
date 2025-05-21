from crewai import Agent
from crewai_tools import CodeInterpreterTool, FileWriterTool

from salesanalysisagent.tools.schema_validator import SchemaValidatorTool


def SchemaValidatorAgent():
    return Agent(
        name="Schema Validator",
        role="Database Engineer",
        goal="Check schema between source {file_path} and target table and generate an executable python code with proper comments,  no need to include any expliantion of code or instruciton on how to execute.",
        backstory="This agent ensures data pipelines are consistent by validating schemas.",
        tools=[
            SchemaValidatorTool(),  # Optional if you have one
            FileWriterTool(),
        ],
        verbose=True,
        allow_delegation=False,
        all_code_execution=True,
    )
