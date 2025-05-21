from crewai import Agent, Task, Crew, Process
from crewai_tools import CodeInterpreterTool, FileWriterTool
import os
from datetime import datetime

from salesanalysisagent.tools.schema_validator import SchemaValidatorTool

# Initialize the tool
code_interpreter = CodeInterpreterTool(unsafe_mode=True)
SCRIPT_PATH = os.path.dirname(__file__)


# Define an agent that uses the tool
programmer_agent = Agent(
     name="Schema Validator",
         role="Python Programmer",
        goal="Check schema between source {file_path} and target table and generate an executable python code with proper comments,  no need to include any expliantion of code or instruciton on how to execute.",
        backstory="This agent ensures data pipelines are consistent by validating schemas.",
        tools=[
            SchemaValidatorTool(),  # Optional if you have one
            # FileWriterTool(),
           code_interpreter
        ],
        verbose=True,
        allow_delegation=False,
        # allow_code_execution=True,
)

# Example task to generate and execute code
coding_task = Task(
    description="Write a Python function to calculate the Fibonacci sequence up to the 10th number and print the result.",
    expected_output="The Fibonacci sequence up to the 10th number.",
    agent=programmer_agent,
)

schema_task = Task(
    #   description= 'based on the differnce in schema and different in data, generate if source has new column add suggest a sql code to add column, if column type or data is mismatched suggest sql code to alter target column type, if not required write a python function using pandas and mysql-connector-python libaray to cast column type to target. make sure during the process there is no data loss or trunction of any kind. Also make sure that no historical data is dropped in a process. code should not have any example, or instruction on how to execute it. Execute the code using CodeInterpreter tool',
    description= """
        Based on the provided schema difference write a python cod.  if source has new columns, suggest a sql code to add columns, if column type or data is mismatched suggest sql code to alter target column type, if not required write a python function using pandas and mysql-connector-python libaray to cast column type to target. make sure during the process there is no data loss or trunction of any kind. Also make sure that no historical data is dropped in a process
        """,
    expected_output="data should be updated into target mysql table",
    agent=programmer_agent,
)

# Create and run the crew
crew = Crew(
    agents=[programmer_agent],
    tasks=[schema_task],
    verbose=True,
    process=Process.sequential,
)

inputs = {
    "file_path": os.path.join(
        SCRIPT_PATH,
        "data",
        "self_healing",
        "new_data.csv",
    ),  # Replace with actual data file path
    "current_year": str(datetime.now().year),
    "libraries_used": ["pandas"]
}
result = crew.kickoff(inputs=inputs)