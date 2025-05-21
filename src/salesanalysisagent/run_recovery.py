import os
from datetime import datetime

from crewai import Crew, Task

from salesanalysisagent.agents.schema_validator_agent import \
    SchemaValidatorAgent
from salesanalysisagent.router.task_router import get_triggered_task_from_log

SCRIPT_PATH = os.path.dirname(__file__)
YAML_CONFIG = os.path.join(SCRIPT_PATH, "config", "tasks.yaml")
LOG_FILE = os.path.join(SCRIPT_PATH, "logs", "error_log.txt")

# Match task
task_data = get_triggered_task_from_log(LOG_FILE, YAML_CONFIG)

if task_data.get("task_key") == "schema_validator_task":
    agent = SchemaValidatorAgent()
else:
    print("No agent matched to handle the error.")
    exit(0)

task = Task(
    name=task_data.get("task_key"),
    description=task_data.get("description"),
    expected_output=task_data.get("expected_output"),
    agent=agent,
)
inputs = {
    "file_path": os.path.join(
        SCRIPT_PATH,
        "data",
        "self_healing",
        "new_data.csv",
    ),  # Replace with actual data file path
    "current_year": str(datetime.now().year),
}
# Setup and run Crew
crew = Crew(agents=[agent], tasks=[task])
crew.kickoff(inputs=inputs) 
