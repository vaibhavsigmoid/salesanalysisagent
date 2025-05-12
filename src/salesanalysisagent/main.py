#!/usr/bin/env python
import os
import sys
import warnings
from datetime import datetime

from salesanalysisagent.crew import SalesAnalysisAgent

warnings.filterwarnings("ignore", category=SyntaxWarning, module="pysbd")


def run():
    """
    Run the agent crew sequentially from loading data to code generation.
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    inputs = {
        "file_path": os.path.join(
            # script_dir,
            # "data",
            # "data_change",
            # "sales.txt",
            # script_dir, "data", "extra_column", "sales.txt"
            script_dir,
            "data",
            "schema_change",
            "sales.txt",
        ),  # Replace with actual data file path
        "current_year": str(datetime.now().year),
    }
    try:
        result = SalesAnalysisAgent().crew().kickoff(inputs=inputs)
        from pprint import pprint

        pprint(result)
        # clean_output_to_python_file(result)
        with open("generated_code.py", "a", encoding="utf-8") as f:
            for task_result in result.tasks_output:
                f.write(f"\n# Output from {task_result.name} ({task_result.agent})\n")
                f.write(task_result.raw)
                f.write("\n")
    except Exception as e:
        print(f"[ERROR] Failed to run the crew: {str(e)}")
        raise e


def train():
    """
    Train the crew agents for few-shot fine-tuning.
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    inputs = {
        "file_path": os.path.join(
            script_dir, "data", "schema_change", "sales.txt"
        ),  # Replace with actual data file path
        "current_year": str(datetime.now().year),
    }
    try:
        SalesAnalysisAgent().crew().train(
            n_iterations=int(sys.argv[1]), filename=sys.argv[2], inputs=inputs
        )
    except Exception as e:
        print(f"[ERROR] Failed to train the crew: {str(e)}")


def replay():
    """
    Replay a specific task within the crew execution.
    """
    try:
        SalesAnalysisAgent().crew().replay(task_id=sys.argv[1])
    except Exception as e:
        print(f"[ERROR] Failed to replay the task: {str(e)}")


def test():
    """
    Test the crew execution using a specified model.
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    inputs = {
        "file_path": os.path.join(
            script_dir, "data", "extra_column", "sales.txt"
        ),  # Replace with actual data file path
        "current_year": str(datetime.now().year),
    }
    try:
        SalesAnalysisAgent().crew().test(
            n_iterations=int(sys.argv[1]), openai_model_name=sys.argv[2], inputs=inputs
        )
    except Exception as e:
        print(f"[ERROR] Failed to test the crew: {str(e)}")


def clean_output_to_python_file(result, output_file_path="generated_code.py"):
    with open(output_file_path, "w", encoding="utf-8") as f:
        # for task_result in tasks_output:
        for task_result in result.tasks_output:
            f.write(f"# Output from {task_result.name} ({task_result.agent})\n")

            # Split raw output into lines
            lines = task_result.raw.strip().splitlines()

            inside_code_block = False

            for line in lines:
                line = line.rstrip()
                # Detect start or end of a code block
                if line.startswith("```"):
                    inside_code_block = not inside_code_block
                    continue

                if inside_code_block:
                    f.write(line + "\n")  # Valid Python code
                else:
                    # Write as comment
                    if line.strip():  # Skip blank lines
                        f.write("# " + line + "\n")

            f.write("\n\n")  # Add spacing between task outputs


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python main.py [run|train|replay|test]")
        sys.exit(1)

    command = sys.argv[1].lower()

    if command == "run":
        run()
    elif command == "train":
        train()
    elif command == "replay":
        replay()
    elif command == "test":
        test()
    else:
        print(f"Unknown command: {command}")
        print("Valid commands: run, train, replay, test")
