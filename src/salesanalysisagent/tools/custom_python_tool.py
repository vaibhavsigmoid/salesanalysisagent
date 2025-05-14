import os
import subprocess

from crewai.tools import BaseTool


class CustomPythonTool(BaseTool):
    name: str = "CustomPythonExecutor"
    description: str = "Executes a saved Python file and returns the output"

    def _run(self, file_path: str) -> str:
        try:
            if not os.path.exists(file_path):
                return f"❌ File not found: {file_path}"
            # Run the Python script and capture output
            result = subprocess.run(
                ["python", file_path],
                capture_output=True,
                text=True,
                check=False,  # Prevents exception on non-zero exit
            )

            output = f"✅ Execution completed.\n\nSTDOUT:\n{result.stdout}"
            if result.stderr:
                output += f"\n\nSTDERR:\n{result.stderr}"

            return output
        except Exception as e:
            return f"❌ Exception during code execution: {str(e)}"
