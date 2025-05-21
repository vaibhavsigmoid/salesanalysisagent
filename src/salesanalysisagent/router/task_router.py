import yaml


def get_triggered_task_from_log(log_file_path: str, yaml_path: str) -> dict | None:
    """
    Reads error log and matches against triggers defined in the YAML config.
    Returns the full task config (as a dict) if a match is found, otherwise None.
    """
    with open(yaml_path, encoding="UTF-8") as f:
        error_map = yaml.safe_load(f)

    with open(log_file_path, encoding="UTF-8") as log:
        content = log.read()

    for task_key, task_config in error_map.items():
        for trigger in task_config.get("triggers_on_errors", []):
            if trigger.lower() in content.lower():
                # Include task_key in the returned dict (optional but useful)
                return {
                    "task_key": task_key,
                    "description": task_config["description"],
                    "expected_output": task_config["expected_output"],
                }

    return None
