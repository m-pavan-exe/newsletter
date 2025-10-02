import json
import logging
from typing import Dict
import sys


def load_full_config_from_json(config_path) -> dict:
    """
    Load the full configuration from a JSON file, with logging and error handling.
    """
    try:
        with open(config_path, "r") as f:
            config = json.load(f)
        logging.info(f"Loaded config from {config_path}")
        return config
    except Exception as e:
        logging.error(f"Failed to load config from {config_path}: {e}")
        sys.exit(1)
