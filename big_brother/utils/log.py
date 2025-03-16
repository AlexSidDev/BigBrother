import logging.config
import yaml
import logging
from pathlib import Path


def setup_logging(config_file: str="configs/logging.yaml"):
    with open(config_file, "r") as f_in:
        config = yaml.safe_load(f_in)
    logging.config.dictConfig(config)