import json
import logging.config
import os
import pathlib

LOG_DIR = pathlib.Path(__file__).parent.resolve().parent.resolve() / "logs"
LOG_FILE = f'flight.log'
LOG_PATH = LOG_DIR / LOG_FILE


def configure(logging_config_path: str = pathlib.Path(__file__).parent.resolve() / "logging_config.json"):
    if not os.path.exists(LOG_DIR):
        os.mkdir(LOG_DIR)

    if not os.path.exists(LOG_PATH):
        f = open(LOG_PATH, 'a').close()  # create empty log file
    else:
        f = open(LOG_PATH, "w").close()  # clear log file
    with open(logging_config_path, "r") as fd:
        conf = json.loads(fd.read())
    conf["handlers"]["file"]["filename"] = LOG_PATH
    logging.config.dictConfig(config=conf)
