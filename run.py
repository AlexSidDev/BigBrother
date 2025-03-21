import subprocess
import sys

from pathlib import Path

import logging.config
import yaml
import logging


# def setup_logging(config_file: str="configs/logging.yaml"):
#     with open(config_file, "r") as f_in:
#         config = yaml.safe_load(f_in)
#     logging.config.dictConfig(config)

#setup_logging()

logger = logging.getLogger("big_brother")
logger.setLevel(logging.DEBUG)
console = logging.StreamHandler()
console_formater = logging.Formatter("[ %(levelname)s ] %(message)s")
console.setFormatter(console_formater)
logger.addHandler(console)

#sys.path.append(str(Path(__file__).parent))


class Application():
    def __init__(self) -> None:
        self.processes = {}
        self.python = "/home/polina/BigBrother/newenv/bin/python3.10"
    
    def docker_compose_up(self) -> None:
        logger.info("Run brokers")
        subprocess.run(["docker-compose", "--file", "configs/docker-compose.yaml", "up", "-d"])

    def docker_compose_stop(self) -> None:
        logger.info("Stop brokers")
        subprocess.run(["docker-compose", "--file", "configs/docker-compose.yaml", "down"])

    def start_processes(self) -> None:
        logger.info("Create processes")
        self.processes["tweet_generating"] = subprocess.Popen([self.python, "./backend/kafka/tweet_generation.py"])
        self.processes["tweet_processing"] = subprocess.Popen([self.python, "./backend/kafka/tweet_processing.py"])
        self.processes["database"] = subprocess.Popen([self.python, "./backend/kafka/database.py"])

        #TODO (@a.klykov) add code to start streamlit
        # self.processes["visualization"] = subprocess.Popen([self.python, "-m", "streamlit", "run", 
        #                                                     "./app/frontend/visualization_consumer/visualization_consumer.py"])

    def stop_processes(self) -> None:
        for name, process in self.processes.items():
            logger.info(f"Stopping {name} process")
            process.kill()
    
    def run(self) -> None:
        self.docker_compose_up()
        self.start_processes()
    
    def stop(self) -> None:
        self.stop_processes()
        self.docker_compose_stop()

if __name__ == "__main__":
    application = Application()

    try:
        logger.info("Run application")
        application.run()
        while True:
            continue
    except KeyboardInterrupt:
        logger.info("Stop application")
        application.stop()