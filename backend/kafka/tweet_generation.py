import ast
import logging
import pandas as pd
import numpy as np
import time
import json

from confluent_kafka import Producer

from utils import delivery_report

logger = logging.getLogger("tweet_producing")
logger.setLevel(logging.DEBUG)
console = logging.StreamHandler()
console_formater = logging.Formatter("[ %(levelname)s ] %(message)s")
console.setFormatter(console_formater)
logger.addHandler(console)


class TweetProducer:
    def __init__(
        self,
        tweet_processor_host: str,
        tweet_processor_port: str, 
        send_topic: str = "raw_data_topic",
        data_path: str = "data/sorted_tweets.csv",
        sleep: bool = False,
    ) -> None:
        self._conf = {
            'bootstrap.servers': f"{tweet_processor_host}:{tweet_processor_port}",
        }
        with open('./configs/application_config.json', 'r') as config:
            self.app_config = json.load(config)
        self._tweet_processor_host = tweet_processor_host
        self._tweet_processor_port = tweet_processor_port
        self._data = pd.read_csv(data_path, converters={'tokens': ast.literal_eval})
        self._send_topic = send_topic
        self._sleep = sleep
        self.sleep_time = self.app_config['sleep']

    def connect(self) -> None:
        try:
            logger.info(f"Connection to {self._tweet_processor_host}:{self._tweet_processor_port}")
            self._producer = Producer(self._conf)
            logger.info("Connection is established")
        except BaseException:
            logger.exception("Connection to TweetProcessor from TweetGenerator is failed")
    
    def send(self) -> None:
        ind = 0
        logger.info(f"Start sending data from TweetProducer to TweetProcessor with following ip {self._tweet_processor_host}:{self._tweet_processor_port}")
        while True:
            df_entry = self._data.iloc[ind, :]
            df_entry['tokens'] = ' '.join(df_entry['tokens'])
            data = df_entry.to_json()
            self._producer.produce(self._send_topic, value=data)
            self._producer.flush()

            if self._sleep:
                time.sleep(self.sleep_time)

            ind += 1


if __name__ == "__main__":
    producer = TweetProducer(
        tweet_processor_host="localhost",
        tweet_processor_port="9095",
        sleep=True
    )
    
    producer.connect()
    producer.send()