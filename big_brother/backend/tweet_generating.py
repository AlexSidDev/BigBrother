import logging
import pandas as pd
import numpy as np
import time

from confluent_kafka import Producer

from utils import delivery_report

logger = logging.getLogger("tweet_producing")


class TweetProducer:
    def __init__(
        self,
        tweet_processor_host: str,
        tweet_processor_port: str, 
        send_topic: str = "raw_data_topic",
        data_path: str = "data/labeled_dataset.csv",
        sleep: bool = False,
    ) -> None:
        self._conf = {
            'bootstrap.servers': f"{tweet_processor_host}:{tweet_processor_port}",
            'default.topic.config': {'api.version.request': True},
            'security.protocol': 'SSL'
        }
        self._tweet_processor_host = tweet_processor_host
        self._tweet_processor_port = tweet_processor_port
        self._data = pd.read_csv(data_path, index_col=0)
        self._send_topic = send_topic
        self._sleep = sleep

    def connect(self) -> None:
        try:
            logger.info(f"Connection to {self._tweet_processor_host}:{self._tweet_processor_port}")
            self._producer = Producer(self._conf)
            logger.info("Connection is established")
        except BaseException:
            logger.exception("Connection to TweetProcessor from TweetGenerator is failed")
    
    def send(self) -> None:
        low = 0
        high = len(self._data)
        logger.info(f"Start sending data from TweetProducer to TweetProcessor with following ip {self._tweet_processor_host}:{self._tweet_processor_port}")
        while True:
            df_entry = self._data.iloc[np.random.randint(low=low, high=high), :]
            data = df_entry.to_json()
            self._producer.produce(self._send_topic, value=data, callback=delivery_report)
            self._producer.flush()

            if self._sleep:
                time.sleep(100)
    
if __name__ == "__main__":
    producer = TweetProducer(
        tweet_processor_host="localhost",
        tweet_processor_port="9095",
        sleep=True
    )
    
    producer.connect()
    producer.send()

    