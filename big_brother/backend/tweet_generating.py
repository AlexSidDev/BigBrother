import logging
import pandas as pd
import numpy as np
import time

from confluent_kafka import Producer

logger = logging.getLogger("tweet_producing")


class TweetGenerator:
    def __init__(
        self,
        tweet_processor_host: str,
        tweet_processor_port: str, 
        send_topic: str,
        sleep: bool,
        data_path: str = "data/labeled_dataset.csv"
    ) -> None:
        self._conf = {
            'bootstrap.servers': f"{tweet_processor_host}:{tweet_processor_port}"
        }
        self._tweet_processor_host = tweet_processor_host
        self._tweet_processor_port = tweet_processor_port
        self._data = pd.read_csv(data_path, index_col=0)
        self._send_topic = send_topic
        self._sleep = sleep

    def connect(self) -> None:
        try:
            self._producer = Producer(self.conf)
        except ConnectionError:
            raise ConnectionError
    
    def send(self) -> None:
        low = 0
        high = len(self._data)
        while True:
            df_entry = self.data.iloc[np.random.randint(low=low, high=high), :]
            data = df_entry.to_json()
            try:
                self._producer.produce(self._send_topic, value=data)
                self._producer.flush()
            except ConnectionError:
                raise ConnectionError

            if self.sleep:
                time.sleep(100)
    
if __name__ == "__main__":
    producer = TweetGenerator(
        tweet_processor_host="localhost",
        tweet_processor_port="9095",
        send_topic="raw_data_topic",
        sleep=True
    )

    try:
        producer.connect()
        logger.info("TweetGenerator is connected to TweetProcessor successfully")
    except ConnectionError:
        logger.exception("Connection to TweetProcessor from TweetGenerator is failed")
    
    try:
        producer.send()
        logger.info(f'Successfully send message into topic "{producer._send_topic}"')
    except ConnectionError:
        logger.exception(f'Fail to send message into topic "{producer._send_topic}"')

    