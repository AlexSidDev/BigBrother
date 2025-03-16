import logging
import time
from typing import Any
from confluent_kafka import Producer, Consumer
import pandas as pd

from utils import delivery_report

logger = logging.getLogger("tweet_processing")


class TweetProcessorProducer:
    def __init__(
        self,
        db_connection_handler_host: str,
        db_connection_handler_port: str, 
        send_topic: str = "db_connection_handler_topic",
        sleep: bool = False
    ) -> None:
        self._conf = {
            'bootstrap.servers': f"{db_connection_handler_host}:{db_connection_handler_port}"
        }
        self._db_connection_handler_host = db_connection_handler_host
        self._db_connection_handler_port = db_connection_handler_port
        self._send_topic = send_topic
        self._sleep = sleep

    def connect(self) -> None:
        logger.info(f"Connection to {self._db_connection_handler_host}:{self._db_connection_handler_port}")
        self._producer = Producer(self._conf)
        logger.info("Connection is established")

    def send(self, data: Any) -> None:
        self._producer.produce(self._send_topic, key='1', value=data, callback=delivery_report)
        self._producer.flush()
        logger.debug(f"Produced: {data}")
        if self._sleep:
            time.sleep(10)


class TweetProcessorConsumer:
    def __init__(
        self,
        tweet_processor_host: str,
        tweet_processor_port: str, 
        read_topic: str = "raw_data_topic",
    ) -> None:
        self._conf = {
            'bootstrap.servers': f"{tweet_processor_host}:{tweet_processor_port}", 
            'group.id': 'dp_consumer',
        }
        self._tweet_processor_host = tweet_processor_host
        self._tweet_processor_port = tweet_processor_port
        self._read_topic = read_topic
            
    @property
    def producer(self) -> Any:
        _producer = TweetProcessorProducer(
            bootstrap_host="localhost",
            bootstrap_port="9096",
            send_topic="processed_data_topic",
            sleep=True
        )
        _producer.connect()

        return _producer

    def connect(self) -> None:
        try:
            logger.info(f"Connection to {self._tweet_processor_host}:{self._tweet_processor_port}")
            self._consumer = Consumer(self._conf)
            self._consumer.subscribe([self._read_topic])
            logger.info("Connection is established")
        except BaseException:
            logger.exception("Connection to TweetProcessor is failed")
        
    def read(self) -> None:
        producer = self.producer
        logger.info(f"Read data from {self._read_topic}")
        while True:
            msg = self._consumer.poll(10)

            if msg is None:
                logger.info("Message is None")
                continue
            if msg.error():
                logger.info(f"Consumer error: {msg.error()}")
                continue

            logger.debug(f"Recieved message: {msg.value()}")
            
            df = pd.read_json(msg.value().decode('utf-8'), orient="index")
            df = df.transpose()
            
            # TODO add here model call

            data = df.to_json()

            logger.info(f"Preprocessed data: {data}")
            _producer.send(data)